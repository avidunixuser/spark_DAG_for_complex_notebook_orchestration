from __future__ import annotations

import json
import time
from dataclasses import replace
from unittest.mock import Mock, patch

from azure.core import MatchConditions
from azure.core.exceptions import HttpResponseError, ResourceExistsError, ResourceNotFoundError

from spark_dag.executors import DatabricksExecutor, FabricExecutor
from spark_dag.leases import ADLSGen2Leases, DataLakeFileLease, validate_lock_location
from spark_dag.model import CollisionError, Status, WorkflowError
from spark_dag.notebook_runtime import parameter_values, run_orchestrator_notebook
from spark_dag.worker import run_child
from tests.support import PreparedAttemptTest


class AdapterTests(PreparedAttemptTest):
    def test_databricks_uses_idempotent_submission_and_disables_native_retry(self):
        engine, _, context = self.prepare_attempt()
        engine.config.data["runtime"]["databricks"]["existing_cluster_id"] = "cluster-test"
        api = Mock()

        def request(method, path, **kwargs):
            if path.endswith("/submit"):
                return {"run_id": 123}
            return {"state": {"life_cycle_state": "TERMINATED", "result_state": "FAILED"}}

        api.do.side_effect = request
        DatabricksExecutor(engine.config, engine.store, api_client=api).execute_batch([context])
        body = api.do.call_args_list[0].kwargs["body"]
        self.assertEqual(len(body["idempotency_token"]), 64)
        self.assertEqual(body["tasks"][0]["max_retries"], 0)
        self.assertEqual(
            json.loads(body["tasks"][0]["notebook_task"]["base_parameters"]["context_json"])["run_id"],
            context.run_id,
        )
        self.assertEqual(
            engine.store.node(self.scope(), context.run_id, context.node_id)["remote_run_id"], 123
        )

    def test_databricks_timeout_waits_for_confirmed_cancellation(self):
        engine, _, context = self.prepare_attempt()
        context = replace(context, deadline=time.time() + 0.04)
        engine.config.data["runtime"]["databricks"]["existing_cluster_id"] = "cluster-test"
        engine.config.data["runtime"]["poll_interval_seconds"] = 0.005
        cancelled = False
        api = Mock()

        def request(method, path, **kwargs):
            nonlocal cancelled
            if path.endswith("/submit"):
                return {"run_id": 123}
            if path.endswith("/cancel"):
                cancelled = True
                return {}
            return {
                "state": {
                    "life_cycle_state": "TERMINATED" if cancelled else "RUNNING",
                    "result_state": "TIMEDOUT",
                }
            }

        api.do.side_effect = request
        DatabricksExecutor(engine.config, engine.store, api_client=api).execute_batch([context])
        record = engine.store.node(self.scope(), context.run_id, context.node_id)
        self.assertTrue(cancelled)
        self.assertEqual(record["status"], Status.TIMED_OUT)
        self.assertFalse(record["execution_uncertain"])
        self.assertTrue(record["retryable"])

    def test_databricks_ambiguous_submission_is_quarantined(self):
        engine, _, context = self.prepare_attempt()
        engine.config.data["runtime"]["databricks"]["existing_cluster_id"] = "cluster-test"
        api = Mock()
        api.do.side_effect = TimeoutError("token=never-log-this")
        DatabricksExecutor(engine.config, engine.store, api_client=api).execute_batch([context])
        record = engine.store.node(self.scope(), context.run_id, context.node_id)
        self.assertTrue(record["execution_uncertain"])
        self.assertFalse(record["retryable"])
        self.assertNotIn("never-log-this", str(record))

    def test_databricks_unconfirmed_cancel_remains_quarantined(self):
        engine, _, context = self.prepare_attempt()
        context = replace(context, deadline=time.time() - 1)
        engine.config.data["runtime"]["databricks"]["existing_cluster_id"] = "cluster-test"
        engine.config.data["runtime"].update(poll_interval_seconds=0.001, cancel_grace_seconds=0.01)
        api = Mock()
        api.do.side_effect = lambda method, path, **kwargs: (
            {"run_id": 123} if path.endswith("/submit") else {"state": {"life_cycle_state": "RUNNING"}}
        )
        DatabricksExecutor(engine.config, engine.store, api_client=api).execute_batch([context])
        record = engine.store.node(self.scope(), context.run_id, context.node_id)
        self.assertTrue(record["execution_uncertain"])
        self.assertEqual(record["status"], Status.TIMED_OUT)

    def test_fabric_uses_native_shared_session_batch(self):
        engine, _, context = self.prepare_attempt()
        notebooks = Mock()

        class PartialFailure(Exception):
            pass

        def run_multiple(dag, config):
            result = run_child(context)
            return {context.node_id: {"exitVal": json.dumps(result), "exception": None}}

        notebooks.notebook.runMultiple.side_effect = run_multiple
        FabricExecutor(
            engine.config, engine.store, notebooks, partial_failure_type=PartialFailure
        ).execute_batch([context])
        dag = notebooks.notebook.runMultiple.call_args.args[0]
        self.assertEqual(dag["concurrency"], 1)
        self.assertEqual(dag["activities"][0]["retry"], 0)
        self.assertEqual(
            dag["activities"][0]["args"]["context_json"],
            json.dumps(context.as_dict(), sort_keys=True, separators=(",", ":")),
        )
        notebooks.notebook.validateDAG.assert_called_once()
        self.assertEqual(
            engine.store.node(self.scope(), context.run_id, context.node_id)["status"], Status.SUCCEEDED
        )

    def test_fabric_partial_failure_preserves_results_and_quarantines_unknown_worker(self):
        engine, _, context = self.prepare_attempt()
        engine.store.claim(context)

        class PartialFailure(Exception):
            def __init__(self):
                self.result = {context.node_id: {"exitVal": "", "exception": TimeoutError("sensitive")}}

        notebooks = Mock()
        notebooks.notebook.runMultiple.side_effect = PartialFailure()
        FabricExecutor(
            engine.config, engine.store, notebooks, partial_failure_type=PartialFailure
        ).execute_batch([context])
        record = engine.store.node(self.scope(), context.run_id, context.node_id)
        self.assertTrue(record["execution_uncertain"])
        self.assertFalse(record["retryable"])
        self.assertNotIn("sensitive", str(record))

    def test_fabric_missing_result_is_not_success(self):
        engine, _, context = self.prepare_attempt()
        notebooks = Mock()
        notebooks.notebook.runMultiple.return_value = {}
        FabricExecutor(
            engine.config, engine.store, notebooks, partial_failure_type=RuntimeError
        ).execute_batch([context])
        record = engine.store.node(self.scope(), context.run_id, context.node_id)
        self.assertEqual(record["error"]["code"], "FABRIC_RESULT_MISSING")
        self.assertTrue(record["execution_uncertain"])

    def test_databricks_parameter_widgets_preserve_injected_values(self):
        dbutils = Mock()
        dbutils.widgets.getAll.return_value = {"context_json": "injected"}
        dbutils.widgets.get.side_effect = lambda key: "injected" if key == "context_json" else "default"
        self.assertEqual(
            parameter_values({"context_json": "", "config_file": "default"}, dbutils)["context_json"],
            "injected",
        )
        dbutils.widgets.text.assert_called_once_with("config_file", "default")

    def test_notebook_setup_errors_do_not_expose_raw_sdk_details(self):
        with patch(
            "spark_dag.notebook_runtime.load_config", side_effect=RuntimeError("token=never-log-this")
        ) as loader:
            with self.assertRaises(WorkflowError) as caught:
                run_orchestrator_notebook(
                    {"config_file": "test.json", "deployment_dir": "", "environment": ""}
                )
        loader.assert_called_once()
        self.assertNotIn("never-log-this", str(caught.exception))

    def adls_leases(self):
        file_system, lease = Mock(), Mock()
        factory = ADLSGen2Leases(
            self.data["locking"],
            file_system_client=file_system,
            lease_client_factory=Mock(return_value=lease),
        )
        return factory, file_system, lease

    def test_adls_file_lease_is_infinite_and_does_not_break_other_owners(self):
        factory, file_system, raw = self.adls_leases()
        lease = factory.acquire("scope", wait=False)
        raw.acquire.assert_called_once_with(lease_duration=-1)
        factory.lease_client_factory.assert_called_once_with(file_system.get_file_client.return_value)
        lease.check()
        lease.release()
        raw.renew.assert_called_once()
        raw.release.assert_called_once()
        raw.break_lease.assert_not_called()
        file_system.get_file_client.return_value.create_file.assert_not_called()

    def test_adls_lock_file_creation_is_conditional(self):
        factory, file_system, _ = self.adls_leases()
        file = file_system.get_file_client.return_value
        file.get_file_properties.side_effect = [ResourceNotFoundError(), Mock()]
        file.create_file.side_effect = ResourceExistsError()
        factory.acquire("scope", wait=False)
        file.create_file.assert_called_once_with(etag="*", match_condition=MatchConditions.IfMissing)
        self.assertEqual(file.get_file_properties.call_count, 2)

    def test_adls_collision_is_not_a_reclaim(self):
        factory, _, raw = self.adls_leases()
        error = HttpResponseError()
        error.status_code = 409
        raw.acquire.side_effect = error
        with self.assertRaises(CollisionError):
            factory.acquire("scope", wait=False)
        raw.break_lease.assert_not_called()

    def test_adls_permission_error_is_not_retried_as_collision(self):
        factory, _, raw = self.adls_leases()
        error = HttpResponseError()
        error.status_code = 403
        raw.acquire.side_effect = error
        with self.assertRaisesRegex(WorkflowError, "lease service"):
            factory.acquire("scope", wait=False)
        self.assertEqual(raw.acquire.call_count, 1)

    def test_uncertain_run_retains_adls_file_lease(self):
        raw = Mock()
        guard = DataLakeFileLease(raw)
        guard.retain()
        guard.release()
        raw.release.assert_not_called()

    def test_lock_location_requires_dfs_and_a_user_owned_directory(self):
        settings = {**self.data["locking"], "account_url": "https://example.dfs.core.windows.net"}
        validate_lock_location(settings)
        for endpoint in ("http://example.dfs.core.windows.net", "https://example.blob.core.windows.net"):
            with self.subTest(endpoint=endpoint), self.assertRaises(WorkflowError):
                validate_lock_location({**settings, "account_url": endpoint})
        settings.update(account_url="https://onelake.dfs.fabric.microsoft.com", file_system="workspace-id")
        with self.assertRaises(WorkflowError):
            validate_lock_location(settings)
        settings["directory"] = "lakehouse-id/Files/dag/locks"
        validate_lock_location(settings)
