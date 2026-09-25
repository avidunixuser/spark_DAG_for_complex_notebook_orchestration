from __future__ import annotations

import io
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
from dataclasses import replace
from unittest.mock import Mock, patch

from spark_dag.components import PLAN_TARGET, execute
from spark_dag.control_store import DeltaEvents, Transaction
from spark_dag.model import CollisionError, Status, TaskFailure, WorkflowError, safe_error
from spark_dag.telemetry import EventLogger
from tests.support import PreparedAttemptTest


class ControlStoreTests(PreparedAttemptTest):
    def test_attempt_claim_is_atomic(self):
        engine, _, context = self.prepare_attempt()
        barrier = threading.Barrier(2)

        def claim():
            barrier.wait(timeout=5)
            try:
                engine.store.claim(context)
                return "claimed"
            except CollisionError:
                return "collision"

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: claim(), range(2)))
        self.assertCountEqual(results, ["claimed", "collision"])
        self.assertEqual(
            engine.store.node(self.scope(), context.run_id, context.node_id)["status"], Status.RUNNING
        )

    def test_stale_attempt_token_cannot_claim(self):
        engine, _, context = self.prepare_attempt()
        with self.assertRaises(CollisionError):
            engine.store.claim(replace(context, claim_id="stale"))

    def test_claim_does_not_depend_on_a_remote_child_clock(self):
        engine, _, context = self.prepare_attempt()
        with patch("spark_dag.control_store.time.time", return_value=time.time() - 30):
            result = engine.store.claim(context)
        self.assertEqual(result["status"], Status.RUNNING)

    def test_transaction_rollback_leaves_no_partial_events(self):
        engine = self.engine()

        def fail(tx):
            tx.put("test", "one", {"status": "first"}, "TEST")
            tx.put("test", "two", {"status": "second"}, "TEST")
            raise RuntimeError("rollback")

        with self.assertRaises(RuntimeError):
            engine.store.backend.transaction(self.scope(), fail)
        self.assertEqual(engine.store.backend.events(self.scope()), [])

    def test_success_requires_final_checkpoint(self):
        engine, _, context = self.prepare_attempt()
        engine.store.claim(context)
        with self.assertRaisesRegex(WorkflowError, "checkpoint"):
            engine.store.finish_attempt(context, Status.SUCCEEDED, outputs={"fake": {}})
        self.assertEqual(
            engine.store.node(self.scope(), context.run_id, context.node_id)["status"], Status.RUNNING
        )

    def test_uncertain_attempt_cannot_retry(self):
        engine, run, context = self.prepare_attempt()
        engine.store.finish_attempt(context, Status.TIMED_OUT, uncertain=True, allow_ready=True)
        with self.assertRaisesRegex(WorkflowError, "uncertain"):
            engine.store.prepare(self.scope(), run["run_id"], context.node_id, run["owner"], "unused", {})

    def test_abandoned_run_requires_explicit_reconciliation(self):
        engine, run, context = self.prepare_attempt()
        engine.store.claim(context)
        with self.assertRaises(CollisionError):
            self.run_dag("resume", run["run_id"])
        with self.assertRaisesRegex(WorkflowError, "Confirm"):
            engine.store.reconcile(self.scope(), run["run_id"], workers_stopped=False, reason="INC-1")
        engine.store.reconcile(self.scope(), run["run_id"], workers_stopped=True, reason="INC-1")
        self.assertEqual(engine.store.run(self.scope(), run["run_id"])["status"], "FAILED")
        with self.assertRaises(CollisionError):
            engine.store.finish_attempt(context, Status.FAILED)
        self.assert_receiving_plan(self.run_dag("resume", run["run_id"]))

    def test_outbox_requires_current_running_claim(self):
        engine, _, context = self.prepare_attempt()
        with self.assertRaises(CollisionError):
            engine.store.put_outbox(context, "payload", {}, operation="inventory_receipt")

    def test_audit_sequence_corruption_is_rejected(self):
        with self.assertRaisesRegex(WorkflowError, "sequence"):
            Transaction("scope", [{"sequence": 2}])

    def test_errors_and_logs_never_echo_sensitive_exception_text(self):
        hidden = "password=not-for-logs person@example.invalid"
        error = safe_error(RuntimeError(hidden))
        self.assertNotIn(hidden, str(error))
        sink = io.StringIO()
        with redirect_stdout(sink):
            EventLogger({"enabled": True, "log_business_run_key": False}).emit(
                {
                    "event_type": "TEST_FAILURE",
                    "timestamp": "now",
                    "payload": {
                        "run_id": "id",
                        "error": error,
                        "parameters": hidden,
                        "business_run_key": hidden,
                    },
                }
            )
        self.assertNotIn("password", sink.getvalue())
        self.assertNotIn("person@", sink.getvalue())
        self.assertIn("INFRASTRUCTURE", sink.getvalue())

    def test_receiving_plan_overflow_fails_before_any_output_commit(self):
        _, _, context = self.prepare_attempt()
        context = replace(
            context,
            parameters={"target": PLAN_TARGET},
            upstream_outputs={"quality_gate": {"accepted": {}}},
        )
        artifacts = Mock()
        artifacts.spark = None
        artifacts.read.return_value = [
            {
                "warehouse_id": "WH-ATL",
                "sku": "SKU-FILTER",
                "unit_of_measure": "EA",
                "quantity": 999999999999999999,
            }
        ] * 10
        with self.assertRaises(TaskFailure) as failure:
            execute("plan_receipts", context, artifacts)
        self.assertEqual(failure.exception.code, "AGGREGATE_OVERFLOW")
        artifacts.write.assert_not_called()

    def test_delta_writes_state_and_audit_in_one_atomic_batch(self):
        events = DeltaEvents.__new__(DeltaEvents)
        events.spark, events.leases = Mock(), Mock()
        events._event_cache, events._cache_lock = {}, threading.RLock()
        events.path = "test-control"
        events.logger = EventLogger({"enabled": False, "log_business_run_key": False})
        events.events = Mock(return_value=[])

        def action(tx):
            tx.put("run", "r", {"run_id": "r", "status": "RUNNING"}, "RUN_STARTED")
            tx.put("node", "r:n", {"run_id": "r", "node_id": "n", "status": "PENDING"}, "NODE_CREATED")
            return "result"

        self.assertEqual(events.transaction("scope", action), "result")
        rows = events.spark.createDataFrame.call_args.args[0]
        self.assertEqual([row["sequence"] for row in rows], [1, 2])
        events.spark.createDataFrame.return_value.write.format.return_value.mode.return_value.save.assert_called_once_with(
            "test-control"
        )
        events.leases.acquire.return_value.check.assert_called_once()
        events.leases.acquire.return_value.release.assert_called_once()

    def test_delta_uncertain_commit_retains_transaction_lease(self):
        events = DeltaEvents.__new__(DeltaEvents)
        events.spark, events.leases = Mock(), Mock()
        events._event_cache, events._cache_lock = {}, threading.RLock()
        events.path = "test-control"
        events.logger = EventLogger({"enabled": False, "log_business_run_key": False})
        events.events = Mock(return_value=[])
        writer = events.spark.createDataFrame.return_value.write.format.return_value.mode.return_value
        writer.save.side_effect = OSError("acknowledgement lost")
        with self.assertRaises(OSError):
            events.transaction("scope", lambda tx: tx.put("run", "r", {"run_id": "r"}, "TEST"))
        events.leases.acquire.return_value.retain.assert_called_once()
