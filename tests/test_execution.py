from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

from spark_dag.model import CollisionError, RunRequest, Status, WorkflowError
from tests.support import BUSINESS_KEY, DeploymentTest


class ExecutionTests(DeploymentTest):
    def test_happy_path_and_data_correctness(self):
        result = self.run_dag()
        self.assert_receiving_plan(result)
        self.assertEqual(result["total_nodes"], 9)
        self.assertEqual(result["successful_nodes"], 7)
        self.assertEqual(result["skipped_condition"], 2)
        transform = result["nodes"]["validate_shipments"]
        self.assertEqual(transform["reject_count"], 2)
        rejects = self.last_engine.services.artifacts.read(transform["outputs"]["rejects"])
        self.assertEqual({row["reason"] for row in rejects}, {"unknown_sku", "invalid_quantity"})

    def test_parallel_branches_and_fan_in_barrier(self):
        for key in ("extract_shipments", "extract_products"):
            self.fault(key, "timeout", delay_seconds=0.15)
        result = self.run_dag()
        self.assert_receiving_plan(result)
        first, second = (result["nodes"][key] for key in ("extract_shipments", "extract_products"))
        self.assertLess(
            max(first["started_at"], second["started_at"]), min(first["ended_at"], second["ended_at"])
        )
        join = result["nodes"]["validate_shipments"]
        self.assertGreaterEqual(join["started_at"], max(first["ended_at"], second["ended_at"]))

    def test_source_connection_limit_serializes_branches(self):
        self.data["concurrency"]["resources"]["source_reads"] = 1
        result = self.run_dag()
        first, second = (result["nodes"][key] for key in ("extract_products", "extract_shipments"))
        self.assertLessEqual(first["ended_at"], second["started_at"])

    def test_conditional_execution_and_completion_cleanup(self):
        self.data["targets"]["inventory_receipts"]["enabled"] = False
        result = self.run_dag()
        self.assertEqual(result["status"], "SUCCEEDED")
        self.assertEqual(result["nodes"]["request_receipts"]["status"], Status.SKIPPED_CONDITION)
        self.assertEqual(result["nodes"]["cleanup"]["status"], Status.SUCCEEDED)
        self.assertEqual(self.last_engine.store.view(self.scope()).all("outbox"), [])

    def test_retryable_failure(self):
        self.fault("validate_shipments", "retryable")
        result = self.run_dag()
        self.assert_receiving_plan(result)
        self.assertEqual(result["nodes"]["validate_shipments"]["attempt"], 2)
        self.assertEqual(result["retried_nodes"], 1)
        events = self.last_engine.store.backend.events(self.scope())
        self.assertEqual(sum(event["event_type"] == "RETRY_SCHEDULED" for event in events), 1)

    def test_retry_backoff_respects_delay_and_cap(self):
        self.fault("validate_shipments", "retryable", attempts=[1, 2])
        self.node("validate_shipments")["retry"].update(
            max_retries=2,
            delay_seconds=0.15,
            backoff=2,
            max_delay_seconds=0.2,
        )
        result = self.run_dag()
        self.assert_receiving_plan(result)
        events = self.last_engine.store.backend.events(self.scope())
        retries = [event for event in events if event["event_type"] == "RETRY_SCHEDULED"]
        self.assertEqual(len(retries), 2)
        for event, expected in zip(retries, [0.15, 0.2], strict=True):
            scheduled_delay = (
                event["payload"]["ready_at"] - datetime.fromisoformat(event["timestamp"]).timestamp()
            )
            self.assertAlmostEqual(scheduled_delay, expected, places=2)
            started = next(
                entry["payload"]["started_at"]
                for entry in events
                if entry["event_type"] == "ATTEMPT_CLAIMED"
                and entry["node_id"] == "validate_shipments"
                and entry["payload"]["attempt"] == event["payload"]["attempt"]
            )
            self.assertGreaterEqual(
                datetime.fromisoformat(started).timestamp() + 0.001, event["payload"]["ready_at"]
            )

    def test_non_retryable_failure_and_dependency_blocking(self):
        self.fault("validate_shipments")
        result = self.run_dag()
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(result["nodes"]["validate_shipments"]["attempt"], 1)
        self.assertEqual(result["nodes"]["quality_gate"]["status"], Status.BLOCKED)
        self.assertEqual(result["nodes"]["plan_receipts"]["status"], Status.BLOCKED)
        self.assertEqual(result["nodes"]["notify_failure"]["status"], Status.SUCCEEDED)
        self.assertEqual(result["nodes"]["cleanup"]["status"], Status.SUCCEEDED)

    def test_recovery_reuses_proven_success(self):
        self.fault("validate_shipments")
        failed = self.run_dag()
        self.data["runtime"]["fault_injection"].clear()
        recovered = self.run_dag("resume", failed["run_id"])
        self.assert_receiving_plan(recovered)
        for key in ("extract_shipments", "extract_products"):
            self.assertEqual(recovered["nodes"][key]["status"], Status.SKIPPED_ALREADY_SATISFIED)
            self.assertEqual(recovered["nodes"][key]["outputs"], failed["nodes"][key]["outputs"])
        self.assertEqual(recovered["parent_run_id"], failed["run_id"])
        self.assertEqual(recovered["nodes"]["validate_shipments"]["status"], Status.SUCCEEDED)

    def test_repeated_failure_during_recovery(self):
        self.fault("validate_shipments")
        first = self.run_dag()
        second = self.run_dag("resume", first["run_id"])
        self.assertEqual(second["status"], "FAILED")
        self.assertEqual(second["nodes"]["extract_shipments"]["status"], Status.SKIPPED_ALREADY_SATISFIED)
        self.data["runtime"]["fault_injection"].clear()
        third = self.run_dag("resume", second["run_id"])
        self.assert_receiving_plan(third)
        self.assertEqual(third["nodes"]["extract_shipments"]["reused_from_run_id"], first["run_id"])

    def test_force_restart_invalidates_only_selected_downstream(self):
        first = self.run_dag()
        result = self.run_dag("force_restart", first["run_id"], "extract_shipments")
        self.assert_receiving_plan(result)
        self.assertEqual(result["restart_mode"], "force_restart")
        self.assertEqual(result["restart_origin"], "extract_shipments")
        self.assertEqual(result["nodes"]["extract_products"]["status"], Status.SKIPPED_ALREADY_SATISFIED)
        for key in ("extract_shipments", "validate_shipments", "plan_receipts"):
            self.assertEqual(result["nodes"][key]["status"], Status.SUCCEEDED)
            self.assertNotEqual(result["nodes"][key]["outputs"], first["nodes"][key]["outputs"])

    def test_force_restart_from_named_checkpoint(self):
        first = self.run_dag()
        result = self.run_dag("force_restart", first["run_id"], "quality_approved")
        self.assert_receiving_plan(result)
        self.assertEqual(result["restart_origin"], "quality_gate")
        self.assertEqual(result["nodes"]["validate_shipments"]["status"], Status.SKIPPED_ALREADY_SATISFIED)

    def test_force_rerun_successful_run_without_duplicate_side_effect(self):
        first = self.run_dag()
        before = self.assert_receiving_plan(first)
        second = self.run_dag("force_rerun")
        self.assertEqual(self.assert_receiving_plan(second), before)
        self.assertEqual(second["skipped_already_satisfied"], 0)
        self.assertEqual(second["restart_mode"], "force_rerun")
        self.assertEqual(second["rerun_reason"], "TEST-123")
        self.assertEqual(second["restart_reason"], "")

    def test_normal_run_cannot_repeat_existing_business_key(self):
        self.run_dag()
        with self.assertRaisesRegex(WorkflowError, "history"):
            self.run_dag()

    def test_resume_requires_failed_latest_matching_business_run(self):
        first = self.run_dag()
        with self.assertRaisesRegex(WorkflowError, "failed"):
            self.run_dag("resume", first["run_id"])
        with self.assertRaisesRegex(WorkflowError, "latest"):
            self.engine().run(RunRequest("2026-09-25", "resume", first["run_id"], reason="INC-1"))

    def test_relevant_configuration_change_invalidates_success(self):
        self.fault("plan_receipts")
        first = self.run_dag()
        self.data["runtime"]["fault_injection"].clear()
        self.data["configuration_version"] = "2.0.1"
        self.data["data_quality"]["minimum_accepted_rows"] = 2
        second = self.run_dag("resume", first["run_id"])
        self.assert_receiving_plan(second)
        self.assertEqual(second["nodes"]["extract_shipments"]["status"], Status.SKIPPED_ALREADY_SATISFIED)
        self.assertEqual(second["nodes"]["validate_shipments"]["status"], Status.SUCCEEDED)
        self.assertEqual(second["nodes"]["quality_gate"]["status"], Status.SUCCEEDED)

    def test_retry_policy_change_preserves_unrelated_success(self):
        self.fault("quality_gate")
        first = self.run_dag()
        self.data["runtime"]["fault_injection"].clear()
        self.node("extract_shipments")["retry"]["delay_seconds"] = 3
        second = self.run_dag("resume", first["run_id"])
        self.assert_receiving_plan(second)
        self.assertEqual(second["nodes"]["extract_shipments"]["status"], Status.SKIPPED_ALREADY_SATISFIED)

    def test_missing_output_invalidates_its_dependents(self):
        self.fault("plan_receipts")
        first = self.run_dag()
        Path(first["nodes"]["extract_shipments"]["outputs"]["shipments"]["path"]).unlink()
        self.data["runtime"]["fault_injection"].clear()
        second = self.run_dag("resume", first["run_id"])
        self.assert_receiving_plan(second)
        self.assertEqual(second["nodes"]["extract_shipments"]["status"], Status.SUCCEEDED)
        self.assertEqual(second["nodes"]["extract_products"]["status"], Status.SKIPPED_ALREADY_SATISFIED)
        self.assertEqual(second["nodes"]["validate_shipments"]["status"], Status.SUCCEEDED)

    def test_corrupt_output_cannot_be_reused(self):
        self.fault("quality_gate")
        first = self.run_dag()
        artifact = Path(first["nodes"]["validate_shipments"]["outputs"]["accepted"]["path"])
        artifact.write_text('{"schema":"wrong","rows":[]}')
        self.data["runtime"]["fault_injection"].clear()
        second = self.run_dag("resume", first["run_id"])
        self.assert_receiving_plan(second)
        self.assertEqual(second["nodes"]["validate_shipments"]["status"], Status.SUCCEEDED)

    def test_incomplete_checkpoint_proof_is_not_reused(self):
        self.fault("quality_gate")
        first = self.run_dag()
        store = self.last_engine.store

        def damage_proof(tx):
            key = store.node_key(first["run_id"], "validate_shipments")
            record = tx.get("node", key)
            record["checkpoint"].pop("name")
            tx.put("node", key, record, "TEST_INCOMPLETE_CHECKPOINT")

        store.backend.transaction(self.scope(), damage_proof)
        self.data["runtime"]["fault_injection"].clear()
        second = self.run_dag("resume", first["run_id"])
        self.assert_receiving_plan(second)
        self.assertEqual(second["nodes"]["validate_shipments"]["status"], Status.SUCCEEDED)

    def test_non_utf8_output_is_invalidated(self):
        self.fault("quality_gate")
        first = self.run_dag()
        Path(first["nodes"]["validate_shipments"]["outputs"]["accepted"]["path"]).write_bytes(b"\xff\xfe\xff")
        self.data["runtime"]["fault_injection"].clear()
        second = self.run_dag("resume", first["run_id"])
        self.assert_receiving_plan(second)

    def test_source_content_change_prevents_stale_skip(self):
        self.fault("quality_gate")
        first = self.run_dag()
        self.add_shipment()
        self.data["runtime"]["fault_injection"].clear()
        second = self.run_dag("resume", first["run_id"])
        self.assertEqual(second["status"], "SUCCEEDED")
        self.assertEqual(second["nodes"]["extract_shipments"]["status"], Status.SUCCEEDED)
        rows = self.last_engine.services.artifacts.read(
            second["nodes"]["plan_receipts"]["outputs"]["receiving_plan"]
        )
        self.assertEqual(next(row for row in rows if row["warehouse_id"] == "WH-ATL")["expected_units"], 130)

    def test_quality_threshold_failure_is_not_retryable(self):
        self.data["data_quality"]["max_reject_count"] = 0
        result = self.run_dag()
        self.assertEqual(result["nodes"]["quality_gate"]["error"]["category"], "DATA_QUALITY")
        self.assertFalse(result["nodes"]["quality_gate"]["retryable"])

    def test_side_effect_commit_before_failure_is_deduplicated_on_retry(self):
        self.fault("request_receipts", "after_effect")
        result = self.run_dag()
        self.assert_receiving_plan(result)
        self.assertEqual(result["nodes"]["request_receipts"]["attempt"], 2)
        events = self.last_engine.store.backend.events(self.scope())
        self.assertEqual(sum(event["event_type"] == "OUTBOX_COMMITTED" for event in events), 1)

    def test_changed_payload_cannot_reuse_same_side_effect_key(self):
        first = self.run_dag()
        self.add_shipment()
        second = self.run_dag("force_rerun")
        self.assertEqual(second["status"], "FAILED")
        self.assertEqual(second["nodes"]["request_receipts"]["error"]["code"], "SIDE_EFFECT_CONFLICT")
        self.assertEqual(len(self.last_engine.store.view(self.scope()).all("outbox")), 1)
        self.assertNotEqual(first["run_id"], second["run_id"])

    def test_pending_receipt_reference_is_repaired_without_duplicate_intent(self):
        first = self.run_dag()
        Path(first["nodes"]["plan_receipts"]["outputs"]["receiving_plan"]["path"]).unlink()
        second = self.run_dag("force_restart", first["run_id"], "plan_receipts")
        self.assert_receiving_plan(second)
        outbox = self.last_engine.store.view(self.scope()).all("outbox")
        self.assertEqual(
            outbox[0]["output_reference"], second["nodes"]["plan_receipts"]["outputs"]["receiving_plan"]
        )
        events = self.last_engine.store.backend.events(self.scope())
        self.assertEqual(sum(event["event_type"] == "OUTBOX_COMMITTED" for event in events), 1)
        self.assertEqual(sum(event["event_type"] == "OUTBOX_REFERENCE_REFRESHED" for event in events), 1)

    def test_manual_non_idempotent_work_requires_approval(self):
        node = self.node("request_receipts")
        node["idempotency"]["strategy"] = "manual"
        node["retry"]["max_retries"] = 0
        self.fault("request_receipts", "after_effect")
        first = self.run_dag()
        self.data["runtime"]["fault_injection"].clear()
        blocked = self.run_dag("resume", first["run_id"])
        self.assertEqual(blocked["nodes"]["request_receipts"]["error"]["code"], "APPROVAL_REQUIRED")
        approved = self.run_dag("resume", blocked["run_id"], approvals=("request_receipts",))
        self.assert_receiving_plan(approved)

    def test_compensation_requires_validated_companion_and_approval(self):
        node = self.node("request_receipts")
        node["idempotency"]["strategy"] = "compensation"
        node["idempotency"]["compensation_node"] = "notify_failure"
        node["retry"]["max_retries"] = 0
        self.fault("request_receipts", "after_effect")
        first = self.run_dag()
        self.assertEqual(first["nodes"]["notify_failure"]["status"], Status.SUCCEEDED)
        self.data["runtime"]["fault_injection"].clear()
        recovered = self.run_dag("resume", first["run_id"], approvals=("request_receipts",))
        self.assert_receiving_plan(recovered)

    def test_manual_approval_cannot_be_bypassed_by_an_intermediate_blocked_run(self):
        self.node("request_receipts")["idempotency"]["strategy"] = "manual"
        self.node("request_receipts")["retry"]["max_retries"] = 0
        self.fault("request_receipts", "after_effect")
        first = self.run_dag()
        self.data["runtime"]["fault_injection"].clear()
        self.fault("extract_shipments")
        blocked = self.run_dag("force_restart", first["run_id"], "extract_shipments")
        self.assertEqual(blocked["nodes"]["request_receipts"]["attempt"], 0)
        self.data["runtime"]["fault_injection"].clear()
        rejected = self.run_dag("resume", blocked["run_id"])
        self.assertEqual(rejected["nodes"]["request_receipts"]["error"]["code"], "APPROVAL_REQUIRED")

    def test_missing_source_fails_bounded_retries_without_abandoning_run(self):
        (self.root / "sample_data" / "shipments").rename(self.root / "sample_data" / "shipments-unavailable")
        result = self.run_dag()
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(result["nodes"]["extract_shipments"]["attempt"], 2)
        self.assertEqual(result["nodes"]["extract_shipments"]["error"]["code"], "SOURCE_UNAVAILABLE")
        self.assertEqual(result["nodes"]["validate_shipments"]["status"], Status.BLOCKED)

    def test_fail_fast_still_runs_failure_and_completion_handlers(self):
        self.data["runtime"]["failure_policy"] = "fail_fast"
        self.data["concurrency"]["max_parallel"] = 1
        self.fault("extract_products")
        result = self.run_dag()
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(result["nodes"]["extract_shipments"]["status"], Status.CANCELLED)
        self.assertEqual(result["nodes"]["notify_failure"]["status"], Status.SUCCEEDED)
        self.assertEqual(result["nodes"]["cleanup"]["status"], Status.SUCCEEDED)

    def test_run_deadline_cancels_ready_work_without_busy_loop(self):
        self.data["runtime"]["run_timeout_seconds"] = 0.1
        self.data["concurrency"]["max_parallel"] = 1
        self.fault("extract_products", "timeout", delay_seconds=0.2)
        started = time.monotonic()
        result = self.run_dag()
        self.assertLess(time.monotonic() - started, 5)
        self.assertEqual(result["status"], "FAILED")
        self.assertGreaterEqual(result["cancelled_nodes"], 1)
        self.assertEqual(result["pending_nodes"] + result["ready_nodes"] + result["running_nodes"], 0)

    def test_partial_artifact_write_is_not_published_as_success(self):
        self.node("validate_shipments")["retry"]["max_retries"] = 0
        self.fault("validate_shipments", "after_output")
        first = self.run_dag()
        self.assertEqual(first["nodes"]["validate_shipments"]["outputs"], {})
        self.assertIsNone(first["nodes"]["validate_shipments"]["checkpoint"])
        self.data["runtime"]["fault_injection"].clear()
        second = self.run_dag("resume", first["run_id"])
        self.assert_receiving_plan(second)

    def test_audit_completeness(self):
        self.fault("extract_shipments", "retryable")
        result = self.run_dag()
        events = self.last_engine.store.backend.events(self.scope())
        self.assertEqual([event["sequence"] for event in events], list(range(1, len(events) + 1)))
        required = {
            "application_id",
            "dag_id",
            "dag_version",
            "run_id",
            "business_run_key",
            "parent_run_id",
            "attempt",
            "node_id",
            "notebook",
            "status",
            "dependency_status",
            "started_at",
            "ended_at",
            "duration_seconds",
            "input_fingerprint",
            "outputs",
            "checkpoint",
            "retry_count",
            "error",
            "configuration_version",
            "code_version",
            "restart_reason",
            "rerun_reason",
        }
        for node in result["nodes"].values():
            self.assertTrue(required <= node.keys())
            if node["status"] == Status.SUCCEEDED:
                self.assertIsNotNone(node["checkpoint"])
                self.assertTrue(node["outputs"])
        self.assertGreater(result["critical_path_duration_seconds"], 0)

    def test_concurrent_orchestrator_collision(self):
        entered, release = threading.Event(), threading.Event()
        first = self.engine()
        delegate = first.executor

        class PausingExecutor:
            def execute_batch(self, contexts):
                entered.set()
                if not release.wait(10):
                    raise AssertionError("Test synchronization timed out")
                delegate.execute_batch(contexts)

        first.executor = PausingExecutor()
        with ThreadPoolExecutor(max_workers=2) as pool:
            running = pool.submit(first.run, RunRequest(BUSINESS_KEY))
            self.assertTrue(entered.wait(10))
            try:
                with self.assertRaises(CollisionError):
                    self.engine().run(RunRequest(BUSINESS_KEY))
            finally:
                release.set()
            self.assertEqual(running.result(timeout=20)["status"], "SUCCEEDED")

    def test_failure_injection_into_every_restartable_node(self):
        main = {
            "extract_shipments",
            "extract_products",
            "validate_shipments",
            "quality_gate",
            "plan_receipts",
            "request_receipts",
            "cleanup",
        }
        for index, key in enumerate([node["id"] for node in self.data["nodes"]]):
            with self.subTest(failed_node=key):
                self.data["control_store"]["path"] = f"matrix-{index}/control.sqlite3"
                self.data["storage"]["path"] = f"matrix-{index}/outputs"
                self.data["reject_data"]["path"] = f"matrix-{index}/rejects"
                self.data["runtime"]["fault_injection"].clear()
                for node in self.data["nodes"]:
                    node["retry"]["max_retries"] = 0
                    node["timeout_seconds"] = 30
                if key == "notify_failure":
                    self.fault("quality_gate")
                elif key == "notify_timeout":
                    self.node("quality_gate")["timeout_seconds"] = 0.1
                    self.fault("quality_gate", "timeout", delay_seconds=0.15)
                self.fault(key)
                first = self.run_dag()
                self.assertEqual(first["nodes"][key]["status"], Status.FAILED)
                del self.data["runtime"]["fault_injection"][key]
                second = self.run_dag("resume", first["run_id"])
                self.assertEqual(second["nodes"][key]["status"], Status.SUCCEEDED)
                if key not in {"extract_shipments", "extract_products"}:
                    self.assertEqual(
                        second["nodes"]["extract_shipments"]["status"], Status.SKIPPED_ALREADY_SATISFIED
                    )
                if key not in main:
                    self.data["runtime"]["fault_injection"].clear()
                    self.node("quality_gate")["timeout_seconds"] = 30
                    second = self.run_dag("resume", second["run_id"])
                self.assert_receiving_plan(second)


class ProcessExecutionTests(DeploymentTest):
    def test_real_child_process_happy_path(self):
        self.assert_receiving_plan(self.run_dag(real_processes=True))

    def test_timeout_terminates_worker_before_recovery(self):
        self.node("extract_shipments")["timeout_seconds"] = 0.5
        self.node("extract_shipments")["retry"]["max_retries"] = 0
        self.fault("extract_shipments", "timeout", delay_seconds=20)
        started = time.monotonic()
        first = self.run_dag(real_processes=True)
        self.assertLess(time.monotonic() - started, 20)
        self.assertEqual(first["nodes"]["extract_shipments"]["status"], Status.TIMED_OUT)
        self.assertFalse(first["nodes"]["extract_shipments"]["execution_uncertain"])
        self.assertEqual(first["nodes"]["notify_timeout"]["status"], Status.SUCCEEDED)
        self.data["runtime"]["fault_injection"].clear()
        self.node("extract_shipments")["timeout_seconds"] = 30
        second = self.run_dag("resume", first["run_id"], real_processes=True)
        self.assert_receiving_plan(second)
        self.assertEqual(second["nodes"]["extract_products"]["status"], Status.SKIPPED_ALREADY_SATISFIED)

    def test_timeout_retry_has_no_overlapping_attempts(self):
        self.node("extract_shipments")["timeout_seconds"] = 3
        self.fault("extract_shipments", "timeout", delay_seconds=20)
        result = self.run_dag(real_processes=True)
        self.assert_receiving_plan(result)
        attempts = [
            event["payload"]
            for event in self.last_engine.store.backend.events(self.scope())
            if event["event_type"] == "ATTEMPT_FINISHED" and event["node_id"] == "extract_shipments"
        ]
        self.assertEqual(len(attempts), 2)
        self.assertEqual(attempts[0]["status"], Status.TIMED_OUT)
        self.assertLessEqual(
            datetime.fromisoformat(attempts[0]["ended_at"]), datetime.fromisoformat(attempts[1]["started_at"])
        )

    def test_crash_before_checkpoint_retries_safely(self):
        self.fault("extract_shipments", "crash")
        result = self.run_dag(real_processes=True)
        self.assert_receiving_plan(result)
        self.assertEqual(result["nodes"]["extract_shipments"]["attempt"], 2)

    def test_cli_propagates_critical_failure(self):
        import subprocess
        import sys

        self.fault("quality_gate")
        self.write()
        process = subprocess.run(
            [
                sys.executable,
                "-m",
                "spark_dag",
                "run",
                "--config",
                str(self.root / "job_config.json"),
                "--business-run-key",
                BUSINESS_KEY,
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(process.returncode, 1)
        result = json.loads(process.stdout.splitlines()[-1])
        self.assertEqual(result["status"], "FAILED")
