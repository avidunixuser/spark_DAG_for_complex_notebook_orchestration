from __future__ import annotations

from spark_dag.dag_validator import descendants, trigger_status, validate_dag
from spark_dag.model import Status, WorkflowError
from tests.support import DeploymentTest


class DagValidationTests(DeploymentTest):
    def test_missing_notebook_definition(self):
        del self.data["notebooks"]["extract_shipments"]
        with self.assertRaisesRegex(WorkflowError, "undefined notebook"):
            validate_dag(self.config())

    def test_missing_notebook_file(self):
        (self.root / "notebooks" / "extract_shipments.ipynb").unlink()
        with self.assertRaisesRegex(WorkflowError, "missing"):
            validate_dag(self.config())

    def test_cycle_detection(self):
        self.node("extract_shipments")["dependencies"] = ["plan_receipts"]
        with self.assertRaisesRegex(WorkflowError, "cycle"):
            validate_dag(self.config())

    def test_missing_dependency_detection(self):
        self.node("quality_gate")["dependencies"] = ["undefined"]
        with self.assertRaisesRegex(WorkflowError, "undefined dependency"):
            validate_dag(self.config())

    def test_duplicate_node_ids(self):
        self.data["nodes"].append(self.data["nodes"][0].copy())
        with self.assertRaisesRegex(WorkflowError, "unique"):
            validate_dag(self.config())

    def test_unreachable_nodes(self):
        self.data["dag"]["entry_nodes"] = ["extract_shipments"]
        with self.assertRaisesRegex(WorkflowError, "every root"):
            validate_dag(self.config())

    def test_invalid_trigger(self):
        self.node("quality_gate")["trigger"]["type"] = "execute_python_expression"
        with self.assertRaises(WorkflowError):
            self.config()

    def test_dependency_trigger_cannot_be_a_root(self):
        self.node("extract_shipments")["trigger"]["type"] = "any_failed"
        with self.assertRaisesRegex(WorkflowError, "root"):
            validate_dag(self.config())

    def test_invalid_configuration_reference(self):
        self.node("extract_shipments")["parameters"]["source"] = {"$config": "sources.missing"}
        with self.assertRaisesRegex(WorkflowError, "reference"):
            validate_dag(self.config())

    def test_upstream_reference_requires_dependency(self):
        self.node("quality_gate")["parameters"]["receiving_plan"] = {
            "$output": "plan_receipts.receiving_plan"
        }
        with self.assertRaisesRegex(WorkflowError, "dependency"):
            validate_dag(self.config())

    def test_invalid_concurrency_settings(self):
        self.data["concurrency"]["max_parallel"] = 3
        with self.assertRaisesRegex(WorkflowError, "capacity"):
            validate_dag(self.config())
        self.data["concurrency"]["max_parallel"] = 0
        with self.assertRaises(WorkflowError):
            self.config()

    def test_resource_capacity_validation(self):
        self.node("extract_shipments")["resources"]["source_reads"] = 3
        with self.assertRaisesRegex(WorkflowError, "capacity"):
            validate_dag(self.config())

    def test_unsupported_restart_policy(self):
        self.node("extract_shipments")["restart"]["policy"] = "trust_status_without_outputs"
        with self.assertRaises(WorkflowError):
            self.config()

    def test_manual_side_effects_cannot_automatically_retry(self):
        self.node("request_receipts")["idempotency"]["strategy"] = "manual"
        with self.assertRaisesRegex(WorkflowError, "cannot retry"):
            validate_dag(self.config())

    def test_compensation_requires_declared_node(self):
        self.node("request_receipts")["idempotency"]["strategy"] = "compensation"
        self.node("request_receipts")["retry"]["max_retries"] = 0
        with self.assertRaisesRegex(WorkflowError, "compensation"):
            validate_dag(self.config())

    def test_deterministic_topological_plan(self):
        first = validate_dag(self.config())
        self.data["nodes"].reverse()
        self.assertEqual(validate_dag(self.config()), first)
        self.assertLess(first.index("extract_products"), first.index("validate_shipments"))
        self.assertLess(first.index("extract_shipments"), first.index("validate_shipments"))
        self.assertEqual(first[-1], "cleanup")

    def test_descendants_preserve_unrelated_branch(self):
        changed = descendants(self.config().nodes, {"extract_shipments"})
        self.assertNotIn("extract_products", changed)
        self.assertIn("plan_receipts", changed)
        self.assertIn("notify_timeout", changed)

    def test_success_failure_timeout_and_completion_triggers(self):
        node = self.node("quality_gate")
        node["dependencies"] = ["a", "b"]
        cases = [
            ("all_success", {"a": Status.SUCCEEDED, "b": Status.SKIPPED_ALREADY_SATISFIED}, None),
            ("all_success", {"a": Status.SUCCEEDED, "b": Status.FAILED}, Status.BLOCKED),
            ("any_success", {"a": Status.SUCCEEDED, "b": Status.SKIPPED_CONDITION}, None),
            ("all_success", {"a": Status.SUCCEEDED, "b": Status.SKIPPED_CONDITION}, Status.SKIPPED_CONDITION),
            ("any_failed", {"a": Status.FAILED, "b": Status.SUCCEEDED}, None),
            ("any_timed_out", {"a": Status.TIMED_OUT, "b": Status.SUCCEEDED}, None),
            ("all_done", {"a": Status.CANCELLED, "b": Status.BLOCKED}, None),
        ]
        for kind, states, expected in cases:
            with self.subTest(trigger=kind, states=states):
                node["trigger"] = {"type": kind}
                self.assertEqual(trigger_status(node, states, {}), expected)
