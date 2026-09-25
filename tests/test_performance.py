from __future__ import annotations

import copy
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

from spark_dag.config_loader import load_config
from spark_dag.control_store import DeltaEvents
from spark_dag.dag_validator import descendants
from spark_dag.model import RunRequest, Status, WorkflowError
from spark_dag.runtime import build_services
from spark_dag.worker import run_child
from tests.support import BUSINESS_KEY, DeploymentTest, InlineExecutor


def overlap_scenario(data, scheduling, delay=0.5):
    data["runtime"]["scheduling"] = scheduling
    tail = copy.deepcopy(next(node for node in data["nodes"] if node["id"] == "cleanup"))
    tail.update(id="fast_tail", dependencies=["extract_customers"], trigger={"type": "all_success"})
    tail["checkpoint"]["name"] = "fast_tail_done"
    data["nodes"].append(tail)
    data["runtime"]["fault_injection"] = {
        "extract_customers": {"kind": "timeout", "attempts": [1], "delay_seconds": delay / 10},
        "extract_orders": {"kind": "timeout", "attempts": [1], "delay_seconds": delay},
        "fast_tail": {"kind": "timeout", "attempts": [1], "delay_seconds": delay},
    }


class MeasuringExecutor(InlineExecutor):
    def __init__(self, config, store):
        super().__init__(config, store)
        self.mutex = threading.Lock()
        self.active = set()
        self.peak = 0
        self.started = {}
        self.finished = {}
        self.resource_peaks = {}
        self.group_peaks = {}

    def _execute(self, context):
        with self.mutex:
            self.active.add(context.node_id)
            self.started[context.node_id] = time.perf_counter()
            self.peak = max(self.peak, len(self.active))
            groups, resources = {}, {}
            for key in self.active:
                node = self.config.nodes[key]
                group = node["concurrency_group"]
                groups[group] = groups.get(group, 0) + 1
                for name, demand in node["resources"].items():
                    resources[name] = resources.get(name, 0) + demand
            for name, count in groups.items():
                self.group_peaks[name] = max(self.group_peaks.get(name, 0), count)
            for name, count in resources.items():
                self.resource_peaks[name] = max(self.resource_peaks.get(name, 0), count)
        try:
            super()._execute(context)
        finally:
            with self.mutex:
                self.finished[context.node_id] = time.perf_counter()
                self.active.remove(context.node_id)

    def execute_batch(self, contexts):
        if len(contexts) == 1:
            self._execute(contexts[0])
        else:
            with ThreadPoolExecutor(max_workers=len(contexts)) as pool:
                list(pool.map(self._execute, contexts))


class PerformanceTests(DeploymentTest):
    def measured_run(self, scheduling):
        overlap_scenario(self.data, scheduling)
        engine = self.engine()
        executor = MeasuringExecutor(engine.config, engine.store)
        engine.executor = executor
        self.last_engine = engine
        result = engine.run(RunRequest(BUSINESS_KEY))
        self.assert_report(result)
        self.assertLessEqual(executor.peak, self.data["concurrency"]["max_parallel"])
        for name, peak in executor.group_peaks.items():
            self.assertLessEqual(peak, self.data["concurrency"]["groups"][name])
        for name, peak in executor.resource_peaks.items():
            self.assertLessEqual(peak, self.data["concurrency"]["resources"][name])
        return result, executor

    def test_eager_successor_does_not_wait_for_unrelated_branch(self):
        _, executor = self.measured_run("eager")
        self.assertLess(executor.started["fast_tail"], executor.finished["extract_orders"])

    def test_barrier_mode_retains_native_wave_semantics(self):
        _, executor = self.measured_run("barrier")
        self.assertGreaterEqual(executor.started["fast_tail"], executor.finished["extract_orders"])

    def test_node_index_is_built_once(self):
        config = self.config()
        self.assertIs(config.nodes, config.nodes)

    def test_child_loads_run_and_dependencies_in_one_snapshot(self):
        cleanup = self.node("cleanup")
        cleanup["dependencies"] = ["extract_orders", "extract_customers"]
        self.data["nodes"] = [self.node("extract_orders"), self.node("extract_customers"), cleanup]
        engine = self.engine()
        counts = {}

        class InputProbe(InlineExecutor):
            def _execute(self, context):
                if context.node_id != "cleanup":
                    return super()._execute(context)
                config = load_config(context.config_path)
                services = build_services(config)
                with patch.object(services.store, "view", wraps=services.store.view) as snapshot:
                    run_child(context, services=services)
                counts["snapshots"] = snapshot.call_count

        engine.executor = InputProbe(engine.config, engine.store)
        result = engine.run(RunRequest(BUSINESS_KEY))
        self.assertEqual(result["status"], "SUCCEEDED")
        self.assertEqual(counts["snapshots"], 1)

    def test_descendant_traversal_visits_edges_once_on_reverse_ordered_chain(self):
        visits = 0

        class Dependencies(list):
            def __iter__(self):
                nonlocal visits
                visits += 1
                return super().__iter__()

        nodes = {
            f"n{index}": {"dependencies": Dependencies([f"n{index - 1}"] if index else [])}
            for index in reversed(range(5000))
        }
        self.assertEqual(len(descendants(nodes, {"n0"})), 5000)
        self.assertEqual(visits, 5000)

    def test_delta_history_fetches_only_new_events(self):
        store = DeltaEvents.__new__(DeltaEvents)
        store._event_cache, store._cache_lock = {}, threading.RLock()
        events = [
            {"scope": "scope", "sequence": sequence, "payload": {"value": sequence}}
            for sequence in range(1, 3)
        ]
        store._read_after = Mock(
            side_effect=lambda scope, cursor: [
                copy.deepcopy(event) for event in events if event["sequence"] > cursor
            ]
        )
        first = store.events("scope")
        first[0]["payload"]["value"] = "caller mutation"
        events.append({"scope": "scope", "sequence": 3, "payload": {"value": 3}})
        self.assertEqual(store.events("scope"), events)
        self.assertEqual(store.events("scope"), events)
        self.assertEqual([call.args[1] for call in store._read_after.call_args_list], [0, 2, 3])
        store.clear_cache("scope")
        self.assertEqual(store.events("scope"), events)
        self.assertEqual(store._read_after.call_args.args[1], 0)

    def test_incremental_history_still_rejects_sequence_gaps(self):
        store = DeltaEvents.__new__(DeltaEvents)
        store._event_cache, store._cache_lock = {}, threading.RLock()
        store._read_after = Mock(return_value=[{"sequence": 2}])
        with self.assertRaisesRegex(WorkflowError, "gap"):
            store.events("scope")

    def test_single_missing_source_exhausts_retries_without_scheduler_stall(self):
        keep = self.node("extract_orders")
        self.data["nodes"] = [keep]
        self.data["dag"]["entry_nodes"] = ["extract_orders"]
        (self.root / "sample_data" / "orders.csv").unlink()
        result = self.run_dag()
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(result["nodes"]["extract_orders"]["attempt"], 2)

    def test_preparation_window_does_not_claim_queued_nodes_early(self):
        self.data["concurrency"]["max_parallel"] = 1
        self.fault("extract_customers", "timeout", delay_seconds=0.15)
        result = self.run_dag()
        self.assert_report(result)
        self.assertGreaterEqual(
            result["nodes"]["extract_orders"]["started_at"],
            result["nodes"]["extract_customers"]["ended_at"],
        )
        events = self.last_engine.store.backend.events(self.scope())
        orders_ready = next(
            event
            for event in events
            if event["node_id"] == "extract_orders" and event["event_type"] == "NODE_READY"
        )
        self.assertGreaterEqual(orders_ready["timestamp"], result["nodes"]["extract_customers"]["ended_at"])

    def test_eager_fail_fast_does_not_cancel_dispatched_ready_attempt(self):
        self.data["runtime"]["failure_policy"] = "fail_fast"
        self.fault("extract_customers")
        self.fault("extract_orders", "timeout", delay_seconds=0.15)
        result = self.run_dag()
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(result["nodes"]["extract_orders"]["status"], Status.SUCCEEDED)
        self.assertEqual(result["nodes"]["cleanup"]["status"], Status.SUCCEEDED)
