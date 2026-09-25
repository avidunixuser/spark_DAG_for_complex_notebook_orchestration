from __future__ import annotations

import re
import time
from collections import Counter, defaultdict
from dataclasses import replace
from datetime import date
from typing import Any

from .checkpoint_manager import can_reuse, input_fingerprint, outputs_valid
from .config_loader import Configuration, resolve_parameters
from .dag_validator import descendants, trigger_status, validate_dag
from .executors import DispatchSession, Executor, LocalExecutor
from .leases import Lease
from .model import (
    BAD,
    SAFE_STRATEGIES,
    SUCCESS,
    TERMINAL,
    AttemptContext,
    Mode,
    RunRequest,
    Status,
    WorkflowError,
    duration,
    fingerprint,
    safe_error,
    utc_now,
)
from .runtime import Services, build_services, log_configuration


def normalize_request(config: Configuration, request: RunRequest) -> RunRequest:
    try:
        mode = Mode(request.mode)
        date.fromisoformat(request.business_run_key)
    except ValueError:
        raise WorkflowError("INVALID_RUN_REQUEST", "Use a supported mode and an ISO business date.") from None
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", request.business_run_key):
        raise WorkflowError("INVALID_RUN_KEY", "Business run keys must be non-sensitive YYYY-MM-DD dates.")
    configured = None
    if config.data["force_rerun"]:
        configured = Mode.FORCE_RERUN
    elif config.data["force_restart"]["enabled"]:
        configured = Mode.FORCE_RESTART
    if configured:
        if mode not in {Mode.NORMAL, configured}:
            raise WorkflowError("CONFLICTING_MODES", "Runtime mode conflicts with the sidecar force setting.")
        mode = configured
    origin = request.restart_from
    if mode == Mode.FORCE_RESTART:
        origin = origin or config.data["force_restart"]["from_node"]
        if not origin:
            raise WorkflowError("MISSING_RESTART_ORIGIN", "Force restart needs a node ID or checkpoint name.")
        if origin not in config.nodes:
            matches = [key for key, node in config.nodes.items() if node["checkpoint"]["name"] == origin]
            if not matches:
                raise WorkflowError("INVALID_RESTART_ORIGIN", "The named node or checkpoint is not defined.")
            origin = matches[0]
    elif origin:
        raise WorkflowError("UNEXPECTED_RESTART_ORIGIN", "Only force restart accepts a restart origin.")
    if mode in {Mode.RESUME, Mode.FORCE_RESTART} and not request.parent_run_id:
        raise WorkflowError("MISSING_PARENT", "Resume and force restart require parent_run_id.")
    if mode == Mode.NORMAL and (request.parent_run_id or request.approved_nodes):
        raise WorkflowError(
            "INVALID_RUN_REQUEST", "A normal run cannot reuse a parent or recovery approvals."
        )
    if mode != Mode.NORMAL or request.approved_nodes:
        if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", request.reason):
            raise WorkflowError(
                "MISSING_REASON", "Recovery needs a non-sensitive incident/change identifier."
            )
    if not set(request.approved_nodes) <= config.nodes.keys():
        raise WorkflowError("INVALID_APPROVAL", "Recovery approvals reference an undefined node.")
    return replace(request, mode=str(mode), restart_from=origin)


class Orchestrator:
    def __init__(
        self, config: Configuration, *, services: Services | None = None, executor: Executor | None = None
    ):
        self.config = config
        self.plan = validate_dag(config)
        self.services = services or build_services(config)
        self.store = self.services.store
        if executor is None and config.data["runtime"]["platform"] != "local":
            raise WorkflowError(
                "MISSING_EXECUTOR", "Cloud orchestration requires an explicitly constructed adapter."
            )
        self.executor = executor or LocalExecutor(config, self.store)
        self.last_run: dict[str, Any] | None = None

    def _context(
        self,
        run: dict[str, Any],
        node: dict[str, Any],
        record: dict[str, Any],
        dependencies: dict[str, dict[str, Any]],
        parameters: dict[str, Any],
        deadline: float,
    ) -> AttemptContext:
        return AttemptContext(
            run_id=run["run_id"],
            business_run_key=run["business_run_key"],
            node_id=node["id"],
            attempt=record["attempt"],
            restart_mode=run["restart_mode"],
            config_path=str(self.config.path),
            deployment_dir=str(self.config.root),
            environment=self.config.data["environment"],
            scope=run["scope"],
            claim_id=record["claim_id"],
            input_fingerprint=record["input_fingerprint"],
            configuration_fingerprint=self.config.fingerprint,
            code_fingerprint=self.config.code_fingerprint,
            deadline=deadline,
            parent_context={
                "parent_run_id": run["parent_run_id"],
                "dag_id": run["dag_id"],
                "dag_version": run["dag_version"],
                "platform": self.config.data["runtime"]["platform"],
            },
            upstream_outputs={key: value["outputs"] for key, value in dependencies.items()},
            dependency_status={key: value["status"] for key, value in dependencies.items()},
            parameters=parameters,
        )

    def _parameters(
        self,
        run: dict[str, Any],
        node: dict[str, Any],
        attempt: int,
        dependencies: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        return resolve_parameters(
            node["parameters"],
            self.config.data,
            {
                "business_run_key": run["business_run_key"],
                "run_id": run["run_id"],
                "node_id": node["id"],
                "attempt": attempt,
                "restart_mode": run["restart_mode"],
            },
            {key: value["outputs"] for key, value in dependencies.items()},
        )

    def _reexecution_allowed(
        self,
        node: dict[str, Any],
        previous: dict[str, dict[str, Any]],
        request: RunRequest,
        attempted_nodes: set[str],
    ) -> None:
        if request.mode == Mode.NORMAL:
            return
        if node["id"] not in attempted_nodes:
            return
        if not node["restart"]["eligible"]:
            raise WorkflowError(
                "RESTART_NOT_ELIGIBLE", "The node requires a separately approved recovery design."
            )
        strategy = node["idempotency"]["strategy"]
        if strategy not in SAFE_STRATEGIES:
            if node["id"] not in request.approved_nodes:
                raise WorkflowError(
                    "APPROVAL_REQUIRED", "Repeating a non-idempotent operation requires explicit approval."
                )
            if strategy == "compensation":
                companion = node["idempotency"]["compensation_node"]
                compensated = previous.get(companion)
                if (
                    not compensated
                    or compensated["status"] not in SUCCESS
                    or not outputs_valid(
                        self.config.nodes[companion], compensated["outputs"], self.services.artifacts
                    )
                ):
                    raise WorkflowError(
                        "COMPENSATION_REQUIRED", "The compensation checkpoint has not been validated."
                    )

    def _retryable(self, node: dict[str, Any], record: dict[str, Any]) -> bool:
        return bool(
            record["status"] in {Status.FAILED, Status.TIMED_OUT}
            and not record["execution_uncertain"]
            and record["retryable"]
            and record["error"]
            and record["error"]["category"] in node["retry"]["categories"]
            and record["attempt"] <= node["retry"]["max_retries"]
            and node["idempotency"]["strategy"] in SAFE_STRATEGIES
            and node["restart"]["eligible"]
        )

    def _summary(self, run: dict[str, Any], *, uncertain: bool = False) -> dict[str, Any]:
        snapshot = self.store.view(run["scope"])
        records = {node["node_id"]: node for node in snapshot.all("node") if node["run_id"] == run["run_id"]}
        counts = Counter(record["status"] for record in records.values())
        active_time: dict[str, float] = defaultdict(float)
        for event in snapshot.events:
            if event["run_id"] == run["run_id"] and event["event_type"] == "ATTEMPT_FINISHED":
                active_time[event["node_id"]] += event["payload"]["duration_seconds"]
        weighted_paths: dict[str, float] = {}
        for key in self.plan:
            weighted_paths[key] = active_time[key] + max(
                (weighted_paths[dependency] for dependency in self.config.nodes[key]["dependencies"]),
                default=0,
            )
        uncertain = uncertain or any(record["execution_uncertain"] for record in records.values())
        failed = any(record["critical"] and record["status"] in BAD for record in records.values())
        status = "RECOVERY_REQUIRED" if uncertain else "FAILED" if failed else "SUCCEEDED"
        return {
            "run_id": run["run_id"],
            "business_run_key": run["business_run_key"],
            "status": status,
            "parent_run_id": run["parent_run_id"],
            "restart_mode": run["restart_mode"],
            "platform": run["platform"],
            "scheduling": self.config.data["runtime"]["scheduling"],
            "restart_origin": run["restart_origin"],
            "restart_reason": run["restart_reason"],
            "rerun_reason": run["rerun_reason"],
            "total_nodes": len(records),
            "successful_nodes": counts[Status.SUCCEEDED],
            "skipped_nodes": counts[Status.SKIPPED_ALREADY_SATISFIED] + counts[Status.SKIPPED_CONDITION],
            "skipped_already_satisfied": counts[Status.SKIPPED_ALREADY_SATISFIED],
            "skipped_condition": counts[Status.SKIPPED_CONDITION],
            "retried_nodes": sum(record["attempt"] > 1 for record in records.values()),
            "failed_nodes": counts[Status.FAILED],
            "timed_out_nodes": counts[Status.TIMED_OUT],
            "blocked_nodes": counts[Status.BLOCKED],
            "cancelled_nodes": counts[Status.CANCELLED],
            "pending_nodes": counts[Status.PENDING],
            "ready_nodes": counts[Status.READY],
            "running_nodes": counts[Status.RUNNING],
            "duration_seconds": duration(run["started_at"], utc_now()),
            "critical_path_duration_seconds": max(weighted_paths.values(), default=0),
            "critical_path_definition": "Longest dependency path weighted by actual attempt execution time; excludes queue/backoff.",
            "failed_node_details": [
                {"node_id": key, "status": record["status"], "error": record["error"]}
                for key, record in records.items()
                if record["status"] in BAD
            ],
            "recommended_operator_action": (
                "Stop and reconcile all workers, then release abandoned leases using the runbook."
                if uncertain
                else "Remediate failures and resume with this run ID."
                if failed
                else "Review warnings and dispatch deduplicated outbox intents using an idempotent consumer."
            ),
            "nodes": records,
        }

    def _execute_graph(self, run: dict[str, Any], request: RunRequest, guard: Lease) -> None:
        scope = run["scope"]
        nodes = self.config.nodes
        history = self.store.view(scope)
        history_nodes = history.all("node")
        previous = {
            record["node_id"]: record for record in history_nodes if record["run_id"] == run["parent_run_id"]
        }
        attempted = {record["node_id"] for record in history_nodes if record["attempt"] > 0}
        invalid = {key for key, record in previous.items() if record["status"] not in SUCCESS}
        if request.mode == Mode.FORCE_RERUN:
            invalid = set(nodes)
        elif request.mode == Mode.FORCE_RESTART:
            invalid.add(request.restart_from)
        invalid = descendants(nodes, invalid)
        settings, limits = self.config.data["runtime"], self.config.data["concurrency"]
        end = time.time() + settings["run_timeout_seconds"]
        with DispatchSession(self.executor, settings["scheduling"], limits["max_parallel"]) as dispatch:
            while True:
                dispatch.reap()
                guard.check()
                active = dispatch.active_nodes
                records = self.store.nodes(scope, run["run_id"])
                if any(record["execution_uncertain"] for record in records.values()):
                    break
                changed = False
                for key in self.plan:
                    record, node = records[key], nodes[key]
                    if key not in active and time.time() < end and self._retryable(node, record):
                        retry = node["retry"]
                        delay = min(
                            retry["max_delay_seconds"],
                            retry["delay_seconds"] * retry["backoff"] ** (record["attempt"] - 1),
                        )
                        dependencies = {dep: records[dep] for dep in node["dependencies"]}
                        parameters = self._parameters(run, node, record["attempt"] + 1, dependencies)
                        try:
                            inputs = input_fingerprint(
                                self.config, node, parameters, dependencies, self.services.artifacts
                            )
                        except WorkflowError:
                            # The child still revalidates a previously unavailable input.
                            inputs = record["input_fingerprint"]
                        records[key] = self.store.prepare(
                            scope,
                            run["run_id"],
                            key,
                            run["owner"],
                            inputs,
                            {dep: value["status"] for dep, value in dependencies.items()},
                            delay=delay,
                        )
                        changed = True
                if not active and all(record["status"] in TERMINAL for record in records.values()):
                    break

                def cancel_pending(records: dict[str, dict[str, Any]], active: set[str]) -> bool:
                    cancelled = False
                    fail_fast = settings["failure_policy"] == "fail_fast" and any(
                        record["critical"]
                        and record["status"] in BAD
                        and not self._retryable(nodes[key], record)
                        for key, record in records.items()
                        if key not in active
                    )
                    for key in self.plan:
                        record, node = records[key], nodes[key]
                        if key in active or record["status"] not in {Status.PENDING, Status.READY}:
                            continue
                        if time.time() >= end or (
                            fail_fast and node["trigger"]["type"] in {"all_success", "any_success"}
                        ):
                            records[key] = self.store.skip(
                                scope,
                                run["run_id"],
                                key,
                                run["owner"],
                                Status.CANCELLED,
                                dependencies={dep: records[dep]["status"] for dep in node["dependencies"]},
                                reason="RUN_DEADLINE" if time.time() >= end else "FAIL_FAST",
                            )
                            cancelled = True
                    return cancelled

                changed = cancel_pending(records, active) or changed
                reserved = set(active)
                groups = Counter(nodes[key]["concurrency_group"] for key in active)
                resources: Counter[str] = Counter()
                for key in active:
                    resources.update(nodes[key]["resources"])

                def fits(
                    node: dict[str, Any], reserved: set[str], groups: Counter[str], resources: Counter[str]
                ) -> bool:
                    return (
                        len(reserved) < limits["max_parallel"]
                        and groups[node["concurrency_group"]] < limits["groups"][node["concurrency_group"]]
                        and all(
                            resources[name] + demand <= limits["resources"][name]
                            for name, demand in node["resources"].items()
                        )
                    )

                def reserve(
                    key: str, reserved: set[str], groups: Counter[str], resources: Counter[str]
                ) -> None:
                    reserved.add(key)
                    groups[nodes[key]["concurrency_group"]] += 1
                    resources.update(nodes[key]["resources"])

                for key in self.plan:
                    record, node = records[key], nodes[key]
                    if (
                        key not in active
                        and record["status"] == Status.READY
                        and record["ready_at"] <= time.time()
                        and fits(node, reserved, groups, resources)
                    ):
                        reserve(key, reserved, groups, resources)

                for key in self.plan:
                    record, node = records[key], nodes[key]
                    if key in active or record["status"] != Status.PENDING:
                        continue
                    dependencies = {dep: records[dep] for dep in node["dependencies"]}
                    states = {dep: value["status"] for dep, value in dependencies.items()}
                    if (
                        active.intersection(dependencies)
                        or any(value not in TERMINAL for value in states.values())
                        or any(self._retryable(nodes[dep], value) for dep, value in dependencies.items())
                    ):
                        continue
                    prerequisite = {**node, "trigger": {"type": node["trigger"]["type"]}}
                    skipped = trigger_status(prerequisite, states, {})
                    if skipped:
                        records[key] = self.store.skip(
                            scope,
                            run["run_id"],
                            key,
                            run["owner"],
                            skipped,
                            dependencies=states,
                            reason="TRIGGER",
                        )
                        changed = True
                        continue
                    if not fits(node, reserved, groups, resources):
                        continue
                    try:
                        parameters = self._parameters(run, node, 1, dependencies)
                        skipped = trigger_status(node, states, parameters)
                        if skipped:
                            records[key] = self.store.skip(
                                scope,
                                run["run_id"],
                                key,
                                run["owner"],
                                skipped,
                                dependencies=states,
                                reason="CONDITION",
                            )
                            changed = True
                            continue
                        inputs = input_fingerprint(
                            self.config, node, parameters, dependencies, self.services.artifacts
                        )
                        if can_reuse(
                            previous.get(key),
                            self.config,
                            node,
                            request.business_run_key,
                            inputs,
                            self.services.artifacts,
                            invalidated=key in invalid,
                        ):
                            records[key] = self.store.skip(
                                scope,
                                run["run_id"],
                                key,
                                run["owner"],
                                Status.SKIPPED_ALREADY_SATISFIED,
                                dependencies=states,
                                reason="VALIDATED_PRIOR_SUCCESS",
                                prior=previous[key],
                                input_fingerprint=inputs,
                            )
                            changed = True
                            continue
                        self._reexecution_allowed(node, previous, request, attempted)
                    except WorkflowError as error:
                        planned = self.store.prepare(
                            scope,
                            run["run_id"],
                            key,
                            run["owner"],
                            fingerprint({"planning_error": error.code}),
                            states,
                        )
                        context = self._context(run, node, planned, dependencies, {}, end)
                        records[key] = self.store.finish_attempt(
                            context,
                            Status.FAILED,
                            error=safe_error(error),
                            allow_ready=True,
                        )
                        if key not in invalid:
                            invalid.update(descendants(nodes, {key}))
                        changed = True
                        continue
                    if key not in invalid:
                        invalid.update(descendants(nodes, {key}))
                    records[key] = self.store.prepare(scope, run["run_id"], key, run["owner"], inputs, states)
                    reserve(key, reserved, groups, resources)
                    changed = True

                changed = cancel_pending(records, active) or changed
                batch = []
                for key in self.plan:
                    record, node = records[key], nodes[key]
                    if key in reserved - active and record["status"] == Status.READY:
                        dependencies = {dep: records[dep] for dep in node["dependencies"]}
                        parameters = self._parameters(run, node, record["attempt"], dependencies)
                        batch.append(
                            self._context(
                                run,
                                node,
                                record,
                                dependencies,
                                parameters,
                                min(end, time.time() + node["timeout_seconds"]),
                            )
                        )
                if batch:
                    if time.time() >= end:
                        cancel_pending(records, active)
                        continue
                    guard.check()
                    dispatch.submit(batch)
                    continue
                if changed:
                    continue
                if dispatch.active_nodes:
                    future_ready = [
                        record["ready_at"]
                        for key, record in records.items()
                        if key not in active
                        and record["status"] == Status.READY
                        and record["ready_at"] > time.time()
                    ]
                    wake = min([end, *future_ready])
                    delay = wake - time.time()
                    dispatch.wait(delay if delay > 0 else settings["poll_interval_seconds"])
                else:
                    waiting = [
                        record["ready_at"] for record in records.values() if record["status"] == Status.READY
                    ]
                    if not waiting:
                        raise WorkflowError(
                            "SCHEDULER_STALLED", "No runnable nodes remain in the validated DAG."
                        )
                    dispatch.wait(max(0, min(end, min(waiting)) - time.time()))

    def run(self, request: RunRequest) -> dict[str, Any]:
        request = normalize_request(self.config, request)
        scope = fingerprint(
            [self.config.data["application_id"], self.config.data["dag"]["id"], request.business_run_key]
        )
        log_configuration(self.config, "orchestrator")
        guard = self.services.leases.acquire(
            f"run:{self.config.data['control_store']['path']}:{scope}", wait=False
        )
        run = None
        try:
            guard.check()
            self.store.clear_cache(scope)
            self.services.artifacts.clear_validation_cache()
            run = self.store.begin_run(scope, request, self.config, self.plan)
            self.last_run = run
            self._execute_graph(run, request, guard)
            summary = self._summary(run)
            self.store.finish_run(scope, run["run_id"], run["owner"], summary)
            if summary["status"] == "RECOVERY_REQUIRED":
                guard.retain()
            return summary
        except BaseException:
            if run is not None:
                guard.retain()
                # Preserve the durable RUNNING record when the store itself is unavailable.
            raise
        finally:
            guard.release()
