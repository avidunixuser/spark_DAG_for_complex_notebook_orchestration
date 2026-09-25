from __future__ import annotations

import json
import math
import subprocess
import sys
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from pathlib import PurePosixPath
from typing import Any, Protocol

from .config_loader import Configuration
from .control_store import ControlStore
from .model import (
    TERMINAL,
    AttemptContext,
    Category,
    Status,
    TaskFailure,
    WorkflowError,
    canonical,
    fingerprint,
    safe_error,
)


class Executor(Protocol):
    def execute_batch(self, contexts: list[AttemptContext]) -> None: ...


class DispatchSession:
    def __init__(self, executor: Executor, scheduling: str, concurrency: int):
        self.executor = executor
        self.scheduling = scheduling
        self.concurrency = concurrency
        self.pool: ThreadPoolExecutor | None = None
        self.pending: dict[Future[None], AttemptContext] = {}

    def __enter__(self) -> DispatchSession:
        if self.scheduling not in {"eager", "barrier"}:
            raise WorkflowError("INVALID_SCHEDULING", "Unsupported notebook scheduling mode.")
        if self.scheduling == "eager":
            self.pool = ThreadPoolExecutor(max_workers=self.concurrency)
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        try:
            if exc_type is None:
                for future in list(self.pending):
                    future.result()
        finally:
            if self.pool is not None:
                # A Future cancellation is not proof that its notebook stopped.
                self.pool.shutdown(wait=True, cancel_futures=False)
            self.pending.clear()

    @property
    def active_nodes(self) -> set[str]:
        return {context.node_id for context in self.pending.values()}

    def submit(self, contexts: list[AttemptContext]) -> None:
        if not contexts or len(self.pending) + len(contexts) > self.concurrency:
            raise WorkflowError("INVALID_DISPATCH", "Dispatch exceeds the configured concurrency budget.")
        if self.active_nodes.intersection(context.node_id for context in contexts):
            raise WorkflowError("DUPLICATE_DISPATCH", "An attempt for this node is already active.")
        if self.pool is None:
            self.executor.execute_batch(contexts)
        else:
            for context in contexts:
                self.pending[self.pool.submit(self.executor.execute_batch, [context])] = context

    def reap(self, timeout: float = 0) -> None:
        if not self.pending:
            return
        completed, _ = wait(self.pending, timeout=timeout, return_when=FIRST_COMPLETED)
        for future in sorted(completed, key=lambda entry: self.pending[entry].node_id):
            future.result()
            del self.pending[future]

    def wait(self, timeout: float) -> None:
        if self.pending:
            self.reap(timeout)
        elif timeout > 0:
            time.sleep(timeout)


class ExecutorBase:
    def __init__(self, config: Configuration, store: ControlStore):
        self.config, self.store = config, store

    def fail(self, context: AttemptContext, error: WorkflowError, *, uncertain: bool = False) -> None:
        self.store.finish_attempt(
            context,
            Status.TIMED_OUT if error.category == Category.TIMEOUT else Status.FAILED,
            error=safe_error(error),
            uncertain=uncertain,
            allow_ready=True,
        )

    def terminal(self, context: AttemptContext) -> bool:
        return self.store.node(context.scope, context.run_id, context.node_id)["status"] in TERMINAL


class LocalExecutor(ExecutorBase):
    def _execute(self, context: AttemptContext) -> None:
        process = subprocess.Popen(
            [sys.executable, "-m", "spark_dag.worker"],
            cwd=context.deployment_dir,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        try:
            stdout, _ = process.communicate(
                canonical(context.as_dict()),
                timeout=max(0.01, context.deadline - time.time()),
            )
        except subprocess.TimeoutExpired:
            # Local handlers cannot spawn detached workers. Kill and reap the actual process,
            # rather than cancelling a Future whose underlying work would continue.
            process.kill()
            process.communicate()
            self.fail(
                context,
                TaskFailure(
                    "DEADLINE_EXCEEDED", "The worker was terminated.", Category.TIMEOUT, retryable=True
                ),
            )
            return
        if self.terminal(context):
            return
        envelope = None
        for line in reversed(stdout.splitlines()):
            try:
                candidate = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(candidate, dict) and candidate.get("contract_version") == 1:
                envelope = candidate
                break
        if envelope and envelope.get("error"):
            error = envelope["error"]
            self.fail(
                context,
                TaskFailure(
                    error["code"],
                    "The child rejected the attempt.",
                    Category(error["category"]),
                    retryable=error["retryable"],
                ),
            )
        else:
            self.fail(
                context,
                TaskFailure(
                    "WORKER_EXITED",
                    "The local worker ended without a final checkpoint.",
                    Category.INFRASTRUCTURE,
                    retryable=True,
                ),
            )

    def execute_batch(self, contexts: list[AttemptContext]) -> None:
        if len(contexts) == 1:
            self._execute(contexts[0])
            return
        with ThreadPoolExecutor(max_workers=len(contexts)) as pool:
            list(pool.map(self._execute, contexts))


class DatabricksExecutor(ExecutorBase):
    TERMINAL_LIFECYCLES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}

    def __init__(self, config: Configuration, store: ControlStore, *, api_client: Any = None):
        super().__init__(config, store)
        if api_client is None:
            from databricks.sdk import WorkspaceClient

            api_client = WorkspaceClient().api_client
        self.api = api_client

    def _get(self, run_id: int) -> dict[str, Any]:
        return self.api.do("GET", "/api/2.1/jobs/runs/get", query={"run_id": run_id})

    def _stopped(self, context: AttemptContext, response: dict[str, Any], *, timed_out: bool = False) -> None:
        if self.terminal(context):
            return
        result = response.get("state", {}).get("result_state")
        if timed_out or result == "TIMEDOUT":
            self.fail(
                context,
                TaskFailure(
                    "REMOTE_TIMEOUT", "Databricks confirmed termination.", Category.TIMEOUT, retryable=True
                ),
            )
        else:
            self.fail(
                context,
                TaskFailure(
                    "REMOTE_NO_CHECKPOINT",
                    "The remote task ended without the child contract.",
                    Category.INFRASTRUCTURE,
                    retryable=False,
                ),
            )

    def _execute(self, context: AttemptContext) -> None:
        settings = self.config.data["runtime"]
        cluster = settings["databricks"]["existing_cluster_id"]
        if not cluster:
            self.fail(
                context,
                WorkflowError("MISSING_CLUSTER", "An existing Databricks cluster must be configured."),
            )
            return
        definition = self.config.data["notebooks"][self.config.nodes[context.node_id]["notebook"]]
        notebook_name = PurePosixPath(definition["path"]).stem
        path = f"{self.config.data['notebook_base_path'].rstrip('/')}/{notebook_name}"
        body = {
            "run_name": f"dag-{context.node_id}",
            "idempotency_token": fingerprint([context.run_id, context.node_id, context.attempt]),
            "tasks": [
                {
                    "task_key": context.node_id,
                    "existing_cluster_id": cluster,
                    "notebook_task": {
                        "notebook_path": path,
                        "base_parameters": {"context_json": canonical(context.as_dict())},
                    },
                    "timeout_seconds": max(1, math.ceil(context.deadline - time.time())),
                    "max_retries": 0,
                }
            ],
        }
        try:
            submitted = self.api.do("POST", "/api/2.1/jobs/runs/submit", body=body)
            remote_id = int(submitted["run_id"])
            self.store.record_remote(context, remote_id)
            while time.time() < context.deadline:
                response = self._get(remote_id)
                if response.get("state", {}).get("life_cycle_state") in self.TERMINAL_LIFECYCLES:
                    self._stopped(context, response)
                    return
                time.sleep(min(settings["poll_interval_seconds"], max(0, context.deadline - time.time())))
            self.api.do("POST", "/api/2.1/jobs/runs/cancel", body={"run_id": remote_id})
            cancellation_deadline = time.monotonic() + settings["cancel_grace_seconds"]
            while time.monotonic() < cancellation_deadline:
                response = self._get(remote_id)
                if response.get("state", {}).get("life_cycle_state") in self.TERMINAL_LIFECYCLES:
                    self._stopped(context, response, timed_out=True)
                    return
                time.sleep(
                    min(settings["poll_interval_seconds"], max(0, cancellation_deadline - time.monotonic()))
                )
            self.fail(
                context,
                TaskFailure(
                    "CANCELLATION_UNCONFIRMED",
                    "Remote termination requires operator reconciliation.",
                    Category.TIMEOUT,
                ),
                uncertain=True,
            )
        except Exception:
            self.fail(
                context,
                TaskFailure(
                    "REMOTE_STATE_UNKNOWN",
                    "The remote run may still be active; reconcile it before recovery.",
                    Category.INFRASTRUCTURE,
                ),
                uncertain=True,
            )

    def execute_batch(self, contexts: list[AttemptContext]) -> None:
        if len(contexts) == 1:
            self._execute(contexts[0])
            return
        with ThreadPoolExecutor(max_workers=len(contexts)) as pool:
            list(pool.map(self._execute, contexts))


class FabricExecutor(ExecutorBase):
    def __init__(
        self,
        config: Configuration,
        store: ControlStore,
        notebookutils: Any,
        *,
        partial_failure_type: type[Exception] | None = None,
    ):
        super().__init__(config, store)
        self.notebooks = notebookutils.notebook
        if partial_failure_type is None:
            from notebookutils.common.exceptions import RunMultipleFailedException

            partial_failure_type = RunMultipleFailedException
        self.partial_failure_type = partial_failure_type

    def execute_batch(self, contexts: list[AttemptContext]) -> None:
        activities = []
        for context in contexts:
            definition = self.config.data["notebooks"][self.config.nodes[context.node_id]["notebook"]]
            activity = {
                "name": context.node_id,
                "path": definition["fabric_name"],
                "timeoutPerCellInSeconds": max(1, math.ceil(context.deadline - time.time())),
                "retry": 0,
                "args": {"context_json": canonical(context.as_dict())},
            }
            workspace = self.config.data["runtime"]["fabric"]["workspace"]
            if workspace:
                activity["workspace"] = workspace
            activities.append(activity)
        dag = {
            "activities": activities,
            "concurrency": len(activities),
            "timeoutInSeconds": max(
                1, math.ceil(max(context.deadline for context in contexts) - time.time())
            ),
        }
        try:
            self.notebooks.validateDAG(dag)
            try:
                results = self.notebooks.runMultiple(dag, {"displayDAGViaGraphviz": False})
            except self.partial_failure_type as error:
                results = error.result
        except Exception:
            for context in contexts:
                self.fail(
                    context,
                    TaskFailure(
                        "FABRIC_STATE_UNKNOWN",
                        "Fabric did not confirm each child's final state.",
                        Category.TIMEOUT if time.time() >= context.deadline else Category.INFRASTRUCTURE,
                    ),
                    uncertain=True,
                )
            return
        for context in contexts:
            if self.terminal(context):
                continue
            result = results.get(context.node_id, {})
            error_code = "FABRIC_NO_CHECKPOINT" if result else "FABRIC_RESULT_MISSING"
            # A native timeout/exception is not proof that Spark side effects stopped.
            self.fail(
                context,
                TaskFailure(
                    error_code,
                    "Reconcile the Fabric session before rerunning unacknowledged work.",
                    Category.TIMEOUT if time.time() >= context.deadline else Category.INFRASTRUCTURE,
                ),
                uncertain=True,
            )
