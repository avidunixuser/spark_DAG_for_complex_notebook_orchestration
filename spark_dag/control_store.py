from __future__ import annotations

import copy
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from typing import Any, Protocol, TypeVar

from .config_loader import Configuration
from .leases import LeaseFactory
from .model import (
    SUCCESS,
    TERMINAL,
    AttemptContext,
    Category,
    CollisionError,
    Mode,
    RunRequest,
    Status,
    WorkflowError,
    canonical,
    duration,
    fingerprint,
    utc_now,
)
from .telemetry import EventLogger

T = TypeVar("T")


class Transaction:
    def __init__(self, scope: str, events: list[dict[str, Any]]):
        self.scope = scope
        self.events = events
        self.pending: list[dict[str, Any]] = []
        self.latest: dict[tuple[str, str], dict[str, Any]] = {}
        sequence = 0
        for event in events:
            if event["sequence"] != sequence + 1:
                raise WorkflowError("AUDIT_CORRUPTION", "Control-event sequence is duplicated or incomplete.")
            sequence = event["sequence"]
            self.latest[(event["entity_type"], event["entity_id"])] = event["payload"]
        self.sequence = sequence

    def get(self, kind: str, key: str) -> dict[str, Any] | None:
        return copy.deepcopy(self.latest.get((kind, key)))

    def all(self, kind: str) -> list[dict[str, Any]]:
        return [copy.deepcopy(value) for (entity, _), value in self.latest.items() if entity == kind]

    def put(self, kind: str, key: str, payload: dict[str, Any], event_type: str) -> dict[str, Any]:
        self.sequence += 1
        value = copy.deepcopy(payload)
        self.latest[(kind, key)] = value
        event = {
            "scope": self.scope,
            "sequence": self.sequence,
            "entity_type": kind,
            "entity_id": key,
            "run_id": value.get("run_id", ""),
            "node_id": value.get("node_id", ""),
            "event_type": event_type,
            "timestamp": utc_now(),
            "payload": value,
        }
        self.pending.append(event)
        return copy.deepcopy(value)


class EventStore(Protocol):
    def transaction(self, scope: str, action: Callable[[Transaction], T]) -> T: ...
    def events(self, scope: str) -> list[dict[str, Any]]: ...
    def clear_cache(self, scope: str) -> None: ...


class SQLiteEvents:
    def __init__(self, path: Path, logger: EventLogger):
        self.path = path
        self.logger = logger
        path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                """CREATE TABLE IF NOT EXISTS control_events (
                    scope TEXT NOT NULL, sequence INTEGER NOT NULL, entity_type TEXT NOT NULL,
                    entity_id TEXT NOT NULL, run_id TEXT NOT NULL, node_id TEXT NOT NULL,
                    event_type TEXT NOT NULL, timestamp TEXT NOT NULL, payload TEXT NOT NULL,
                    PRIMARY KEY(scope, sequence))"""
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @staticmethod
    def _read(connection: sqlite3.Connection, scope: str) -> list[dict[str, Any]]:
        import json

        result = []
        for row in connection.execute(
            "SELECT * FROM control_events WHERE scope=? ORDER BY sequence", (scope,)
        ):
            event = dict(row)
            event["payload"] = json.loads(event["payload"])
            result.append(event)
        return result

    def events(self, scope: str) -> list[dict[str, Any]]:
        with closing(self._connect()) as connection:
            return self._read(connection, scope)

    def clear_cache(self, scope: str) -> None:
        pass

    def transaction(self, scope: str, action: Callable[[Transaction], T]) -> T:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            tx = Transaction(scope, self._read(connection, scope))
            result = action(tx)
            connection.executemany(
                "INSERT INTO control_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        event["scope"],
                        event["sequence"],
                        event["entity_type"],
                        event["entity_id"],
                        event["run_id"],
                        event["node_id"],
                        event["event_type"],
                        event["timestamp"],
                        canonical(event["payload"]),
                    )
                    for event in tx.pending
                ],
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
        for event in tx.pending:
            self.logger.emit(event)
        return result


DELTA_EVENT_SCHEMA = (
    "scope STRING, sequence LONG, entity_type STRING, entity_id STRING, run_id STRING, "
    "node_id STRING, event_type STRING, timestamp STRING, payload STRING"
)


class DeltaEvents:
    """Append state snapshots and audit events in the same Delta commit, under an external mutex."""

    def __init__(self, spark: Any, path: str, leases: LeaseFactory, logger: EventLogger):
        from delta.tables import DeltaTable

        self.spark, self.path, self.leases, self.logger = spark, path, leases, logger
        self._event_cache: dict[str, list[dict[str, Any]]] = {}
        self._cache_lock = threading.RLock()
        if not DeltaTable.isDeltaTable(spark, path):
            guard = leases.acquire(f"initialize:{path}")
            writing = False
            try:
                if not DeltaTable.isDeltaTable(spark, path):
                    guard.check()
                    writing = True
                    spark.createDataFrame([], DELTA_EVENT_SCHEMA).write.format("delta").partitionBy(
                        "scope"
                    ).mode("errorifexists").save(path)
            except BaseException:
                if writing:
                    guard.retain()
                raise
            finally:
                guard.release()

    def clear_cache(self, scope: str) -> None:
        with self._cache_lock:
            self._event_cache.pop(scope, None)

    def _read_after(self, scope: str, sequence: int) -> list[dict[str, Any]]:
        import json

        from pyspark.sql import functions as F

        frame = self.spark.read.format("delta").load(self.path).where(F.col("scope") == scope)
        if sequence:
            frame = frame.where(F.col("sequence") > sequence)
        rows = frame.orderBy("sequence").collect()
        result = []
        for row in rows:
            event = row.asDict()
            event["payload"] = json.loads(event["payload"])
            result.append(event)
        return result

    def _remember(self, scope: str, events: list[dict[str, Any]]) -> None:
        with self._cache_lock:
            cached = self._event_cache.setdefault(scope, [])
            for event in events:
                sequence = event["sequence"]
                if not isinstance(sequence, int) or sequence < 1:
                    raise WorkflowError("AUDIT_CORRUPTION", "The control-event sequence is invalid.")
                if sequence <= len(cached):
                    if cached[sequence - 1] != event:
                        raise WorkflowError("AUDIT_CORRUPTION", "A committed control event changed.")
                elif sequence == len(cached) + 1:
                    cached.append(copy.deepcopy(event))
                else:
                    raise WorkflowError("AUDIT_CORRUPTION", "The control-event sequence has a gap.")

    def events(self, scope: str) -> list[dict[str, Any]]:
        with self._cache_lock:
            sequence = len(self._event_cache.get(scope, []))
            self._remember(scope, self._read_after(scope, sequence))
            return copy.deepcopy(self._event_cache.get(scope, []))

    def transaction(self, scope: str, action: Callable[[Transaction], T]) -> T:
        guard = self.leases.acquire(f"control:{self.path}:{scope}")
        writing = False
        try:
            tx = Transaction(scope, self.events(scope))
            result = action(tx)
            if tx.pending:
                records = [{**event, "payload": canonical(event["payload"])} for event in tx.pending]
                frame = self.spark.createDataFrame(records, DELTA_EVENT_SCHEMA)
                guard.check()
                writing = True
                frame.write.format("delta").mode("append").save(self.path)
                self._remember(scope, tx.pending)
        except BaseException:
            if writing:
                # A network error may follow a committed write. Never retry a blind append.
                guard.retain()
            raise
        finally:
            guard.release()
        for event in tx.pending:
            self.logger.emit(event)
        return result


class ControlStore:
    def __init__(self, events: EventStore):
        self.backend = events

    def view(self, scope: str) -> Transaction:
        return Transaction(scope, self.backend.events(scope))

    def clear_cache(self, scope: str) -> None:
        self.backend.clear_cache(scope)

    @staticmethod
    def node_key(run_id: str, node_id: str) -> str:
        return f"{run_id}:{node_id}"

    def node(self, scope: str, run_id: str, node_id: str) -> dict[str, Any]:
        result = self.view(scope).get("node", self.node_key(run_id, node_id))
        if result is None:
            raise WorkflowError("UNKNOWN_NODE", "No control record exists for this node.")
        return result

    def run(self, scope: str, run_id: str) -> dict[str, Any]:
        result = self.view(scope).get("run", run_id)
        if result is None:
            raise WorkflowError("UNKNOWN_RUN", "No control record exists for this run.")
        return result

    def nodes(self, scope: str, run_id: str) -> dict[str, dict[str, Any]]:
        return {node["node_id"]: node for node in self.view(scope).all("node") if node["run_id"] == run_id}

    def begin_run(
        self, scope: str, request: RunRequest, config: Configuration, plan: list[str]
    ) -> dict[str, Any]:
        def begin(tx: Transaction) -> dict[str, Any]:
            runs = sorted(tx.all("run"), key=lambda run: run["attempt"])
            if any(run["status"] in {"RUNNING", "RECOVERY_REQUIRED"} for run in runs):
                raise CollisionError()
            if request.mode == Mode.NORMAL and runs:
                raise WorkflowError(
                    "RUN_EXISTS", "This business key has history; select resume or force rerun."
                )
            parent = tx.get("run", request.parent_run_id) if request.parent_run_id else None
            if request.mode in {Mode.RESUME, Mode.FORCE_RESTART}:
                if parent is None or not runs or parent["run_id"] != runs[-1]["run_id"]:
                    raise WorkflowError(
                        "INVALID_PARENT", "Resume/restart requires the latest run of this business key."
                    )
                if request.mode == Mode.RESUME and parent["status"] != "FAILED":
                    raise WorkflowError("INVALID_PARENT", "Resume requires a failed or reconciled run.")
            if request.mode == Mode.FORCE_RERUN and runs:
                parent = runs[-1]
            run_id, now = str(uuid.uuid4()), utc_now()
            common = {
                "application_id": config.data["application_id"],
                "dag_id": config.data["dag"]["id"],
                "dag_version": config.data["dag"]["version"],
                "run_id": run_id,
                "business_run_key": request.business_run_key,
                "parent_run_id": parent["run_id"] if parent else None,
                "configuration_version": config.data["configuration_version"],
                "configuration_fingerprint": config.fingerprint,
                "code_version": config.data["code_version"],
                "code_fingerprint": config.code_fingerprint,
                "restart_mode": str(request.mode),
                "platform": config.data["runtime"]["platform"],
                "restart_reason": request.reason if request.mode in {Mode.RESUME, Mode.FORCE_RESTART} else "",
                "rerun_reason": request.reason if request.mode == Mode.FORCE_RERUN else "",
                "restart_origin": request.restart_from,
            }
            run = {
                **common,
                "scope": scope,
                "status": "RUNNING",
                "owner": str(uuid.uuid4()),
                "attempt": len(runs) + 1,
                "started_at": now,
                "ended_at": None,
                "duration_seconds": None,
                "plan": plan,
                "approved_nodes": list(request.approved_nodes),
                "summary": None,
            }
            tx.put("run", run_id, run, "RUN_STARTED")
            for node_id in plan:
                node = config.nodes[node_id]
                tx.put(
                    "node",
                    self.node_key(run_id, node_id),
                    {
                        **common,
                        "node_id": node_id,
                        "notebook": node["notebook"],
                        "original_stage": node["original_stage"],
                        "status": Status.PENDING,
                        "previous_status": None,
                        "attempt": 0,
                        "retry_count": 0,
                        "claim_id": None,
                        "dependency_status": {},
                        "started_at": None,
                        "ended_at": None,
                        "duration_seconds": 0.0,
                        "input_fingerprint": None,
                        "outputs": {},
                        "checkpoint": None,
                        "metrics": {},
                        "row_counts": {},
                        "warning_count": 0,
                        "reject_count": 0,
                        "retryable": False,
                        "error": None,
                        "execution_uncertain": False,
                        "critical": node["critical"],
                        "ready_at": 0.0,
                        "remote_run_id": None,
                    },
                    "NODE_CREATED",
                )
            return run

        return self.backend.transaction(scope, begin)

    @staticmethod
    def _running(tx: Transaction, run_id: str, owner: str | None = None) -> dict[str, Any]:
        run = tx.get("run", run_id)
        if run is None or run["status"] != "RUNNING" or (owner and run["owner"] != owner):
            raise CollisionError()
        return run

    def prepare(
        self,
        scope: str,
        run_id: str,
        node_id: str,
        owner: str,
        input_fingerprint: str,
        dependencies: dict[str, str],
        *,
        delay: float = 0,
    ) -> dict[str, Any]:
        def change(tx: Transaction) -> dict[str, Any]:
            self._running(tx, run_id, owner)
            key = self.node_key(run_id, node_id)
            node = tx.get("node", key)
            if node is None or node["status"] not in {Status.PENDING, Status.FAILED, Status.TIMED_OUT}:
                raise CollisionError()
            if node["execution_uncertain"]:
                raise WorkflowError("RECONCILIATION_REQUIRED", "An uncertain attempt cannot be retried.")
            node.update(
                previous_status=node["status"],
                status=Status.READY,
                attempt=node["attempt"] + 1,
                claim_id=str(uuid.uuid4()),
                input_fingerprint=input_fingerprint,
                dependency_status=dependencies,
                ready_at=time.time() + delay,
                started_at=None,
                ended_at=None,
                duration_seconds=0.0,
                outputs={},
                checkpoint=None,
                retryable=False,
                error=None,
                remote_run_id=None,
            )
            node["retry_count"] = node["attempt"] - 1
            return tx.put("node", key, node, "RETRY_SCHEDULED" if node["retry_count"] else "NODE_READY")

        return self.backend.transaction(scope, change)

    def claim(self, context: AttemptContext) -> dict[str, Any]:
        def change(tx: Transaction) -> dict[str, Any]:
            self._running(tx, context.run_id)
            key = self.node_key(context.run_id, context.node_id)
            node = tx.get("node", key)
            if (
                node is None
                or node["status"] != Status.READY
                or node["claim_id"] != context.claim_id
                or node["attempt"] != context.attempt
                or node["input_fingerprint"] != context.input_fingerprint
                or node["configuration_fingerprint"] != context.configuration_fingerprint
                or node["code_fingerprint"] != context.code_fingerprint
            ):
                raise CollisionError()
            node.update(
                previous_status=Status.READY,
                status=Status.RUNNING,
                started_at=utc_now(),
                deadline=context.deadline,
            )
            return tx.put("node", key, node, "ATTEMPT_CLAIMED")

        return self.backend.transaction(context.scope, change)

    def finish_attempt(
        self,
        context: AttemptContext,
        status: str,
        *,
        outputs: dict[str, Any] | None = None,
        checkpoint: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
        metrics: dict[str, Any] | None = None,
        row_counts: dict[str, int] | None = None,
        warning_count: int = 0,
        reject_count: int = 0,
        uncertain: bool = False,
        allow_ready: bool = False,
    ) -> dict[str, Any]:
        def change(tx: Transaction) -> dict[str, Any]:
            self._running(tx, context.run_id)
            key = self.node_key(context.run_id, context.node_id)
            node = tx.get("node", key)
            if node is None or node["claim_id"] != context.claim_id or node["attempt"] != context.attempt:
                raise CollisionError()
            if node["status"] in TERMINAL:
                return node
            permitted = {Status.RUNNING, Status.READY} if allow_ready else {Status.RUNNING}
            if node["status"] not in permitted:
                raise CollisionError()
            final_status = status
            final_error = error
            if status == Status.SUCCEEDED:
                if (
                    not outputs
                    or not checkpoint
                    or checkpoint.get("outputs_fingerprint") != fingerprint(outputs)
                ):
                    raise WorkflowError(
                        "MISSING_CHECKPOINT", "Success requires validated outputs and a final checkpoint."
                    )
                if time.time() > context.deadline:
                    final_status = Status.TIMED_OUT
                    final_error = {
                        "category": Category.TIMEOUT,
                        "code": "DEADLINE_EXCEEDED",
                        "message": "Work ended after the attempt deadline.",
                        "retryable": True,
                    }
            now = utc_now()
            node.update(
                previous_status=node["status"],
                status=final_status,
                ended_at=now,
                duration_seconds=duration(node["started_at"], now) if node["started_at"] else 0.0,
                outputs=outputs if final_status == Status.SUCCEEDED else {},
                checkpoint=checkpoint if final_status == Status.SUCCEEDED else None,
                error=final_error,
                retryable=bool(final_error and final_error["retryable"]),
                metrics=metrics or {},
                row_counts=row_counts or {},
                warning_count=warning_count,
                reject_count=reject_count,
                execution_uncertain=uncertain,
            )
            return tx.put("node", key, node, "ATTEMPT_FINISHED")

        return self.backend.transaction(context.scope, change)

    def skip(
        self,
        scope: str,
        run_id: str,
        node_id: str,
        owner: str,
        status: str,
        *,
        dependencies: dict[str, str],
        reason: str,
        prior: dict[str, Any] | None = None,
        input_fingerprint: str | None = None,
    ) -> dict[str, Any]:
        def change(tx: Transaction) -> dict[str, Any]:
            self._running(tx, run_id, owner)
            key = self.node_key(run_id, node_id)
            node = tx.get("node", key)
            permitted = {Status.PENDING, Status.READY} if status == Status.CANCELLED else {Status.PENDING}
            if node is None or node["status"] not in permitted:
                raise CollisionError()
            node.update(
                previous_status=node["status"],
                status=status,
                ended_at=utc_now(),
                dependency_status=dependencies,
                skip_reason=reason,
                input_fingerprint=input_fingerprint,
            )
            if status == Status.SKIPPED_ALREADY_SATISFIED:
                if not prior or prior["status"] not in SUCCESS:
                    raise WorkflowError("UNPROVEN_SKIP", "Reuse requires a validated prior success.")
                for field in (
                    "outputs",
                    "checkpoint",
                    "metrics",
                    "row_counts",
                    "warning_count",
                    "reject_count",
                ):
                    node[field] = copy.deepcopy(prior[field])
                node["reused_from_run_id"] = prior.get("reused_from_run_id", prior["run_id"])
            return tx.put(
                "node",
                key,
                node,
                "NODE_SKIPPED" if status not in {Status.BLOCKED, Status.CANCELLED} else "NODE_BLOCKED",
            )

        return self.backend.transaction(scope, change)

    def record_remote(self, context: AttemptContext, remote_run_id: int) -> None:
        def change(tx: Transaction) -> None:
            self._running(tx, context.run_id)
            key = self.node_key(context.run_id, context.node_id)
            node = tx.get("node", key)
            if node is None or node["claim_id"] != context.claim_id:
                raise CollisionError()
            node["remote_run_id"] = remote_run_id
            tx.put("node", key, node, "REMOTE_RUN_SUBMITTED")

        self.backend.transaction(context.scope, change)

    def put_outbox(
        self, context: AttemptContext, payload_digest: str, reference: dict[str, Any]
    ) -> dict[str, Any]:
        effect_id = fingerprint({"scope": context.scope, "node": context.node_id, "effect": "delivery"})

        def change(tx: Transaction) -> dict[str, Any]:
            self._running(tx, context.run_id)
            node = tx.get("node", self.node_key(context.run_id, context.node_id))
            if node is None or node["status"] != Status.RUNNING or node["claim_id"] != context.claim_id:
                raise CollisionError()
            existing = tx.get("outbox", effect_id)
            if existing and existing["payload_digest"] != payload_digest:
                raise WorkflowError(
                    "SIDE_EFFECT_CONFLICT",
                    "A business-key delivery already exists for different content.",
                    Category.BUSINESS,
                )
            if existing is None:
                tx.put(
                    "outbox",
                    effect_id,
                    {
                        "effect_id": effect_id,
                        "scope": context.scope,
                        "run_id": context.run_id,
                        "node_id": context.node_id,
                        "payload_digest": payload_digest,
                        "output_reference": reference,
                        "status": "PENDING_DELIVERY",
                        "created_at": utc_now(),
                    },
                    "OUTBOX_COMMITTED",
                )
            elif existing["status"] == "PENDING_DELIVERY" and existing["output_reference"] != reference:
                existing.update(output_reference=reference, last_reference_run_id=context.run_id)
                tx.put("outbox", effect_id, existing, "OUTBOX_REFERENCE_REFRESHED")
            return {
                "backend": "outbox",
                "scope": context.scope,
                "effect_id": effect_id,
                "digest": payload_digest,
                "row_count": 1,
            }

        return self.backend.transaction(context.scope, change)

    def finish_run(self, scope: str, run_id: str, owner: str, summary: dict[str, Any]) -> dict[str, Any]:
        def change(tx: Transaction) -> dict[str, Any]:
            run = self._running(tx, run_id, owner)
            nodes = [node for node in tx.all("node") if node["run_id"] == run_id]
            if summary["status"] != "RECOVERY_REQUIRED" and any(
                node["status"] not in TERMINAL for node in nodes
            ):
                raise WorkflowError("UNFINISHED_RUN", "Cannot close a run with nonterminal nodes.")
            run.update(
                status=summary["status"],
                ended_at=utc_now(),
                duration_seconds=summary["duration_seconds"],
                summary=summary,
            )
            return tx.put("run", run_id, run, "RUN_FINISHED")

        return self.backend.transaction(scope, change)

    def reconcile(self, scope: str, run_id: str, *, workers_stopped: bool, reason: str) -> None:
        if not workers_stopped or not reason:
            raise WorkflowError(
                "RECONCILIATION_REQUIRED", "Confirm every worker has stopped and provide an incident ID."
            )

        def change(tx: Transaction) -> None:
            run = tx.get("run", run_id)
            if run is None or run["status"] not in {"RUNNING", "RECOVERY_REQUIRED"}:
                raise WorkflowError(
                    "INVALID_RECONCILIATION", "Only an abandoned or uncertain run can be reconciled."
                )
            for node in tx.all("node"):
                if node["run_id"] == run_id and (
                    node["status"] not in TERMINAL or node["execution_uncertain"]
                ):
                    node.update(
                        previous_status=node["status"],
                        status=Status.CANCELLED,
                        execution_uncertain=False,
                        ended_at=utc_now(),
                        retryable=False,
                        reconciliation_reason=reason,
                    )
                    tx.put("node", self.node_key(run_id, node["node_id"]), node, "ATTEMPT_RECONCILED")
            run.update(status="FAILED", ended_at=utc_now(), reconciliation_reason=reason)
            tx.put("run", run_id, run, "RUN_RECONCILED")

        self.backend.transaction(scope, change)
