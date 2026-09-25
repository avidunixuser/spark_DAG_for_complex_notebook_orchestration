from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any


class Status(StrEnum):
    PENDING = "PENDING"
    READY = "READY"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    SKIPPED_ALREADY_SATISFIED = "SKIPPED_ALREADY_SATISFIED"
    SKIPPED_CONDITION = "SKIPPED_CONDITION"
    BLOCKED = "BLOCKED"
    CANCELLED = "CANCELLED"


class Mode(StrEnum):
    NORMAL = "normal"
    RESUME = "resume"
    FORCE_RESTART = "force_restart"
    FORCE_RERUN = "force_rerun"


class Category(StrEnum):
    BUSINESS = "BUSINESS"
    INFRASTRUCTURE = "INFRASTRUCTURE"
    TIMEOUT = "TIMEOUT"
    CONFIGURATION = "CONFIGURATION"
    DATA_QUALITY = "DATA_QUALITY"
    DEPENDENCY = "DEPENDENCY"
    RETRYABLE_APPLICATION = "RETRYABLE_APPLICATION"
    NON_RETRYABLE_APPLICATION = "NON_RETRYABLE_APPLICATION"


SUCCESS = {Status.SUCCEEDED, Status.SKIPPED_ALREADY_SATISFIED}
BAD = {Status.FAILED, Status.TIMED_OUT, Status.BLOCKED, Status.CANCELLED}
TERMINAL = SUCCESS | BAD | {Status.SKIPPED_CONDITION}
SAFE_STRATEGIES = {"natural", "overwrite", "merge", "deduplicate", "checkpoint"}


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def fingerprint(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def duration(start: str, end: str) -> float:
    return max(0.0, (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds())


class WorkflowError(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        category: Category = Category.CONFIGURATION,
        *,
        retryable: bool = False,
    ):
        super().__init__(message)
        self.code = code
        self.category = category
        self.retryable = retryable


class CollisionError(WorkflowError):
    def __init__(self):
        super().__init__("CLAIM_COLLISION", "Work is already owned; do not start a second orchestrator.")


class TaskFailure(WorkflowError):
    pass


def safe_error(error: BaseException) -> dict[str, Any]:
    # Exception messages can contain credentials, SQL, source rows, or personal data.
    if isinstance(error, WorkflowError):
        return {
            "category": str(error.category),
            "code": error.code if error.code.isidentifier() else "APPLICATION_ERROR",
            "message": (
                str(error)
                if type(error) in {WorkflowError, CollisionError}
                else f"{error.category} failure; consult the runbook using the error code."
            ),
            "retryable": error.retryable,
        }
    return {
        "category": str(Category.INFRASTRUCTURE),
        "code": "UNEXPECTED_ERROR",
        "message": "Unexpected execution failure; inspect access-controlled platform diagnostics.",
        "retryable": False,
    }


@dataclass(frozen=True)
class RunRequest:
    business_run_key: str
    mode: str = Mode.NORMAL
    parent_run_id: str | None = None
    restart_from: str | None = None
    reason: str = ""
    approved_nodes: tuple[str, ...] = ()


@dataclass(frozen=True)
class AttemptContext:
    run_id: str
    business_run_key: str
    node_id: str
    attempt: int
    restart_mode: str
    config_path: str
    deployment_dir: str
    environment: str
    scope: str
    claim_id: str
    input_fingerprint: str
    configuration_fingerprint: str
    code_fingerprint: str
    deadline: float
    parent_context: dict[str, Any]
    upstream_outputs: dict[str, dict[str, Any]]
    dependency_status: dict[str, str]
    parameters: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> AttemptContext:
        try:
            context = cls(**value)
        except (TypeError, ValueError):
            raise WorkflowError("INVALID_CONTEXT", "The child attempt context is incomplete.") from None
        if (
            not isinstance(context.attempt, int)
            or isinstance(context.attempt, bool)
            or context.attempt < 1
            or not all(
                isinstance(v, str) and v
                for v in (context.run_id, context.node_id, context.claim_id, context.scope)
            )
            or not isinstance(context.deadline, (int, float))
            or context.restart_mode not in set(Mode)
            or not isinstance(context.parameters, dict)
            or not isinstance(context.upstream_outputs, dict)
            or not isinstance(context.dependency_status, dict)
            or not isinstance(context.parent_context, dict)
        ):
            raise WorkflowError("INVALID_CONTEXT", "The child attempt context is invalid.")
        return context


@dataclass
class ProcessingResult:
    outputs: dict[str, dict[str, Any]]
    metrics: dict[str, int | float] = field(default_factory=dict)
    row_counts: dict[str, int] = field(default_factory=dict)
    warning_count: int = 0
    reject_count: int = 0
