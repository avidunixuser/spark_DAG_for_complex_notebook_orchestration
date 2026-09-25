from __future__ import annotations

import json
from collections.abc import Callable
from functools import wraps
from typing import Any, ParamSpec, TypeVar

from .config_loader import load_config
from .executors import DatabricksExecutor, FabricExecutor
from .model import AttemptContext, Category, RunRequest, WorkflowError, canonical, safe_error
from .orchestrator import Orchestrator
from .runtime import build_services
from .worker import run_child

P = ParamSpec("P")
T = TypeVar("T")


def _public_errors(function: Callable[P, T]) -> Callable[P, T]:
    @wraps(function)
    def boundary(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return function(*args, **kwargs)
        except Exception as error:
            sanitized = safe_error(error)
            raise WorkflowError(
                sanitized["code"],
                f"{sanitized['code']}: {sanitized['message']}",
                Category(sanitized["category"]),
                retryable=sanitized["retryable"],
            ) from None

    return boundary


def parameter_values(defaults: dict[str, str], dbutils: Any = None) -> dict[str, str]:
    if dbutils is None:
        return defaults
    existing = dbutils.widgets.getAll()
    for name, value in defaults.items():
        if name not in existing:
            dbutils.widgets.text(name, value)
    return {name: dbutils.widgets.get(name) for name in defaults}


@_public_errors
def run_orchestrator_notebook(parameters: dict[str, str], *, spark: Any = None) -> dict[str, Any]:
    config = load_config(
        parameters["config_file"],
        deployment_dir=parameters["deployment_dir"] or None,
        environment=parameters["environment"] or None,
        notebook="orchestrator",
    )
    platform = config.data["runtime"]["platform"]
    notebookutils = None
    if platform == "fabric":
        import notebookutils
    if platform != "local":
        if spark is None:
            raise WorkflowError("MISSING_SPARK", "The orchestrator requires the platform Spark session.")
        for name, value in config.data["spark"]["settings"].items():
            spark.conf.set(name, value)
    services = build_services(
        config, spark=spark if platform != "local" else None, notebookutils=notebookutils
    )
    executor = None
    if platform == "fabric":
        executor = FabricExecutor(config, services.store, notebookutils)
    elif platform == "databricks":
        executor = DatabricksExecutor(config, services.store)
    try:
        approvals = json.loads(parameters["approved_nodes_json"])
    except json.JSONDecodeError:
        raise WorkflowError(
            "INVALID_APPROVAL", "approved_nodes_json must be a JSON array of node IDs."
        ) from None
    if not isinstance(approvals, list) or not all(isinstance(node, str) for node in approvals):
        raise WorkflowError("INVALID_APPROVAL", "approved_nodes_json must be a JSON array of node IDs.")
    request = RunRequest(
        parameters["business_run_key"],
        parameters["restart_mode"],
        parameters["parent_run_id"] or None,
        parameters["restart_from"] or None,
        parameters["reason"],
        tuple(approvals),
    )
    result = Orchestrator(config, services=services, executor=executor).run(request)
    print(canonical({key: value for key, value in result.items() if key != "nodes"}))
    if result["status"] != "SUCCEEDED":
        raise WorkflowError(
            "RUN_FAILED", f"Run {result['run_id']} requires the operator action in its persisted summary."
        )
    return result


@_public_errors
def run_child_notebook(context_json: str, component: str, *, spark: Any = None) -> dict[str, Any]:
    try:
        context = AttemptContext.from_dict(json.loads(context_json))
    except json.JSONDecodeError:
        raise WorkflowError(
            "INVALID_CONTEXT", "context_json must contain the structured attempt contract."
        ) from None
    notebookutils = None
    if context.parent_context.get("platform") == "fabric":
        import notebookutils
    return run_child(context, component=component, spark=spark, notebookutils=notebookutils)


def exit_result(result: dict[str, Any], dbutils: Any = None) -> None:
    if result["platform"] == "fabric":
        import notebookutils

        notebookutils.notebook.exit(canonical(result))
    elif result["platform"] == "databricks":
        if dbutils is None:
            raise WorkflowError("MISSING_DBUTILS", "Databricks notebook exit requires dbutils.")
        dbutils.notebook.exit(canonical(result))
    else:
        print(canonical(result))
