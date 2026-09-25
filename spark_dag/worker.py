from __future__ import annotations

import json
import math
import os
import sys
import time
from typing import Any

from . import components
from .checkpoint_manager import checkpoint, input_fingerprint, outputs_valid
from .config_loader import load_config, resolve_parameters
from .model import (
    SUCCESS,
    AttemptContext,
    Category,
    Status,
    TaskFailure,
    WorkflowError,
    canonical,
    fingerprint,
    safe_error,
)
from .runtime import Services, build_services, log_configuration


class ChildFailed(WorkflowError):
    def __init__(self, result: dict[str, Any]):
        error = result["error"]
        super().__init__(
            error["code"], error["message"], Category(error["category"]), retryable=error["retryable"]
        )
        self.result = result


def run_child(
    context: AttemptContext,
    *,
    component: str | None = None,
    spark: Any = None,
    notebookutils: Any = None,
    services: Services | None = None,
) -> dict[str, Any]:
    config = load_config(
        context.config_path, deployment_dir=context.deployment_dir, environment=context.environment
    )
    if context.node_id not in config.nodes:
        raise WorkflowError("UNKNOWN_NODE", "The child node is not in this deployment.")
    node = config.nodes[context.node_id]
    definition = config.data["notebooks"][node["notebook"]]
    log_configuration(config, context.node_id, context.run_id)
    if component is not None and component != definition["component"]:
        raise WorkflowError("WRONG_NOTEBOOK", "The invoked notebook does not implement this node.")
    if (
        context.configuration_fingerprint != config.fingerprint
        or context.code_fingerprint != config.code_fingerprint
    ):
        raise WorkflowError("CONFIGURATION_CHANGED", "The deployment changed after this attempt was planned.")
    expected_scope = fingerprint(
        [config.data["application_id"], config.data["dag"]["id"], context.business_run_key]
    )
    if context.scope != expected_scope or not math.isfinite(context.deadline):
        raise WorkflowError("INVALID_CONTEXT", "The child context scope or deadline is invalid.")
    services = services or build_services(config, spark=spark, notebookutils=notebookutils)
    store, artifacts = services.store, services.artifacts
    artifacts.clear_validation_cache()
    snapshot = store.view(context.scope)
    run = snapshot.get("run", context.run_id)
    if run is None:
        raise WorkflowError("UNKNOWN_RUN", "No control record exists for this run.")
    if (
        run["business_run_key"] != context.business_run_key
        or run["restart_mode"] != context.restart_mode
        or context.parent_context.get("dag_id") != run["dag_id"]
        or context.parent_context.get("dag_version") != run["dag_version"]
        or context.parent_context.get("parent_run_id") != run["parent_run_id"]
    ):
        raise WorkflowError("INVALID_CONTEXT", "The child context does not match its orchestration record.")
    store.claim(context)
    try:
        if time.time() >= context.deadline:
            raise TaskFailure(
                "DEADLINE_EXCEEDED", "The attempt deadline has passed.", Category.TIMEOUT, retryable=True
            )
        dependencies = {}
        for key in node["dependencies"]:
            dependency = snapshot.get("node", store.node_key(context.run_id, key))
            if dependency is None:
                raise WorkflowError("UNKNOWN_NODE", "A dependency has no control record.")
            dependencies[key] = dependency
        for key, dependency in dependencies.items():
            if dependency["status"] in SUCCESS and not outputs_valid(
                config.nodes[key],
                dependency["outputs"],
                artifacts,
            ):
                raise TaskFailure(
                    "UPSTREAM_OUTPUT_INVALID",
                    "An upstream committed output is unavailable.",
                    Category.DATA_QUALITY,
                )
        upstream = {key: value["outputs"] for key, value in dependencies.items()}
        parameters = resolve_parameters(node["parameters"], config.data, context.as_dict(), upstream)
        if (
            parameters != context.parameters
            or upstream != context.upstream_outputs
            or {key: value["status"] for key, value in dependencies.items()} != context.dependency_status
        ):
            raise WorkflowError(
                "INPUT_CONTEXT_CHANGED", "Attempt inputs differ from the committed dependency records."
            )
        if input_fingerprint(config, node, parameters, dependencies, artifacts) != context.input_fingerprint:
            raise TaskFailure("INPUT_CHANGED", "An input changed after planning.", Category.DATA_QUALITY)
        fault = config.data["runtime"]["fault_injection"].get(context.node_id, {})
        inject = context.attempt in fault.get("attempts", [])
        if inject:
            kind = fault["kind"]
            if kind == "retryable":
                raise TaskFailure(
                    "INJECTED_RETRYABLE",
                    "Injected transient failure.",
                    Category.RETRYABLE_APPLICATION,
                    retryable=True,
                )
            if kind == "permanent":
                raise TaskFailure(
                    "INJECTED_PERMANENT", "Injected permanent failure.", Category.NON_RETRYABLE_APPLICATION
                )
            if kind == "timeout":
                time.sleep(fault.get("delay_seconds", node["timeout_seconds"] * 2))
            if kind == "crash":
                if config.data["runtime"]["platform"] != "local":
                    raise WorkflowError(
                        "UNSAFE_FAULT", "Process-exit injection is supported only by the local runner."
                    )
                os._exit(91)
        if time.time() >= context.deadline:
            raise TaskFailure(
                "DEADLINE_EXCEEDED", "The attempt deadline has passed.", Category.TIMEOUT, retryable=True
            )
        result = components.execute(definition["component"], context, artifacts)
        if inject and fault["kind"] == "after_output":
            raise TaskFailure(
                "INJECTED_AFTER_OUTPUT",
                "Injected failure after artifact commit.",
                Category.RETRYABLE_APPLICATION,
                retryable=True,
            )
        if not outputs_valid(node, result.outputs, artifacts, use_cached=True):
            raise TaskFailure("OUTPUT_INVALID", "Required outputs failed validation.", Category.DATA_QUALITY)
        if input_fingerprint(config, node, parameters, dependencies, artifacts) != context.input_fingerprint:
            raise TaskFailure(
                "INPUT_CHANGED", "The source changed while the attempt was running.", Category.DATA_QUALITY
            )
        final = store.finish_attempt(
            context,
            Status.SUCCEEDED,
            outputs=result.outputs,
            checkpoint=checkpoint(node, context.input_fingerprint, result.outputs),
            metrics=result.metrics,
            row_counts=result.row_counts,
            warning_count=result.warning_count,
            reject_count=result.reject_count,
        )
    except Exception as error:
        classification = safe_error(error)
        if not isinstance(error, WorkflowError):
            classification["category"] = node["failure_classification"]
        final = store.finish_attempt(
            context,
            Status.TIMED_OUT if classification["category"] == Category.TIMEOUT else Status.FAILED,
            error=classification,
        )
    if final["status"] != Status.SUCCEEDED:
        raise ChildFailed(final) from None
    return final


def main() -> int:
    try:
        context = AttemptContext.from_dict(json.load(sys.stdin))
        result = run_child(context)
        print(canonical({"contract_version": 1, "result": result}))
        return 0
    except ChildFailed as error:
        print(canonical({"contract_version": 1, "result": error.result, "error": safe_error(error)}))
        return 1
    except Exception as error:
        print(canonical({"contract_version": 1, "error": safe_error(error)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
