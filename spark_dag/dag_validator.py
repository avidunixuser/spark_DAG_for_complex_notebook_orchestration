from __future__ import annotations

import heapq
from collections import Counter
from typing import Any

from .config_loader import Configuration, lookup
from .model import BAD, SAFE_STRATEGIES, SUCCESS, Status, WorkflowError

TRIGGERS = {"all_success", "any_success", "all_done", "any_failed", "any_timed_out"}


def descendants(nodes: dict[str, dict[str, Any]], origins: set[str]) -> set[str]:
    children: dict[str, list[str]] = {key: [] for key in nodes}
    for key, node in nodes.items():
        for dependency in node["dependencies"]:
            children.setdefault(dependency, []).append(key)
    result = set(origins)
    pending = list(origins)
    while pending:
        for child in children.get(pending.pop(), []):
            if child not in result:
                result.add(child)
                pending.append(child)
    return result


def _references(value: Any, config: Configuration, node: dict[str, Any]) -> None:
    if isinstance(value, list):
        for child in value:
            _references(child, config, node)
    elif isinstance(value, dict):
        special = set(value) & {"$config", "$runtime", "$output"}
        if special:
            if len(value) != 1:
                raise WorkflowError("INVALID_REFERENCE", "Reference objects cannot contain other fields.")
            kind = next(iter(special))
            reference = value[kind]
            if not isinstance(reference, str):
                raise WorkflowError("INVALID_REFERENCE", "Reference names must be strings.")
            if kind == "$config":
                if reference.split(".")[0] not in {"sources", "targets", "data_quality", "notifications"}:
                    raise WorkflowError(
                        "INVALID_REFERENCE", "Business parameters must reference business settings."
                    )
                lookup(config.data, reference)
            elif kind == "$runtime":
                if reference not in {"business_run_key", "run_id", "node_id", "attempt", "restart_mode"}:
                    raise WorkflowError("INVALID_REFERENCE", "Unsupported runtime parameter reference.")
            else:
                parts = reference.split(".")
                if (
                    len(parts) != 2
                    or parts[0] not in node["dependencies"]
                    or parts[1] not in config.nodes[parts[0]]["expected_outputs"]
                ):
                    raise WorkflowError(
                        "INVALID_REFERENCE", "Output references must name direct dependency outputs."
                    )
        else:
            for child in value.values():
                _references(child, config, node)


def validate_dag(config: Configuration) -> list[str]:
    data = config.data
    listed = data["nodes"]
    if any(count > 1 for count in Counter(node["id"] for node in listed).values()):
        raise WorkflowError("DUPLICATE_NODE", "DAG node IDs must be unique.")
    nodes = config.nodes
    if len(set(node["checkpoint"]["name"] for node in listed)) != len(listed):
        raise WorkflowError("DUPLICATE_CHECKPOINT", "Checkpoint names must be unique.")
    entries = set(data["dag"]["entry_nodes"])
    if not entries <= nodes.keys():
        raise WorkflowError("UNDEFINED_ENTRY", "Every entry node must be defined.")
    resources = data["concurrency"]["resources"]
    groups = data["concurrency"]["groups"]
    if data["concurrency"]["max_parallel"] > data["spark"]["max_concurrent_notebooks"]:
        raise WorkflowError("INVALID_CONCURRENCY", "Notebook concurrency exceeds configured Spark capacity.")
    for name, definition in data["notebooks"].items():
        path = (config.root / definition["path"]).resolve()
        if not path.is_relative_to(config.root) or not path.is_file() or path.suffix != ".ipynb":
            raise WorkflowError(
                "MISSING_NOTEBOOK", f"Notebook source is missing from the deployment: {name}."
            )
        from .config_loader import read_json

        notebook = read_json(path)
        if not isinstance(notebook, dict) or notebook.get("nbformat") != 4 or not notebook.get("cells"):
            raise WorkflowError(
                "INVALID_NOTEBOOK", "Notebook sources must be valid nonempty nbformat 4 documents."
            )
    for node in listed:
        if node["notebook"] not in data["notebooks"]:
            raise WorkflowError("MISSING_NOTEBOOK", "A node references an undefined notebook.")
        if not set(node["dependencies"]) <= nodes.keys():
            raise WorkflowError("UNDEFINED_DEPENDENCY", "A node references an undefined dependency.")
        if len(node["dependencies"]) != len(set(node["dependencies"])):
            raise WorkflowError("DUPLICATE_DEPENDENCY", "Dependencies must be unique.")
        if node["trigger"]["type"] not in TRIGGERS:
            raise WorkflowError("INVALID_TRIGGER", "The trigger type is not supported.")
        if not node["dependencies"] and node["trigger"]["type"] not in {"all_success", "all_done"}:
            raise WorkflowError("INVALID_TRIGGER", "A root cannot have a dependency-only trigger.")
        if node["concurrency_group"] not in groups:
            raise WorkflowError("INVALID_CONCURRENCY", "The concurrency group is undefined.")
        for resource, demand in node["resources"].items():
            if resource not in resources or demand > resources[resource]:
                raise WorkflowError(
                    "INVALID_CONCURRENCY", "Resource demand exceeds or lacks a configured capacity."
                )
        strategy = node["idempotency"]["strategy"]
        if strategy not in SAFE_STRATEGIES and node["retry"]["max_retries"]:
            raise WorkflowError("UNSAFE_RETRY", "Non-idempotent nodes cannot retry automatically.")
        if strategy == "compensation":
            companion = node["idempotency"].get("compensation_node")
            if companion not in nodes or companion == node["id"]:
                raise WorkflowError(
                    "INVALID_COMPENSATION", "A compensation strategy needs a separate DAG node."
                )
        condition = node["trigger"].get("condition")
        if condition and condition["parameter"] not in node["parameters"]:
            raise WorkflowError(
                "INVALID_CONDITION", "Conditional triggers must reference a declared node parameter."
            )
        _references(node["parameters"], config, node)
    from .components import COMPONENTS

    if any(data["notebooks"][node["notebook"]]["component"] not in COMPONENTS for node in listed):
        raise WorkflowError("UNKNOWN_COMPONENT", "A notebook references an unregistered business component.")
    if not set(data["runtime"]["fault_injection"]) <= nodes.keys():
        raise WorkflowError("UNKNOWN_FAULT_NODE", "Fault injection references an undefined node.")
    indegree = {key: len(node["dependencies"]) for key, node in nodes.items()}
    children = {key: [] for key in nodes}
    for key, node in nodes.items():
        for dependency in node["dependencies"]:
            children[dependency].append(key)
    ready = [key for key, count in indegree.items() if count == 0]
    heapq.heapify(ready)
    plan: list[str] = []
    while ready:
        key = heapq.heappop(ready)
        plan.append(key)
        for child in children[key]:
            indegree[child] -= 1
            if indegree[child] == 0:
                heapq.heappush(ready, child)
    if len(plan) != len(nodes):
        raise WorkflowError("DAG_CYCLE", "The DAG contains a cycle.")
    roots = {key for key, node in nodes.items() if not node["dependencies"]}
    if entries != roots or descendants(nodes, entries) != set(nodes):
        raise WorkflowError("UNREACHABLE_NODE", "Entry nodes must cover every root and reachable component.")
    return plan


def trigger_status(node: dict[str, Any], states: dict[str, str], parameters: dict[str, Any]) -> str | None:
    dependencies = [states[key] for key in node["dependencies"]]
    kind = node["trigger"]["type"]
    matches = {
        "all_success": all(value in SUCCESS for value in dependencies),
        "any_success": any(value in SUCCESS for value in dependencies),
        "all_done": True,
        "any_failed": any(
            value in {Status.FAILED, Status.TIMED_OUT, Status.BLOCKED} for value in dependencies
        ),
        "any_timed_out": Status.TIMED_OUT in dependencies,
    }[kind]
    if not matches:
        if kind in {"all_success", "any_success"} and any(value in BAD for value in dependencies):
            return Status.BLOCKED
        return Status.SKIPPED_CONDITION
    condition = node["trigger"].get("condition")
    if condition and parameters[condition["parameter"]] != condition["equals"]:
        return Status.SKIPPED_CONDITION
    return None


def make_manifest(config: Configuration) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "reference_only": True,
        "dag": config.data["dag"],
        "topological_plan": validate_dag(config),
        "nodes": [
            {
                key: node[key]
                for key in (
                    "id",
                    "notebook",
                    "original_stage",
                    "description",
                    "dependencies",
                    "trigger",
                    "parameters",
                    "expected_outputs",
                    "timeout_seconds",
                    "retry",
                    "restart",
                    "idempotency",
                    "checkpoint",
                    "failure_classification",
                    "critical",
                    "concurrency_group",
                    "resources",
                )
            }
            for node in config.data["nodes"]
        ],
    }
