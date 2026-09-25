from __future__ import annotations

from typing import Any

from .artifacts import ArtifactIO
from .config_loader import Configuration
from .model import SAFE_STRATEGIES, SUCCESS, Status, fingerprint, utc_now


def input_fingerprint(
    config: Configuration,
    node: dict[str, Any],
    parameters: dict[str, Any],
    dependencies: dict[str, dict[str, Any]],
    artifacts: ArtifactIO,
) -> str:
    return fingerprint(
        {
            "application": config.data["application_id"],
            "dag": config.data["dag"]["id"],
            "environment": config.data["environment"],
            "code": config.code_fingerprint,
            "code_version": config.data["code_version"],
            "node": {
                key: node[key]
                for key in (
                    "id",
                    "notebook",
                    "parameters",
                    "expected_outputs",
                    "idempotency",
                    "checkpoint",
                    "trigger",
                )
            },
            "definition": config.data["notebooks"][node["notebook"]],
            "parameters": artifacts.parameter_fingerprints(parameters),
            "storage": config.data["storage"],
            "reject_data": config.data["reject_data"],
            "spark_settings": config.data["spark"]["settings"],
            "dependencies": {
                key: {
                    "status": Status.SUCCEEDED if value["status"] in SUCCESS else value["status"],
                    "outputs": {
                        name: {"digest": output["digest"], "row_count": output["row_count"]}
                        for name, output in value["outputs"].items()
                    },
                }
                for key, value in dependencies.items()
            },
        }
    )


def outputs_valid(
    node: dict[str, Any], outputs: dict[str, Any], artifacts: ArtifactIO, *, use_cached: bool = False
) -> bool:
    return set(outputs) == set(node["expected_outputs"]) and all(
        artifacts.validate(output, use_cached=use_cached) for output in outputs.values()
    )


def checkpoint(node: dict[str, Any], inputs: str, outputs: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": node["checkpoint"]["name"],
        "input_fingerprint": inputs,
        "outputs_fingerprint": fingerprint(outputs),
        "committed_at": utc_now(),
    }


def can_reuse(
    prior: dict[str, Any] | None,
    config: Configuration,
    node: dict[str, Any],
    business_run_key: str,
    inputs: str,
    artifacts: ArtifactIO,
    *,
    invalidated: bool,
) -> bool:
    if (
        invalidated
        or prior is None
        or prior["status"] not in SUCCESS
        or prior["business_run_key"] != business_run_key
        or prior["dag_id"] != config.data["dag"]["id"]
        or prior["dag_version"]
        not in {config.data["dag"]["version"], *config.data["dag"]["compatible_versions"]}
        or prior["code_version"] != config.data["code_version"]
        or prior["code_fingerprint"] != config.code_fingerprint
        or prior["input_fingerprint"] != inputs
        or not node["restart"]["eligible"]
        or node["restart"]["policy"] != "reuse_valid"
        or prior.get("execution_uncertain")
        or node["idempotency"]["strategy"] not in SAFE_STRATEGIES
    ):
        return False
    proof = prior.get("checkpoint")
    return bool(
        isinstance(proof, dict)
        and proof.get("name") == node["checkpoint"]["name"]
        and proof.get("input_fingerprint") == inputs
        and proof.get("outputs_fingerprint") == fingerprint(prior["outputs"])
        and outputs_valid(node, prior["outputs"], artifacts)
    )
