from __future__ import annotations

import argparse
import json
from pathlib import Path

from .config_loader import load_config
from .dag_validator import make_manifest, validate_dag
from .model import Mode, RunRequest, WorkflowError, canonical, fingerprint, safe_error
from .orchestrator import Orchestrator
from .runtime import build_services


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description="Restartable Spark notebook DAG reference runtime")
    commands = root.add_subparsers(dest="command", required=True)
    for name in ("validate", "manifest", "run", "inspect", "reconcile"):
        command = commands.add_parser(name)
        command.add_argument("--config", default="job_config.json")
        command.add_argument("--deployment-dir")
        command.add_argument("--environment")
        if name in {"run", "inspect", "reconcile"}:
            command.add_argument("--business-run-key", required=True)
        if name == "run":
            command.add_argument("--mode", choices=list(Mode), default=Mode.NORMAL)
            command.add_argument("--parent-run-id")
            command.add_argument("--restart-from")
            command.add_argument("--reason", default="")
            command.add_argument("--approve-node", action="append", default=[])
        if name == "manifest":
            command.add_argument("--output", default="dag_manifest.json")
        if name in {"inspect", "reconcile"}:
            command.add_argument("--run-id", required=True)
        if name == "reconcile":
            command.add_argument("--workers-stopped", action="store_true")
            command.add_argument("--reason", required=True)
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        config = load_config(args.config, deployment_dir=args.deployment_dir, environment=args.environment)
        if args.command == "validate":
            result = {"plan": validate_dag(config), "configuration_fingerprint": config.fingerprint}
        elif args.command == "manifest":
            result = make_manifest(config)
            Path(args.output).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        elif args.command == "run":
            result = Orchestrator(config).run(
                RunRequest(
                    args.business_run_key,
                    args.mode,
                    args.parent_run_id,
                    args.restart_from,
                    args.reason,
                    tuple(args.approve_node),
                )
            )
        else:
            if config.data["runtime"]["platform"] != "local":
                raise WorkflowError(
                    "NOTEBOOK_REQUIRED",
                    "Cloud inspection/reconciliation requires the deployed Spark notebook.",
                )
            services = build_services(config)
            scope = fingerprint(
                [config.data["application_id"], config.data["dag"]["id"], args.business_run_key]
            )
            if args.command == "reconcile":
                services.store.reconcile(
                    scope, args.run_id, workers_stopped=args.workers_stopped, reason=args.reason
                )
            result = services.store.run(scope, args.run_id)
        print(canonical(result))
        return 1 if result.get("status") in {"FAILED", "RECOVERY_REQUIRED"} else 0
    except Exception as error:
        print(canonical({"error": safe_error(error)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
