from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .artifacts import ArtifactIO
from .config_loader import Configuration
from .control_store import ControlStore, DeltaEvents, SQLiteEvents
from .leases import ADLSGen2Leases, LeaseFactory, LocalLeases
from .model import utc_now
from .telemetry import EventLogger


@dataclass
class Services:
    store: ControlStore
    artifacts: ArtifactIO
    leases: LeaseFactory


def build_services(config: Configuration, *, spark: Any = None, notebookutils: Any = None) -> Services:
    logger = EventLogger(config.data["logging"])
    if config.data["control_store"]["backend"] == "sqlite":
        leases: LeaseFactory = LocalLeases()
        backend = SQLiteEvents(config.local_path(config.data["control_store"]["path"]), logger)
    else:
        leases = ADLSGen2Leases(config.data["locking"], notebookutils=notebookutils)
        backend = DeltaEvents(spark, config.data["control_store"]["path"], leases, logger)
    store = ControlStore(backend)
    return Services(store, ArtifactIO(config, store, spark), leases)


def log_configuration(config: Configuration, node_id: str, run_id: str = "") -> None:
    EventLogger(config.data["logging"]).emit(
        {
            "event_type": "CONFIGURATION_LOADED",
            "timestamp": utc_now(),
            "payload": {
                "node_id": node_id,
                "run_id": run_id,
                "dag_id": config.data["dag"]["id"],
                "configuration_version": config.data["configuration_version"],
                "configuration_fingerprint": config.fingerprint,
                "code_version": config.data["code_version"],
            },
        }
    )
