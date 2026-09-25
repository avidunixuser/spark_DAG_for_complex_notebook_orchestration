from __future__ import annotations

from typing import Any

from .model import canonical


class EventLogger:
    def __init__(self, settings: dict[str, Any]):
        self.settings = settings

    def emit(self, event: dict[str, Any]) -> None:
        if not self.settings["enabled"]:
            return
        record = event["payload"]
        allowed = (
            "application_id",
            "dag_id",
            "dag_version",
            "run_id",
            "node_id",
            "attempt",
            "status",
            "previous_status",
            "started_at",
            "ended_at",
            "duration_seconds",
            "configuration_version",
            "code_version",
            "retry_count",
            "restart_mode",
            "configuration_fingerprint",
            "execution_uncertain",
            "plan",
        )
        message = {key: record[key] for key in allowed if key in record}
        message.update(event_type=event["event_type"], timestamp=event["timestamp"])
        if self.settings["log_business_run_key"] and "business_run_key" in record:
            message["business_run_key"] = record["business_run_key"]
        if record.get("error"):
            message["error_category"] = record["error"]["category"]
            message["error_code"] = record["error"]["code"]
        print(canonical(message), flush=True)
