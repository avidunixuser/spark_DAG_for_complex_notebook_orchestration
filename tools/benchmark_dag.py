"""Measure the same isolated synthetic DAG with barrier and eager scheduling."""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    from spark_dag.config_loader import load_config
    from spark_dag.model import RunRequest
    from tests.support import BUSINESS_KEY, DeploymentTest
    from tests.test_performance import MeasuringExecutor, overlap_scenario

    parser = argparse.ArgumentParser()
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--output", default="performance-results.json")
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error("repetitions must be positive")
    measurements = []
    for repetition in range(args.repetitions):
        for scheduling in ("barrier", "eager"):
            case = DeploymentTest()
            case.setUp()
            try:
                overlap_scenario(case.data, scheduling)
                engine = case.engine()
                executor = MeasuringExecutor(engine.config, engine.store)
                engine.executor = executor
                case.last_engine = engine
                started = time.perf_counter()
                summary = engine.run(RunRequest(BUSINESS_KEY))
                elapsed = time.perf_counter() - started
                case.assert_report(summary)
                overlap = executor.started["fast_tail"] < executor.finished["extract_orders"]
                assert overlap == (scheduling == "eager")
                assert executor.peak <= engine.config.data["concurrency"]["max_parallel"]
                measurements.append(
                    {
                        "repetition": repetition + 1,
                        "scheduling": scheduling,
                        "duration_seconds": round(elapsed, 4),
                        "peak_children": executor.peak,
                        "successor_overlapped_unrelated_branch": overlap,
                        "data_correctness": "passed",
                        "delivery_intents": 1,
                    }
                )
            finally:
                case.tearDown()
    medians = {
        scheduling: statistics.median(
            row["duration_seconds"] for row in measurements if row["scheduling"] == scheduling
        )
        for scheduling in ("barrier", "eager")
    }
    config = load_config(deployment_dir=ROOT)
    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "environment": {"os": platform.system(), "python": platform.python_version()},
        "scope": "Synthetic local scheduling benchmark; not a Fabric/Databricks cloud SLA.",
        "code_fingerprint": config.code_fingerprint,
        "configuration_fingerprint": config.fingerprint,
        "synthetic_delays_seconds": {"extract_customers": 0.05, "extract_orders": 0.5, "fast_tail": 0.5},
        "median_seconds": medians,
        "median_speedup": round(medians["barrier"] / medians["eager"], 3),
        "measurements": measurements,
    }
    Path(args.output).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: result[key] for key in ("median_seconds", "median_speedup")}))


if __name__ == "__main__":
    main()
