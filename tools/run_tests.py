"""Run tests and persist observed results, including failure-injection subtests."""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import time
import unittest
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class RecordingResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.records = []
        self.started = {}

    def startTest(self, test):
        self.started[test.id()] = time.monotonic()
        super().startTest(test)

    def record(self, test, outcome, detail=None):
        started = self.started.get(test.id())
        self.records.append(
            {
                "test": test.id(),
                "outcome": outcome,
                "duration_seconds": round(time.monotonic() - started, 4) if started is not None else None,
                **({"detail": detail} if detail else {}),
            }
        )

    def addSuccess(self, test):
        super().addSuccess(test)
        self.record(test, "passed")

    def addFailure(self, test, err):
        super().addFailure(test, err)
        self.record(test, "failed", self._exc_info_to_string(err, test))

    def addError(self, test, err):
        super().addError(test, err)
        self.record(test, "error", self._exc_info_to_string(err, test))

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        self.record(test, "skipped", reason)

    def addSubTest(self, test, subtest, err):
        super().addSubTest(test, subtest, err)
        self.record(
            subtest,
            "passed" if err is None else "failed",
            None if err is None else self._exc_info_to_string(err, test),
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="test-results.json")
    parser.add_argument(
        "--spark", action="store_true", help="Require a real local Spark runtime; failures are not skipped."
    )
    parser.add_argument(
        "--delta", action="store_true", help="Also require native Delta integration (Java and Maven access)."
    )
    args = parser.parse_args()
    if args.spark:
        os.environ["SPARK_DAG_SPARK_TESTS"] = "1"
    if args.delta:
        os.environ["SPARK_DAG_DELTA_TESTS"] = "1"
    started = time.monotonic()
    suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"), top_level_dir=str(ROOT))
    result = unittest.TextTestRunner(verbosity=1, resultclass=RecordingResult).run(suite)
    from spark_dag.config_loader import load_config

    configuration = load_config(deployment_dir=ROOT)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "python": platform.python_version(),
        "operating_system": platform.system(),
        "architecture": platform.machine(),
        "code_fingerprint": configuration.code_fingerprint,
        "configuration_fingerprint": configuration.fingerprint,
        "spark_version": version("pyspark") if args.spark or args.delta else None,
        "delta_version": version("delta-spark") if args.delta else None,
        "duration_seconds": round(time.monotonic() - started, 3),
        "tests_run": result.testsRun,
        "failures": len(result.failures),
        "errors": len(result.errors),
        "skipped": len(result.skipped),
        "successful": result.wasSuccessful(),
        "scope": "Reference DAG, not a conversion acceptance test for an original DataStage job.",
        "cloud_validation": "Fabric, Databricks, and ADLS Gen2 file APIs are contract-tested with fakes, not live services.",
        "spark_requested": args.spark,
        "delta_requested": args.delta,
        "results": result.records,
    }
    Path(args.output).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
