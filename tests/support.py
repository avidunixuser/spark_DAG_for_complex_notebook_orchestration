from __future__ import annotations

import copy
import json
import os
import shutil
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from spark_dag.artifacts import filesystem_path
from spark_dag.checkpoint_manager import input_fingerprint
from spark_dag.config_loader import load_config
from spark_dag.executors import ExecutorBase
from spark_dag.model import CollisionError, RunRequest, fingerprint
from spark_dag.orchestrator import Orchestrator
from spark_dag.worker import ChildFailed, run_child

ROOT = Path(__file__).resolve().parents[1]
BUSINESS_KEY = "2026-09-24"


class InlineExecutor(ExecutorBase):
    """Real child contract and SQLite transactions, without interpreter startup for every test."""

    def _execute(self, context):
        try:
            run_child(context)
        except ChildFailed:
            # The persisted failure is consumed by the orchestrator's retry/trigger policy.
            if not self.terminal(context):
                raise
        except CollisionError:
            record = self.store.node(context.scope, context.run_id, context.node_id)
            fields = (
                "claim_id",
                "attempt",
                "input_fingerprint",
                "configuration_fingerprint",
                "code_fingerprint",
            )
            mismatches = [field for field in fields if record[field] != getattr(context, field)]
            raise AssertionError(
                f"Unexpected child collision: node={context.node_id}, state={record['status']}, "
                f"mismatched_fields={mismatches}"
            ) from None

    def execute_batch(self, contexts):
        with ThreadPoolExecutor(max_workers=len(contexts)) as pool:
            list(pool.map(self._execute, contexts))


class DeploymentTest(unittest.TestCase):
    def setUp(self):
        directory = ROOT / ".runtime" if os.name == "nt" else None
        if directory is not None:
            directory.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(prefix="spark-dag-test-", dir=directory)
        self.root = Path(self.temporary.name)
        for name in (
            "job_config.json",
            "job_config.schema.json",
            "orchestrator.ipynb",
            "child_template.ipynb",
        ):
            shutil.copy2(ROOT / name, self.root / name)
        for name in ("notebooks", "sample_data"):
            shutil.copytree(ROOT / name, self.root / name)
        self.data = json.loads((self.root / "job_config.json").read_text(encoding="utf-8"))
        self.data["logging"]["enabled"] = False
        self.data["runtime"]["allow_fault_injection"] = True
        self.write()

    def tearDown(self):
        shutil.rmtree(filesystem_path(self.root))
        self.temporary.cleanup()

    def write(self):
        (self.root / "job_config.json").write_text(json.dumps(self.data, indent=2), encoding="utf-8")

    def config(self, **kwargs):
        self.write()
        return load_config(deployment_dir=self.root, **kwargs)

    def engine(self, *, real_processes=False):
        engine = Orchestrator(self.config())
        if not real_processes:
            engine.executor = InlineExecutor(engine.config, engine.store)
        return engine

    def run_dag(self, mode="normal", parent=None, origin=None, approvals=(), *, real_processes=False):
        engine = self.engine(real_processes=real_processes)
        self.last_engine = engine
        return engine.run(
            RunRequest(BUSINESS_KEY, mode, parent, origin, "TEST-123" if mode != "normal" else "", approvals)
        )

    def node(self, key):
        return next(node for node in self.data["nodes"] if node["id"] == key)

    def fault(self, key, kind="permanent", attempts=None, **kwargs):
        self.data["runtime"]["fault_injection"][key] = {"kind": kind, "attempts": attempts or [1], **kwargs}

    def scope(self):
        return fingerprint([self.data["application_id"], self.data["dag"]["id"], BUSINESS_KEY])

    def assert_report(self, summary):
        self.assertEqual(summary["status"], "SUCCEEDED", summary["failed_node_details"])
        reference = summary["nodes"]["publish_report"]["outputs"]["report"]
        rows = self.last_engine.services.artifacts.read(reference)
        self.assertEqual(
            sorted(rows, key=lambda row: row["region"]),
            [
                {"region": "North", "total_cents": 1500, "order_count": 2},
                {"region": "South", "total_cents": 2050, "order_count": 1},
            ],
        )
        outbox = self.last_engine.store.view(self.scope()).all("outbox")
        self.assertEqual(len(outbox), 1)
        self.assertEqual(outbox[0]["status"], "PENDING_DELIVERY")
        return copy.deepcopy(rows)


class PreparedAttemptTest(DeploymentTest):
    def prepare_attempt(self):
        engine = self.engine()
        run = engine.store.begin_run(self.scope(), RunRequest(BUSINESS_KEY), engine.config, engine.plan)
        node = engine.config.nodes["extract_orders"]
        parameters = engine._parameters(run, node, 1, {})
        inputs = input_fingerprint(engine.config, node, parameters, {}, engine.services.artifacts)
        ready = engine.store.prepare(self.scope(), run["run_id"], node["id"], run["owner"], inputs, {})
        context = engine._context(run, node, ready, {}, parameters, time.time() + 30)
        return engine, run, context
