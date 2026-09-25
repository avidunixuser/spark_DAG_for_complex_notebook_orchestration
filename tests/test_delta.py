from __future__ import annotations

import os
import shutil
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from spark_dag.artifacts import ArtifactIO
from spark_dag.control_store import DeltaEvents
from spark_dag.model import CollisionError
from spark_dag.telemetry import EventLogger
from tests.support import PreparedAttemptTest


class ThreadLeases:
    """Test-only stand-in for the distributed lease service; not a deployment backend."""

    def __init__(self):
        self.mutex = threading.Lock()
        self.locks = {}

    def acquire(self, name, *, wait=True):
        with self.mutex:
            lock = self.locks.setdefault(name, threading.Lock())
        if not lock.acquire(timeout=30 if wait else 0):
            raise CollisionError()

        class Guard:
            retained = False

            def check(self):
                if not lock.locked():
                    raise CollisionError()

            def retain(self):
                self.retained = True

            def release(self):
                if not self.retained:
                    lock.release()

        return Guard()


@unittest.skipUnless(
    os.environ.get("SPARK_DAG_DELTA_TESTS") == "1",
    "Run tools/run_tests.py --delta with native Delta prerequisites.",
)
class DeltaIntegrationTests(PreparedAttemptTest):
    @classmethod
    def setUpClass(cls):
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession

        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        builder = (
            SparkSession.builder.master("local[2]")
            .appName("dag-delta-tests")
            .config(
                "spark.ui.enabled",
                "false",
            )
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.databricks.delta.snapshotPartitions", "2")
        )
        cls.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        super().setUp()
        self.delta_temporary = tempfile.TemporaryDirectory(prefix="spark-dag-delta-")
        self.delta_root = Path(self.delta_temporary.name)

    def tearDown(self):
        self.delta_temporary.cleanup()
        super().tearDown()

    def test_real_delta_state_and_audit_commit(self):
        store = DeltaEvents(
            self.spark,
            str(self.delta_root / "events"),
            ThreadLeases(),
            EventLogger({"enabled": False, "log_business_run_key": False}),
        )
        scope = self.scope()

        def initialize(tx):
            tx.put("run", "r", {"run_id": "r", "status": "RUNNING"}, "RUN_STARTED")
            tx.put("node", "r:n", {"run_id": "r", "node_id": "n", "status": "PENDING"}, "NODE_CREATED")

        store.transaction(scope, initialize)
        self.assertEqual([event["sequence"] for event in store.events(scope)], [1, 2])
        store.transaction(
            scope,
            lambda tx: tx.put(
                "node",
                "r:n",
                {**tx.get("node", "r:n"), "status": "RUNNING"},
                "ATTEMPT_CLAIMED",
            ),
        )
        events = store.events(scope)
        self.assertEqual(events[-1]["payload"]["status"], "RUNNING")
        self.assertEqual(len(events), 3)
        from delta.tables import DeltaTable

        # Empty initialization, then exactly two application commits.
        self.assertEqual(DeltaTable.forPath(self.spark, store.path).history().count(), 3)

    def test_real_delta_output_validation_pins_identity_and_version(self):
        engine, _, context = self.prepare_attempt()
        engine.config.data["storage"] = {"backend": "delta", "path": str(self.delta_root / "artifacts")}
        artifacts = ArtifactIO(engine.config, engine.store, self.spark)
        first = artifacts.write(context, "example", [{"value": 2}, {"value": 1}], "value LONG")
        second = artifacts.write(
            replace(context, attempt=2), "example", [{"value": 1}, {"value": 2}], "value LONG"
        )
        self.assertEqual(first["digest"], second["digest"])
        self.assertTrue(artifacts.validate(first))
        from delta.tables import DeltaTable

        DeltaTable.forPath(self.spark, first["path"]).update(set={"value": "3"})
        self.assertTrue(artifacts.validate(first))
        self.assertEqual(sorted(row["value"] for row in artifacts.read(first).collect()), [1, 2])
        shutil.rmtree(Path(first["path"]))
        self.assertFalse(artifacts.validate(first))

    def test_delta_source_fingerprint_reads_metadata_not_dataset_rows(self):
        engine, _, _ = self.prepare_attempt()
        artifacts = ArtifactIO(engine.config, engine.store, self.spark)
        path = str(self.delta_root / "source")
        self.spark.createDataFrame([{"value": 1}], "value LONG").write.format("delta").save(path)
        source = {"format": "delta", "path": path, "schema": "value LONG", "snapshot_version": 0}
        with patch.object(
            artifacts, "_delta_digest", side_effect=AssertionError("Source fingerprint scanned rows")
        ):
            first = artifacts.source_fingerprint(source)
            self.spark.createDataFrame([{"value": 2}], "value LONG").write.format("delta").mode(
                "append"
            ).save(path)
            self.assertEqual(first, artifacts.source_fingerprint(source))
            changed_version = artifacts.source_fingerprint({**source, "snapshot_version": 1})
            self.assertNotEqual(first, changed_version)
            shutil.rmtree(Path(path))
            self.spark.createDataFrame([{"value": 1}], "value LONG").write.format("delta").save(path)
            self.assertNotEqual(first, artifacts.source_fingerprint(source))

    def test_delta_proof_cache_avoids_rehashing_committed_outputs(self):
        engine, _, context = self.prepare_attempt()
        engine.config.data["storage"] = {"backend": "delta", "path": str(self.delta_root / "artifacts")}
        artifacts = ArtifactIO(engine.config, engine.store, self.spark)
        with patch.object(artifacts, "_delta_digest", wraps=artifacts._delta_digest) as digest:
            reference = artifacts.write(context, "example", [{"value": 1}], "value LONG")
            self.assertEqual(digest.call_count, 1)
            self.assertTrue(artifacts.validate(reference, use_cached=True))
            self.assertEqual(artifacts.read(reference).count(), 1)
            self.assertEqual(digest.call_count, 1)
            self.assertTrue(artifacts.validate(reference))
            self.assertEqual(digest.call_count, 2)
            self.assertFalse(artifacts.validate({**reference, "digest": "changed"}, use_cached=True))
            artifacts.clear_validation_cache()
            self.assertTrue(artifacts.validate(reference, use_cached=True))
            self.assertEqual(digest.call_count, 4)

    def test_delta_event_cache_refreshes_another_writers_commits(self):
        leases = ThreadLeases()
        logger = EventLogger({"enabled": False, "log_business_run_key": False})
        path = str(self.delta_root / "events")
        first = DeltaEvents(self.spark, path, leases, logger)
        second = DeltaEvents(self.spark, path, leases, logger)
        first.transaction(
            "scope", lambda tx: tx.put("run", "r", {"run_id": "r", "status": "RUNNING"}, "START")
        )
        self.assertEqual(len(second.events("scope")), 1)
        second.transaction(
            "scope", lambda tx: tx.put("run", "r", {"run_id": "r", "status": "FAILED"}, "FINISH")
        )
        with patch.object(first, "_read_after", wraps=first._read_after) as read:
            records = first.events("scope")
        self.assertEqual(read.call_args.args, ("scope", 1))
        self.assertEqual(len(records), 2)
        self.assertEqual(records[-1]["payload"]["status"], "FAILED")
