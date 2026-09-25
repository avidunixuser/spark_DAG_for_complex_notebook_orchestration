from __future__ import annotations

import copy
import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from spark_dag.artifacts import ArtifactIO
from spark_dag.components import (
    PRODUCT_SCHEMA,
    SHIPMENT_SCHEMA,
    _transformed,
    transform_python,
    transform_spark,
)
from spark_dag.model import Category, Status, TaskFailure, canonical
from spark_dag.worker import ChildFailed, run_child
from tests.support import DeploymentTest, product, shipment, write_xml


@unittest.skipUnless(
    os.environ.get("SPARK_DAG_SPARK_TESTS") == "1", "Run tools/run_tests.py --spark with Java 17/21."
)
class SparkIntegrationTests(DeploymentTest):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("dag-integration-tests")
            .config(
                "spark.ui.enabled",
                "false",
            )
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def engine(self, *, real_processes=False):
        engine = super().engine()
        spark = self.spark

        class SparkExecutor:
            def execute_batch(self, contexts):
                def execute(context):
                    try:
                        run_child(context, spark=spark)
                    except ChildFailed:
                        if engine.store.node(context.scope, context.run_id, context.node_id)[
                            "status"
                        ] not in {
                            Status.FAILED,
                            Status.TIMED_OUT,
                        }:
                            raise

                with ThreadPoolExecutor(max_workers=len(contexts)) as pool:
                    list(pool.map(execute, contexts))

        engine.executor = SparkExecutor()
        return engine

    def test_real_spark_data_correctness_before_and_after_restart(self):
        for node in self.data["nodes"]:
            node["timeout_seconds"] = 180
        self.data["runtime"]["run_timeout_seconds"] = 600
        self.fault("plan_receipts")
        first = self.run_dag()
        self.assertEqual(first["nodes"]["validate_shipments"]["status"], Status.SUCCEEDED)
        self.assertEqual(first["nodes"]["plan_receipts"]["status"], Status.FAILED)
        self.data["runtime"]["fault_injection"].clear()
        second = self.run_dag("resume", first["run_id"])
        self.assert_receiving_plan(second)
        self.assertEqual(second["nodes"]["extract_shipments"]["status"], Status.SKIPPED_ALREADY_SATISFIED)
        self.assertEqual(second["nodes"]["validate_shipments"]["status"], Status.SKIPPED_ALREADY_SATISFIED)
        third = self.run_dag("force_rerun")
        self.assert_receiving_plan(third)

    def test_python_and_spark_transformations_match_edge_cases(self):
        shipments = [
            shipment(shipment_id="ASN-A", quantity="001"),
            shipment(shipment_id="ASN-A", quantity="001"),
            shipment(shipment_id="ASN-A", line_id="2", quantity="4"),
            shipment(shipment_id="ASN-B", quantity="2"),
            shipment(shipment_id="ASN-B", quantity="3"),
            shipment(shipment_id="ASN-C", sku="SKU-UNKNOWN", quantity="5"),
            shipment(shipment_id="ASN-D", quantity=None),
            shipment(shipment_id="ASN-E", quantity="-1"),
            shipment(shipment_id=None, quantity="5"),
            shipment(shipment_id="", quantity="5"),
            shipment(shipment_id="ASN-F", sku=None, quantity="5"),
            shipment(shipment_id="ASN-G", quantity="999999999999999999999999"),
            shipment(shipment_id="ASN-H", quantity="0"),
            shipment(shipment_id="ASN-I", quantity=" 12"),
            shipment(shipment_id="ASN-J", line_id=""),
            shipment(shipment_id="ASN-K", warehouse_id=" "),
            shipment(shipment_id="ASN-L", unit_of_measure="CASE"),
            shipment(shipment_id="ASN-M", unit_of_measure=None),
        ]
        products = [product()]
        expected = transform_python(shipments, products, 1000)
        actual = transform_spark(
            self.spark.createDataFrame(shipments, SHIPMENT_SCHEMA),
            self.spark.createDataFrame(products, PRODUCT_SCHEMA),
            1000,
        )
        for python_rows, frame in zip(expected, actual, strict=True):
            self.assertEqual(
                sorted(python_rows, key=canonical),
                sorted([row.asDict() for row in frame.collect()], key=canonical),
            )

    def test_shared_transform_cache_is_released_on_failure(self):
        from spark_dag.components import _classify_spark

        shipments = self.spark.createDataFrame([shipment()], SHIPMENT_SCHEMA)
        products = self.spark.createDataFrame([product()], PRODUCT_SCHEMA)
        classified = _classify_spark(shipments, products, 1000)
        with patch("spark_dag.components._classify_spark", return_value=classified):
            with self.assertRaisesRegex(RuntimeError, "injected"):
                with _transformed(shipments, products, 1000, use_spark=True) as (accepted, rejected):
                    self.assertTrue(classified.is_cached)
                    self.assertEqual(accepted.count(), 1)
                    self.assertEqual(rejected.count(), 0)
                    raise RuntimeError("injected")
        self.assertFalse(classified.is_cached)

    def test_distributed_xml_matches_local_parser_and_refreshes_file_membership(self):
        engine = super().engine()
        source = engine.config.data["sources"]["shipments"]
        local = engine.services.artifacts.read_source(source)
        artifacts = ArtifactIO(engine.config, engine.store, self.spark)
        distributed = [row.asDict() for row in artifacts.read_source(source).collect()]
        self.assertEqual(sorted(local, key=canonical), sorted(distributed, key=canonical))
        nested = self.root / "sample_data" / "shipments" / "partner"
        nested.mkdir()
        write_xml(nested / "new.XML", "shipment_batch", [shipment(shipment_id="ASN-PARTNER")])
        refreshed = [row.asDict() for row in artifacts.read_source(source).collect()]
        self.assertEqual(len(refreshed), len(local) + 1)
        self.assertIn("ASN-PARTNER", {row["shipment_id"] for row in refreshed})

    def test_distributed_xml_bounds_fail_before_parsing(self):
        engine = super().engine()
        artifacts = ArtifactIO(engine.config, engine.store, self.spark)
        for limit, value in (("max_file_bytes", 10), ("max_files", 1)):
            source = copy.deepcopy(engine.config.data["sources"]["shipments"])
            source["xml"][limit] = value
            with self.subTest(limit=limit), self.assertRaises(TaskFailure) as error:
                artifacts.read_source(source)
            self.assertEqual(error.exception.code, "XML_INPUT_LIMIT")
            self.assertEqual(error.exception.category, Category.DATA_QUALITY)
