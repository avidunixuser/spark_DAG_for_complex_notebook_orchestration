from __future__ import annotations

import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from spark_dag.components import (
    CUSTOMER_SCHEMA,
    ORDER_SCHEMA,
    _transformed,
    transform_python,
    transform_spark,
)
from spark_dag.model import Status, canonical
from spark_dag.worker import ChildFailed, run_child
from tests.support import DeploymentTest


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
        self.fault("publish_report")
        first = self.run_dag()
        self.assertEqual(first["nodes"]["transform_orders"]["status"], Status.SUCCEEDED)
        self.assertEqual(first["nodes"]["publish_report"]["status"], Status.FAILED)
        self.data["runtime"]["fault_injection"].clear()
        second = self.run_dag("resume", first["run_id"])
        self.assert_report(second)
        self.assertEqual(second["nodes"]["extract_orders"]["status"], Status.SKIPPED_ALREADY_SATISFIED)
        self.assertEqual(second["nodes"]["transform_orders"]["status"], Status.SKIPPED_ALREADY_SATISFIED)
        third = self.run_dag("force_rerun")
        self.assert_report(third)

    def test_python_and_spark_transformations_match_edge_cases(self):
        orders = [
            {"order_id": "a", "customer_id": "c1", "amount_cents": "001"},
            {"order_id": "a", "customer_id": "c1", "amount_cents": "001"},
            {"order_id": "b", "customer_id": "c1", "amount_cents": "2"},
            {"order_id": "b", "customer_id": "c1", "amount_cents": "3"},
            {"order_id": "c", "customer_id": "missing", "amount_cents": "5"},
            {"order_id": "d", "customer_id": "c1", "amount_cents": None},
            {"order_id": "e", "customer_id": "c1", "amount_cents": "-1"},
            {"order_id": None, "customer_id": "c1", "amount_cents": "5"},
            {"order_id": "", "customer_id": "c1", "amount_cents": "5"},
            {"order_id": "f", "customer_id": None, "amount_cents": "5"},
            {"order_id": "g", "customer_id": "c1", "amount_cents": "999999999999999999999999"},
            {"order_id": "h", "customer_id": "c1", "amount_cents": "0"},
            {"order_id": "i", "customer_id": "c1", "amount_cents": " 12"},
        ]
        customers = [{"customer_id": "c1", "region": "North"}]
        expected = transform_python(orders, customers, 1000)
        actual = transform_spark(
            self.spark.createDataFrame(orders, ORDER_SCHEMA),
            self.spark.createDataFrame(customers, CUSTOMER_SCHEMA),
            1000,
        )
        for python_rows, frame in zip(expected, actual, strict=True):
            self.assertEqual(
                sorted(python_rows, key=canonical),
                sorted([row.asDict() for row in frame.collect()], key=canonical),
            )

    def test_shared_transform_cache_is_released_on_failure(self):
        from spark_dag.components import _classify_spark

        orders = self.spark.createDataFrame(
            [{"order_id": "a", "customer_id": "c1", "amount_cents": "1"}], ORDER_SCHEMA
        )
        customers = self.spark.createDataFrame([{"customer_id": "c1", "region": "North"}], CUSTOMER_SCHEMA)
        classified = _classify_spark(orders, customers, 1000)
        with patch("spark_dag.components._classify_spark", return_value=classified):
            with self.assertRaisesRegex(RuntimeError, "injected"):
                with _transformed(orders, customers, 1000, use_spark=True) as (accepted, rejected):
                    self.assertTrue(classified.is_cached)
                    self.assertEqual(accepted.count(), 1)
                    self.assertEqual(rejected.count(), 0)
                    raise RuntimeError("injected")
        self.assertFalse(classified.is_cached)
