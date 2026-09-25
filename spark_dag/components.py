from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from .artifacts import ArtifactIO
from .model import AttemptContext, Category, ProcessingResult, TaskFailure, WorkflowError

ORDER_SCHEMA = "order_id STRING, customer_id STRING, amount_cents STRING"
CUSTOMER_SCHEMA = "customer_id STRING, region STRING"
ACCEPTED_SCHEMA = "order_id STRING, customer_id STRING, amount_cents LONG, region STRING"
REJECT_SCHEMA = "order_id STRING, customer_id STRING, amount_cents STRING, reason STRING"
REPORT_SCHEMA = "region STRING, total_cents LONG, order_count LONG"


def transform_python(
    orders: list[dict[str, Any]],
    customers: list[dict[str, Any]],
    maximum: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    dimension: dict[str, str] = {}
    for row in customers:
        key, region = row["customer_id"], row["region"]
        if not key or not region or key in dimension:
            raise TaskFailure(
                "INVALID_DIMENSION", "Customer keys must be unique and nonempty.", Category.DATA_QUALITY
            )
        dimension[key] = region
    by_key: dict[str | None, set[tuple[Any, ...]]] = defaultdict(set)
    for row in orders:
        by_key[row["order_id"]].add((row["customer_id"], row["amount_cents"]))
    accepted, rejected = [], []
    for key, versions in by_key.items():
        for customer, raw in sorted(versions, key=lambda pair: (str(pair[0]), str(pair[1]))):
            reason = None
            if not key:
                reason = "missing_order_key"
            elif len(versions) != 1:
                reason = "conflicting_order_key"
            elif not raw or not raw.isascii() or not raw.isdecimal() or len(raw) > 18 or int(raw) > maximum:
                reason = "invalid_amount"
            elif customer not in dimension:
                reason = "unknown_customer"
            if reason:
                rejected.append(
                    {"order_id": key, "customer_id": customer, "amount_cents": raw, "reason": reason}
                )
            else:
                accepted.append(
                    {
                        "order_id": key,
                        "customer_id": customer,
                        "amount_cents": int(raw),
                        "region": dimension[customer],
                    }
                )
    return accepted, rejected


def _classify_spark(orders: Any, customers: Any, maximum: int) -> Any:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    missing = (
        F.col("customer_id").isNull()
        | (F.col("customer_id") == "")
        | F.col("region").isNull()
        | (F.col("region") == "")
    )
    dimension = customers.agg(
        F.count("*").alias("rows"),
        F.countDistinct("customer_id").alias("keys"),
        F.sum(F.when(missing, 1).otherwise(0)).alias("invalid"),
    ).first()
    if dimension["rows"] != dimension["keys"] or dimension["invalid"]:
        raise TaskFailure(
            "INVALID_DIMENSION", "Customer keys must be unique and nonempty.", Category.DATA_QUALITY
        )
    distinct = orders.dropDuplicates(["order_id", "customer_id", "amount_cents"])
    distinct = distinct.withColumn("_versions", F.count("*").over(Window.partitionBy("order_id")))
    joined = distinct.join(customers, "customer_id", "left")
    amount_is_digits = F.coalesce(F.col("amount_cents").rlike("^[0-9]{1,18}$"), F.lit(False))
    amount = F.when(amount_is_digits, F.col("amount_cents").cast("long"))
    reason = (
        F.when(F.col("order_id").isNull() | (F.col("order_id") == ""), F.lit("missing_order_key"))
        .when(F.col("_versions") != 1, F.lit("conflicting_order_key"))
        .when(~amount_is_digits | (amount > maximum), F.lit("invalid_amount"))
        .when(F.col("region").isNull(), F.lit("unknown_customer"))
    )
    return joined.withColumn("_amount_cents", amount).withColumn("reason", reason).drop("_versions")


def _transform_outputs(classified: Any) -> tuple[Any, Any]:
    from pyspark.sql import functions as F

    rejected = classified.where("reason IS NOT NULL").select(
        "order_id", "customer_id", "amount_cents", "reason"
    )
    accepted = classified.where("reason IS NULL").select(
        "order_id",
        "customer_id",
        F.col("_amount_cents").alias("amount_cents"),
        "region",
    )
    return accepted, rejected


def transform_spark(orders: Any, customers: Any, maximum: int) -> tuple[Any, Any]:
    return _transform_outputs(_classify_spark(orders, customers, maximum))


@contextmanager
def _transformed(orders: Any, customers: Any, maximum: int, *, use_spark: bool) -> Iterator[tuple[Any, Any]]:
    if not use_spark:
        yield transform_python(orders, customers, maximum)
        return
    from pyspark import StorageLevel

    classified = _classify_spark(orders, customers, maximum).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        yield _transform_outputs(classified)
    finally:
        classified.unpersist()


def _source(context: AttemptContext, artifacts: ArtifactIO, name: str, schema: str) -> ProcessingResult:
    data = artifacts.read_source(context.parameters["source"])
    reference = artifacts.write(context, name, data, schema)
    return ProcessingResult({name: reference}, row_counts={name: reference["row_count"]})


def execute(component: str, context: AttemptContext, artifacts: ArtifactIO) -> ProcessingResult:
    upstream = context.upstream_outputs
    if component == "extract_orders":
        return _source(context, artifacts, "orders", ORDER_SCHEMA)
    if component == "extract_customers":
        return _source(context, artifacts, "customers", CUSTOMER_SCHEMA)
    if component == "transform_orders":
        orders = artifacts.read(upstream["extract_orders"]["orders"])
        customers = artifacts.read(upstream["extract_customers"]["customers"])
        maximum = context.parameters["rules"]["max_amount_cents"]
        with _transformed(orders, customers, maximum, use_spark=artifacts.spark is not None) as (
            accepted,
            rejects,
        ):
            good = artifacts.write(context, "accepted", accepted, ACCEPTED_SCHEMA)
            bad = artifacts.write(context, "rejects", rejects, REJECT_SCHEMA, rejects=True)
        return ProcessingResult(
            {"accepted": good, "rejects": bad},
            row_counts={"accepted": good["row_count"], "rejects": bad["row_count"]},
            reject_count=bad["row_count"],
            warning_count=int(bad["row_count"] > 0),
        )
    if component == "quality_gate":
        good, bad = upstream["transform_orders"]["accepted"], upstream["transform_orders"]["rejects"]
        rules = context.parameters["rules"]
        if bad["row_count"] > rules["max_reject_count"] or good["row_count"] < rules["minimum_accepted_rows"]:
            raise TaskFailure(
                "QUALITY_GATE_FAILED", "The configured data-quality threshold failed.", Category.DATA_QUALITY
            )
        return ProcessingResult(
            {"accepted": good}, row_counts={"accepted": good["row_count"]}, reject_count=bad["row_count"]
        )
    if component == "publish_report":
        target = context.parameters["target"]
        if target != {"group_by": "region", "amount_column": "amount_cents"}:
            raise WorkflowError(
                "UNSUPPORTED_SAMPLE_TARGET", "The sample report requires region and integer amount_cents."
            )
        accepted = artifacts.read(upstream["quality_gate"]["accepted"])
        if artifacts.spark is not None:
            from pyspark.sql import functions as F

            report = accepted.groupBy("region").agg(
                F.sum(F.col("amount_cents").cast("decimal(38,0)")).alias("total_cents"),
                F.count("*").alias("order_count"),
            )
            if report.where(F.col("total_cents") > 9223372036854775807).limit(1).count():
                raise TaskFailure(
                    "AGGREGATE_OVERFLOW",
                    "The report exceeds its signed 64-bit cents contract.",
                    Category.DATA_QUALITY,
                )
            report = report.withColumn("total_cents", F.col("total_cents").cast("long"))
        else:
            totals: dict[str, dict[str, Any]] = {}
            for row in accepted:
                entry = totals.setdefault(
                    row["region"], {"region": row["region"], "total_cents": 0, "order_count": 0}
                )
                entry["total_cents"] += row["amount_cents"]
                entry["order_count"] += 1
                if entry["total_cents"] > 9223372036854775807:
                    raise TaskFailure(
                        "AGGREGATE_OVERFLOW",
                        "The report exceeds its signed 64-bit cents contract.",
                        Category.DATA_QUALITY,
                    )
            report = list(totals.values())
        reference = artifacts.write(context, "report", report, REPORT_SCHEMA)
        return ProcessingResult({"report": reference}, row_counts={"report": reference["row_count"]})
    if component == "deliver_report":
        report = upstream["publish_report"]["report"]
        receipt = artifacts.store.put_outbox(context, report["digest"], report)
        fault = artifacts.config.data["runtime"]["fault_injection"].get(context.node_id, {})
        if fault.get("kind") == "after_effect" and context.attempt in fault["attempts"]:
            raise TaskFailure(
                "INJECTED_AFTER_EFFECT",
                "Injected failure after outbox commit.",
                Category.RETRYABLE_APPLICATION,
                retryable=True,
            )
        return ProcessingResult({"receipt": receipt}, metrics={"delivery_intents": 1})
    if component == "control_notice":
        reference = artifacts.write(
            context,
            "notice",
            [{"kind": context.parameters["kind"]}],
            "kind STRING",
        )
        return ProcessingResult({"notice": reference})
    if component == "cleanup":
        reference = artifacts.write(context, "receipt", [{"completed": True}], "completed BOOLEAN")
        return ProcessingResult({"receipt": reference})
    raise WorkflowError(
        "UNKNOWN_COMPONENT", "The notebook component has no registered business implementation."
    )


COMPONENTS = {
    "extract_orders",
    "extract_customers",
    "transform_orders",
    "quality_gate",
    "publish_report",
    "deliver_report",
    "control_notice",
    "cleanup",
}
