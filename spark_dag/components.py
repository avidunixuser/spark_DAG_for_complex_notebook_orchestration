from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from .artifacts import ArtifactIO
from .canonical_xml import PRODUCT_SCHEMA, SHIPMENT_SCHEMA
from .model import AttemptContext, Category, ProcessingResult, TaskFailure, WorkflowError

ACCEPTED_SCHEMA = SHIPMENT_SCHEMA.replace("quantity STRING", "quantity LONG")
REJECT_SCHEMA = SHIPMENT_SCHEMA + ", reason STRING"
PLAN_GROUPS = ("warehouse_id", "sku", "unit_of_measure")
PLAN_SCHEMA = (
    "warehouse_id STRING, sku STRING, unit_of_measure STRING, expected_units LONG, shipment_lines LONG"
)
PLAN_TARGET = {"group_by": list(PLAN_GROUPS), "quantity_column": "quantity"}


def _missing(value: str | None) -> bool:
    return value is None or not value.strip()


def transform_python(
    shipments: list[dict[str, Any]],
    products: list[dict[str, Any]],
    maximum: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    dimension: dict[str, str] = {}
    for row in products:
        sku = row["sku"]
        if any(_missing(row[key]) for key in ("sku", "description", "unit_of_measure")) or sku in dimension:
            raise TaskFailure(
                "INVALID_DIMENSION",
                "Product records require unique SKUs and nonempty fields.",
                Category.DATA_QUALITY,
            )
        dimension[sku] = row["unit_of_measure"]
    by_key: dict[tuple[str | None, str | None], set[tuple[Any, ...]]] = defaultdict(set)
    for row in shipments:
        by_key[(row["shipment_id"], row["line_id"])].add(
            (row["sku"], row["warehouse_id"], row["quantity"], row["unit_of_measure"])
        )
    accepted, rejected = [], []
    for (shipment_id, line_id), versions in by_key.items():
        for sku, warehouse, raw, unit in sorted(
            versions, key=lambda values: tuple(str(value) for value in values)
        ):
            reason = None
            if _missing(shipment_id) or _missing(line_id):
                reason = "missing_shipment_key"
            elif len(versions) != 1:
                reason = "conflicting_shipment_key"
            elif (
                not raw
                or not raw.isascii()
                or not raw.isdecimal()
                or len(raw) > 18
                or not 0 < int(raw) <= maximum
            ):
                reason = "invalid_quantity"
            elif sku not in dimension:
                reason = "unknown_sku"
            elif _missing(warehouse):
                reason = "missing_warehouse"
            elif unit != dimension[sku]:
                reason = "unit_mismatch"
            row = {
                "shipment_id": shipment_id,
                "line_id": line_id,
                "sku": sku,
                "warehouse_id": warehouse,
                "quantity": raw,
                "unit_of_measure": unit,
            }
            if reason:
                rejected.append({**row, "reason": reason})
            else:
                accepted.append({**row, "quantity": int(raw)})
    return accepted, rejected


def _classify_spark(shipments: Any, products: Any, maximum: int) -> Any:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    def missing(name: str) -> Any:
        return F.col(name).isNull() | (F.trim(F.col(name)) == "")

    invalid_product = missing("sku") | missing("description") | missing("unit_of_measure")
    dimension = products.agg(
        F.count("*").alias("rows"),
        F.countDistinct("sku").alias("keys"),
        F.sum(F.when(invalid_product, 1).otherwise(0)).alias("invalid"),
    ).first()
    if dimension["rows"] != dimension["keys"] or dimension["invalid"]:
        raise TaskFailure(
            "INVALID_DIMENSION",
            "Product records require unique SKUs and nonempty fields.",
            Category.DATA_QUALITY,
        )
    distinct = shipments.dropDuplicates(shipments.columns).withColumn(
        "_versions",
        F.count("*").over(Window.partitionBy("shipment_id", "line_id")),
    )
    catalog = products.select("sku", F.col("unit_of_measure").alias("_product_unit"))
    joined = distinct.join(catalog, "sku", "left")
    digits = F.coalesce(F.col("quantity").rlike("^[0-9]{1,18}$"), F.lit(False))
    quantity = F.when(digits, F.col("quantity").cast("long"))
    reason = (
        F.when(missing("shipment_id") | missing("line_id"), F.lit("missing_shipment_key"))
        .when(F.col("_versions") != 1, F.lit("conflicting_shipment_key"))
        .when(~digits | (quantity <= 0) | (quantity > maximum), F.lit("invalid_quantity"))
        .when(F.col("_product_unit").isNull(), F.lit("unknown_sku"))
        .when(missing("warehouse_id"), F.lit("missing_warehouse"))
        .when(
            F.col("unit_of_measure").isNull() | (F.col("unit_of_measure") != F.col("_product_unit")),
            F.lit("unit_mismatch"),
        )
    )
    return joined.withColumn("_parsed_quantity", quantity).withColumn("reason", reason).drop("_versions")


def _transform_outputs(classified: Any) -> tuple[Any, Any]:
    from pyspark.sql import functions as F

    columns = ("shipment_id", "line_id", "sku", "warehouse_id", "quantity", "unit_of_measure")
    rejected = classified.where("reason IS NOT NULL").select(*columns, "reason")
    accepted = classified.where("reason IS NULL").select(
        *[
            F.col("_parsed_quantity").alias("quantity") if name == "quantity" else F.col(name)
            for name in columns
        ],
    )
    return accepted, rejected


def transform_spark(shipments: Any, products: Any, maximum: int) -> tuple[Any, Any]:
    return _transform_outputs(_classify_spark(shipments, products, maximum))


@contextmanager
def _transformed(
    shipments: Any, products: Any, maximum: int, *, use_spark: bool
) -> Iterator[tuple[Any, Any]]:
    if not use_spark:
        yield transform_python(shipments, products, maximum)
        return
    from pyspark import StorageLevel

    classified = _classify_spark(shipments, products, maximum).persist(StorageLevel.MEMORY_AND_DISK)
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
    if component == "extract_shipments":
        return _source(context, artifacts, "shipments", SHIPMENT_SCHEMA)
    if component == "extract_products":
        return _source(context, artifacts, "products", PRODUCT_SCHEMA)
    if component == "validate_shipments":
        shipments = artifacts.read(upstream["extract_shipments"]["shipments"])
        products = artifacts.read(upstream["extract_products"]["products"])
        maximum = context.parameters["rules"]["max_quantity"]
        with _transformed(shipments, products, maximum, use_spark=artifacts.spark is not None) as (
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
        good, bad = upstream["validate_shipments"]["accepted"], upstream["validate_shipments"]["rejects"]
        rules = context.parameters["rules"]
        if bad["row_count"] > rules["max_reject_count"] or good["row_count"] < rules["minimum_accepted_rows"]:
            raise TaskFailure(
                "QUALITY_GATE_FAILED", "The configured data-quality threshold failed.", Category.DATA_QUALITY
            )
        return ProcessingResult(
            {"accepted": good},
            row_counts={"accepted": good["row_count"]},
            reject_count=bad["row_count"],
        )
    if component == "plan_receipts":
        if context.parameters["target"] != PLAN_TARGET:
            raise WorkflowError(
                "UNSUPPORTED_SAMPLE_TARGET", "Receiving plans group warehouse, SKU, and unit of measure."
            )
        accepted = artifacts.read(upstream["quality_gate"]["accepted"])
        if artifacts.spark is not None:
            from pyspark.sql import functions as F

            plan = accepted.groupBy(*PLAN_GROUPS).agg(
                F.sum(F.col("quantity").cast("decimal(38,0)")).alias("expected_units"),
                F.count("*").alias("shipment_lines"),
            )
            if plan.where(F.col("expected_units") > 9223372036854775807).limit(1).count():
                raise TaskFailure(
                    "AGGREGATE_OVERFLOW",
                    "The receiving plan exceeds its signed 64-bit units contract.",
                    Category.DATA_QUALITY,
                )
            plan = plan.withColumn("expected_units", F.col("expected_units").cast("long"))
        else:
            totals: dict[tuple[str, ...], dict[str, Any]] = {}
            for row in accepted:
                key = tuple(row[name] for name in PLAN_GROUPS)
                entry = totals.setdefault(
                    key,
                    {**{name: row[name] for name in PLAN_GROUPS}, "expected_units": 0, "shipment_lines": 0},
                )
                entry["expected_units"] += row["quantity"]
                entry["shipment_lines"] += 1
                if entry["expected_units"] > 9223372036854775807:
                    raise TaskFailure(
                        "AGGREGATE_OVERFLOW",
                        "The receiving plan exceeds its signed 64-bit units contract.",
                        Category.DATA_QUALITY,
                    )
            plan = list(totals.values())
        reference = artifacts.write(context, "receiving_plan", plan, PLAN_SCHEMA)
        return ProcessingResult(
            {"receiving_plan": reference}, row_counts={"receiving_plan": reference["row_count"]}
        )
    if component == "request_receipts":
        plan = upstream["plan_receipts"]["receiving_plan"]
        receipt = artifacts.store.put_outbox(context, plan["digest"], plan, operation="inventory_receipt")
        fault = artifacts.config.data["runtime"]["fault_injection"].get(context.node_id, {})
        if fault.get("kind") == "after_effect" and context.attempt in fault["attempts"]:
            raise TaskFailure(
                "INJECTED_AFTER_EFFECT",
                "Injected failure after outbox commit.",
                Category.RETRYABLE_APPLICATION,
                retryable=True,
            )
        return ProcessingResult({"receipt": receipt}, metrics={"receipt_intents": 1})
    if component == "control_notice":
        reference = artifacts.write(context, "notice", [{"kind": context.parameters["kind"]}], "kind STRING")
        return ProcessingResult({"notice": reference})
    if component == "cleanup":
        reference = artifacts.write(context, "receipt", [{"completed": True}], "completed BOOLEAN")
        return ProcessingResult({"receipt": reference})
    raise WorkflowError(
        "UNKNOWN_COMPONENT", "The notebook component has no registered business implementation."
    )


COMPONENTS = {
    "extract_shipments",
    "extract_products",
    "validate_shipments",
    "quality_gate",
    "plan_receipts",
    "request_receipts",
    "control_notice",
    "cleanup",
}
