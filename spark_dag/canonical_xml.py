"""Versioned, bounded canonical XML contracts for warehouse receiving."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from xml.etree.ElementTree import ParseError

from defusedxml.common import DefusedXmlException
from defusedxml.ElementTree import fromstring

from .model import Category, TaskFailure, WorkflowError

NAMESPACE = "urn:example:supply-chain:canonical:1"
XML_VERSION = "1.0"
CONTRACTS = {
    "shipment_batch": {
        "root": "ShipmentBatch",
        "record": "ShipmentLine",
        "fields": (
            ("shipmentId", "shipment_id"),
            ("lineId", "line_id"),
            ("sku", "sku"),
            ("warehouseId", "warehouse_id"),
            ("quantity", "quantity"),
            ("unitOfMeasure", "unit_of_measure"),
        ),
    },
    "product_catalog": {
        "root": "ProductCatalog",
        "record": "Product",
        "fields": (("sku", "sku"), ("description", "description"), ("unitOfMeasure", "unit_of_measure")),
    },
}


def schema_for(contract: str) -> str:
    if contract not in CONTRACTS:
        raise WorkflowError("UNKNOWN_XML_CONTRACT", "The canonical XML contract is not supported.")
    return ", ".join(f"{column} STRING" for _, column in CONTRACTS[contract]["fields"])


SHIPMENT_SCHEMA = schema_for("shipment_batch")
PRODUCT_SCHEMA = schema_for("product_catalog")


def _xml_error(code: str) -> TaskFailure:
    return TaskFailure(
        code, "Canonical XML does not satisfy the declared input contract.", Category.DATA_QUALITY
    )


def parse_document(content: bytes | bytearray, settings: dict[str, Any]) -> list[dict[str, str | None]]:
    if len(content) > settings["max_file_bytes"]:
        raise _xml_error("XML_FILE_TOO_LARGE")
    if settings["schema_version"] != XML_VERSION:
        raise _xml_error("UNSUPPORTED_XML_VERSION")
    contract = CONTRACTS[settings["contract"]]
    try:
        root = fromstring(bytes(content), forbid_dtd=True, forbid_entities=True, forbid_external=True)
    except DefusedXmlException:
        raise _xml_error("UNSAFE_XML") from None
    except (ParseError, UnicodeError, ValueError):
        raise _xml_error("MALFORMED_XML") from None

    def qualified(name: str) -> str:
        return f"{{{NAMESPACE}}}{name}"

    if root.tag != qualified(contract["root"]) or root.attrib != {"schemaVersion": XML_VERSION}:
        raise _xml_error("XML_ENVELOPE_MISMATCH")
    if root.text and root.text.strip():
        raise _xml_error("XML_STRUCTURE_MISMATCH")
    if not len(root):
        raise _xml_error("EMPTY_XML_BATCH")
    if len(root) > settings["max_records_per_file"]:
        raise _xml_error("XML_RECORD_LIMIT")
    result = []
    expected = [qualified(tag) for tag, _ in contract["fields"]]
    for record in root:
        if (
            record.tag != qualified(contract["record"])
            or record.attrib
            or (record.text and record.text.strip())
            or (record.tail and record.tail.strip())
            or [field.tag for field in record] != expected
        ):
            raise _xml_error("XML_STRUCTURE_MISMATCH")
        row = {}
        for field, (_, column) in zip(record, contract["fields"], strict=True):
            if field.attrib or len(field) or (field.tail and field.tail.strip()):
                raise _xml_error("XML_STRUCTURE_MISMATCH")
            row[column] = field.text if field.text else None
        result.append(row)
    return result


def local_files(root: Path, settings: dict[str, Any]) -> list[Path]:
    try:
        if not root.is_dir():
            raise OSError("Source directory unavailable")
        files = []
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix.lower() != ".xml":
                continue
            if not path.resolve().is_relative_to(root.resolve()):
                raise _xml_error("XML_PATH_OUTSIDE_SOURCE")
            files.append(path)
            if len(files) > settings["max_files"]:
                raise _xml_error("XML_FILE_LIMIT")
        if not files:
            raise OSError("No canonical XML files")
        return sorted(files, key=lambda path: path.relative_to(root).as_posix())
    except OSError:
        raise TaskFailure(
            "SOURCE_UNAVAILABLE",
            "The canonical XML input set is unavailable.",
            Category.INFRASTRUCTURE,
            retryable=True,
        ) from None


def read_bounded(path: Path, maximum: int) -> bytes:
    try:
        with path.open("rb") as source:
            content = source.read(maximum + 1)
    except OSError:
        raise TaskFailure(
            "SOURCE_UNAVAILABLE",
            "A canonical XML file cannot be read.",
            Category.INFRASTRUCTURE,
            retryable=True,
        ) from None
    if len(content) > maximum:
        raise _xml_error("XML_FILE_TOO_LARGE")
    return content
