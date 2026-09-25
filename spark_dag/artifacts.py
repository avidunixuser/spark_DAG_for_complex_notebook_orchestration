from __future__ import annotations

import csv
import hashlib
import json
import os
import uuid
from pathlib import Path
from typing import Any

from .canonical_xml import local_files, parse_document, read_bounded
from .config_loader import Configuration
from .control_store import ControlStore
from .model import AttemptContext, Category, TaskFailure, WorkflowError, canonical, fingerprint


def filesystem_path(path: Path) -> Path:
    value = str(path.resolve())
    if os.name == "nt" and not value.startswith("\\\\?\\"):
        value = "\\\\?\\UNC\\" + value[2:] if value.startswith("\\\\") else "\\\\?\\" + value
    return Path(value)


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class ArtifactIO:
    def __init__(self, config: Configuration, store: ControlStore, spark: Any = None):
        self.config, self.store, self.spark = config, store, spark
        self.backend = config.data["storage"]["backend"]
        self._validated_delta: set[str] = set()
        if self.backend == "delta" and spark is None:
            raise WorkflowError("MISSING_SPARK", "Delta artifact operations require a Spark session.")

    def source_fingerprint(self, source: dict[str, Any]) -> dict[str, Any]:
        if source["format"] == "xml":
            if "://" not in source["path"]:
                root = self.config.local_path(source["path"])
                entries = []
                for path in local_files(root, source["xml"]):
                    content = read_bounded(path, source["xml"]["max_file_bytes"])
                    entries.append(
                        {
                            "path": path.relative_to(root).as_posix(),
                            "bytes": len(content),
                            "sha256": hashlib.sha256(content).hexdigest(),
                        }
                    )
                return {**source, "file_count": len(entries), "content_fingerprint": fingerprint(entries)}
            from pyspark.sql import functions as F

            files = self._xml_files(source).select(
                "path",
                "length",
                F.sha2("content", 256).alias("sha256"),
            )
            return {**source, "content": self._delta_digest(files)}
        if source["format"] == "csv":
            try:
                return {**source, "content_digest": _file_digest(self.config.local_path(source["path"]))}
            except OSError:
                raise TaskFailure(
                    "SOURCE_UNAVAILABLE",
                    "The landed source snapshot cannot be read.",
                    Category.INFRASTRUCTURE,
                    retryable=True,
                ) from None
        from delta.tables import DeltaTable

        frame = self.read_source(source)
        table_id = DeltaTable.forPath(self.spark, source["path"]).detail().select("id").first()["id"]
        return {**source, "table_id": table_id, "snapshot_schema": frame.schema.json()}

    def clear_validation_cache(self) -> None:
        self._validated_delta.clear()

    def parameter_fingerprints(self, value: Any) -> Any:
        if isinstance(value, dict):
            if value.get("format") in {"xml", "csv", "delta"} and "path" in value and "schema" in value:
                return self.source_fingerprint(value)
            return {key: self.parameter_fingerprints(child) for key, child in value.items()}
        if isinstance(value, list):
            return [self.parameter_fingerprints(child) for child in value]
        return value

    def read_source(self, source: dict[str, Any]) -> Any:
        if source["format"] == "xml":
            settings = source["xml"]
            if self.spark is None:
                root = self.config.local_path(source["path"])
                return [
                    row
                    for path in local_files(root, settings)
                    for row in parse_document(read_bounded(path, settings["max_file_bytes"]), settings)
                ]
            from pyspark.sql import functions as F
            from pyspark.sql.types import ArrayType

            schema = self.spark.createDataFrame([], source["schema"]).schema
            parse = F.udf(lambda content: parse_document(content, settings), ArrayType(schema))
            return (
                self._xml_files(source).select(F.explode(parse("content")).alias("record")).select("record.*")
            )
        if source["format"] == "delta":
            if self.spark is None:
                raise WorkflowError("MISSING_SPARK", "A Delta source requires Spark.")
            frame = (
                self.spark.read.format("delta")
                .option("versionAsOf", source["snapshot_version"])
                .load(source["path"])
            )
            expected = self.spark.createDataFrame([], source["schema"]).schema
            if frame.schema != expected:
                raise TaskFailure(
                    "SOURCE_SCHEMA_MISMATCH", "The snapshot schema changed.", Category.DATA_QUALITY
                )
            return frame
        path = self.config.local_path(source["path"])
        if self.spark is not None:
            return (
                self.spark.read.schema(source["schema"])
                .option("header", True)
                .option("mode", "FAILFAST")
                .csv(str(path))
            )
        try:
            with path.open(newline="", encoding="utf-8") as stream:
                reader = csv.DictReader(stream)
                expected_names = [column.strip().split()[0] for column in source["schema"].split(",")]
                if reader.fieldnames != expected_names:
                    raise TaskFailure(
                        "SOURCE_SCHEMA_MISMATCH",
                        "CSV header differs from its contract.",
                        Category.DATA_QUALITY,
                    )
                rows = []
                for row in reader:
                    if None in row or any(value is None for value in row.values()):
                        raise TaskFailure(
                            "MALFORMED_SOURCE",
                            "CSV row width differs from its header.",
                            Category.DATA_QUALITY,
                        )
                    rows.append({key: value if value != "" else None for key, value in row.items()})
                return rows
        except OSError:
            raise TaskFailure(
                "SOURCE_UNAVAILABLE",
                "The landed source snapshot cannot be read.",
                Category.INFRASTRUCTURE,
                retryable=True,
            ) from None

    def _xml_files(self, source: dict[str, Any]) -> Any:
        if self.spark is None:
            raise WorkflowError("MISSING_SPARK", "Distributed XML ingestion requires a Spark session.")
        from pyspark.errors import AnalysisException
        from pyspark.sql import functions as F

        path = source["path"] if "://" in source["path"] else str(self.config.local_path(source["path"]))
        try:
            self.spark.catalog.refreshByPath(path)
            files = (
                self.spark.read.format("binaryFile")
                .option("recursiveFileLookup", "true")
                .option(
                    "pathGlobFilter",
                    "*.[xX][mM][lL]",
                )
                .load(path)
            )
            counts = files.agg(F.count("*").alias("files"), F.max("length").alias("largest")).first()
        except AnalysisException as error:
            if error.getErrorClass() == "PATH_NOT_FOUND":
                raise TaskFailure(
                    "SOURCE_UNAVAILABLE",
                    "The canonical XML input set is unavailable.",
                    Category.INFRASTRUCTURE,
                    retryable=True,
                ) from None
            raise
        if not counts["files"]:
            raise TaskFailure(
                "SOURCE_UNAVAILABLE",
                "No canonical XML files were found.",
                Category.INFRASTRUCTURE,
                retryable=True,
            )
        if (
            counts["files"] > source["xml"]["max_files"]
            or counts["largest"] > source["xml"]["max_file_bytes"]
        ):
            raise TaskFailure(
                "XML_INPUT_LIMIT", "The XML input set exceeds its configured bounds.", Category.DATA_QUALITY
            )
        return files

    @staticmethod
    def _delta_digest(frame: Any) -> dict[str, Any]:
        from pyspark.sql import functions as F

        serialized = F.to_json(
            F.struct(*[F.col(name) for name in sorted(frame.columns)]),
            options={"ignoreNullFields": "false", "timeZone": "UTC"},
        )
        hashed = frame.select(F.sha2(serialized, 256).alias("_row_digest"))
        sums = [
            F.sum(F.conv(F.substring("_row_digest", index * 16 + 1, 16), 16, 10).cast("decimal(38,0)")).alias(
                f"sum_{index}"
            )
            for index in range(4)
        ]
        aggregate = hashed.agg(F.count("*").alias("count"), *sums).first()
        return {
            "row_count": aggregate["count"],
            "schema": frame.schema.json(),
            "hash_sums": [str(aggregate[f"sum_{index}"] or 0) for index in range(4)],
        }

    def write(
        self,
        context: AttemptContext,
        name: str,
        rows: Any,
        schema: str,
        *,
        rejects: bool = False,
    ) -> dict[str, Any]:
        base = self.config.data["reject_data"]["path"] if rejects else self.config.data["storage"]["path"]
        parts = (context.scope, context.run_id, context.node_id, str(context.attempt), name)
        if any(not part or "/" in part or "\\" in part or part in {".", ".."} for part in parts):
            raise WorkflowError(
                "INVALID_ARTIFACT_PATH", "Artifact identifiers must not contain path components."
            )
        if self.backend == "delta":
            from delta.tables import DeltaTable

            path = "/".join([base.rstrip("/"), *parts])
            frame = rows if hasattr(rows, "write") else self.spark.createDataFrame(rows, schema)
            frame.write.format("delta").mode("errorifexists").save(path)
            table = DeltaTable.forPath(self.spark, path)
            version = int(table.history(1).select("version").first()["version"])
            committed = self.spark.read.format("delta").option("versionAsOf", version).load(path)
            proof = self._delta_digest(committed)
            reference = {
                "backend": "delta",
                "path": path,
                "version": version,
                "table_id": table.detail().select("id").first()["id"],
                "digest": fingerprint(proof),
                "row_count": proof["row_count"],
                "schema": proof["schema"],
            }
            self._validated_delta.add(fingerprint(reference))
            return reference
        if hasattr(rows, "collect"):
            rows = [row.asDict() for row in rows.collect()]
        content = canonical({"schema": schema, "rows": sorted(rows, key=canonical)}).encode("utf-8")
        root = self.config.local_path(base)
        path = filesystem_path(root.joinpath(*parts).with_suffix(".json"))
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
        try:
            with temporary.open("xb") as output:
                output.write(content)
                output.flush()
                os.fsync(output.fileno())
            if path.exists():
                raise WorkflowError("ARTIFACT_COLLISION", "An immutable attempt artifact already exists.")
            os.replace(temporary, path)
            if os.name != "nt":
                descriptor = os.open(path.parent, os.O_RDONLY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
        finally:
            if temporary.exists():
                temporary.unlink()
        return {
            "backend": "local",
            "path": str(path),
            "digest": hashlib.sha256(content).hexdigest(),
            "row_count": len(rows),
            "schema": schema,
        }

    def validate(self, reference: dict[str, Any], *, use_cached: bool = False) -> bool:
        backend = reference.get("backend")
        if backend == "outbox":
            if not {"scope", "effect_id", "digest"} <= reference.keys():
                return False
            effect = self.store.view(reference["scope"]).get("outbox", reference["effect_id"])
            return bool(effect and effect["payload_digest"] == reference["digest"])
        if backend == "local":
            if not {"path", "digest", "row_count", "schema"} <= reference.keys():
                return False
            path = Path(reference["path"])
            try:
                content = path.read_bytes()
                document = json.loads(content)
                return (
                    hashlib.sha256(content).hexdigest() == reference["digest"]
                    and document["schema"] == reference["schema"]
                    and len(document["rows"]) == reference["row_count"]
                )
            except (FileNotFoundError, json.JSONDecodeError, UnicodeError, KeyError, TypeError):
                return False
            except OSError:
                raise TaskFailure(
                    "OUTPUT_UNREADABLE",
                    "Output access could not be validated.",
                    Category.INFRASTRUCTURE,
                ) from None
        if backend == "delta":
            from delta.tables import DeltaTable
            from pyspark.errors import AnalysisException

            if not {"path", "version", "table_id", "digest", "row_count", "schema"} <= reference.keys():
                return False
            key = fingerprint(reference)
            if use_cached and key in self._validated_delta:
                return True
            self._validated_delta.discard(key)
            if not DeltaTable.isDeltaTable(self.spark, reference["path"]):
                return False
            try:
                table_id = (
                    DeltaTable.forPath(self.spark, reference["path"]).detail().select("id").first()["id"]
                )
                frame = (
                    self.spark.read.format("delta")
                    .option("versionAsOf", reference["version"])
                    .load(reference["path"])
                )
                proof = self._delta_digest(frame)
                valid = (
                    table_id == reference["table_id"]
                    and fingerprint(proof) == reference["digest"]
                    and proof["row_count"] == reference["row_count"]
                    and proof["schema"] == reference["schema"]
                )
                if valid:
                    self._validated_delta.add(key)
                return valid
            except AnalysisException as error:
                if error.getErrorClass() in {
                    "PATH_NOT_FOUND",
                    "DELTA_MISSING_TRANSACTION_LOG",
                    "DELTA_TABLE_NOT_FOUND",
                    "DELTA_CANNOT_TIME_TRAVEL_VERSION_NOT_EXIST",
                    "DELTA_FILE_NOT_FOUND",
                }:
                    return False
                raise
        return False

    def read(self, reference: dict[str, Any]) -> Any:
        if not self.validate(reference, use_cached=True):
            raise TaskFailure(
                "OUTPUT_INVALID", "A committed artifact is missing or invalid.", Category.DATA_QUALITY
            )
        if reference["backend"] == "delta":
            return (
                self.spark.read.format("delta")
                .option("versionAsOf", reference["version"])
                .load(reference["path"])
            )
        if reference["backend"] != "local":
            raise WorkflowError("INVALID_ARTIFACT_READ", "Receipts cannot be read as datasets.")
        with Path(reference["path"]).open(encoding="utf-8") as source:
            document = json.load(source)
        if self.spark is not None:
            return self.spark.createDataFrame(document["rows"], document["schema"])
        return document["rows"]
