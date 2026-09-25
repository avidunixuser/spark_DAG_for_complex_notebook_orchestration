from __future__ import annotations

import copy
import hashlib
import json
import re
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlsplit

from jsonschema import Draft202012Validator

from .canonical_xml import schema_for
from .leases import is_dfs_host, validate_lock_location
from .model import WorkflowError, fingerprint

SCHEMA_VERSION = "1.2"
SECRET_KEYS = re.compile(
    r"(?:^|[._-])(password|passwd|token|access_token|access_key|account_key|client_secret|connection_string|sas_token)(?:$|[._-])",
    re.IGNORECASE,
)


def deep_merge(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise WorkflowError("DUPLICATE_JSON_KEY", "Duplicate JSON properties are not allowed.")
        result[key] = value
    return result


def _nonfinite(_: str) -> None:
    raise WorkflowError("MALFORMED_JSON", "Non-finite numbers are not valid configuration values.")


def read_json(path: Path) -> Any:
    try:
        with path.open(encoding="utf-8-sig") as source:
            return json.load(source, object_pairs_hook=_unique_object, parse_constant=_nonfinite)
    except FileNotFoundError:
        raise WorkflowError(
            "MISSING_SIDECAR", "The requested sidecar or schema file does not exist."
        ) from None
    except (json.JSONDecodeError, UnicodeError):
        raise WorkflowError("MALFORMED_JSON", "The sidecar must contain well-formed UTF-8 JSON.") from None
    except OSError:
        raise WorkflowError(
            "UNREADABLE_SIDECAR", "The sidecar cannot be read with the current identity."
        ) from None


def resolve_deployment(
    config_file: str = "job_config.json", deployment_dir: str | Path | None = None
) -> Path:
    requested = Path(config_file).expanduser()
    if requested.is_absolute():
        resolved = requested.resolve()
        if deployment_dir and resolved.parent != Path(deployment_dir).expanduser().resolve():
            raise WorkflowError("SIDECAR_LOCATION", "The sidecar must be in the deployment folder.")
        return resolved
    if requested.name != str(requested):
        raise WorkflowError("SIDECAR_LOCATION", "Use a file name or an absolute resolved sidecar path.")
    if deployment_dir:
        return Path(deployment_dir).expanduser().resolve() / requested
    current = Path.cwd().resolve()
    # Notebook sources are at the deployment root or its immediate notebooks subfolder.
    candidates = [current]
    if current.name == "notebooks":
        candidates.append(current.parent)
    for candidate in candidates:
        if (candidate / requested).is_file():
            return candidate / requested
    raise WorkflowError(
        "MISSING_SIDECAR",
        "No sidecar in the current deployment; supply deployment_dir or an absolute config_file.",
    )


def _check_secrets(value: Any) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "secret_references" and isinstance(child, dict):
                for definition in child.values():
                    _check_secrets(definition)
                continue
            if SECRET_KEYS.search(key) or re.search(r"fs\.azure\.account\.key\.", key, re.IGNORECASE):
                raise WorkflowError("INLINE_SECRET", "Use secret references, never inline credential fields.")
            _check_secrets(child)
    elif isinstance(value, list):
        for child in value:
            _check_secrets(child)
    elif isinstance(value, str) and "://" in value:
        parsed = urlsplit(value)
        container_address = parsed.scheme in {"abfs", "abfss"} and not parsed.password
        if (parsed.username and not container_address) or parsed.password or parsed.query:
            raise WorkflowError(
                "CREDENTIAL_IN_URI", "Configuration URIs cannot contain user info or query tokens."
            )


def lookup(config: dict[str, Any], reference: str) -> Any:
    current: Any = config
    for part in reference.split("."):
        if not isinstance(current, dict) or part not in current:
            raise WorkflowError("INVALID_REFERENCE", "A configuration reference cannot be resolved.")
        current = current[part]
    return copy.deepcopy(current)


def _abfss_uri(value: str) -> str:
    address = urlsplit(value.rstrip("/"))
    if (
        address.scheme != "abfss"
        or not address.username
        or not is_dfs_host(address.hostname or "")
        or address.password is not None
        or address.port not in {None, 443}
        or address.query
        or address.fragment
        or "\\" in value
        or any(part in {".", "..", ""} for part in address.path.split("/")[1:])
    ):
        raise WorkflowError(
            "INVALID_LAKEHOUSE", "Use a canonical credential-free ADLS Gen2 or OneLake ABFSS URI."
        )
    return f"abfss://{address.username}@{address.hostname}{address.path}"


def resolve_lakehouse_paths(config: dict[str, Any]) -> None:
    root = _abfss_uri(config["lakehouse"]["root_uri"])
    address = urlsplit(root)
    if (address.hostname or "").endswith(".fabric.microsoft.com") and len(
        PurePosixPath(address.path).parts
    ) != 2:
        raise WorkflowError("INVALID_LAKEHOUSE", "A OneLake root must identify an existing Lakehouse item.")

    def qualify(path: str) -> str:
        if "://" in path:
            return _abfss_uri(path)
        parts = path.split("/")
        if (
            len(parts) < 2
            or parts[0] not in {"Tables", "Files"}
            or any(part in {"", ".", ".."} for part in parts)
            or "\\" in path
        ):
            raise WorkflowError(
                "INVALID_LAKEHOUSE_PATH", "Relative Lakehouse paths must be inside Tables or Files."
            )
        return f"{root}/{path}"

    config["lakehouse"]["root_uri"] = root
    for section in ("control_store", "storage", "reject_data"):
        config[section]["path"] = qualify(config[section]["path"])
    for source in config["sources"].values():
        if source["format"] not in {"xml", "delta"}:
            raise WorkflowError(
                "UNVERSIONED_CLOUD_SOURCE",
                "Cloud sources must be canonical XML input sets or versioned Delta snapshots.",
            )
        source["path"] = qualify(source["path"])


def resolve_parameters(
    value: Any,
    config: dict[str, Any],
    runtime: dict[str, Any],
    upstream: dict[str, dict[str, Any]],
) -> Any:
    if isinstance(value, dict):
        references = set(value) & {"$config", "$runtime", "$output"}
        if references:
            if len(value) != 1 or len(references) != 1:
                raise WorkflowError("INVALID_REFERENCE", "References must be single-property objects.")
            kind = next(iter(references))
            reference = value[kind]
            if not isinstance(reference, str):
                raise WorkflowError("INVALID_REFERENCE", "Reference names must be strings.")
            if kind == "$config":
                return lookup(config, reference)
            if kind == "$runtime":
                return lookup(runtime, reference)
            parts = reference.split(".")
            if len(parts) != 2 or parts[0] not in upstream or parts[1] not in upstream[parts[0]]:
                raise WorkflowError("INVALID_REFERENCE", "An upstream output reference cannot be resolved.")
            return copy.deepcopy(upstream[parts[0]][parts[1]])
        return {key: resolve_parameters(child, config, runtime, upstream) for key, child in value.items()}
    if isinstance(value, list):
        return [resolve_parameters(child, config, runtime, upstream) for child in value]
    return value


@dataclass(frozen=True)
class Configuration:
    data: dict[str, Any]
    path: Path
    fingerprint: str
    code_fingerprint: str

    @property
    def root(self) -> Path:
        return self.path.parent

    @cached_property
    def nodes(self) -> dict[str, dict[str, Any]]:
        return {node["id"]: node for node in self.data["nodes"]}

    def local_path(self, value: str) -> Path:
        candidate = Path(value).expanduser()
        return candidate.resolve() if candidate.is_absolute() else (self.root / candidate).resolve()


def load_config(
    config_file: str = "job_config.json",
    *,
    deployment_dir: str | Path | None = None,
    environment: str | None = None,
    notebook: str | None = None,
) -> Configuration:
    path = resolve_deployment(config_file, deployment_dir)
    raw = read_json(path)
    if not isinstance(raw, dict):
        raise WorkflowError("INVALID_CONFIGURATION", "The sidecar root must be a JSON object.")
    if raw.get("schema_version") != SCHEMA_VERSION:
        raise WorkflowError("UNSUPPORTED_SCHEMA", "Supported configuration schema version: 1.2.")
    selected = environment or raw.get("environment")
    overrides = raw.get("environment_overrides", {})
    if not isinstance(overrides, dict) or selected not in overrides:
        raise WorkflowError("UNKNOWN_ENVIRONMENT", "The selected environment has no explicit override entry.")
    if not isinstance(overrides[selected], dict):
        raise WorkflowError("INVALID_CONFIGURATION", "Environment overrides must be JSON objects.")
    if set(overrides[selected]) & {"schema_version", "environment_overrides"}:
        raise WorkflowError("INVALID_OVERRIDE", "Environment overrides cannot replace schema metadata.")
    effective = deep_merge(raw, overrides[selected])
    effective["environment"] = selected
    effective["environment_overrides"] = {}
    schema = read_json(path.parent / "job_config.schema.json")
    Draft202012Validator.check_schema(schema)
    errors = sorted(Draft202012Validator(schema).iter_errors(effective), key=lambda e: str(list(e.path)))
    if errors:
        # Do not echo invalid instances or jsonschema's message (which embeds values).
        location = ".".join(str(part) for part in errors[0].absolute_schema_path)
        raise WorkflowError("INVALID_CONFIGURATION", f"Configuration violates schema rule: {location}.")
    _check_secrets(raw)
    if "__REQUIRED__" in json.dumps(effective):
        raise WorkflowError(
            "UNCONFIGURED_PLATFORM", "Replace required platform placeholders before execution."
        )
    if notebook and notebook not in effective["notebooks"]:
        raise WorkflowError("MISSING_NOTEBOOK", "The current notebook is not defined in the sidecar.")
    platform = effective["runtime"]["platform"]
    if platform != "local" and (
        effective["control_store"]["backend"] != "delta"
        or effective["storage"]["backend"] != "delta"
        or not effective["locking"]["enabled"]
    ):
        raise WorkflowError(
            "UNSAFE_CLOUD_STORE", "Cloud runs require Delta artifacts, Delta control, and leases."
        )
    if platform != "local" and effective["locking"]["credential"] == "development":
        raise WorkflowError(
            "UNSAFE_CLOUD_IDENTITY", "Cloud runs require an explicit production identity provider."
        )
    if platform != "local":
        resolve_lakehouse_paths(effective)
        validate_lock_location(effective["locking"])
    if platform == "fabric" and effective["runtime"]["scheduling"] != "barrier":
        raise WorkflowError(
            "UNSAFE_SHARED_SESSION_SCHEDULING", "Fabric requires native runMultiple barrier scheduling."
        )
    if platform == "local" and effective["control_store"]["backend"] != "sqlite":
        raise WorkflowError("UNSAFE_LOCAL_STORE", "The subprocess reference runner requires SQLite control.")
    if effective["runtime"]["fault_injection"] and not effective["runtime"]["allow_fault_injection"]:
        raise WorkflowError("FAULT_INJECTION_DISABLED", "Fault injection must be explicitly enabled.")
    if effective["force_restart"]["enabled"] and effective["force_rerun"]:
        raise WorkflowError("CONFLICTING_MODES", "Force restart and force rerun are mutually exclusive.")
    for source in effective["sources"].values():
        if source["format"] == "xml" and source["schema"] != schema_for(source["xml"]["contract"]):
            raise WorkflowError(
                "XML_SCHEMA_MISMATCH", "The source schema must match its canonical XML contract."
            )
        if source.get("secret_ref") and source["secret_ref"] not in effective["secret_references"]:
            raise WorkflowError("INVALID_SECRET_REFERENCE", "A source references an undefined secret.")
    reference = effective["notifications"]["secret_ref"]
    if reference and reference not in effective["secret_references"]:
        raise WorkflowError("INVALID_SECRET_REFERENCE", "Notifications reference an undefined secret.")
    code = hashlib.sha256()
    files = sorted(Path(__file__).parent.glob("*.py"))
    files += sorted((path.parent / "notebooks").glob("*.ipynb"))
    for source in files:
        code.update(source.name.encode())
        code.update(source.read_text(encoding="utf-8").replace("\r\n", "\n").encode())
    return Configuration(effective, path, fingerprint(effective), code.hexdigest())
