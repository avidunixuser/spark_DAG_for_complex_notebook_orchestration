from __future__ import annotations

import base64
import json
import time
from collections.abc import Callable
from pathlib import PurePosixPath
from typing import Any, Protocol
from urllib.parse import urlsplit

from .model import Category, CollisionError, WorkflowError, fingerprint


class Lease(Protocol):
    def check(self) -> None: ...
    def release(self) -> None: ...
    def retain(self) -> None: ...


class LeaseFactory(Protocol):
    def acquire(self, name: str, *, wait: bool = True) -> Lease: ...


class LocalLease:
    """SQLite's persistent RUNNING row and BEGIN IMMEDIATE provide local exclusion."""

    def check(self) -> None:
        pass

    def release(self) -> None:
        pass

    def retain(self) -> None:
        pass


class LocalLeases:
    def acquire(self, name: str, *, wait: bool = True) -> Lease:
        return LocalLease()


class DataLakeFileLease:
    def __init__(self, lease: Any):
        self.lease = lease
        self.retained = False

    def check(self) -> None:
        try:
            self.lease.renew()
        except Exception:
            raise WorkflowError(
                "LEASE_LOST",
                "Lock ownership cannot be confirmed. Stop and reconcile all workers.",
                Category.INFRASTRUCTURE,
            ) from None

    def release(self) -> None:
        if not self.retained:
            try:
                self.lease.release()
            except Exception:
                raise WorkflowError(
                    "LEASE_RELEASE_FAILED",
                    "The lease remains subject to operator reconciliation.",
                    Category.INFRASTRUCTURE,
                ) from None

    def retain(self) -> None:
        self.retained = True


class FabricStorageCredential:
    def __init__(self, credentials: Any):
        self.credentials = credentials

    def get_token(self, *scopes: str, **kwargs: Any) -> Any:
        from azure.core.credentials import AccessToken

        if any(scope != "https://storage.azure.com/.default" for scope in scopes):
            raise WorkflowError("INVALID_TOKEN_SCOPE", "The Fabric credential is scoped only to storage.")
        token = self.credentials.getToken("storage")
        try:
            payload = token.split(".")[1]
            expiry = int(json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))["exp"])
        except (IndexError, ValueError, KeyError, TypeError):
            raise WorkflowError(
                "INVALID_STORAGE_TOKEN", "The storage identity did not return a valid expiry."
            ) from None
        if expiry <= time.time():
            raise WorkflowError("EXPIRED_STORAGE_TOKEN", "The storage identity returned an expired token.")
        # Reading expiry is not authentication; Azure Storage verifies the token's signature.
        return AccessToken(token, expiry)


def is_dfs_host(host: str) -> bool:
    return any(
        host.endswith(suffix)
        for suffix in (
            ".dfs.core.windows.net",
            ".dfs.core.usgovcloudapi.net",
            ".dfs.core.chinacloudapi.cn",
            ".dfs.fabric.microsoft.com",
        )
    )


def validate_lock_location(settings: dict[str, Any]) -> None:
    address = urlsplit(settings["account_url"])
    host = address.hostname or ""
    if (
        address.scheme != "https"
        or not host
        or not is_dfs_host(host)
        or address.path not in {"", "/"}
        or address.query
        or address.fragment
        or address.username
        or address.port not in {None, 443}
    ):
        raise WorkflowError("INVALID_LEASE_ENDPOINT", "Use an HTTPS ADLS Gen2 DFS account endpoint.")
    directory = PurePosixPath(settings["directory"])
    if (
        directory.is_absolute()
        or not directory.parts
        or any(part in {"", ".", ".."} for part in settings["directory"].split("/"))
        or "\\" in settings["directory"]
        or not settings["file_system"]
        or any(char in settings["file_system"] for char in "/\\?#")
    ):
        raise WorkflowError(
            "INVALID_LOCK_DIRECTORY", "Use an existing file system and a relative lock directory."
        )
    if host.endswith(".fabric.microsoft.com") and (len(directory.parts) < 3 or directory.parts[1] != "Files"):
        raise WorkflowError(
            "INVALID_LOCK_DIRECTORY", "OneLake lock files must be below an existing lakehouse's Files folder."
        )


class ADLSGen2Leases:
    def __init__(
        self,
        settings: dict[str, Any],
        *,
        file_system_client: Any = None,
        lease_client_factory: Callable[[Any], Any] | None = None,
        notebookutils: Any = None,
    ):
        from azure.storage.filedatalake import DataLakeLeaseClient, DataLakeServiceClient

        self.settings = settings
        self.lease_client_factory = lease_client_factory or DataLakeLeaseClient
        if file_system_client is not None:
            self.file_system = file_system_client
            return
        from azure.identity import (
            DefaultAzureCredential,
            ManagedIdentityCredential,
            WorkloadIdentityCredential,
        )

        validate_lock_location(settings)
        provider = settings["credential"]
        if provider == "managed_identity":
            credential = ManagedIdentityCredential(client_id=settings["client_id"] or None)
        elif provider == "workload_identity":
            credential = WorkloadIdentityCredential()
        elif provider == "fabric_user":
            if notebookutils is None:
                raise WorkflowError(
                    "MISSING_FABRIC_CONTEXT", "Fabric storage authentication needs notebookutils."
                )
            credential = FabricStorageCredential(notebookutils.credentials)
        else:
            credential = DefaultAzureCredential()
        service = DataLakeServiceClient(
            settings["account_url"],
            credential=credential,
            connection_timeout=15,
            read_timeout=30,
            retry_total=2,
        )
        self.file_system = service.get_file_system_client(settings["file_system"])

    def acquire(self, name: str, *, wait: bool = True) -> DataLakeFileLease:
        from azure.core import MatchConditions
        from azure.core.exceptions import (
            HttpResponseError,
            ResourceExistsError,
            ResourceModifiedError,
            ResourceNotFoundError,
        )

        file = self.file_system.get_file_client(f"{self.settings['directory']}/{fingerprint(name)}.lock")
        try:
            file.get_file_properties()
        except ResourceNotFoundError:
            try:
                file.create_file(etag="*", match_condition=MatchConditions.IfMissing)
            except (ResourceExistsError, ResourceModifiedError):
                # Conditional creation cannot replace another owner's lock file.
                file.get_file_properties()
        deadline = time.monotonic() + (self.settings["wait_seconds"] if wait else 0)
        while True:
            try:
                lease = self.lease_client_factory(file)
                lease.acquire(lease_duration=-1)
                return DataLakeFileLease(lease)
            except HttpResponseError as error:
                if error.status_code not in {409, 412}:
                    raise WorkflowError(
                        "LEASE_ACQUIRE_FAILED",
                        "The storage lease service rejected the operation.",
                        Category.INFRASTRUCTURE,
                    ) from None
                if time.monotonic() >= deadline:
                    raise CollisionError() from None
                time.sleep(min(0.2, max(0, deadline - time.monotonic())))
