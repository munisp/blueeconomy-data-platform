"""Environment-driven storage configuration for ADLS Gen2 lakehouse roots.

Lakehouse roots are resolved from environment variables only; no endpoint,
account name, filesystem or credential is hardcoded. The production backend is
Azure Data Lake Storage Gen2 via ``abfs://`` URIs, with the Azure Government
cloud selected through ``BLUEECONOMY_AZURE_CLOUD=AzureUSGovernment``. A local
filesystem backend exists only behind an explicit opt-in variable for approved
development and conformance runs, mirroring the Kafka localhost gate.
Credentials are never embedded in URIs; they are injected by the environment
at deployment time.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from blueeconomy_data_platform.segregation import LakehouseScope

AZURE_CLOUDS: dict[str, str] = {
    "AzureCloud": "dfs.core.windows.net",
    "AzureUSGovernment": "dfs.core.usgovcloudapi.net",
}
ACCOUNT_NAME_PATTERN = re.compile(r"^[a-z0-9]{3,24}$")
FILESYSTEM_NAME_PATTERN = re.compile(r"^[a-z0-9]([a-z0-9-]{1,61}[a-z0-9])?$")

ENV_BACKEND = "BLUEECONOMY_STORAGE_BACKEND"
ENV_AZURE_CLOUD = "BLUEECONOMY_AZURE_CLOUD"
ENV_STORAGE_ACCOUNT = "BLUEECONOMY_STORAGE_ACCOUNT"
ENV_STORAGE_FILESYSTEM = "BLUEECONOMY_STORAGE_FILESYSTEM"
ENV_LOCAL_ROOT = "BLUEECONOMY_LOCAL_LAKEHOUSE_ROOT"
ENV_ALLOW_LOCAL = "BLUEECONOMY_ALLOW_LOCAL_STORAGE"

BACKEND_ADLS = "adls-gen2"
BACKEND_LOCAL = "local"


class StorageConfigurationError(ValueError):
    """Raised when storage configuration is absent or invalid; access fails closed."""


def _require_env(env: Mapping[str, str], name: str) -> str:
    value = env.get(name, "")
    if not value or value != value.strip():
        raise StorageConfigurationError(f"environment variable {name} must be set and canonical")
    return value


@dataclass(frozen=True)
class AdlsGen2Config:
    """Validated ADLS Gen2 account coordinates for one Azure cloud."""

    account: str
    filesystem: str
    cloud: str

    @property
    def endpoint_suffix(self) -> str:
        return AZURE_CLOUDS[self.cloud]

    @property
    def abfs_base_uri(self) -> str:
        return f"abfs://{self.filesystem}@{self.account}.{self.endpoint_suffix}"


def load_adls_config(env: Mapping[str, str]) -> AdlsGen2Config:
    """Load and validate ADLS Gen2 coordinates, failing closed on any gap."""
    cloud = _require_env(env, ENV_AZURE_CLOUD)
    if cloud not in AZURE_CLOUDS:
        raise StorageConfigurationError(
            f"{ENV_AZURE_CLOUD} must be one of {sorted(AZURE_CLOUDS)}; "
            "use AzureUSGovernment for the CVFF segregated lakehouse"
        )
    account = _require_env(env, ENV_STORAGE_ACCOUNT)
    if not ACCOUNT_NAME_PATTERN.fullmatch(account):
        raise StorageConfigurationError(
            f"{ENV_STORAGE_ACCOUNT} must be a valid storage account name (3-24 lowercase alnum)"
        )
    filesystem = _require_env(env, ENV_STORAGE_FILESYSTEM)
    if not FILESYSTEM_NAME_PATTERN.fullmatch(filesystem):
        raise StorageConfigurationError(
            f"{ENV_STORAGE_FILESYSTEM} must be a valid ADLS Gen2 filesystem name"
        )
    return AdlsGen2Config(account=account, filesystem=filesystem, cloud=cloud)


def load_local_root(env: Mapping[str, str]) -> str:
    """Resolve the explicitly gated local lakehouse root for development runs."""
    if env.get(ENV_ALLOW_LOCAL, "") != "true":
        raise StorageConfigurationError(
            f"local storage requires {ENV_ALLOW_LOCAL}=true as an explicit development opt-in"
        )
    root = Path(_require_env(env, ENV_LOCAL_ROOT))
    if not root.is_absolute():
        raise StorageConfigurationError(f"{ENV_LOCAL_ROOT} must be an absolute path")
    return str(root)


def resolve_lakehouse_root(scope: LakehouseScope, env: Mapping[str, str] | None = None) -> str:
    """Resolve the segregated scope root URI from environment configuration.

    Fails closed when the backend is unset or unknown, when ADLS coordinates
    are incomplete, or when the local backend has not been explicitly gated.
    The returned URI contains no credentials.
    """
    if not isinstance(scope, LakehouseScope):
        raise StorageConfigurationError("lakehouse scope must be a LakehouseScope value")
    environment = os.environ if env is None else env
    backend = environment.get(ENV_BACKEND, "")
    if backend == BACKEND_ADLS:
        base = load_adls_config(environment).abfs_base_uri
    elif backend == BACKEND_LOCAL:
        base = load_local_root(environment)
    elif not backend:
        raise StorageConfigurationError(
            f"{ENV_BACKEND} is not set; refusing to assume a storage backend"
        )
    else:
        raise StorageConfigurationError(
            f"{ENV_BACKEND}={backend!r} is not a supported backend "
            f"({BACKEND_ADLS!r} or {BACKEND_LOCAL!r})"
        )
    return f"{base}/{scope.layer_prefix}"
