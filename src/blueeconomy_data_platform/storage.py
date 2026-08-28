"""Environment-driven, cloud-agnostic storage configuration for lakehouse roots.

Lakehouse roots are resolved from environment variables only; no endpoint,
account name, bucket, filesystem or credential is hardcoded. This module is the
only place cloud specifics live: segregation, medallion and access-policy
layers consume the resolved URIs and never branch on a provider. Supported
backends, selected by ``BLUEECONOMY_STORAGE_BACKEND``:

- ``adls`` — Azure Data Lake Storage Gen2 via ``abfs://`` URIs, with the Azure
  Government cloud selected through ``BLUEECONOMY_AZURE_CLOUD=AzureUSGovernment``.
- ``s3`` — S3-compatible object storage (AWS S3, MinIO, Ceph, or GCS via the
  S3-interoperability endpoint) via ``s3://`` URIs, with endpoint, region and
  transport-security coordinates from the environment.
- ``local-gated`` — a local filesystem backend that exists only behind an
  explicit opt-in variable for approved development and conformance runs,
  mirroring the Kafka localhost gate.

Credentials are never embedded in URIs and never returned by this module; they
are injected by the environment at deployment time (managed/workload identity
for ADLS, the standard ``AWS_*`` credential chain for S3-compatible storage).
The historical backend values ``adls-gen2`` and ``local`` remain accepted as
aliases of the canonical names so existing deployments keep resolving.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from blueeconomy_data_platform.segregation import LakehouseScope

AZURE_CLOUDS: dict[str, str] = {
    "AzureCloud": "dfs.core.windows.net",
    "AzureUSGovernment": "dfs.core.usgovcloudapi.net",
}
ACCOUNT_NAME_PATTERN = re.compile(r"^[a-z0-9]{3,24}$")
FILESYSTEM_NAME_PATTERN = re.compile(r"^[a-z0-9]([a-z0-9-]{1,61}[a-z0-9])?$")
S3_BUCKET_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
S3_IP_ADDRESS_PATTERN = re.compile(r"^[0-9]{1,3}(\.[0-9]{1,3}){3}$")
S3_REGION_PATTERN = re.compile(r"^[a-z]{2}(-gov)?-[a-z]+-[0-9]$")
S3_MAX_KEY_BYTES = 1024

ENV_BACKEND = "BLUEECONOMY_STORAGE_BACKEND"
ENV_AZURE_CLOUD = "BLUEECONOMY_AZURE_CLOUD"
ENV_STORAGE_ACCOUNT = "BLUEECONOMY_STORAGE_ACCOUNT"
ENV_STORAGE_FILESYSTEM = "BLUEECONOMY_STORAGE_FILESYSTEM"
ENV_LOCAL_ROOT = "BLUEECONOMY_LOCAL_LAKEHOUSE_ROOT"
ENV_ALLOW_LOCAL = "BLUEECONOMY_ALLOW_LOCAL_STORAGE"
ENV_S3_BUCKET = "BLUEECONOMY_S3_BUCKET"
ENV_S3_REGION = "BLUEECONOMY_S3_REGION"
ENV_S3_ENDPOINT_URL = "BLUEECONOMY_S3_ENDPOINT_URL"
ENV_S3_SECURE = "BLUEECONOMY_S3_SECURE"

BACKEND_ADLS = "adls"
BACKEND_S3 = "s3"
BACKEND_LOCAL = "local-gated"
SUPPORTED_BACKENDS = (BACKEND_ADLS, BACKEND_S3, BACKEND_LOCAL)
# Historical names accepted so existing deployments keep resolving unchanged.
BACKEND_ALIASES = {"adls-gen2": BACKEND_ADLS, "local": BACKEND_LOCAL}


class StorageConfigurationError(ValueError):
    """Raised when storage configuration is absent or invalid; access fails closed."""


def _require_env(env: Mapping[str, str], name: str) -> str:
    value = env.get(name, "")
    if not value or value != value.strip():
        raise StorageConfigurationError(f"environment variable {name} must be set and canonical")
    return value


def _canonical_backend(raw_backend: str) -> str:
    backend = BACKEND_ALIASES.get(raw_backend, raw_backend)
    if not backend:
        raise StorageConfigurationError(
            f"{ENV_BACKEND} is not set; refusing to assume a storage backend"
        )
    if backend not in SUPPORTED_BACKENDS:
        raise StorageConfigurationError(
            f"{ENV_BACKEND}={raw_backend!r} is not a supported backend "
            f"({', '.join(repr(item) for item in SUPPORTED_BACKENDS)})"
        )
    return backend


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


@dataclass(frozen=True)
class S3Config:
    """Validated S3-compatible storage coordinates (bucket, region, endpoint).

    ``endpoint_url`` is unset for AWS S3 (the regional AWS endpoint is derived
    by the storage client) and set to the deployment endpoint for MinIO, Ceph
    or the GCS S3-interoperability API. ``secure`` selects TLS transport and
    may only be disabled against an explicit custom endpoint.
    """

    bucket: str
    region: str
    endpoint_url: str | None
    secure: bool

    @property
    def s3_base_uri(self) -> str:
        return f"s3://{self.bucket}"

    def storage_options(self) -> dict[str, str]:
        """Non-secret deltalake/object_store options derived from the configuration.

        Credentials are deliberately absent: the object_store client resolves
        them from the standard environment credential chain at runtime.
        """
        options = {"AWS_REGION": self.region}
        if self.endpoint_url is not None:
            options["AWS_ENDPOINT_URL"] = self.endpoint_url
        if not self.secure:
            options["AWS_ALLOW_HTTP"] = "true"
        return options


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


def validate_s3_bucket_name(bucket: str) -> str:
    """Validate an S3 bucket name, failing closed on any rule violation."""
    if not S3_BUCKET_PATTERN.fullmatch(bucket):
        raise StorageConfigurationError(
            "S3 bucket names must be 3-63 characters of lowercase letters, digits, dots "
            "and hyphens, starting and ending with a letter or digit"
        )
    if S3_IP_ADDRESS_PATTERN.fullmatch(bucket):
        raise StorageConfigurationError("S3 bucket names must not look like an IP address")
    if ".." in bucket or ".-" in bucket or "-." in bucket:
        raise StorageConfigurationError(
            "S3 bucket names must not contain consecutive dots or dot-hyphen adjacency"
        )
    return bucket


def validate_s3_key(key: str) -> str:
    """Validate an S3 object key or key prefix, failing closed on any violation."""
    if not key or key.startswith("/"):
        raise StorageConfigurationError("S3 keys must be non-empty and must not start with '/'")
    if len(key.encode("utf-8")) > S3_MAX_KEY_BYTES:
        raise StorageConfigurationError(f"S3 keys must not exceed {S3_MAX_KEY_BYTES} UTF-8 bytes")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in key):
        raise StorageConfigurationError("S3 keys must not contain control characters")
    segments = key.split("/")
    if any(segment in {"", ".", ".."} for segment in segments):
        raise StorageConfigurationError("S3 keys must not contain empty, '.' or '..' path segments")
    return key


def validate_s3_uri(uri: str) -> tuple[str, str]:
    """Validate a credential-free ``s3://bucket/key`` URI and return ``(bucket, key)``."""
    if not uri or uri != uri.strip():
        raise StorageConfigurationError("S3 URI must be canonical non-empty text")
    parsed = urlsplit(uri)
    if parsed.scheme != "s3":
        raise StorageConfigurationError(f"S3 URI {uri!r} must use the s3:// scheme")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise StorageConfigurationError(
            "S3 URIs must not contain embedded credentials, query parameters or fragments"
        )
    bucket = validate_s3_bucket_name(parsed.netloc)
    if not parsed.path.startswith("/"):
        raise StorageConfigurationError(f"S3 URI {uri!r} must name an object key")
    key = validate_s3_key(parsed.path[1:])
    return bucket, key


def _validate_s3_endpoint(endpoint_url: str, secure: bool) -> str:
    parsed = urlsplit(endpoint_url)
    expected_scheme = "https" if secure else "http"
    if parsed.scheme != expected_scheme:
        raise StorageConfigurationError(
            f"{ENV_S3_ENDPOINT_URL} must use the {expected_scheme}:// scheme when "
            f"{ENV_S3_SECURE}={str(secure).lower()}"
        )
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise StorageConfigurationError(
            f"{ENV_S3_ENDPOINT_URL} must not contain credentials, query parameters or fragments"
        )
    if parsed.path not in ("", "/"):
        raise StorageConfigurationError(f"{ENV_S3_ENDPOINT_URL} must not contain a path")
    if parsed.hostname is None or not parsed.hostname:
        raise StorageConfigurationError(f"{ENV_S3_ENDPOINT_URL} must name a valid host")
    try:
        port = parsed.port
    except ValueError as error:
        raise StorageConfigurationError(f"{ENV_S3_ENDPOINT_URL} has an invalid port") from error
    if port is not None and not 1 <= port <= 65535:
        raise StorageConfigurationError(f"{ENV_S3_ENDPOINT_URL} has an invalid port")
    return endpoint_url


def load_s3_config(env: Mapping[str, str]) -> S3Config:
    """Load and validate S3-compatible coordinates, failing closed on any gap."""
    bucket = validate_s3_bucket_name(_require_env(env, ENV_S3_BUCKET))
    region = _require_env(env, ENV_S3_REGION)
    if not S3_REGION_PATTERN.fullmatch(region):
        raise StorageConfigurationError(
            f"{ENV_S3_REGION} must be a valid region identifier (for example us-east-1)"
        )
    secure_raw = _require_env(env, ENV_S3_SECURE)
    if secure_raw not in ("true", "false"):
        raise StorageConfigurationError(f"{ENV_S3_SECURE} must be exactly 'true' or 'false'")
    secure = secure_raw == "true"
    endpoint_raw = env.get(ENV_S3_ENDPOINT_URL, "")
    endpoint_url: str | None = None
    if endpoint_raw:
        if endpoint_raw != endpoint_raw.strip():
            raise StorageConfigurationError(f"{ENV_S3_ENDPOINT_URL} must be canonical")
        endpoint_url = _validate_s3_endpoint(endpoint_raw, secure)
    elif not secure:
        raise StorageConfigurationError(
            f"{ENV_S3_SECURE}=false is only permitted against an explicit "
            f"{ENV_S3_ENDPOINT_URL} (for example a MinIO deployment); "
            "AWS S3 transport is always TLS"
        )
    return S3Config(bucket=bucket, region=region, endpoint_url=endpoint_url, secure=secure)


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

    Fails closed when the backend is unset or unknown, when backend
    coordinates are incomplete, or when the local backend has not been
    explicitly gated. The returned URI contains no credentials.
    """
    if not isinstance(scope, LakehouseScope):
        raise StorageConfigurationError("lakehouse scope must be a LakehouseScope value")
    environment = os.environ if env is None else env
    backend = _canonical_backend(environment.get(ENV_BACKEND, ""))
    if backend == BACKEND_ADLS:
        base = load_adls_config(environment).abfs_base_uri
    elif backend == BACKEND_S3:
        base = load_s3_config(environment).s3_base_uri
    else:
        base = load_local_root(environment)
    return f"{base}/{scope.layer_prefix}"


def resolve_storage_options(env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Resolve non-secret storage-client options for the configured backend.

    Segregation and medallion layers pass these options to deltalake so cloud
    specifics stay in this module. ADLS Gen2 and the gated local backend need
    no extra options (ADLS credentials arrive through the ambient Azure
    identity environment); the S3 backend maps its validated coordinates to
    the object_store ``AWS_*`` option names. Credentials are never included.
    """
    environment = os.environ if env is None else env
    backend = _canonical_backend(environment.get(ENV_BACKEND, ""))
    if backend == BACKEND_S3:
        return load_s3_config(environment).storage_options()
    if backend == BACKEND_ADLS:
        load_adls_config(environment)
        return {}
    load_local_root(environment)
    return {}
