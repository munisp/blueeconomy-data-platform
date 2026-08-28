from __future__ import annotations

import pytest

from blueeconomy_data_platform.segregation import LakehouseScope
from blueeconomy_data_platform.storage import (
    StorageConfigurationError,
    load_adls_config,
    load_s3_config,
    resolve_lakehouse_root,
    resolve_storage_options,
    validate_s3_uri,
)


def gov_env() -> dict[str, str]:
    return {
        "BLUEECONOMY_STORAGE_BACKEND": "adls-gen2",
        "BLUEECONOMY_AZURE_CLOUD": "AzureUSGovernment",
        "BLUEECONOMY_STORAGE_ACCOUNT": "cvffsegregatedstore",
        "BLUEECONOMY_STORAGE_FILESYSTEM": "lakehouse",
    }


def test_azure_government_abfs_roots() -> None:
    env = gov_env()
    cvff_root = resolve_lakehouse_root(LakehouseScope.CVFF, env)
    assert cvff_root == ("abfs://lakehouse@cvffsegregatedstore.dfs.core.usgovcloudapi.net/cvff")
    platform_root = resolve_lakehouse_root(LakehouseScope.PLATFORM, env)
    assert platform_root.endswith(".dfs.core.usgovcloudapi.net/platform")
    assert "windows.net" not in cvff_root


def test_commercial_cloud_suffix() -> None:
    env = gov_env()
    env["BLUEECONOMY_AZURE_CLOUD"] = "AzureCloud"
    assert resolve_lakehouse_root(LakehouseScope.CVFF, env).startswith(
        "abfs://lakehouse@cvffsegregatedstore.dfs.core.windows.net/cvff"
    )


def test_missing_backend_fails_closed() -> None:
    with pytest.raises(StorageConfigurationError, match="BLUEECONOMY_STORAGE_BACKEND"):
        resolve_lakehouse_root(LakehouseScope.CVFF, {})


def test_unknown_backend_fails_closed() -> None:
    env = gov_env()
    env["BLUEECONOMY_STORAGE_BACKEND"] = "gcs-native"
    with pytest.raises(StorageConfigurationError, match="not a supported backend"):
        resolve_lakehouse_root(LakehouseScope.CVFF, env)


def test_missing_adls_coordinates_fail_closed() -> None:
    for missing in (
        "BLUEECONOMY_AZURE_CLOUD",
        "BLUEECONOMY_STORAGE_ACCOUNT",
        "BLUEECONOMY_STORAGE_FILESYSTEM",
    ):
        env = gov_env()
        del env[missing]
        with pytest.raises(StorageConfigurationError, match=missing):
            resolve_lakehouse_root(LakehouseScope.CVFF, env)


def test_invalid_cloud_and_account_fail_closed() -> None:
    env = gov_env()
    env["BLUEECONOMY_AZURE_CLOUD"] = "AzureChinaCloud"
    with pytest.raises(StorageConfigurationError, match="AzureUSGovernment"):
        load_adls_config(env)
    env = gov_env()
    env["BLUEECONOMY_STORAGE_ACCOUNT"] = "Bad_Account"
    with pytest.raises(StorageConfigurationError, match="storage account name"):
        load_adls_config(env)


def test_local_backend_requires_explicit_gate(tmp_path_factory: pytest.TempPathFactory) -> None:
    root = tmp_path_factory.mktemp("lakehouse")
    env = {"BLUEECONOMY_STORAGE_BACKEND": "local"}
    with pytest.raises(StorageConfigurationError, match="explicit development opt-in"):
        resolve_lakehouse_root(LakehouseScope.CVFF, env)
    env["BLUEECONOMY_ALLOW_LOCAL_STORAGE"] = "true"
    with pytest.raises(StorageConfigurationError, match="BLUEECONOMY_LOCAL_LAKEHOUSE_ROOT"):
        resolve_lakehouse_root(LakehouseScope.CVFF, env)
    env["BLUEECONOMY_LOCAL_LAKEHOUSE_ROOT"] = str(root)
    assert resolve_lakehouse_root(LakehouseScope.CVFF, env) == f"{root}/cvff"


def test_local_backend_rejects_relative_root() -> None:
    env = {
        "BLUEECONOMY_STORAGE_BACKEND": "local",
        "BLUEECONOMY_ALLOW_LOCAL_STORAGE": "true",
        "BLUEECONOMY_LOCAL_LAKEHOUSE_ROOT": "relative/path",
    }
    with pytest.raises(StorageConfigurationError, match="absolute path"):
        resolve_lakehouse_root(LakehouseScope.CVFF, env)


def test_resolved_uris_carry_no_credentials() -> None:
    uri = resolve_lakehouse_root(LakehouseScope.CVFF, gov_env())
    assert "@" not in uri.split("//", 1)[1].split("/", 1)[1]
    assert "?" not in uri and "#" not in uri


def s3_env(**overrides: str) -> dict[str, str]:
    env = {
        "BLUEECONOMY_STORAGE_BACKEND": "s3",
        "BLUEECONOMY_S3_BUCKET": "blueeconomy-lakehouse",
        "BLUEECONOMY_S3_REGION": "us-east-1",
        "BLUEECONOMY_S3_SECURE": "true",
    }
    env.update(overrides)
    return env


def test_canonical_and_legacy_backend_names_resolve() -> None:
    env = gov_env()
    env["BLUEECONOMY_STORAGE_BACKEND"] = "adls"
    assert resolve_lakehouse_root(LakehouseScope.CVFF, env).startswith("abfs://")
    env["BLUEECONOMY_STORAGE_BACKEND"] = "adls-gen2"
    assert resolve_lakehouse_root(LakehouseScope.CVFF, env).startswith("abfs://")


def test_s3_backend_resolves_credential_free_bucket_root() -> None:
    cvff_root = resolve_lakehouse_root(LakehouseScope.CVFF, s3_env())
    assert cvff_root == "s3://blueeconomy-lakehouse/cvff"
    assert "@" not in cvff_root and "?" not in cvff_root and "#" not in cvff_root
    for scope in (LakehouseScope.SEAFARER, LakehouseScope.FISHERIES, LakehouseScope.ISR):
        assert (
            resolve_lakehouse_root(scope, s3_env()) == f"s3://blueeconomy-lakehouse/{scope.value}"
        )


def test_s3_minio_endpoint_and_insecure_gate() -> None:
    env = s3_env(**{"BLUEECONOMY_S3_ENDPOINT_URL": "https://minio.storage.example:9000"})
    config = load_s3_config(env)
    assert config.endpoint_url == "https://minio.storage.example:9000"
    assert config.storage_options() == {
        "AWS_REGION": "us-east-1",
        "AWS_ENDPOINT_URL": "https://minio.storage.example:9000",
    }
    insecure = s3_env(
        **{
            "BLUEECONOMY_S3_ENDPOINT_URL": "http://minio.minio.svc.cluster.local:9000",
            "BLUEECONOMY_S3_SECURE": "false",
        }
    )
    assert load_s3_config(insecure).storage_options()["AWS_ALLOW_HTTP"] == "true"


def test_s3_misconfiguration_fails_closed() -> None:
    for missing in ("BLUEECONOMY_S3_BUCKET", "BLUEECONOMY_S3_REGION", "BLUEECONOMY_S3_SECURE"):
        env = s3_env()
        del env[missing]
        with pytest.raises(StorageConfigurationError, match=missing):
            resolve_lakehouse_root(LakehouseScope.CVFF, env)
    with pytest.raises(StorageConfigurationError, match="S3 bucket"):
        load_s3_config(s3_env(BLUEECONOMY_S3_BUCKET="Bad_Bucket"))
    with pytest.raises(StorageConfigurationError, match="IP address"):
        load_s3_config(s3_env(BLUEECONOMY_S3_BUCKET="192.168.0.1"))
    with pytest.raises(StorageConfigurationError, match="region"):
        load_s3_config(s3_env(BLUEECONOMY_S3_REGION="US-EAST-1"))
    with pytest.raises(StorageConfigurationError, match="'true' or 'false'"):
        load_s3_config(s3_env(BLUEECONOMY_S3_SECURE="yes"))
    # Plain HTTP is only allowed against an explicit custom endpoint.
    with pytest.raises(StorageConfigurationError, match="AWS S3 transport is always TLS"):
        load_s3_config(s3_env(BLUEECONOMY_S3_SECURE="false"))
    with pytest.raises(StorageConfigurationError, match="https://"):
        load_s3_config(s3_env(BLUEECONOMY_S3_ENDPOINT_URL="http://minio.example:9000"))
    with pytest.raises(StorageConfigurationError, match="credentials"):
        load_s3_config(s3_env(BLUEECONOMY_S3_ENDPOINT_URL="https://key:secret@minio.example:9000"))


def test_s3_uri_validation_bucket_and_key_rules() -> None:
    assert validate_s3_uri("s3://blueeconomy-lakehouse/cvff/cvff_bronze/events") == (
        "blueeconomy-lakehouse",
        "cvff/cvff_bronze/events",
    )
    for bad in (
        "s3://bucket",  # missing key
        "s3://bucket//double-slash",  # empty segment
        "s3://bucket/../escape",  # dot-dot segment
        "s3://bucket//key",  # empty segment
        "s3://key:secret@bucket/key",  # embedded credentials
        "s3://bucket/key?versionId=1",  # query
        "s3://UPPERCASE/key",  # invalid bucket
        "https://bucket/key",  # wrong scheme
        "s3://bucket//leading",  # empty segment
    ):
        with pytest.raises(StorageConfigurationError):
            validate_s3_uri(bad)


def test_resolve_storage_options_per_backend() -> None:
    assert resolve_storage_options(gov_env()) == {}
    assert resolve_storage_options(s3_env()) == {"AWS_REGION": "us-east-1"}
    with pytest.raises(StorageConfigurationError):
        resolve_storage_options({"BLUEECONOMY_STORAGE_BACKEND": "s3"})
