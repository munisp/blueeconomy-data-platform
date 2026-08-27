from __future__ import annotations

import pytest

from blueeconomy_data_platform.segregation import LakehouseScope
from blueeconomy_data_platform.storage import (
    StorageConfigurationError,
    load_adls_config,
    resolve_lakehouse_root,
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
    env["BLUEECONOMY_STORAGE_BACKEND"] = "s3"
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
