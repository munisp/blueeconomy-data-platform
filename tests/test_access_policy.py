from __future__ import annotations

import pytest

from blueeconomy_data_platform.access_policy import (
    AccessDeniedError,
    allowed_schemas,
    authorize_read,
    authorize_write,
)

CVFF_ROLES = ("independent-auditor", "nimasa-approver", "cbn-observer")


@pytest.mark.parametrize("role", CVFF_ROLES)
@pytest.mark.parametrize("schema", ("cvff_bronze", "cvff_silver", "cvff_gold"))
def test_cvff_scoped_roles_read_cvff_schemas(role: str, schema: str) -> None:
    authorize_read([role], schema)


@pytest.mark.parametrize("role", CVFF_ROLES)
@pytest.mark.parametrize("schema", ("platform_bronze", "platform_silver", "platform_gold"))
def test_cvff_scoped_roles_cannot_read_platform_schemas(role: str, schema: str) -> None:
    with pytest.raises(AccessDeniedError):
        authorize_read([role], schema)


def test_fmmbe_oversight_reads_platform_but_not_cvff() -> None:
    authorize_read(["fmmbe-oversight"], "platform_bronze")
    for schema in ("cvff_bronze", "cvff_silver", "cvff_gold"):
        with pytest.raises(AccessDeniedError):
            authorize_read(["fmmbe-oversight"], schema)


def test_unknown_role_fails_closed() -> None:
    with pytest.raises(AccessDeniedError, match="unknown role"):
        authorize_read(["superuser"], "platform_bronze")
    with pytest.raises(AccessDeniedError, match="unknown role"):
        allowed_schemas(["nimasa-approver", "root"])


def test_empty_roles_fail_closed() -> None:
    with pytest.raises(AccessDeniedError):
        authorize_read([], "cvff_bronze")


def test_unknown_schema_fails_closed() -> None:
    with pytest.raises(AccessDeniedError):
        authorize_read(["independent-auditor"], "cvff_public")
    with pytest.raises(AccessDeniedError):
        authorize_read(["fmmbe-oversight"], "platform_internal")


def test_union_of_roles_widens_only_granted_scopes() -> None:
    granted = allowed_schemas(["independent-auditor", "fmmbe-oversight"])
    assert granted == {
        "cvff_bronze",
        "cvff_silver",
        "cvff_gold",
        "platform_bronze",
        "platform_silver",
        "platform_gold",
    }
    authorize_read(["independent-auditor", "fmmbe-oversight"], "cvff_gold")
    assert allowed_schemas(["fmmbe-oversight"]) == {
        "platform_bronze",
        "platform_silver",
        "platform_gold",
    }


def test_governance_roles_are_read_only() -> None:
    for role in (*CVFF_ROLES, "fmmbe-oversight"):
        with pytest.raises(AccessDeniedError, match="read-only"):
            authorize_write([role], "cvff_bronze")
        with pytest.raises(AccessDeniedError, match="read-only"):
            authorize_write([role], "platform_bronze")
