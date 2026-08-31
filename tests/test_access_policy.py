from __future__ import annotations

import pytest

from blueeconomy_data_platform.access_policy import (
    AccessDeniedError,
    Clearance,
    allowed_schemas,
    authorize_read,
    authorize_write,
    clearance_permits,
    filter_records_by_clearance,
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


def test_clearance_levels_are_strictly_ordered() -> None:
    assert Clearance.UNCLASSIFIED < Clearance.RESTRICTED < Clearance.CONFIDENTIAL
    assert Clearance.CONFIDENTIAL < Clearance.SECRET
    assert Clearance.from_label("secret") is Clearance.SECRET
    with pytest.raises(ValueError, match="fails closed"):
        Clearance.from_label("top-secret")
    with pytest.raises(ValueError):
        Clearance.from_label(" CONFIDENTIAL")


def test_phase2_scope_roles_read_only_their_scope() -> None:
    authorize_read(["seafarer-registry"], "seafarer_bronze", clearance="CONFIDENTIAL")
    authorize_read(["fisheries-operations"], "fisheries_gold", clearance="RESTRICTED")
    authorize_read(["isr-analyst"], "isr_silver", clearance=Clearance.SECRET)
    for role, schema in (
        ("seafarer-registry", "fisheries_bronze"),
        ("fisheries-operations", "isr_gold"),
        ("isr-analyst", "seafarer_gold"),
        ("isr-analyst", "cvff_gold"),
        ("insurer-aggregator", "platform_bronze"),
    ):
        with pytest.raises(AccessDeniedError):
            authorize_read([role], schema, clearance="SECRET")


def test_insurer_aggregator_reads_only_isr_gold_aggregates() -> None:
    authorize_read(["insurer-aggregator"], "isr_gold", clearance="CONFIDENTIAL")
    for schema in ("isr_bronze", "isr_silver"):
        with pytest.raises(AccessDeniedError):
            authorize_read(["insurer-aggregator"], schema, clearance="SECRET")


def test_clearance_floor_enforcement_fails_closed() -> None:
    # Missing clearance is denied above the UNCLASSIFIED floor.
    with pytest.raises(AccessDeniedError, match="requires a clearance claim"):
        authorize_read(["isr-analyst"], "isr_bronze")
    with pytest.raises(AccessDeniedError, match="requires a clearance claim"):
        authorize_read(["seafarer-registry"], "seafarer_silver")
    # Insufficient clearance is denied; sufficient clearance is granted.
    with pytest.raises(AccessDeniedError, match="below the SECRET floor"):
        authorize_read(["isr-analyst"], "isr_silver", "CONFIDENTIAL")
    authorize_read(["isr-analyst"], "isr_bronze", clearance="SECRET")
    with pytest.raises(AccessDeniedError, match="below the CONFIDENTIAL floor"):
        authorize_read(["insurer-aggregator"], "isr_gold", clearance="RESTRICTED")
    # Unknown clearance labels fail closed even when a role grant exists.
    with pytest.raises(AccessDeniedError, match="fails closed"):
        authorize_read(["isr-analyst"], "isr_gold", clearance="cosmic")
    with pytest.raises(AccessDeniedError):
        authorize_read(["fmmbe-oversight"], "platform_bronze", clearance="cosmic")
    # Existing UNCLASSIFIED-floor schemas keep working without a clearance claim.
    authorize_read(["fmmbe-oversight"], "platform_bronze")
    authorize_read(["independent-auditor"], "cvff_gold")


def test_row_level_clearance_filtering() -> None:
    assert clearance_permits("SECRET", "CONFIDENTIAL")
    assert clearance_permits(Clearance.RESTRICTED, "UNCLASSIFIED")
    assert not clearance_permits("CONFIDENTIAL", "SECRET")
    assert not clearance_permits("cosmic", "SECRET")
    assert not clearance_permits("SECRET", "top-secret")
    records = [
        {"event_id": "a", "record_classification": "CONFIDENTIAL"},
        {"event_id": "b", "record_classification": "SECRET"},
        {"event_id": "c"},  # unlabelled is withheld
        {"event_id": "d", "record_classification": "bogus"},
    ]
    visible = filter_records_by_clearance(records, "CONFIDENTIAL")
    assert [row["event_id"] for row in visible] == ["a"]
    assert filter_records_by_clearance(records, "SECRET") == records[:2]


# ---------------------------------------------------------------------------
# Phase-8 scope roles
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("role", ("mrv-reader", "mrv-verifier", "mrv-flag-admin"))
def test_phase8_mrv_roles_read_only_mrv_schemas(role: str) -> None:
    # MRV evidence is CONFIDENTIAL throughout: the clearance floor applies.
    for schema in ("mrv_bronze", "mrv_silver", "mrv_gold"):
        authorize_read([role], schema, clearance=Clearance.CONFIDENTIAL)
        with pytest.raises(AccessDeniedError):
            authorize_read([role], schema)
    for schema in (
        "platform_gold",
        "cvff_gold",
        "bluecarbon_gold",
        "isr_gold",
    ):
        with pytest.raises(AccessDeniedError):
            authorize_read([role], schema, clearance=Clearance.SECRET)
    with pytest.raises(AccessDeniedError, match="read-only"):
        authorize_write([role], "mrv_bronze")


@pytest.mark.parametrize("role", ("bc-registry-admin", "bc-auditor"))
def test_phase8_bluecarbon_roles_read_only_bluecarbon_schemas(role: str) -> None:
    # Evidence layers sit at the CONFIDENTIAL floor; the gold public_registry
    # projection is the scope's only UNCLASSIFIED (public) artefact.
    for schema in ("bluecarbon_bronze", "bluecarbon_silver"):
        authorize_read([role], schema, clearance=Clearance.CONFIDENTIAL)
        with pytest.raises(AccessDeniedError):
            authorize_read([role], schema)
    authorize_read([role], "bluecarbon_gold")
    for schema in ("mrv_gold", "cvff_gold", "platform_bronze"):
        with pytest.raises(AccessDeniedError):
            authorize_read([role], schema, clearance=Clearance.SECRET)


def test_phase8_stats_reader_reads_platform_gold_only() -> None:
    authorize_read(["stats-reader"], "platform_gold")
    for schema in ("platform_bronze", "platform_silver", "mrv_gold", "bluecarbon_gold"):
        with pytest.raises(AccessDeniedError):
            authorize_read(["stats-reader"], schema)
