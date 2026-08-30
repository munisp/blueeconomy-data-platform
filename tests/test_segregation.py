from __future__ import annotations

import pytest

from blueeconomy_data_platform.segregation import (
    BoundaryViolationError,
    LakehouseScope,
    SegregatedDeltaWriter,
    build_lakehouse_roots,
    enforce_event_scope,
    enforce_topic_scope,
    require_scope_table_uri,
    scope_for_classification,
    scope_for_topic,
)


def platform_event() -> dict[str, object]:
    return {"event_id": "evt-platform-1", "data_classification": "internal"}


def cvff_event() -> dict[str, object]:
    return {
        "event_id": "evt-cvff-1",
        "data_classification": "fiduciary_segregated",
    }


def test_classification_mapping_is_fail_closed() -> None:
    assert scope_for_classification("fiduciary_segregated") is LakehouseScope.CVFF
    for classification in ("public", "internal", "confidential", "restricted", "highly_restricted"):
        assert scope_for_classification(classification) is LakehouseScope.PLATFORM
    with pytest.raises(BoundaryViolationError):
        scope_for_classification("unknown-tag")


def test_topic_namespace_mapping_is_fail_closed() -> None:
    assert scope_for_topic("cvff.ledger.commitments") is LakehouseScope.CVFF
    assert scope_for_topic("ports.calls.v1") is LakehouseScope.PLATFORM
    assert scope_for_topic("ferries.manifest.v1") is LakehouseScope.PLATFORM
    with pytest.raises(BoundaryViolationError):
        scope_for_topic("blueeconomy.events.local")


def test_cvff_writer_rejects_platform_root_and_vice_versa() -> None:
    with pytest.raises(BoundaryViolationError, match="cvff scope root"):
        SegregatedDeltaWriter(LakehouseScope.CVFF, "/lakehouse/platform")
    with pytest.raises(BoundaryViolationError, match="platform scope root"):
        SegregatedDeltaWriter(LakehouseScope.PLATFORM, "/lakehouse/cvff")


def test_roots_use_segregated_layer_names() -> None:
    roots = build_lakehouse_roots(
        LakehouseScope.CVFF, "abfs://fs@acct.dfs.core.usgovcloudapi.net/cvff"
    )
    assert roots.bronze.endswith("/cvff/cvff_bronze/events")
    assert roots.silver.endswith("/cvff/cvff_silver/events")
    assert roots.gold.endswith("/cvff/cvff_gold/events")
    platform = build_lakehouse_roots(LakehouseScope.PLATFORM, "/lakehouse/platform")
    assert platform.bronze.endswith("/platform/platform_bronze/events")


def test_table_uri_boundary_guard() -> None:
    require_scope_table_uri(
        LakehouseScope.CVFF, "abfs://fs@acct.dfs.core.usgovcloudapi.net/cvff/cvff_bronze/events"
    )
    with pytest.raises(BoundaryViolationError):
        require_scope_table_uri(LakehouseScope.CVFF, "/lakehouse/platform/platform_bronze/events")
    with pytest.raises(BoundaryViolationError):
        require_scope_table_uri(LakehouseScope.PLATFORM, "/lakehouse/cvff/cvff_bronze/events")


def test_platform_writer_rejects_cvff_classified_event() -> None:
    writer = SegregatedDeltaWriter(LakehouseScope.PLATFORM, "/lakehouse/platform")
    with pytest.raises(BoundaryViolationError, match="platform writer cannot accept"):
        writer.guard_write("bronze", [cvff_event()])


def test_cvff_writer_rejects_platform_classified_event() -> None:
    writer = SegregatedDeltaWriter(LakehouseScope.CVFF, "/lakehouse/cvff")
    with pytest.raises(BoundaryViolationError, match="cvff writer cannot accept"):
        writer.guard_write("bronze", [platform_event()])


def test_cvff_writer_accepts_cvff_event_and_topic() -> None:
    writer = SegregatedDeltaWriter(LakehouseScope.CVFF, "/lakehouse/cvff")
    uri = writer.guard_write("bronze", [cvff_event()], kafka_topic="cvff.ledger.commitments")
    assert uri == "/lakehouse/cvff/cvff_bronze/events"


def test_writer_rejects_cross_boundary_topic() -> None:
    cvff_writer = SegregatedDeltaWriter(LakehouseScope.CVFF, "/lakehouse/cvff")
    with pytest.raises(BoundaryViolationError, match="cvff writer cannot consume"):
        cvff_writer.guard_write("bronze", [cvff_event()], kafka_topic="ports.calls.v1")
    platform_writer = SegregatedDeltaWriter(LakehouseScope.PLATFORM, "/lakehouse/platform")
    with pytest.raises(BoundaryViolationError, match="platform writer cannot consume"):
        platform_writer.guard_write(
            "bronze", [platform_event()], kafka_topic="cvff.ledger.commitments"
        )


def test_enforce_helpers_fail_closed_on_mixed_batch() -> None:
    with pytest.raises(BoundaryViolationError):
        enforce_event_scope([cvff_event(), platform_event()], LakehouseScope.CVFF)
    with pytest.raises(BoundaryViolationError):
        enforce_event_scope([platform_event()], LakehouseScope.CVFF)
    with pytest.raises(BoundaryViolationError):
        enforce_topic_scope("ferries.manifest.v1", LakehouseScope.CVFF)


def test_guard_write_rejects_empty_batch() -> None:
    writer = SegregatedDeltaWriter(LakehouseScope.CVFF, "/lakehouse/cvff")
    with pytest.raises(BoundaryViolationError, match="empty event batch"):
        writer.guard_write("bronze", [])


# ---------------------------------------------------------------------------
# Phase-8 scopes: mrv + bluecarbon
# ---------------------------------------------------------------------------


def mrv_event() -> dict[str, object]:
    return {"event_id": "evt-mrv-1", "data_classification": "mrv_confidential"}


def bluecarbon_event() -> dict[str, object]:
    return {"event_id": "evt-bc-1", "data_classification": "bluecarbon_internal"}


def test_phase8_classification_mapping_is_fail_closed() -> None:
    assert scope_for_classification("mrv_confidential") is LakehouseScope.MRV
    assert scope_for_classification("bluecarbon_internal") is LakehouseScope.BLUECARBON


def test_phase8_topic_namespace_mapping() -> None:
    assert scope_for_topic("mrv.fuel-reports") is LakehouseScope.MRV
    assert scope_for_topic("mrv.soc") is LakehouseScope.MRV
    assert scope_for_topic("bluecarbon.projects") is LakehouseScope.BLUECARBON
    assert scope_for_topic("bluecarbon.ledger") is LakehouseScope.BLUECARBON
    with pytest.raises(BoundaryViolationError):
        scope_for_topic("mrvx.events")


def test_phase8_event_type_namespace_mapping() -> None:
    from blueeconomy_data_platform.segregation import scope_for_event_type

    assert scope_for_event_type("mrv.fuel-report.v1") is LakehouseScope.MRV
    assert scope_for_event_type("mrv.emissions-annual.v1") is LakehouseScope.MRV
    assert scope_for_event_type("bluecarbon.project.v1") is LakehouseScope.BLUECARBON
    assert scope_for_event_type("bluecarbon.retirement.v1") is LakehouseScope.BLUECARBON


def test_phase8_canonical_classification_maps_to_scope_label() -> None:
    from blueeconomy_data_platform.segregation import map_canonical_classification

    assert map_canonical_classification("CONFIDENTIAL", "mrv.fuel-report.v1") == "mrv_confidential"
    assert map_canonical_classification("INTERNAL", "mrv.soc.v1") == "mrv_confidential"
    assert (
        map_canonical_classification("INTERNAL", "bluecarbon.project.v1") == "bluecarbon_internal"
    )
    assert (
        map_canonical_classification("CONFIDENTIAL", "bluecarbon.evidence.v1")
        == "bluecarbon_internal"
    )


def test_phase8_roots_use_segregated_layer_names() -> None:
    mrv_roots = build_lakehouse_roots(LakehouseScope.MRV, "/lakehouse/mrv")
    assert mrv_roots.bronze.endswith("/mrv/mrv_bronze/events")
    assert mrv_roots.silver.endswith("/mrv/mrv_silver/events")
    assert mrv_roots.gold.endswith("/mrv/mrv_gold/events")
    bc_roots = build_lakehouse_roots(LakehouseScope.BLUECARBON, "s3://bucket/bluecarbon")
    assert bc_roots.silver == "s3://bucket/bluecarbon/bluecarbon_silver/events"


def test_phase8_writers_reject_cross_boundary_everything() -> None:
    mrv_writer = SegregatedDeltaWriter(LakehouseScope.MRV, "/lakehouse/mrv")
    bc_writer = SegregatedDeltaWriter(LakehouseScope.BLUECARBON, "/lakehouse/bluecarbon")
    platform_writer = SegregatedDeltaWriter(LakehouseScope.PLATFORM, "/lakehouse/platform")
    cvff_writer = SegregatedDeltaWriter(LakehouseScope.CVFF, "/lakehouse/cvff")

    # Wrong classification for the writer's scope.
    with pytest.raises(BoundaryViolationError):
        mrv_writer.guard_write("bronze", [bluecarbon_event()])
    with pytest.raises(BoundaryViolationError):
        bc_writer.guard_write("bronze", [mrv_event()])
    with pytest.raises(BoundaryViolationError):
        mrv_writer.guard_write("bronze", [platform_event()])
    with pytest.raises(BoundaryViolationError):
        platform_writer.guard_write("bronze", [mrv_event()])
    with pytest.raises(BoundaryViolationError):
        platform_writer.guard_write("bronze", [bluecarbon_event()])
    with pytest.raises(BoundaryViolationError):
        cvff_writer.guard_write("bronze", [mrv_event()])

    # Wrong topic for the writer's scope.
    with pytest.raises(BoundaryViolationError):
        mrv_writer.guard_write("bronze", [mrv_event()], kafka_topic="bluecarbon.projects")
    with pytest.raises(BoundaryViolationError):
        bc_writer.guard_write("bronze", [bluecarbon_event()], kafka_topic="mrv.fuel-reports")
    with pytest.raises(BoundaryViolationError):
        platform_writer.guard_write("bronze", [platform_event()], kafka_topic="mrv.voyages")

    # Accepted in-boundary writes resolve to the scope's own roots.
    assert (
        mrv_writer.guard_write("bronze", [mrv_event()], kafka_topic="mrv.fuel-reports")
        == "/lakehouse/mrv/mrv_bronze/events"
    )
    assert (
        bc_writer.guard_write("silver", [bluecarbon_event()], kafka_topic="bluecarbon.ledger")
        == "/lakehouse/bluecarbon/bluecarbon_silver/events"
    )


def test_phase8_scope_root_must_terminate_in_scope_component() -> None:
    with pytest.raises(BoundaryViolationError, match="mrv scope root must terminate"):
        SegregatedDeltaWriter(LakehouseScope.MRV, "/lakehouse/bluecarbon")
    with pytest.raises(BoundaryViolationError, match="bluecarbon scope root must terminate"):
        SegregatedDeltaWriter(LakehouseScope.BLUECARBON, "/lakehouse/mrv")
    with pytest.raises(BoundaryViolationError, match="platform scope root"):
        SegregatedDeltaWriter(LakehouseScope.PLATFORM, "/lakehouse/mrv")
    with pytest.raises(BoundaryViolationError):
        require_scope_table_uri(LakehouseScope.MRV, "/lakehouse/bluecarbon/bluecarbon_gold/x")
    with pytest.raises(BoundaryViolationError):
        require_scope_table_uri(LakehouseScope.BLUECARBON, "/lakehouse/mrv/mrv_gold/x")
    with pytest.raises(BoundaryViolationError):
        require_scope_table_uri(LakehouseScope.PLATFORM, "/lakehouse/mrv/mrv_gold/x")


def test_scope_layer_table_uri_stays_inside_the_boundary() -> None:
    from blueeconomy_data_platform.segregation import scope_layer_table_uri

    uri = scope_layer_table_uri(
        LakehouseScope.PLATFORM, "/lakehouse/platform", "gold", "port_kpi_runs"
    )
    assert uri == "/lakehouse/platform/platform_gold/port_kpi_runs"
    uri = scope_layer_table_uri(LakehouseScope.MRV, "/lakehouse/mrv", "gold", "vessel_annual")
    assert uri == "/lakehouse/mrv/mrv_gold/vessel_annual"
    uri = scope_layer_table_uri(
        LakehouseScope.BLUECARBON, "/lakehouse/bluecarbon", "gold", "public_registry"
    )
    assert uri == "/lakehouse/bluecarbon/bluecarbon_gold/public_registry"
    with pytest.raises(BoundaryViolationError):
        scope_layer_table_uri(LakehouseScope.PLATFORM, "/lakehouse/platform", "gold", "bad name!")
    with pytest.raises(BoundaryViolationError):
        scope_layer_table_uri(LakehouseScope.MRV, "/lakehouse/bluecarbon", "gold", "vessel_annual")
