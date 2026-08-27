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
