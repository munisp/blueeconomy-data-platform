"""Phase-8 generic segregated-scope bronze/silver medallion tests (mrv, bluecarbon)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from deltalake import DeltaTable

from blueeconomy_data_platform.scope_medallion import (
    ScopeKafkaRecordMetadata,
    append_scope_bronze,
    append_scope_silver,
    build_silver_record,
    require_generic_medallion_scope,
    silver_dedup_key,
)
from blueeconomy_data_platform.segregation import (
    BoundaryViolationError,
    LakehouseScope,
    SegregatedDeltaWriter,
)

BASE_TIME = datetime(2026, 9, 2, 6, 30, tzinfo=UTC)


def scope_event(scope: str, event_id: str, event_type: str) -> dict[str, object]:
    classification = f"{scope}_confidential" if scope == "mrv" else "bluecarbon_internal"
    return {
        "event_id": event_id,
        "event_type": event_type,
        "producer": "blueeconomy-geo-service" if scope == "mrv" else "financial-controls",
        "occurred_at": BASE_TIME,
        "recorded_at": BASE_TIME,
        "data_classification": classification,
        "source_system": "mrv-api" if scope == "mrv" else "bluecarbon-api",
        "source_record_reference": f"src-{event_id}",
        "correlation_id": None,
        "payload_json": json.dumps({"reportId": f"report-{event_id}"}),
        "ingested_at": BASE_TIME,
    }


def test_generic_pipeline_scope_allowlist_fails_closed() -> None:
    require_generic_medallion_scope(LakehouseScope.MRV)
    require_generic_medallion_scope(LakehouseScope.BLUECARBON)
    for scope in (
        LakehouseScope.PLATFORM,
        LakehouseScope.CVFF,
        LakehouseScope.SEAFARER,
        LakehouseScope.FISHERIES,
        LakehouseScope.ISR,
    ):
        with pytest.raises(BoundaryViolationError):
            require_generic_medallion_scope(scope)


def test_kafka_metadata_enforces_topic_scope() -> None:
    ScopeKafkaRecordMetadata(LakehouseScope.MRV, "mrv.fuel-reports", 0, 42)
    ScopeKafkaRecordMetadata(LakehouseScope.BLUECARBON, "bluecarbon.ledger", 1, 7)
    with pytest.raises(BoundaryViolationError):
        ScopeKafkaRecordMetadata(LakehouseScope.MRV, "bluecarbon.projects", 0, 0)
    with pytest.raises(BoundaryViolationError):
        ScopeKafkaRecordMetadata(LakehouseScope.BLUECARBON, "mrv.voyages", 0, 0)
    with pytest.raises(ValueError, match="partition"):
        ScopeKafkaRecordMetadata(LakehouseScope.MRV, "mrv.voyages", -1, 0)
    with pytest.raises(ValueError, match="offset"):
        ScopeKafkaRecordMetadata(LakehouseScope.MRV, "mrv.voyages", 0, -1)


def test_dedup_key_is_sha256_of_topic_partition_offset_event_id() -> None:
    metadata = ScopeKafkaRecordMetadata(LakehouseScope.MRV, "mrv.fuel-reports", 0, 9)
    key = silver_dedup_key(metadata, "evt-1")
    import hashlib

    assert key == hashlib.sha256(b"mrv.fuel-reports/0/9/evt-1").hexdigest()


@pytest.mark.parametrize("scope", [LakehouseScope.MRV, LakehouseScope.BLUECARBON])
def test_bronze_silver_roundtrip_and_replay(tmp_path: Path, scope: LakehouseScope) -> None:
    writer = SegregatedDeltaWriter(scope, str(tmp_path / scope.value))
    topic = "mrv.fuel-reports" if scope is LakehouseScope.MRV else "bluecarbon.projects"
    event_type = "mrv.fuel-report.v1" if scope is LakehouseScope.MRV else "bluecarbon.project.v1"
    events = [scope_event(scope.value, f"evt-{index}", event_type) for index in range(3)]

    bronze_version, written, present = append_scope_bronze(writer, events, kafka_topic=topic)
    assert (written, present) == (3, 0)
    assert bronze_version == 0
    description = DeltaTable(writer.table_uri("bronze")).metadata().description or ""
    assert "retention: hot=30d, cold=7y" in description

    records = [
        build_silver_record(event, ScopeKafkaRecordMetadata(scope, topic, 0, index))
        for index, event in enumerate(events)
    ]
    silver_version, written, present = append_scope_silver(writer, records)
    assert (written, present) == (3, 0)

    # Idempotent replay: identical dedup keys never double-count.
    _, written, present = append_scope_silver(writer, records)
    assert (written, present) == (0, 3)
    assert DeltaTable(writer.table_uri("silver")).to_pyarrow_table().num_rows == 3, (
        "replayed Kafka records must never produce a second silver row"
    )
    assert silver_version >= 0


@pytest.mark.parametrize("scope", [LakehouseScope.MRV, LakehouseScope.BLUECARBON])
def test_conflicting_dedup_replay_fails_closed(tmp_path: Path, scope: LakehouseScope) -> None:
    writer = SegregatedDeltaWriter(scope, str(tmp_path / scope.value))
    topic = "mrv.voyages" if scope is LakehouseScope.MRV else "bluecarbon.ledger"
    event_type = "mrv.voyage.v1" if scope is LakehouseScope.MRV else "bluecarbon.ledger-movement.v1"
    metadata = ScopeKafkaRecordMetadata(scope, topic, 0, 0)
    original = build_silver_record(scope_event(scope.value, "evt-1", event_type), metadata)
    append_scope_silver(writer, [original])

    tampered = dict(original)
    tampered["payload_json"] = json.dumps({"reportId": "forged"})
    with pytest.raises(ValueError, match="dedup_key reuse conflicts"):
        append_scope_silver(writer, [tampered])


@pytest.mark.parametrize("scope", [LakehouseScope.MRV, LakehouseScope.BLUECARBON])
def test_cross_scope_writes_are_refused(tmp_path: Path, scope: LakehouseScope) -> None:
    other = LakehouseScope.BLUECARBON if scope is LakehouseScope.MRV else LakehouseScope.MRV
    writer = SegregatedDeltaWriter(scope, str(tmp_path / scope.value))
    foreign = scope_event(other.value, "evt-foreign", "mrv.fuel-report.v1")
    with pytest.raises(BoundaryViolationError):
        append_scope_bronze(writer, [foreign], kafka_topic="mrv.fuel-reports")


def test_empty_silver_batch_fails_closed(tmp_path: Path) -> None:
    writer = SegregatedDeltaWriter(LakehouseScope.MRV, str(tmp_path / "mrv"))
    with pytest.raises(ValueError, match="empty silver batch"):
        append_scope_silver(writer, [])
