from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from deltalake import DeltaTable

from blueeconomy_data_platform.medallion import (
    KafkaRecordMetadata,
    RetentionPolicy,
    append_bronze,
    append_silver,
    build_silver_record,
    curate_gold,
    extract_ledger_commit_hash,
    retention_report,
    silver_dedup_key,
)
from blueeconomy_data_platform.segregation import (
    BoundaryViolationError,
    LakehouseScope,
    SegregatedDeltaWriter,
)

LEDGER_HASH_A = "a" * 64
LEDGER_HASH_B = "b" * 64


def cvff_event(event_id: str, ledger_hash: str = LEDGER_HASH_A) -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_type": "cvff.ledger.commitment.v1",
        "producer": "cvff-ledger-adapter",
        "occurred_at": datetime(2026, 8, 12, tzinfo=UTC),
        "recorded_at": datetime(2026, 8, 12, 0, 0, 1, tzinfo=UTC),
        "data_classification": "fiduciary_segregated",
        "source_system": "cvff-ledger",
        "source_record_reference": f"ledger-{event_id}",
        "correlation_id": None,
        "payload_json": json.dumps({"ledgerCommitHash": ledger_hash, "amount": "1000.00"}),
        "ingested_at": datetime(2026, 8, 12, 0, 0, 2, tzinfo=UTC),
    }


def metadata(offset: int, topic: str = "cvff.ledger.commitments") -> KafkaRecordMetadata:
    return KafkaRecordMetadata(topic=topic, partition=0, offset=offset)


def cvff_writer(tmp_path: Path) -> SegregatedDeltaWriter:
    return SegregatedDeltaWriter(LakehouseScope.CVFF, str(tmp_path / "cvff"))


def platform_writer(tmp_path: Path) -> SegregatedDeltaWriter:
    return SegregatedDeltaWriter(LakehouseScope.PLATFORM, str(tmp_path / "platform"))


def test_bronze_append_is_physically_segregated(tmp_path: Path) -> None:
    writer = cvff_writer(tmp_path)
    version, written, present = append_bronze(
        writer, [cvff_event("evt-1")], kafka_topic="cvff.ledger.commitments"
    )
    assert (written, present) == (1, 0)
    bronze_uri = writer.table_uri("bronze")
    assert "/cvff/cvff_bronze/events" in bronze_uri
    table = DeltaTable(bronze_uri)
    assert table.metadata().configuration["delta.appendOnly"] == "true"
    assert "hot=30d" in table.metadata().description
    assert "cold=7y" in table.metadata().description
    assert table.to_pyarrow_table().num_rows == 1
    assert version == 0


def test_bronze_rejects_platform_event_and_platform_writer_rejects_cvff(tmp_path: Path) -> None:
    writer = cvff_writer(tmp_path)
    platform_event = cvff_event("evt-2")
    platform_event["data_classification"] = "internal"
    with pytest.raises(BoundaryViolationError):
        append_bronze(writer, [platform_event])
    with pytest.raises(BoundaryViolationError):
        append_bronze(platform_writer(tmp_path), [cvff_event("evt-3")])
    assert not (tmp_path / "platform" / "platform_bronze").exists()


def test_silver_dedup_replays_same_offset_and_ledger_hash(tmp_path: Path) -> None:
    writer = cvff_writer(tmp_path)
    first = build_silver_record(cvff_event("evt-10"), metadata(offset=41))
    second = build_silver_record(cvff_event("evt-11"), metadata(offset=42))

    _version, written, present = append_silver(writer, [first, second])
    assert (written, present) == (2, 0)

    # Replay of the exact same Kafka offsets: deduplicated, never duplicated.
    _version, written, present = append_silver(writer, [first, second])
    assert (written, present) == (0, 2)
    silver_uri = writer.table_uri("silver")
    assert DeltaTable(silver_uri).to_pyarrow_table().num_rows == 2


def test_silver_dedup_key_combines_offset_and_ledger_hash() -> None:
    key_a = silver_dedup_key(metadata(offset=41), LEDGER_HASH_A)
    assert key_a != silver_dedup_key(metadata(offset=42), LEDGER_HASH_A)
    assert key_a != silver_dedup_key(metadata(offset=41), LEDGER_HASH_B)
    assert key_a == silver_dedup_key(metadata(offset=41), LEDGER_HASH_A)
    with pytest.raises(BoundaryViolationError):
        KafkaRecordMetadata(topic="ports.calls.v1", partition=0, offset=1)


def test_silver_rejects_conflicting_dedup_key_reuse(tmp_path: Path) -> None:
    writer = cvff_writer(tmp_path)
    record = build_silver_record(cvff_event("evt-20"), metadata(offset=7))
    append_silver(writer, [record])
    conflicting = dict(record)
    conflicting["payload_json"] = json.dumps(
        {"ledgerCommitHash": LEDGER_HASH_A, "amount": "9999.99"}
    )
    with pytest.raises(ValueError, match="dedup_key reuse conflicts"):
        append_silver(writer, [conflicting])


def test_silver_requires_ledger_commit_hash() -> None:
    event = cvff_event("evt-30")
    event["payload_json"] = json.dumps({"amount": "1.00"})
    with pytest.raises(ValueError, match="ledgerCommitHash"):
        build_silver_record(event, metadata(offset=1))
    with pytest.raises(ValueError, match="ledgerCommitHash"):
        extract_ledger_commit_hash(cvff_event("evt-31", ledger_hash="ZZ" * 32))


def test_silver_and_gold_reject_platform_scope(tmp_path: Path) -> None:
    writer = platform_writer(tmp_path)
    record = build_silver_record(cvff_event("evt-40"), metadata(offset=1))
    with pytest.raises(BoundaryViolationError):
        append_silver(writer, [record])
    with pytest.raises(BoundaryViolationError):
        curate_gold(writer)


def test_gold_curates_one_row_per_ledger_commitment(tmp_path: Path) -> None:
    writer = cvff_writer(tmp_path)
    records = [
        build_silver_record(cvff_event("evt-50", LEDGER_HASH_A), metadata(offset=50)),
        build_silver_record(cvff_event("evt-51", LEDGER_HASH_A), metadata(offset=51)),
        build_silver_record(cvff_event("evt-52", LEDGER_HASH_B), metadata(offset=52)),
    ]
    append_silver(writer, records)
    _version, row_count = curate_gold(writer)
    assert row_count == 2
    gold_rows = {
        row["ledger_commit_hash"]: row
        for row in DeltaTable(writer.table_uri("gold")).to_pyarrow_table().to_pylist()
    }
    assert gold_rows[LEDGER_HASH_A]["record_count"] == 2
    assert gold_rows[LEDGER_HASH_B]["record_count"] == 1
    assert json.loads(gold_rows[LEDGER_HASH_A]["event_ids_json"]) == ["evt-50", "evt-51"]


def test_retention_policy_tiers_and_validation(tmp_path: Path) -> None:
    policy = RetentionPolicy()
    reference = datetime(2026, 8, 12, tzinfo=UTC)
    assert policy.tier_for(reference - timedelta(days=10), reference) == "hot"
    assert policy.tier_for(reference - timedelta(days=365), reference) == "cold"
    assert policy.tier_for(reference - timedelta(days=8 * 365), reference) == "expired"
    with pytest.raises(ValueError, match="must not be later"):
        policy.tier_for(reference + timedelta(days=1), reference)
    with pytest.raises(ValueError):
        RetentionPolicy(hot_days=0)
    with pytest.raises(ValueError):
        RetentionPolicy(hot_days=30, cold_years=0)

    writer = cvff_writer(tmp_path)
    append_bronze(writer, [cvff_event("evt-60")])
    counts = retention_report(writer, policy, reference=datetime(2026, 8, 20, tzinfo=UTC))
    assert counts == {"hot": 1, "cold": 0, "expired": 0}
