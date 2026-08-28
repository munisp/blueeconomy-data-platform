from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from deltalake import DeltaTable

from blueeconomy_data_platform.export_consignment import (
    EXPORT_CONSIGNMENT_SCHEMA,
    assemble_export_consignment_gold,
    build_consignment_records,
    export_consignment_table_uri,
)
from blueeconomy_data_platform.medallion import append_bronze
from blueeconomy_data_platform.segregation import (
    BoundaryViolationError,
    LakehouseScope,
    SegregatedDeltaWriter,
)

BASE_TIME = datetime(2026, 8, 12, 8, 0, tzinfo=UTC)


def fisheries_event(
    event_id: str,
    event_type: str,
    payload: dict[str, object],
    occurred_at: datetime = BASE_TIME,
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "producer": "fisheries-gateway",
        "occurred_at": occurred_at,
        "recorded_at": occurred_at,
        "data_classification": "fisheries_operational",
        "source_system": "fisheries-operations",
        "source_record_reference": f"src-{event_id}",
        "correlation_id": None,
        "payload_json": json.dumps(payload),
        "ingested_at": occurred_at,
    }


def consignment_events(consignment_id: str = "CONS-2026-0001") -> list[dict[str, object]]:
    return [
        fisheries_event(
            f"{consignment_id}-catch",
            "fisheries.catch.v1",
            {
                "consignmentId": consignment_id,
                "speciesCode": "TUNA-YFT",
                "catchWeightKg": 125.5,
            },
        ),
        fisheries_event(
            f"{consignment_id}-custody-1",
            "fisheries.custody.v1",
            {"consignmentId": consignment_id, "from": "vessel", "to": "coldstore"},
        ),
        fisheries_event(
            f"{consignment_id}-cold-1",
            "coldchain.temperature.v1",
            {"consignmentId": consignment_id, "temperatureCelsius": -18.2},
        ),
        fisheries_event(
            f"{consignment_id}-cold-2",
            "coldchain.temperature.v1",
            {"consignmentId": consignment_id, "temperatureCelsius": -17.6},
            occurred_at=datetime(2026, 8, 12, 9, 0, tzinfo=UTC),
        ),
        fisheries_event(
            f"{consignment_id}-export",
            "export.declaration.v1",
            {"consignmentId": consignment_id, "exportReference": "EXP-8899"},
        ),
    ]


def fisheries_writer(tmp_path: Path) -> SegregatedDeltaWriter:
    return SegregatedDeltaWriter(LakehouseScope.FISHERIES, str(tmp_path / "fisheries"))


def test_build_consignment_record_assembles_catch_custody_coldchain() -> None:
    records = build_consignment_records(consignment_events())
    assert len(records) == 1
    record = records[0]
    assert record["consignment_id"] == "CONS-2026-0001"
    assert record["catch_event_id"] == "CONS-2026-0001-catch"
    assert record["species_code"] == "TUNA-YFT"
    assert record["catch_weight_kg"] == 125.5
    assert record["custody_event_count"] == 1
    assert json.loads(record["custody_event_ids_json"]) == ["CONS-2026-0001-custody-1"]
    assert record["coldchain_sample_count"] == 2
    assert record["min_temperature_celsius"] == -18.2
    assert record["max_temperature_celsius"] == -17.6
    assert len(record["coldchain_digest_sha256"]) == 64
    assert record["export_reference"] == "EXP-8899"
    assert len(json.loads(record["source_event_ids_json"])) == 5


def test_coldchain_digest_is_deterministic_and_tamper_evident() -> None:
    first = build_consignment_records(consignment_events())[0]
    reordered = consignment_events()
    reordered[2], reordered[3] = reordered[3], reordered[2]
    second = build_consignment_records(reordered)[0]
    assert first["coldchain_digest_sha256"] == second["coldchain_digest_sha256"]
    tampered = consignment_events()
    tampered[2]["payload_json"] = json.dumps(
        {"consignmentId": "CONS-2026-0001", "temperatureCelsius": 4.5}
    )
    third = build_consignment_records(tampered)[0]
    assert third["coldchain_digest_sha256"] != first["coldchain_digest_sha256"]


def test_consignment_assembly_fails_closed() -> None:
    # No catch event.
    with pytest.raises(ValueError, match="exactly one fisheries.catch"):
        build_consignment_records(consignment_events()[1:])
    # Duplicate catch events.
    events = consignment_events()
    duplicate = dict(events[0])
    duplicate["event_id"] = "CONS-2026-0001-catch-2"
    with pytest.raises(ValueError, match="exactly one fisheries.catch"):
        build_consignment_records([*events, duplicate])
    # Missing consignment id.
    orphan = fisheries_event("orphan", "fisheries.catch.v1", {"speciesCode": "X"})
    with pytest.raises(ValueError, match="consignmentId"):
        build_consignment_records([orphan])
    # Implausible temperature.
    hot = fisheries_event(
        "hot", "coldchain.temperature.v1", {"consignmentId": "C1", "temperatureCelsius": 95.0}
    )
    with pytest.raises(ValueError, match="sanity range"):
        build_consignment_records([*consignment_events(), hot])
    # Ungoverned event family.
    stray = fisheries_event("stray", "fisheries.anecdote.v1", {"consignmentId": "C1"})
    with pytest.raises(ValueError, match="outside the governed"):
        build_consignment_records([*consignment_events(), stray])
    with pytest.raises(ValueError, match="empty event batch"):
        build_consignment_records([])


def test_gold_assembly_over_fisheries_scope(tmp_path: Path) -> None:
    writer = fisheries_writer(tmp_path)
    events = consignment_events() + consignment_events("CONS-2026-0002")
    append_bronze(writer, events, kafka_topic="fisheries.catch.v1")
    version, count = assemble_export_consignment_gold(writer)
    assert version == 0
    assert count == 2
    table_uri = export_consignment_table_uri(writer)
    assert table_uri.endswith("/fisheries/fisheries_gold/export_consignments")
    table = DeltaTable(table_uri)
    assert table.schema().to_arrow() == EXPORT_CONSIGNMENT_SCHEMA
    rows = {row["consignment_id"]: row for row in table.to_pyarrow_table().to_pylist()}
    assert rows["CONS-2026-0002"]["coldchain_sample_count"] == 2
    assert rows["CONS-2026-0001"]["export_reference"] == "EXP-8899"


def test_gold_assembly_requires_fisheries_scope(tmp_path: Path) -> None:
    for scope, root in (
        (LakehouseScope.CVFF, "cvff"),
        (LakehouseScope.ISR, "isr"),
        (LakehouseScope.SEAFARER, "seafarer"),
        (LakehouseScope.PLATFORM, "platform"),
    ):
        writer = SegregatedDeltaWriter(scope, str(tmp_path / root))
        with pytest.raises(BoundaryViolationError, match="fisheries boundary"):
            assemble_export_consignment_gold(writer)
    with pytest.raises(ValueError, match="before the fisheries bronze table exists"):
        assemble_export_consignment_gold(fisheries_writer(tmp_path))
