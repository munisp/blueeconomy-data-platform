"""Blue-Carbon scope gold tests (phase 8): public_registry projection."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from deltalake import DeltaTable

from blueeconomy_data_platform.bluecarbon_gold import (
    BLUECARBON_GOLD_SCHEMA,
    CREDIT_BLOCK_EVENT_TYPE,
    GOLD_TABLE_NAME,
    PROJECT_EVENT_TYPE,
    RETIREMENT_EVENT_TYPE,
    assemble_bluecarbon_public_registry_gold,
    bluecarbon_gold_table_uri,
    build_public_registry_rows,
)
from blueeconomy_data_platform.ingest import load_schema
from blueeconomy_data_platform.kafka_ingest import decode_event
from blueeconomy_data_platform.segregation import LakehouseScope, SegregatedDeltaWriter
from signing_helpers import load_test_verifier

BASE_TIME = datetime(2026, 9, 5, 11, 45, tzinfo=UTC)
SCHEMA = Path(__file__).resolve().parents[1] / "schemas" / "event-envelope.schema.json"
FIXTURE = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "envelopes"
    / "financial-controls-bluecarbon.json"
)


def project_event(
    event_id: str,
    project_id: str,
    state: str,
    occurred_at: datetime,
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "projectId": project_id,
        "projectName": "Ondo Mangrove Restoration",
        "state": state,
        "ecosystem": "mangrove",
        "methodology": "VM0033 v2.1",
        "countryCode": "NG",
        "strataCentroids": [{"stratum": "mangrove-a", "latitude": 6.05, "longitude": 4.79}],
        "externalRegistry": "verra",
        "externalProjectId": "VCS-4127",
        # Confidential fields that must never reach the public projection.
        "proponentPii": {"contact": "redacted"},
        "evidenceUris": ["s3://confidential/evidence-1.bin"],
        "monitoringReportDetail": {"raw": "secret"},
    }
    if extra:
        payload.update(extra)
    return {
        "event_id": event_id,
        "event_type": PROJECT_EVENT_TYPE,
        "occurred_at": occurred_at,
        "payload_json": json.dumps(payload),
    }


def block_event(
    event_id: str,
    block_id: str,
    project_id: str,
    vintage: str,
    status: str,
    quantity: float,
    buffer_pool: bool = False,
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_type": CREDIT_BLOCK_EVENT_TYPE,
        "occurred_at": BASE_TIME,
        "payload_json": json.dumps(
            {
                "blockId": block_id,
                "projectId": project_id,
                "serial": f"NG-BC-2026-0001-{vintage}-{block_id}",
                "vintage": vintage,
                "status": status,
                "quantityKgCo2e": quantity,
                "bufferPool": buffer_pool,
            }
        ),
    }


def retirement_event(
    event_id: str, retirement_id: str, project_id: str, quantity: float
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_type": RETIREMENT_EVENT_TYPE,
        "occurred_at": BASE_TIME,
        "payload_json": json.dumps(
            {
                "retirementId": retirement_id,
                "projectId": project_id,
                "serial": f"NG-BC-2026-0001-2026-{retirement_id}",
                "beneficiary": "Example Airline Ltd",
                "purpose": "voluntary offset claim",
                "quantityKgCo2e": quantity,
                "artifactSha256": "d" * 64,
                "retiredAt": "2026-09-05T12:00:00Z",
            }
        ),
    }


def test_bluecarbon_fixture_envelope_decodes_into_bluecarbon_scope() -> None:
    validator = load_schema(SCHEMA)
    envelope = json.loads(FIXTURE.read_text(encoding="utf-8"))
    normalized = decode_event(json.dumps(envelope).encode("utf-8"), validator, load_test_verifier())
    assert normalized["event_type"] == "bluecarbon.project.v1"
    assert normalized["data_classification"] == "bluecarbon_internal"


def test_public_projection_contains_only_allowlisted_fields() -> None:
    events = [
        project_event("evt-p1", "NG-BC-2026-0001", "REGISTERED", BASE_TIME),
        block_event("evt-b1", "B1", "NG-BC-2026-0001", "2026", "ACTIVE", 1000.0),
        block_event("evt-b2", "B2", "NG-BC-2026-0001", "2026", "RETIRED", 250.0),
        block_event("evt-b3", "B3", "NG-BC-2026-0001", "2026", "CANCELLED", 50.0),
        block_event("evt-b4", "B4", "NG-BC-2026-0001", "2026", "ACTIVE", 200.0, buffer_pool=True),
        retirement_event("evt-r1", "R1", "NG-BC-2026-0001", 250.0),
    ]
    rows = build_public_registry_rows(events, curated_at=BASE_TIME)
    assert len(rows) == 1
    row = rows[0]
    assert row["project_id"] == "NG-BC-2026-0001"
    assert row["state"] == "REGISTERED"
    assert row["methodology"] == "VM0033 v2.1"
    assert row["external_registry"] == "verra"
    assert row["external_project_id"] == "VCS-4127"
    assert json.loads(row["strata_centroids_json"]) == [
        {"stratum": "mangrove-a", "latitude": 6.05, "longitude": 4.79}
    ]
    totals = json.loads(row["vintage_totals_json"])
    # Issued totals include the 200 kg buffer-pool allocation (issued credits
    # held in the buffer account); the buffer balance is reported separately.
    assert totals == [
        {
            "vintage": "2026",
            "issued_kg_co2e": 1500.0,
            "retired_kg_co2e": 250.0,
            "cancelled_kg_co2e": 50.0,
        }
    ]
    assert row["buffer_balance_kg_co2e"] == pytest.approx(200.0)
    retirements = json.loads(row["retirements_json"])
    assert retirements[0]["beneficiary"] == "Example Airline Ltd"
    assert retirements[0]["artifact_sha256"] == "d" * 64
    assert set(json.loads(row["source_event_ids_json"])) == {
        "evt-p1",
        "evt-b1",
        "evt-b2",
        "evt-b3",
        "evt-b4",
        "evt-r1",
    }

    # Fail-closed projection-by-construction: no confidential source field
    # appears anywhere in the gold row.
    serialized = json.dumps(row, default=str)
    for forbidden in ("proponentPii", "evidenceUris", "monitoringReportDetail", "redacted"):
        assert forbidden not in serialized
    # Gold schema columns are exactly the public projection contract.
    assert set(row) == {field.name for field in BLUECARBON_GOLD_SCHEMA}


def test_latest_project_state_wins() -> None:
    earlier = datetime(2026, 9, 1, tzinfo=UTC)
    later = datetime(2026, 9, 6, tzinfo=UTC)
    events = [
        project_event("evt-old", "NG-BC-2026-0001", "REGISTERED", earlier),
        project_event("evt-new", "NG-BC-2026-0001", "ISSUED", later),
    ]
    rows = build_public_registry_rows(events, curated_at=BASE_TIME)
    assert [row["state"] for row in rows] == ["ISSUED"]


def test_unknown_block_status_fails_closed() -> None:
    events = [
        project_event("evt-p1", "NG-BC-2026-0001", "REGISTERED", BASE_TIME),
        block_event("evt-b1", "B1", "NG-BC-2026-0001", "2026", "ON_HOLD", 10.0),
    ]
    with pytest.raises(ValueError, match="status"):
        build_public_registry_rows(events, curated_at=BASE_TIME)


def test_gold_end_to_end_and_boundary(tmp_path: Path) -> None:
    from blueeconomy_data_platform.scope_medallion import (
        ScopeKafkaRecordMetadata,
        append_scope_silver,
        build_silver_record,
    )

    writer = SegregatedDeltaWriter(LakehouseScope.BLUECARBON, str(tmp_path / "bluecarbon"))
    source = project_event("evt-p1", "NG-BC-2026-0001", "REGISTERED", BASE_TIME)
    event = {
        "event_id": source["event_id"],
        "event_type": source["event_type"],
        "producer": "financial-controls",
        "occurred_at": BASE_TIME,
        "recorded_at": BASE_TIME,
        "data_classification": "bluecarbon_internal",
        "source_system": "bluecarbon-api",
        "source_record_reference": "src-1",
        "correlation_id": None,
        "payload_json": source["payload_json"],
        "ingested_at": BASE_TIME,
    }
    append_scope_silver(
        writer,
        [
            build_silver_record(
                event,
                ScopeKafkaRecordMetadata(LakehouseScope.BLUECARBON, "bluecarbon.projects", 0, 0),
            )
        ],
    )
    version, row_count = assemble_bluecarbon_public_registry_gold(writer)
    assert row_count == 1
    gold = DeltaTable(bluecarbon_gold_table_uri(writer))
    assert gold.version() == version
    persisted = gold.to_pyarrow_table().to_pylist()
    assert persisted[0]["project_id"] == "NG-BC-2026-0001"
    assert GOLD_TABLE_NAME == "public_registry"

    mrv_writer = SegregatedDeltaWriter(LakehouseScope.MRV, str(tmp_path / "mrv"))
    with pytest.raises(ValueError, match="bluecarbon boundary"):
        assemble_bluecarbon_public_registry_gold(mrv_writer)


def test_gold_fails_closed_without_silver(tmp_path: Path) -> None:
    writer = SegregatedDeltaWriter(LakehouseScope.BLUECARBON, str(tmp_path / "bluecarbon"))
    with pytest.raises(ValueError, match="before the silver table exists"):
        assemble_bluecarbon_public_registry_gold(writer)
