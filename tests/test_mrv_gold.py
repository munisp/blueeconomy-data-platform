"""MRV scope gold assembly tests (phase 8): vessel_annual + signature negatives."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from deltalake import DeltaTable

from blueeconomy_data_platform.ingest import load_schema
from blueeconomy_data_platform.kafka_ingest import decode_event
from blueeconomy_data_platform.mrv_gold import (
    ANNUAL_REPORT_EVENT_TYPE,
    SOC_EVENT_TYPE,
    assemble_mrv_vessel_annual_gold,
    build_vessel_annual_rows,
    mrv_gold_table_uri,
)
from blueeconomy_data_platform.segregation import LakehouseScope, SegregatedDeltaWriter
from blueeconomy_data_platform.signature_verification import SignatureVerificationError
from signing_helpers import load_test_verifier, signed_envelope_bytes

BASE_TIME = datetime(2026, 9, 2, 6, 30, tzinfo=UTC)
SCHEMA = Path(__file__).resolve().parents[1] / "schemas" / "event-envelope.schema.json"
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "envelopes" / "geo-service-mrv.json"


def load_mrv_fixture() -> dict[str, object]:
    document: dict[str, object] = json.loads(FIXTURE.read_text(encoding="utf-8"))
    return document


def annual_report_event(
    event_id: str,
    imo: str,
    year: int,
    report_id: str,
    state: str,
    occurred_at: datetime,
    totals: dict[str, object] | None = None,
    attained_cii: float | None = 4.21,
    rating: str | None = "B",
) -> dict[str, object]:
    payload: dict[str, object] = {
        "reportId": report_id,
        "imoNumber": imo,
        "calendarYear": year,
        "state": state,
        "attainedCii": attained_cii,
        "requiredCii": 4.8,
        "ciiRating": rating,
        "totals": totals if totals is not None else {"co2Tonnes": 1284.375, "HFO_RME-RMK": 412.5},
    }
    return {
        "event_id": event_id,
        "event_type": ANNUAL_REPORT_EVENT_TYPE,
        "occurred_at": occurred_at,
        "payload_json": json.dumps(payload),
    }


def soc_event(
    event_id: str, report_id: str, artifact: str, occurred_at: datetime
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_type": SOC_EVENT_TYPE,
        "occurred_at": occurred_at,
        "payload_json": json.dumps(
            {"reportId": report_id, "socId": f"soc-{report_id}", "artifactSha256": artifact}
        ),
    }


def test_mrv_fixture_envelope_decodes_into_mrv_scope() -> None:
    validator = load_schema(SCHEMA)
    normalized = decode_event(
        json.dumps(load_mrv_fixture()).encode("utf-8"), validator, load_test_verifier()
    )
    assert normalized["event_type"] == "mrv.fuel-report.v1"
    assert normalized["data_classification"] == "mrv_confidential"
    payload = json.loads(normalized["payload_json"])
    assert payload["provenance"]["principalId"] == "svc-mrv-api"


def test_forged_signature_is_refused() -> None:
    validator = load_schema(SCHEMA)
    verifier = load_test_verifier()
    envelope = load_mrv_fixture()

    # Tampered payload under the original signature: payload mismatch.
    forged = json.loads(json.dumps(envelope))
    resource = forged["fhir"]["entry"][0]["resource"]  # type: ignore[index]
    resource["id"] = "mrv-fuel-report-FORGED"  # type: ignore[index]
    with pytest.raises(SignatureVerificationError, match="payload-mismatch"):
        decode_event(json.dumps(forged).encode("utf-8"), validator, verifier)

    # A corrupted signature segment never verifies under the envelope's kid.
    corrupted = json.loads(json.dumps(envelope))
    signature = corrupted["provenance"]["signature"]  # type: ignore[index]
    header_part, payload_part, signature_part = signature.split(".")
    flipped = ("A" if signature_part[0] != "A" else "B") + signature_part[1:]
    corrupted["provenance"]["signature"] = f"{header_part}.{payload_part}.{flipped}"  # type: ignore[index]
    with pytest.raises(SignatureVerificationError, match="invalid-signature"):
        decode_event(json.dumps(corrupted).encode("utf-8"), validator, verifier)

    # Unknown kid.
    with pytest.raises(SignatureVerificationError, match="unknown-kid"):
        decode_event(signed_envelope_bytes(envelope, kid="mrv-attacker-9"), validator, verifier)


def test_gold_requires_verified_reports_only() -> None:
    events = [
        annual_report_event("evt-draft", "9081716", 2026, "rep-1", "DRAFT", BASE_TIME),
        annual_report_event("evt-sub", "9081716", 2026, "rep-1", "SUBMITTED", BASE_TIME),
    ]
    rows = build_vessel_annual_rows(events, curated_at=BASE_TIME)
    assert rows == [], "no unverified report may produce a gold row"


def test_gold_assembly_row_content_and_soc_join() -> None:
    artifact = "c" * 64
    events = [
        annual_report_event("evt-ver", "9081716", 2026, "rep-1", "VERIFIED", BASE_TIME),
        soc_event("evt-soc", "rep-1", artifact, BASE_TIME),
    ]
    rows = build_vessel_annual_rows(events, curated_at=BASE_TIME)
    assert len(rows) == 1
    row = rows[0]
    assert row["imo_number"] == "9081716"
    assert row["calendar_year"] == 2026
    assert row["report_id"] == "rep-1"
    assert row["co2_tonnes"] == pytest.approx(1284.375)
    assert row["attained_cii"] == pytest.approx(4.21)
    assert row["required_cii"] == pytest.approx(4.8)
    assert row["cii_rating"] == "B"
    assert row["soc_artifact_sha256"] == artifact
    assert json.loads(row["source_event_ids_json"]) == ["evt-soc", "evt-ver"]


def test_soc_for_unverified_report_fails_closed() -> None:
    events = [
        annual_report_event("evt-draft", "9081716", 2026, "rep-1", "SUBMITTED", BASE_TIME),
        soc_event("evt-soc", "rep-1", "c" * 64, BASE_TIME),
    ]
    with pytest.raises(ValueError, match="not VERIFIED"):
        build_vessel_annual_rows(events, curated_at=BASE_TIME)


def test_latest_verified_report_wins_deterministically() -> None:
    earlier = datetime(2026, 9, 1, tzinfo=UTC)
    later = datetime(2026, 9, 3, tzinfo=UTC)
    events = [
        annual_report_event(
            "evt-new", "9081716", 2026, "rep-2", "VERIFIED", later, totals={"co2Tonnes": 100.0}
        ),
        annual_report_event(
            "evt-old", "9081716", 2026, "rep-1", "VERIFIED", earlier, totals={"co2Tonnes": 90.0}
        ),
    ]
    rows = build_vessel_annual_rows(events, curated_at=BASE_TIME)
    assert [row["report_id"] for row in rows] == ["rep-2"]
    assert rows[0]["co2_tonnes"] == pytest.approx(100.0)


def test_invalid_imo_and_rating_fail_closed() -> None:
    with pytest.raises(ValueError, match="imoNumber"):
        build_vessel_annual_rows(
            [annual_report_event("e1", "908171", 2026, "rep-1", "VERIFIED", BASE_TIME)],
            curated_at=BASE_TIME,
        )
    with pytest.raises(ValueError, match="ciiRating"):
        build_vessel_annual_rows(
            [
                annual_report_event(
                    "e2", "9081716", 2026, "rep-1", "VERIFIED", BASE_TIME, rating="F"
                )
            ],
            curated_at=BASE_TIME,
        )


def test_artifact_hash_stability() -> None:
    events = [annual_report_event("evt-ver", "9081716", 2026, "rep-1", "VERIFIED", BASE_TIME)]
    first = build_vessel_annual_rows(list(events), curated_at=BASE_TIME)
    second = build_vessel_annual_rows(list(reversed(events)), curated_at=BASE_TIME)
    assert first[0]["fuel_totals_json"] == second[0]["fuel_totals_json"]


def test_mrv_gold_end_to_end_and_boundary(tmp_path: Path) -> None:
    writer = SegregatedDeltaWriter(LakehouseScope.MRV, str(tmp_path / "mrv"))
    silver_rows = [
        {
            "event_id": "evt-ver",
            "event_type": ANNUAL_REPORT_EVENT_TYPE,
            "occurred_at": BASE_TIME,
            "payload_json": json.dumps(
                {
                    "reportId": "rep-1",
                    "imoNumber": "9081716",
                    "calendarYear": 2026,
                    "state": "VERIFIED",
                    "totals": {"co2Tonnes": 1284.375},
                }
            ),
        }
    ]
    rows = build_vessel_annual_rows(silver_rows, curated_at=BASE_TIME)
    assert len(rows) == 1

    # Full assembly against a real silver table.
    from blueeconomy_data_platform.scope_medallion import (
        ScopeKafkaRecordMetadata,
        append_scope_silver,
        build_silver_record,
    )

    event = {
        "event_id": "evt-ver",
        "event_type": ANNUAL_REPORT_EVENT_TYPE,
        "producer": "blueeconomy-geo-service",
        "occurred_at": BASE_TIME,
        "recorded_at": BASE_TIME,
        "data_classification": "mrv_confidential",
        "source_system": "mrv-api",
        "source_record_reference": "src-1",
        "correlation_id": None,
        "payload_json": silver_rows[0]["payload_json"],
        "ingested_at": BASE_TIME,
    }
    append_scope_silver(
        writer,
        [
            build_silver_record(
                event, ScopeKafkaRecordMetadata(LakehouseScope.MRV, "mrv.annual-reports", 0, 0)
            )
        ],
    )
    version, row_count = assemble_mrv_vessel_annual_gold(writer)
    assert row_count == 1
    gold = DeltaTable(mrv_gold_table_uri(writer))
    assert gold.version() == version
    persisted = gold.to_pyarrow_table().to_pylist()
    assert persisted[0]["imo_number"] == "9081716"
    assert persisted[0]["co2_tonnes"] == pytest.approx(1284.375)

    # Idempotent rebuild: same silver content, same derived rows.
    version2, row_count2 = assemble_mrv_vessel_annual_gold(writer)
    assert row_count2 == 1
    assert version2 == version + 1

    # The gold assembly refuses any other scope by construction.
    platform_writer = SegregatedDeltaWriter(LakehouseScope.PLATFORM, str(tmp_path / "platform"))
    with pytest.raises(ValueError, match="only defined for the mrv boundary"):
        assemble_mrv_vessel_annual_gold(platform_writer)


def test_mrv_gold_fails_closed_without_silver(tmp_path: Path) -> None:
    writer = SegregatedDeltaWriter(LakehouseScope.MRV, str(tmp_path / "mrv"))
    with pytest.raises(ValueError, match="before the silver table exists"):
        assemble_mrv_vessel_annual_gold(writer)
