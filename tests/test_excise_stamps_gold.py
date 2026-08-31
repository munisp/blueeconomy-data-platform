"""excise_stamp_facts gold projection: deterministic 1:1, fail-closed money."""

import json
from datetime import UTC, datetime

import pytest

from blueeconomy_data_platform.excise_stamps_gold import (
    STAMP_EVENT_TYPES,
    assemble_excise_stamps_gold,
    build_excise_stamp_fact_rows,
)
from blueeconomy_data_platform.segregation import (
    PLATFORM_TOPIC_PREFIXES,
    LakehouseScope,
    SegregatedDeltaWriter,
)

CURATED = datetime(2026, 8, 2, tzinfo=UTC)


def _silver_event(event_id, event_type, fields, occurred_at=None):
    return {
        "event_id": event_id,
        "event_type": event_type,
        "occurred_at": occurred_at or datetime(2026, 8, 1, 10, 0, tzinfo=UTC),
        "payload_json": json.dumps(fields),
    }


def test_stamps_prefix_admitted_to_platform_scope():
    assert "stamps." in PLATFORM_TOPIC_PREFIXES


def test_assessed_projection_1_to_1():
    events = [
        _silver_event("evt-1", "stamps.assessed.v1", {
            "@type": "type.googleapis.com/blueeconomy.contracts.v1.TaxStampAssessed",
            "assessmentId": "a-1",
            "declarationRef": "DECL-1",
            "consigneeTin": "12345678-0001",
            "totalDutyKobo": 1250000,
            "stampsRequired": 500,
            "riskTier": "LOW",
        }),
        _silver_event("evt-2", "stamps.issued.v1", {
            "batchId": "b-1", "assessmentId": "a-1", "quantity": 500,
        }),
        _silver_event("evt-3", "ports.booking.v1", {"booking": "x"}),  # ignored
    ]
    rows = build_excise_stamp_fact_rows(events, CURATED)
    assert [r["event_id"] for r in rows] == ["evt-1", "evt-2"]
    assert rows[0]["total_duty_kobo"] == 1250000
    assert rows[0]["quantity"] == 500
    assert rows[0]["assessment_id"] == "a-1"
    assert rows[0]["declaration_ref"] == "DECL-1"
    assert rows[1]["batch_id"] == "b-1"
    assert rows[1]["total_duty_kobo"] is None
    assert rows[1]["quantity"] == 500


def test_all_stamp_event_types_project():
    events = [
        _silver_event("evt-a", event_type, fields)
        for event_type, fields in [
            ("stamps.approved.v1", {"assessmentId": "a-1", "approvalsRequired": 3}),
            ("stamps.activated.v1", {"batchId": "b-1", "activatedCount": 500}),
        ]
    ]
    rows = build_excise_stamp_fact_rows(events, CURATED)
    assert [r["event_type"] for r in rows] == ["stamps.approved.v1", "stamps.activated.v1"]
    assert rows[1]["quantity"] == 500


def test_deterministic_ordering():
    early = datetime(2026, 8, 1, 9, 0, tzinfo=UTC)
    late = datetime(2026, 8, 1, 11, 0, tzinfo=UTC)
    events = [
        _silver_event("evt-b", "stamps.approved.v1", {"assessmentId": "a", "approvalsRequired": 1}, late),
        _silver_event("evt-a", "stamps.activated.v1", {"batchId": "b", "activatedCount": 1}, early),
    ]
    rows = build_excise_stamp_fact_rows(events, CURATED)
    assert [r["event_id"] for r in rows] == ["evt-a", "evt-b"]
    assert rows == build_excise_stamp_fact_rows(list(reversed(events)), CURATED)


def test_malformed_money_fails_closed():
    for bad in (12.5, -1, "1250000", None, True):
        events = [_silver_event("evt-1", "stamps.assessed.v1", {
            "assessmentId": "a-1", "declarationRef": "D-1",
            "totalDutyKobo": bad, "stampsRequired": 10,
        })]
        with pytest.raises(ValueError):
            build_excise_stamp_fact_rows(events, CURATED)


def test_malformed_quantity_fails_closed():
    events = [_silver_event("evt-1", "stamps.issued.v1", {
        "batchId": "b-1", "assessmentId": "a-1", "quantity": 0,
    })]
    with pytest.raises(ValueError):
        build_excise_stamp_fact_rows(events, CURATED)


def test_missing_payload_fails_closed():
    with pytest.raises(ValueError):
        build_excise_stamp_fact_rows(
            [{"event_id": "e", "event_type": "stamps.assessed.v1",
              "occurred_at": datetime(2026, 8, 1, tzinfo=UTC), "payload_json": None}],
            CURATED,
        )


def test_scope_guard(tmp_path):
    writer = SegregatedDeltaWriter(LakehouseScope.MRV, str(tmp_path / "mrv"))
    with pytest.raises(ValueError, match="platform"):
        assemble_excise_stamps_gold(writer)


def test_event_type_set():
    assert STAMP_EVENT_TYPES == (
        "stamps.assessed.v1", "stamps.approved.v1", "stamps.issued.v1", "stamps.activated.v1"
    )
