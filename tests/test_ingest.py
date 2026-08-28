from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from blueeconomy_data_platform.ingest import (
    append_events,
    load_schema,
    normalize_event,
    read_and_validate_events,
    reject_conflicting_event_replays,
    validate_maritime_position,
    validate_output_path,
    validate_table_uri,
)


def valid_event() -> dict[str, object]:
    """A minimal canonical platform envelope (envelopeVersion 1.0)."""
    return {
        "envelopeVersion": "1.0",
        "eventId": "0c1f5a2e-7b3d-4e6f-8a90-b1c2d3e4f506",
        "eventType": "safety.telemetry.received",
        "occurredAt": "2026-08-12T00:00:00Z",
        "producer": "approved-gateway",
        "correlationId": "correlation-001",
        "fhir": {
            "resourceType": "Bundle",
            "type": "message",
            "entry": [{"resource": {"integrity_status": "verified"}}],
        },
        "provenance": {
            "principalId": "svc-approved-gateway",
            "principalRole": "gateway",
            "signature": "a" * 64,
            "ledgerCommitHash": "b" * 64,
        },
        "classification": "INTERNAL",
    }


def internal_event() -> dict[str, object]:
    """The internal event shape accepted directly by normalize_event."""
    return {
        "event_id": "event-001",
        "event_type": "safety.telemetry.received",
        "producer": "approved-gateway",
        "occurred_at": "2026-08-12T00:00:00Z",
        "recorded_at": "2026-08-12T00:00:01Z",
        "data_classification": "internal",
        "source_system": "approved-gateway",
        "source_record_reference": "source-record-001",
        "correlation_id": "correlation-001",
        "payload": {"integrity_status": "verified"},
    }


def schema_path() -> Path:
    return Path(__file__).resolve().parents[1] / "schemas" / "event-envelope.schema.json"


def write_ndjson(path: Path, records: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")


def test_real_local_delta_ingestion_is_idempotent(tmp_path: Path) -> None:
    input_path = tmp_path / "events.ndjson"
    write_ndjson(input_path, [valid_event()])
    events = read_and_validate_events(input_path, load_schema(schema_path()))
    table_path = str(tmp_path / "delta-events")

    version, written, existing = append_events(table_path, events)
    assert (version, written, existing) == (0, 1, 0)

    version, written, existing = append_events(table_path, events)
    assert version == 0
    assert (written, existing) == (0, 1)
    assert DeltaTable(table_path).to_pyarrow_table().num_rows == 1


def test_rejects_conflicting_event_id_reuse(tmp_path: Path) -> None:
    input_path = tmp_path / "events.ndjson"
    first = valid_event()
    write_ndjson(input_path, [first])
    events = read_and_validate_events(input_path, load_schema(schema_path()))
    table_path = str(tmp_path / "delta-events")
    append_events(table_path, events)

    second = valid_event()
    second["fhir"]["entry"][0]["resource"] = {"integrity_status": "failed"}  # type: ignore[index]
    write_ndjson(input_path, [second])
    conflicting = read_and_validate_events(input_path, load_schema(schema_path()))
    try:
        append_events(table_path, conflicting)
    except ValueError as error:
        assert "event_id reuse conflicts" in str(error)
    else:
        raise AssertionError("conflicting event ID reuse was accepted")

    retained = DeltaTable(table_path).to_pyarrow_table().to_pylist()
    assert json.loads(retained[0]["payload_json"])["integrity_status"] == "verified"


def test_accepts_valid_maritime_position_payload() -> None:
    event = internal_event()
    event["event_type"] = "maritime.position.v1"
    event["payload"] = {
        "asset_id": "vessel-001",
        "latitude": 6.45,
        "longitude": 3.39,
        "speed_knots": 8.5,
        "heading_degrees": 270.0,
    }
    normalized = normalize_event(event)
    assert normalized["event_type"] == "maritime.position.v1"


def test_rejects_out_of_range_maritime_position() -> None:
    payload = {
        "asset_id": "vessel-001",
        "latitude": 91.0,
        "longitude": 3.39,
        "speed_knots": 8.5,
        "heading_degrees": 270.0,
    }
    try:
        validate_maritime_position(payload)
    except ValueError as error:
        assert "latitude must be between" in str(error)
    else:
        raise AssertionError("out-of-range maritime latitude was accepted")


def test_rejects_negative_maritime_speed() -> None:
    payload = {
        "asset_id": "vessel-001",
        "latitude": 6.45,
        "longitude": 3.39,
        "speed_knots": -1.0,
        "heading_degrees": 270.0,
    }
    try:
        validate_maritime_position(payload)
    except ValueError as error:
        assert "speed_knots must not be negative" in str(error)
    else:
        raise AssertionError("negative maritime speed was accepted")


def test_rejects_recorded_time_before_occurrence() -> None:
    event = internal_event()
    event["recorded_at"] = "2026-08-11T23:59:59Z"
    try:
        normalize_event(event)
    except ValueError as error:
        assert "occurred_at must not be later" in str(error)
    else:
        raise AssertionError("reversed event time was accepted")


def test_rejects_non_finite_json_number(tmp_path: Path) -> None:
    input_path = tmp_path / "events.ndjson"
    input_path.write_text(
        json.dumps(valid_event()).replace('"integrity_status": "verified"', '"measurement": NaN')
        + "\n",
        encoding="utf-8",
    )
    try:
        read_and_validate_events(input_path, load_schema(schema_path()))
    except ValueError as error:
        assert "non-finite JSON number" in str(error)
    else:
        raise AssertionError("non-finite JSON value was accepted")


def test_rejects_embedded_table_uri_credentials() -> None:
    try:
        validate_table_uri("s3://access:secret@approved-bucket/events")
    except ValueError as error:
        assert "embedded credentials" in str(error)
    else:
        raise AssertionError("embedded table URI credentials were accepted")


def test_report_cannot_overwrite_input(tmp_path: Path) -> None:
    input_path = tmp_path / "events.ndjson"
    schema = tmp_path / "schema.json"
    input_path.write_text("{}\n", encoding="utf-8")
    schema.write_text("{}\n", encoding="utf-8")
    try:
        validate_output_path(input_path, schema, input_path)
    except ValueError as error:
        assert "must not overwrite" in str(error)
    else:
        raise AssertionError("report was permitted to overwrite input")


def test_replay_guard_tolerates_filtered_read_after_merge_write(tmp_path: Path) -> None:
    """Regression: deltalake 1.6.2 + pyarrow 25 raise ArrowNotImplementedError
    ("Function 'greater_equal' has no kernel matching input types
    (string, string_view)") on filtered ``to_pyarrow_table`` reads of tables
    containing merge-written files: merge output types strings as
    string_view, which the pyarrow filter kernel cannot compare with string
    literals. The replay-conflict guard must catch that specific failure and
    fall back to an unfiltered read with Python-side filtering."""
    input_path = tmp_path / "events.ndjson"
    first = valid_event()
    second = valid_event()
    second["eventId"] = "1d2e3f40-5a6b-4c7d-8e9f-0a1b2c3d4e5f"
    second["correlationId"] = "correlation-002"
    write_ndjson(input_path, [first, second])
    events = read_and_validate_events(input_path, load_schema(schema_path()))
    table_path = str(tmp_path / "delta-events")
    write_deltalake(table_path, pa.Table.from_pylist(events), mode="error")

    # A merge that updates one row rewrites the file and copies the
    # untouched row into merge-written (string_view-typed) output.
    mutated = dict(events[0], payload_json='{"integrity_status":"mutated"}')
    DeltaTable(table_path).merge(
        source=pa.Table.from_pylist([mutated]),
        predicate="target.event_id = source.event_id",
        source_alias="source",
        target_alias="target",
    ).when_matched_update_all().execute()

    retained = DeltaTable(table_path)
    # Identical replay of the untouched row passes the guard even though the
    # filtered read had to fall back to an unfiltered scan.
    reject_conflicting_event_replays(retained, [events[1]])

    # A conflicting replay is still detected through the fallback path.
    conflicting = dict(events[1], payload_json='{"integrity_status":"failed"}')
    try:
        reject_conflicting_event_replays(retained, [conflicting])
    except ValueError as error:
        assert "event_id reuse conflicts" in str(error)
    else:
        raise AssertionError("conflicting replay against merge-written table was accepted")
