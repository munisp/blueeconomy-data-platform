from __future__ import annotations

import json
from pathlib import Path

from deltalake import DeltaTable

from blueeconomy_data_platform.ingest import (
    append_events,
    load_schema,
    normalize_event,
    read_and_validate_events,
    validate_maritime_position,
    validate_output_path,
    validate_table_uri,
)


def valid_event() -> dict[str, object]:
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
    second["payload"] = {"integrity_status": "failed"}
    write_ndjson(input_path, [second])
    conflicting = read_and_validate_events(input_path, load_schema(schema_path()))
    try:
        append_events(table_path, conflicting)
    except ValueError as error:
        assert "event_id reuse conflicts" in str(error)
    else:
        raise AssertionError("conflicting event ID reuse was accepted")

    retained = DeltaTable(table_path).to_pyarrow_table().to_pylist()
    assert retained[0]["payload_json"] == '{"integrity_status":"verified"}'


def test_accepts_valid_maritime_position_payload() -> None:
    event = valid_event()
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
    event = valid_event()
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
