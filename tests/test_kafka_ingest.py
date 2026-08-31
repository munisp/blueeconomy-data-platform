from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from blueeconomy_data_platform.ingest import (
    load_schema,
    map_canonical_envelope,
    normalize_event,
)
from blueeconomy_data_platform.kafka_ingest import (
    decode_event,
    enforce_record_classification,
    validate_report_path,
    validate_transport,
)
from blueeconomy_data_platform.segregation import LakehouseScope
from signing_helpers import load_test_verifier, signed_envelope_bytes


def arguments(**overrides: object) -> argparse.Namespace:
    values: dict[str, object] = {
        "bootstrap_servers": "127.0.0.1:59092",
        "topic": "blueeconomy.events.local",
        "group_id": "blueeconomy-data-platform-local",
        "security_protocol": "PLAINTEXT",
        "ssl_ca_location": None,
        "sasl_mechanism": None,
        "allow_insecure_localhost": True,
        "max_messages": 1,
        "idle_timeout_seconds": 5.0,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def valid_event() -> dict[str, object]:
    """A minimal canonical platform envelope (envelopeVersion 1.0)."""
    return {
        "envelopeVersion": "1.0",
        "eventId": "2b3c4d5e-6f70-4819-8a2b-3c4d5e6f7081",
        "eventType": "safety.telemetry.validated",
        "occurredAt": "2026-08-12T12:00:00Z",
        "producer": "blueeconomy-waterway-safety",
        "correlationId": "correlation-kafka-001",
        "fhir": {
            "resourceType": "Bundle",
            "type": "message",
            "entry": [{"resource": {"payload_sha256": "a" * 64}}],
        },
        "provenance": {
            "principalId": "svc-waterway-safety",
            "principalRole": "telemetry-gateway",
            "signature": "b" * 64,
            "ledgerCommitHash": "c" * 64,
        },
        "classification": "INTERNAL",
    }


def test_plaintext_requires_explicit_localhost_gate() -> None:
    with pytest.raises(ValueError, match="restricted to explicit localhost"):
        validate_transport(arguments(allow_insecure_localhost=False))
    with pytest.raises(ValueError, match="restricted to explicit localhost"):
        validate_transport(arguments(bootstrap_servers="kafka.agency.example:9092"))
    configuration = validate_transport(arguments())
    assert configuration["enable.auto.commit"] is False
    assert configuration["enable.auto.offset.store"] is False


def test_decode_event_enforces_committed_schema_and_normalization() -> None:
    schema = Path(__file__).parents[1] / "schemas" / "event-envelope.schema.json"
    validator = load_schema(schema)
    normalized = decode_event(signed_envelope_bytes(valid_event()), validator, load_test_verifier())
    assert normalized["event_id"] == "2b3c4d5e-6f70-4819-8a2b-3c4d5e6f7081"
    assert json.loads(normalized["payload_json"])["payload_sha256"] == "a" * 64


def test_decode_event_rejects_undeclared_fields() -> None:
    schema = Path(__file__).parents[1] / "schemas" / "event-envelope.schema.json"
    validator = load_schema(schema)
    event = valid_event()
    event["undeclared"] = True
    with pytest.raises(ValueError, match="event-envelope validation"):
        decode_event(json.dumps(event).encode("utf-8"), validator, load_test_verifier())


def test_report_cannot_overwrite_schema(tmp_path: Path) -> None:
    schema = tmp_path / "event.schema.json"
    schema.write_text("{}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="must not overwrite"):
        validate_report_path(schema, schema)


def test_lakehouse_scope_flag_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    from blueeconomy_data_platform.kafka_ingest import parse_arguments

    argv = [
        "blueeconomy-ingest-kafka",
        "--bootstrap-servers",
        "127.0.0.1:59092",
        "--topic",
        "cvff.ledger.commitments",
        "--group-id",
        "local",
        "--security-protocol",
        "PLAINTEXT",
        "--allow-insecure-localhost",
        "--max-messages",
        "1",
        "--table-uri",
        "/lakehouse/cvff/cvff_bronze/events",
        "--dlq-topic",
        "cvff.ledger.commitments.dlq",
        "--dlq-table-uri",
        "/lakehouse/cvff/cvff_bronze/dlq_quarantine",
        "--schema",
        "schemas/event-envelope.schema.json",
        "--report",
        "/tmp/report.json",
    ]
    monkeypatch.setattr("sys.argv", argv)
    with pytest.raises(SystemExit):
        parse_arguments()
    monkeypatch.setattr("sys.argv", [*argv, "--lakehouse-scope", "cvff"])
    assert parse_arguments().lakehouse_scope == "cvff"
    for scope in ("seafarer", "fisheries", "isr"):
        monkeypatch.setattr("sys.argv", [*argv, "--lakehouse-scope", scope])
        assert parse_arguments().lakehouse_scope == scope


def isr_event(event_id: str, label: object = "SECRET") -> dict[str, object]:
    event = valid_event()
    event["eventId"] = event_id
    event["eventType"] = "maritime.isr.detection.v1"
    event["classification"] = "CONFIDENTIAL"
    if label is not None:
        event["recordClassification"] = label
    return event


def isr_uuid(ordinal: int) -> str:
    return f"00000000-0000-4000-8000-{ordinal:012d}"


def test_isr_record_classification_label_is_persisted_as_column() -> None:
    schema = Path(__file__).parents[1] / "schemas" / "event-envelope.schema.json"
    validator = load_schema(schema)
    normalized = decode_event(
        signed_envelope_bytes(isr_event(isr_uuid(1))), validator, load_test_verifier()
    )
    assert normalized["record_classification"] == "SECRET"
    assert normalized["data_classification"] == "isr_classified"
    unlabelled = normalize_event(map_canonical_envelope(valid_event()))
    assert "record_classification" not in unlabelled


def test_record_classification_label_fails_closed_on_unknown_value() -> None:
    schema = Path(__file__).parents[1] / "schemas" / "event-envelope.schema.json"
    validator = load_schema(schema)
    with pytest.raises(ValueError, match="event-envelope validation"):
        decode_event(
            json.dumps(isr_event(isr_uuid(2), label="TOP-SECRET")).encode(),
            validator,
            load_test_verifier(),
        )
    with pytest.raises(ValueError, match="clearance label"):
        normalize_event(map_canonical_envelope(isr_event(isr_uuid(3), label="Secret Squirrel")))


def test_isr_label_persists_as_delta_column_for_row_level_filtering(tmp_path: Path) -> None:
    from deltalake import DeltaTable

    from blueeconomy_data_platform.access_policy import filter_records_by_clearance
    from blueeconomy_data_platform.ingest import append_events

    schema = Path(__file__).parents[1] / "schemas" / "event-envelope.schema.json"
    validator = load_schema(schema)
    events = [
        decode_event(
            signed_envelope_bytes(isr_event(isr_uuid(6), label="SECRET")),
            validator,
            load_test_verifier(),
        ),
        decode_event(
            signed_envelope_bytes(isr_event(isr_uuid(7), label="CONFIDENTIAL")),
            validator,
            load_test_verifier(),
        ),
    ]
    enforce_record_classification(events, LakehouseScope.ISR)
    table_uri = str(tmp_path / "isr" / "isr_bronze" / "events")
    append_events(table_uri, events)
    rows = (
        DeltaTable(table_uri)
        .to_pyarrow_table(columns=["event_id", "record_classification"])
        .to_pylist()
    )
    assert {row["record_classification"] for row in rows} == {"SECRET", "CONFIDENTIAL"}
    visible = filter_records_by_clearance(rows, "CONFIDENTIAL")
    assert [row["event_id"] for row in visible] == [isr_uuid(7)]


def test_isr_scope_rejects_unlabelled_records_and_other_scopes_do_not() -> None:
    labelled = [normalize_event(map_canonical_envelope(isr_event(isr_uuid(4))))]
    enforce_record_classification(labelled, LakehouseScope.ISR)
    unlabelled = [normalize_event(map_canonical_envelope(isr_event(isr_uuid(5), label=None)))]
    with pytest.raises(ValueError, match="missing its record_classification label"):
        enforce_record_classification(unlabelled, LakehouseScope.ISR)
    # The label mandate is ISR-specific; other scopes accept unlabelled records.
    for scope in (
        LakehouseScope.PLATFORM,
        LakehouseScope.CVFF,
        LakehouseScope.SEAFARER,
        LakehouseScope.FISHERIES,
    ):
        enforce_record_classification(unlabelled, scope)
