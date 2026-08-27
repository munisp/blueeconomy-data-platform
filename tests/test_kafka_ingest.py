from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from blueeconomy_data_platform.ingest import load_schema
from blueeconomy_data_platform.kafka_ingest import (
    decode_event,
    validate_report_path,
    validate_transport,
)


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
    return {
        "event_id": "event-kafka-001",
        "event_type": "safety.telemetry.validated",
        "producer": "blueeconomy-waterway-safety",
        "occurred_at": "2026-08-12T12:00:00Z",
        "recorded_at": "2026-08-12T12:00:01Z",
        "data_classification": "internal",
        "source_system": "approved-local-conformance",
        "source_record_reference": "record-kafka-001",
        "payload": {"payload_sha256": "a" * 64},
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
    normalized = decode_event(
        json.dumps(valid_event(), separators=(",", ":")).encode("utf-8"), validator
    )
    assert normalized["event_id"] == "event-kafka-001"
    assert json.loads(normalized["payload_json"])["payload_sha256"] == "a" * 64


def test_decode_event_rejects_undeclared_fields() -> None:
    schema = Path(__file__).parents[1] / "schemas" / "event-envelope.schema.json"
    validator = load_schema(schema)
    event = valid_event()
    event["undeclared"] = True
    with pytest.raises(ValueError, match="event-envelope validation"):
        decode_event(json.dumps(event).encode("utf-8"), validator)


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
