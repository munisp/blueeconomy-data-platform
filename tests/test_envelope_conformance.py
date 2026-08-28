"""Producer-to-ingest conformance for the canonical platform event envelope.

Each fixture under ``tests/fixtures/envelopes`` is a minimal real sample of
the envelope emitted by one governed producer's own outbox code
(``internal/outbox/envelope.go`` or ``src/events/envelope.ts`` in the
producer repository), rendered in the canonical platform contract
(``blueeconomy.contracts.v1.EventEnvelope``, envelopeVersion 1.0). Every
sample must validate against the committed JSON schema and map to the
producer's mandatory internal lakehouse scope label.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from blueeconomy_data_platform.access_policy import Clearance
from blueeconomy_data_platform.ingest import load_schema, map_canonical_envelope
from blueeconomy_data_platform.kafka_ingest import decode_event
from blueeconomy_data_platform.segregation import (
    BoundaryViolationError,
    LakehouseScope,
    enforce_event_scope,
    scope_for_classification,
)

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "envelopes"
SCHEMA = Path(__file__).resolve().parents[1] / "schemas" / "event-envelope.schema.json"

# fixture name -> (producer, mandatory scope, internal classification label)
PRODUCER_EXPECTATIONS = {
    "ferry-ticketing.json": ("ferry-ticketing", LakehouseScope.PLATFORM, "internal"),
    "financial-controls.json": (
        "financial-controls",
        LakehouseScope.CVFF,
        "fiduciary_segregated",
    ),
    "fisheries-traceability.json": (
        "fisheries-traceability",
        LakehouseScope.FISHERIES,
        "fisheries_operational",
    ),
    "port-interoperability.json": (
        "s1-port-interoperability",
        LakehouseScope.PLATFORM,
        "internal",
    ),
    "credential-verification.json": (
        "credential-verification",
        LakehouseScope.SEAFARER,
        "seafarer_confidential",
    ),
}


def load_fixture(name: str) -> dict[str, object]:
    document: dict[str, object] = json.loads((FIXTURES / name).read_text(encoding="utf-8"))
    return document


def test_every_governed_producer_has_a_fixture() -> None:
    fixture_names = {path.name for path in FIXTURES.glob("*.json")}
    assert fixture_names == set(PRODUCER_EXPECTATIONS)


@pytest.mark.parametrize("fixture_name", sorted(PRODUCER_EXPECTATIONS))
def test_producer_envelope_validates_and_maps_to_scope(fixture_name: str) -> None:
    producer, scope, label = PRODUCER_EXPECTATIONS[fixture_name]
    validator = load_schema(SCHEMA)
    envelope = load_fixture(fixture_name)

    # Schema validation passes for the real emitted shape.
    assert list(validator.iter_errors(envelope)) == []
    assert envelope["producer"] == producer

    # Canonical classification maps to the internal lowercase scope label,
    # and the label resolves to the producer's mandatory segregated scope.
    internal = map_canonical_envelope(envelope)
    assert internal["data_classification"] == label
    assert scope_for_classification(internal["data_classification"]) is scope

    # The full Kafka decode path normalizes the envelope, retains the domain
    # resource and provenance in the payload, and passes the scope boundary
    # enforced by the scope's own writer.
    normalized = decode_event(json.dumps(envelope).encode("utf-8"), validator)
    assert normalized["event_id"] == envelope["eventId"]
    assert normalized["event_type"] == envelope["eventType"]
    assert normalized["data_classification"] == label
    enforce_event_scope([normalized], scope)
    retained_payload = json.loads(normalized["payload_json"])
    assert retained_payload["provenance"] == envelope["provenance"]
    # These producers do not emit per-record clearance labels.
    assert "record_classification" not in normalized


@pytest.mark.parametrize("fixture_name", sorted(PRODUCER_EXPECTATIONS))
def test_producer_envelope_rejected_by_other_segregated_scopes(fixture_name: str) -> None:
    _, scope, _ = PRODUCER_EXPECTATIONS[fixture_name]
    validator = load_schema(SCHEMA)
    normalized = decode_event(json.dumps(load_fixture(fixture_name)).encode("utf-8"), validator)
    # Every other scope's writer must reject the event: platform and
    # segregated scopes are mutually exclusive boundaries.
    for other in LakehouseScope:
        if other is scope:
            continue
        with pytest.raises(BoundaryViolationError):
            enforce_event_scope([normalized], other)


def test_record_classification_maps_to_internal_clearance_label() -> None:
    validator = load_schema(SCHEMA)
    envelope = load_fixture("fisheries-traceability.json")
    envelope["recordClassification"] = "RESTRICTED"
    normalized = decode_event(json.dumps(envelope).encode("utf-8"), validator)
    assert normalized["record_classification"] == "RESTRICTED"
    assert Clearance.from_label(normalized["record_classification"]) is Clearance.RESTRICTED


def test_canonical_mapping_fails_closed() -> None:
    validator = load_schema(SCHEMA)
    # FIDUCIARY_SEGREGATED can never map onto a platform-scope event type.
    envelope = load_fixture("ferry-ticketing.json")
    envelope["classification"] = "FIDUCIARY_SEGREGATED"
    with pytest.raises(BoundaryViolationError):
        decode_event(json.dumps(envelope).encode("utf-8"), validator)
    # A cvff event type without FIDUCIARY_SEGREGATED fails closed.
    envelope = load_fixture("financial-controls.json")
    envelope["classification"] = "INTERNAL"
    with pytest.raises(BoundaryViolationError):
        decode_event(json.dumps(envelope).encode("utf-8"), validator)
    # A classification outside the canonical vocabulary fails schema validation.
    envelope = load_fixture("ferry-ticketing.json")
    envelope["classification"] = "highly_restricted"
    with pytest.raises(ValueError, match="event-envelope validation"):
        decode_event(json.dumps(envelope).encode("utf-8"), validator)
    # The retired snake_case dialect is rejected outright.
    with pytest.raises(ValueError, match="event-envelope validation"):
        decode_event(
            json.dumps(
                {
                    "event_id": "event-legacy-001",
                    "event_type": "ports.gate.scan.v1",
                    "producer": "legacy-producer",
                    "occurred_at": "2026-08-12T12:00:00Z",
                    "recorded_at": "2026-08-12T12:00:01Z",
                    "data_classification": "internal",
                    "source_system": "legacy",
                    "source_record_reference": "legacy-001",
                    "payload": {},
                }
            ).encode("utf-8"),
            validator,
        )
