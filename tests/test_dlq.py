from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

import pytest
from deltalake import DeltaTable

from blueeconomy_data_platform.dlq import (
    REASON_DUPLICATE_EVENT_ID,
    REASON_MALFORMED_ENVELOPE,
    DeadLetterError,
    DeadLetterQueue,
    build_dlq_record,
    reason_for_error,
)
from blueeconomy_data_platform.ingest import load_schema
from blueeconomy_data_platform.kafka_ingest import collect_messages, decode_event
from blueeconomy_data_platform.signature_verification import SignatureVerificationError
from signing_helpers import load_test_verifier, signed_envelope_bytes

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "event-envelope.schema.json"
TOPIC = "ports.calls.v1"


class FakeMessage:
    def __init__(self, value: bytes | None, offset: int, partition: int = 0) -> None:
        self._value = value
        self._offset = offset
        self._partition = partition

    def value(self) -> bytes | None:
        return self._value

    def topic(self) -> str:
        return TOPIC

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def error(self) -> None:
        return None


class FakeConsumer:
    def __init__(self, messages: list[FakeMessage]) -> None:
        self._queue = list(messages)

    def poll(self, timeout: float) -> FakeMessage | None:
        if not self._queue:
            return None
        return self._queue.pop(0)


class FakeSink:
    def __init__(self, fail: bool = False) -> None:
        self.records: list[dict[str, Any]] = []
        self.fail = fail

    def quarantine(
        self,
        value: bytes | None,
        source_topic: str,
        source_partition: int,
        source_offset: int,
        reason: str,
        detail: str,
    ) -> None:
        if self.fail:
            raise DeadLetterError("simulated DLQ outage")
        self.records.append(
            {
                "value": value,
                "topic": source_topic,
                "partition": source_partition,
                "offset": source_offset,
                "reason": reason,
                "detail": detail,
            }
        )


class FakeProducer:
    def __init__(self, flush_remaining: int = 0, delivery_error: str | None = None) -> None:
        self.produced: list[dict[str, Any]] = []
        self._flush_remaining = flush_remaining
        self._delivery_error = delivery_error

    def produce(self, topic: str, value: bytes, key: bytes, on_delivery: Any) -> None:
        self.produced.append({"topic": topic, "value": value, "key": key})
        on_delivery(self._delivery_error, None)

    def flush(self, timeout: float) -> int:
        return self._flush_remaining


def valid_envelope(event_id: str) -> dict[str, object]:
    return {
        "envelopeVersion": "1.0",
        "eventId": event_id,
        "eventType": "ports.gate.scan.v1",
        "occurredAt": "2026-08-12T12:00:00Z",
        "producer": "s1-port-interoperability",
        "correlationId": f"corr-{event_id}",
        "fhir": {
            "resourceType": "Bundle",
            "type": "message",
            "entry": [{"resource": {"payload_sha256": "a" * 64}}],
        },
        "provenance": {
            "principalId": "svc-port",
            "principalRole": "gate-adapter",
            "signature": "b" * 64,
            "ledgerCommitHash": "c" * 64,
        },
        "classification": "INTERNAL",
    }


def decode(value: bytes | None) -> dict[str, Any]:
    return decode_event(value, load_schema(SCHEMA_PATH), load_test_verifier())


def collect(consumer: Any, sink: Any, maximum: int = 10) -> Any:
    return collect_messages(consumer, decode, maximum, 0.2, sink)


def test_poison_message_goes_to_dlq_and_pipeline_continues() -> None:
    good = signed_envelope_bytes(valid_envelope("2b3c4d5e-6f70-4819-8a2b-3c4d5e6f7081"))
    messages = [
        FakeMessage(b"{not json", offset=0),
        FakeMessage(good, offset=1),
        FakeMessage(b"", offset=2),
    ]
    sink = FakeSink()
    events, consumed, reason_counts = collect(FakeConsumer(messages), sink)
    assert [event["event_id"] for event in events] == ["2b3c4d5e-6f70-4819-8a2b-3c4d5e6f7081"]
    # Both poison messages and the valid one join the commit set.
    assert [message.offset() for message in consumed] == [0, 1, 2]
    assert [record["offset"] for record in sink.records] == [0, 2]
    assert all(record["reason"] == REASON_MALFORMED_ENVELOPE for record in sink.records)
    assert reason_counts == {REASON_MALFORMED_ENVELOPE: 2}


def test_signature_failures_are_quarantined_with_reason_code() -> None:
    envelope = valid_envelope("2b3c4d5e-6f70-4819-8a2b-3c4d5e6f7082")
    signed = json.loads(signed_envelope_bytes(envelope))
    signed["occurredAt"] = "2026-08-12T13:00:00Z"  # tampered after signing
    tampered = json.dumps(signed).encode("utf-8")
    sink = FakeSink()
    events, consumed, reason_counts = collect(FakeConsumer([FakeMessage(tampered, offset=7)]), sink)
    assert events == []
    assert sink.records[0]["reason"] == "payload-mismatch"
    assert reason_counts == {"payload-mismatch": 1}
    assert [message.offset() for message in consumed] == [7]


def test_duplicate_event_id_in_batch_is_quarantined() -> None:
    first = signed_envelope_bytes(valid_envelope("2b3c4d5e-6f70-4819-8a2b-3c4d5e6f7083"))
    second = signed_envelope_bytes(valid_envelope("2b3c4d5e-6f70-4819-8a2b-3c4d5e6f7083"))
    sink = FakeSink()
    events, _, reason_counts = collect(
        FakeConsumer([FakeMessage(first, 0), FakeMessage(second, 1)]), sink
    )
    assert len(events) == 1
    assert sink.records[0]["reason"] == REASON_DUPLICATE_EVENT_ID
    assert reason_counts == {REASON_DUPLICATE_EVENT_ID: 1}


def test_dlq_outage_fails_closed_before_any_commit() -> None:
    sink = FakeSink(fail=True)
    with pytest.raises(DeadLetterError):
        collect(FakeConsumer([FakeMessage(b"{not json", offset=0)]), sink)


def test_reason_for_error_maps_signature_reasons() -> None:
    error = SignatureVerificationError("unknown-kid", "kid not trusted")
    assert reason_for_error(error) == "unknown-kid"
    assert reason_for_error(ValueError("boom")) == REASON_MALFORMED_ENVELOPE


def test_build_dlq_record_preserves_message_verbatim() -> None:
    payload = b"\xff\xfe binary poison \x00"
    record = build_dlq_record(payload, TOPIC, 1, 42, "malformed-envelope", "bad json", "group-1")
    assert record["schema_version"] == "blueeconomy.lakehouse.dlq-record.v1"
    assert base64.b64decode(record["message_value_base64"]) == payload
    assert record["source_partition"] == 1
    assert record["source_offset"] == 42
    assert record["reason_code"] == "malformed-envelope"
    assert len(record["dlq_event_id"]) == 64
    assert record["consumer_group_sha256"] != "group-1"


def test_dead_letter_queue_quarantines_to_topic_and_table(tmp_path: Path) -> None:
    producer = FakeProducer()
    queue = DeadLetterQueue(
        producer=producer,  # type: ignore[arg-type]
        dlq_topic=f"{TOPIC}.dlq",
        quarantine_table_uri=str(tmp_path / "platform" / "dlq_quarantine"),
        consumer_group="group-1",
    )
    queue.quarantine(b"{poison", TOPIC, 0, 5, "malformed-envelope", "bad json")
    assert len(producer.produced) == 1
    produced = producer.produced[0]
    assert produced["topic"] == f"{TOPIC}.dlq"
    envelope = json.loads(produced["value"].decode("utf-8"))
    assert envelope["reason_code"] == "malformed-envelope"
    table = DeltaTable(str(tmp_path / "platform" / "dlq_quarantine"))
    rows = table.to_pyarrow_table().to_pylist()
    assert len(rows) == 1
    assert rows[0]["event_type"] == "lakehouse.dlq.quarantine.v1"
    quarantine_payload = json.loads(rows[0]["payload_json"])
    assert quarantine_payload["source_offset"] == 5
    assert base64.b64decode(quarantine_payload["message_value_base64"]) == b"{poison"
    assert table.metadata().configuration["delta.appendOnly"] == "true"
    # Re-quarantine of the same offset is idempotent on the dlq_event_id.
    queue.quarantine(b"{poison", TOPIC, 0, 5, "malformed-envelope", "bad json")
    assert (
        DeltaTable(str(tmp_path / "platform" / "dlq_quarantine")).to_pyarrow_table().num_rows == 1
    )


def test_dead_letter_queue_fails_closed_on_produce_failure(tmp_path: Path) -> None:
    for producer in (FakeProducer(flush_remaining=1), FakeProducer(delivery_error="broker down")):
        queue = DeadLetterQueue(
            producer=producer,  # type: ignore[arg-type]
            dlq_topic=f"{TOPIC}.dlq",
            quarantine_table_uri=str(tmp_path / "platform" / "dlq_quarantine2"),
            consumer_group="group-1",
        )
        with pytest.raises(DeadLetterError):
            queue.quarantine(b"x", TOPIC, 0, 0, "malformed-envelope", "bad")
