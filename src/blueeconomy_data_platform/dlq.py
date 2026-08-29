"""Mandatory dead-letter quarantine for poison Kafka messages (Gap #45).

A malformed or unverifiable message must never halt the governed ingestion
pipeline and must never be silently dropped. Every rejected message is
quarantined, mirroring the security-operations quarantine pattern:

1. the verbatim message bytes are wrapped in a DLQ envelope (reason code,
   bounded error detail, source topic/partition/offset, SHA-256 of the
   payload, consumer-group reference and quarantine timestamp) and produced
   to the scope's dead-letter topic; and
2. the same quarantine record is appended to an append-only Delta
   quarantine table for audit and replay tooling.

Only after both quarantine writes succeed may the poisoned offset be
committed. If either quarantine path fails the pipeline halts without
committing, so a poison message is reprocessed (and re-quarantined) rather
than lost — quarantine is fail-closed, never skip-and-forget.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from confluent_kafka import Producer

from blueeconomy_data_platform.ingest import MAX_LINE_BYTES, append_events
from blueeconomy_data_platform.signature_verification import SignatureVerificationError

LOGGER = logging.getLogger("blueeconomy_data_platform.dlq")

DLQ_SCHEMA_VERSION = "blueeconomy.lakehouse.dlq-record.v1"
MAX_DLQ_DETAIL_BYTES = 2048
REASON_MALFORMED_ENVELOPE = "malformed-envelope"
REASON_DUPLICATE_EVENT_ID = "duplicate-event-id"

DLQ_TABLE_DESCRIPTION = (
    "Governed append-only quarantine table for dead-lettered Kafka messages "
    "(poison envelopes preserved verbatim with reason codes)"
)


class DeadLetterError(RuntimeError):
    """Raised when a message cannot be quarantined; the pipeline must halt."""


class DeadLetterSink(Protocol):
    """Quarantine channel for one rejected message; implementations fail closed."""

    def quarantine(
        self,
        value: bytes | None,
        source_topic: str,
        source_partition: int,
        source_offset: int,
        reason: str,
        detail: str,
    ) -> None: ...


def reason_for_error(error: Exception) -> str:
    """Map a decode failure to a stable DLQ reason code."""
    if isinstance(error, SignatureVerificationError):
        return error.reason
    return REASON_MALFORMED_ENVELOPE


def build_dlq_record(
    value: bytes | None,
    source_topic: str,
    source_partition: int,
    source_offset: int,
    reason: str,
    detail: str,
    consumer_group: str,
) -> dict[str, Any]:
    """Build the quarantine record persisted to the DLQ topic and table.

    The original message is preserved verbatim (base64) so replay tooling can
    reprocess it after the producing defect is fixed; nothing about the
    poison message is interpreted or trusted.
    """
    raw = value if isinstance(value, bytes) else b""
    if len(raw) > MAX_LINE_BYTES:
        # Defensive bound: quarantine must still succeed for oversized poison.
        raw = raw[:MAX_LINE_BYTES]
    detail_bytes = detail.encode("utf-8", errors="replace")[:MAX_DLQ_DETAIL_BYTES]
    return {
        "schema_version": DLQ_SCHEMA_VERSION,
        "dlq_event_id": hashlib.sha256(
            f"{source_topic}/{source_partition}/{source_offset}/{reason}".encode("utf-8")
        ).hexdigest(),
        "source_topic": source_topic,
        "source_partition": source_partition,
        "source_offset": source_offset,
        "reason_code": reason,
        "error_detail": detail_bytes.decode("utf-8", errors="replace"),
        "message_sha256": hashlib.sha256(raw).hexdigest(),
        "message_value_base64": base64.b64encode(raw).decode("ascii"),
        "consumer_group_sha256": hashlib.sha256(consumer_group.encode("utf-8")).hexdigest(),
        "quarantined_at": datetime.now(UTC).isoformat(),
    }


@dataclass(frozen=True)
class DeadLetterQueue:
    """Fail-closed DLQ sink: Kafka dead-letter topic plus quarantine Delta table."""

    producer: Producer
    dlq_topic: str
    quarantine_table_uri: str
    consumer_group: str

    def quarantine(
        self,
        value: bytes | None,
        source_topic: str,
        source_partition: int,
        source_offset: int,
        reason: str,
        detail: str,
    ) -> None:
        record = build_dlq_record(
            value,
            source_topic,
            source_partition,
            source_offset,
            reason,
            detail,
            self.consumer_group,
        )
        delivery_errors: list[str] = []

        def _delivery(error: Any, _message: Any) -> None:
            if error is not None:
                delivery_errors.append(str(error))

        self.producer.produce(
            self.dlq_topic,
            value=json.dumps(record, separators=(",", ":")).encode("utf-8"),
            key=record["dlq_event_id"].encode("ascii"),
            on_delivery=_delivery,
        )
        remaining = self.producer.flush(30.0)
        if remaining or delivery_errors:
            raise DeadLetterError(
                f"DLQ produce to {self.dlq_topic!r} was not confirmed "
                f"(unflushed={remaining}, errors={delivery_errors}); halting without commit"
            )
        self._append_quarantine_row(record)
        LOGGER.warning(
            "message quarantined",
            extra={
                "reason": reason,
                "source_topic": source_topic,
                "source_partition": source_partition,
                "source_offset": source_offset,
            },
        )

    def _append_quarantine_row(self, record: dict[str, Any]) -> None:
        if self._already_quarantined(record["dlq_event_id"]):
            # Crash-replay idempotency: the same source offset quarantines to
            # the same dlq_event_id; a retained row means the quarantine
            # write already landed before an earlier halt.
            return
        row = {
            "event_id": record["dlq_event_id"],
            "event_type": "lakehouse.dlq.quarantine.v1",
            "producer": "blueeconomy-data-platform",
            "occurred_at": datetime.fromisoformat(record["quarantined_at"]),
            "recorded_at": datetime.fromisoformat(record["quarantined_at"]),
            "data_classification": "internal",
            "source_system": "blueeconomy-data-platform-dlq",
            "source_record_reference": record["message_sha256"],
            "correlation_id": None,
            "payload_json": json.dumps(record, sort_keys=True),
            "ingested_at": datetime.now(UTC),
        }
        try:
            append_events(
                self.quarantine_table_uri,
                [row],
                table_description=DLQ_TABLE_DESCRIPTION,
            )
        except Exception as error:
            raise DeadLetterError(
                f"DLQ quarantine table append failed for {self.quarantine_table_uri!r}: "
                f"{error}; halting without commit"
            ) from error

    def _already_quarantined(self, dlq_event_id: str) -> bool:
        from deltalake import DeltaTable
        from deltalake.exceptions import TableNotFoundError

        try:
            table = DeltaTable(self.quarantine_table_uri)
        except TableNotFoundError:
            return False
        existing = table.to_pyarrow_table(
            columns=["event_id"], filters=[("event_id", "=", dlq_event_id)]
        )
        return bool(existing.num_rows > 0)


__all__ = [
    "DLQ_SCHEMA_VERSION",
    "DeadLetterError",
    "DeadLetterQueue",
    "DeadLetterSink",
    "REASON_DUPLICATE_EVENT_ID",
    "REASON_MALFORMED_ENVELOPE",
    "build_dlq_record",
    "reason_for_error",
]
