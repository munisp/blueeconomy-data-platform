"""Generic segregated-scope medallion pipeline for the phase-8 scopes.

The MRV emissions scope (``mrv.*`` topics) and the Blue-Carbon registry
scope (``bluecarbon.*`` topics) share one medallion shape:

- **bronze** retains the raw validated envelope exactly as ingested
  (append-only, ``delta.appendOnly=true``), with the default retention
  policy of 30 days hot / 7 years cold committed in the table description.
- **silver** is deduplicated on the composite key
  ``dedup_key = sha256(topic/partition/offset/eventId)`` (spec: mrv scope
  §3.2), so a replayed Kafka record never produces a second silver row and
  a conflicting replay fails closed.
- **gold** products are curated named tables under the scope's gold layer
  directory, assembled by the scope-specific modules
  (:mod:`blueeconomy_data_platform.mrv_gold` and
  :mod:`blueeconomy_data_platform.bluecarbon_gold`).

Every write passes through :class:`SegregatedDeltaWriter`, so the scope
boundary is enforced at every layer exactly as for the earlier scopes.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError, TableNotFoundError

from blueeconomy_data_platform.ingest import (
    MAX_COMMIT_ATTEMPTS,
    append_events,
    read_identity_rows,
)
from blueeconomy_data_platform.medallion import RetentionPolicy
from blueeconomy_data_platform.segregation import (
    BoundaryViolationError,
    LakehouseScope,
    SegregatedDeltaWriter,
    enforce_topic_scope,
)

# Scopes served by this generic pipeline; every other scope fails closed.
GENERIC_MEDALLION_SCOPES = frozenset({LakehouseScope.MRV, LakehouseScope.BLUECARBON})

SILVER_IDENTITY_COLUMNS = (
    "event_id",
    "event_type",
    "producer",
    "occurred_at",
    "recorded_at",
    "data_classification",
    "source_system",
    "source_record_reference",
    "correlation_id",
    "payload_json",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
)


def require_generic_medallion_scope(scope: LakehouseScope) -> None:
    """Fail closed unless the scope is served by this generic pipeline."""
    if scope not in GENERIC_MEDALLION_SCOPES:
        raise BoundaryViolationError(
            f"the generic bronze/silver medallion pipeline is only defined for "
            f"{sorted(item.value for item in GENERIC_MEDALLION_SCOPES)}; "
            f"got {scope.value!r}"
        )


class ScopeKafkaRecordMetadata:
    """Kafka coordinates of one consumed record inside a segregated scope."""

    __slots__ = ("topic", "partition", "offset")

    def __init__(self, scope: LakehouseScope, topic: str, partition: int, offset: int) -> None:
        require_generic_medallion_scope(scope)
        enforce_topic_scope(topic, scope)
        if not isinstance(partition, int) or isinstance(partition, bool) or partition < 0:
            raise ValueError("kafka partition must be a non-negative integer")
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise ValueError("kafka offset must be a non-negative integer")
        self.topic = topic
        self.partition = partition
        self.offset = offset


def silver_dedup_key(metadata: ScopeKafkaRecordMetadata, event_id: str) -> str:
    """Composite silver dedup key: Kafka coordinates plus the envelope event id."""
    material = f"{metadata.topic}/{metadata.partition}/{metadata.offset}/{event_id}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def build_silver_record(
    event: dict[str, Any], metadata: ScopeKafkaRecordMetadata
) -> dict[str, Any]:
    """Project a validated bronze row into the deduplicated silver shape."""
    event_id = event.get("event_id")
    if not isinstance(event_id, str) or not event_id:
        raise ValueError("bronze event is missing event_id")
    record = {column: event.get(column) for column in SILVER_IDENTITY_COLUMNS[:10]}
    record.update(
        {
            "kafka_topic": metadata.topic,
            "kafka_partition": metadata.partition,
            "kafka_offset": metadata.offset,
            "dedup_key": silver_dedup_key(metadata, event_id),
            "promoted_at": datetime.now(UTC),
        }
    )
    return record


def append_scope_bronze(
    writer: SegregatedDeltaWriter,
    events: list[dict[str, Any]],
    kafka_topic: str | None = None,
    retention: RetentionPolicy | None = None,
) -> tuple[int, int, int]:
    """Append raw validated events to the scope's segregated bronze table."""
    require_generic_medallion_scope(writer.scope)
    policy = retention or RetentionPolicy()
    table_uri = writer.guard_write("bronze", events, kafka_topic)
    return append_events(
        table_uri,
        events,
        table_description=policy.as_table_description(writer.scope.value.upper()),
    )


def append_scope_silver(
    writer: SegregatedDeltaWriter,
    records: list[dict[str, Any]],
) -> tuple[int, int, int]:
    """Insert-only merge of silver rows keyed by the composite dedup key.

    Returns ``(table_version, records_written, records_already_present)``; a
    replayed Kafka record with an identical dedup key is counted as already
    present and is never duplicated, and a conflicting reuse of a key fails
    closed against the retained immutable silver content.
    """
    require_generic_medallion_scope(writer.scope)
    if not records:
        raise ValueError("refusing to promote an empty silver batch")
    # Phase-7/8 OTel span (no-op when telemetry is disabled); silver stage of
    # the medallion DAG and the object_store (S3/MinIO) client span.
    from blueeconomy_data_platform.telemetry import get_tracer

    with get_tracer().start_as_current_span("lakehouse.silver.append") as span:
        span.set_attribute("lakehouse.scope", writer.scope.value)
        span.set_attribute("lakehouse.rows", len(records))
        version, written, already_present = _append_scope_silver(writer, records)
        span.set_attribute("lakehouse.records_written", written)
        span.set_attribute("lakehouse.records_already_present", already_present)
        span.set_attribute("lakehouse.table_version", version)
        return version, written, already_present


def _append_scope_silver(
    writer: SegregatedDeltaWriter,
    records: list[dict[str, Any]],
) -> tuple[int, int, int]:
    keys = [str(record["dedup_key"]) for record in records]
    if len(set(keys)) != len(keys):
        raise ValueError("silver batch repeats a dedup_key")
    scope = writer.scope
    table_uri = writer.table_uri("silver")
    arrow_table = pa.Table.from_pylist(records)
    try:
        DeltaTable(table_uri, without_files=True)
    except TableNotFoundError:
        write_deltalake(
            table_uri,
            arrow_table,
            mode="error",
            name=f"{scope.value}_silver_events",
            description=(
                f"{scope.value.upper()} segregated silver table deduplicated by "
                "Kafka offset and envelope event id"
            ),
            configuration={"delta.appendOnly": "true"},
        )
        return DeltaTable(table_uri).version(), len(records), 0

    for attempt in range(1, MAX_COMMIT_ATTEMPTS + 1):
        table = DeltaTable(table_uri)
        if table.metadata().configuration.get("delta.appendOnly") != "true":
            raise ValueError("existing silver table is not configured with delta.appendOnly=true")
        _reject_conflicting_dedup_replays(table, records)
        try:
            metrics = (
                table.merge(
                    source=arrow_table,
                    predicate="target.dedup_key = source.dedup_key",
                    source_alias="source",
                    target_alias="target",
                )
                .when_not_matched_insert_all()
                .execute()
            )
            written = int(metrics["num_target_rows_inserted"])
            retained = DeltaTable(table_uri)
            _reject_conflicting_dedup_replays(retained, records)
            return retained.version(), written, len(records) - written
        except CommitFailedError:
            if attempt == MAX_COMMIT_ATTEMPTS:
                raise
    raise RuntimeError("silver Delta commit retry loop exhausted unexpectedly")


def _reject_conflicting_dedup_replays(table: DeltaTable, records: list[dict[str, Any]]) -> None:
    keys = [str(record["dedup_key"]) for record in records]
    existing_rows = read_identity_rows(
        table, [*SILVER_IDENTITY_COLUMNS, "dedup_key"], "dedup_key", keys
    )
    existing_by_key = {str(row["dedup_key"]): row for row in existing_rows}
    conflicts: list[str] = []
    for record in records:
        existing = existing_by_key.get(str(record["dedup_key"]))
        if existing is None:
            continue
        if any(existing[column] != record[column] for column in SILVER_IDENTITY_COLUMNS):
            conflicts.append(str(record["dedup_key"]))
    if conflicts:
        raise ValueError(
            "dedup_key reuse conflicts with retained immutable silver content: "
            + ", ".join(sorted(conflicts))
        )


__all__ = [
    "GENERIC_MEDALLION_SCOPES",
    "SILVER_IDENTITY_COLUMNS",
    "ScopeKafkaRecordMetadata",
    "append_scope_bronze",
    "append_scope_silver",
    "build_silver_record",
    "require_generic_medallion_scope",
    "silver_dedup_key",
]
