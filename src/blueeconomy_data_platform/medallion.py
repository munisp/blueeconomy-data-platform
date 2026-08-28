"""CVFF fiduciary-segregated medallion pipeline (bronze, silver, gold).

Bronze retains the raw validated envelope exactly as ingested, under a
retention policy of 30 days hot storage and 7 years cold storage by default.
Silver is deduplicated on the composite key of Kafka offset and the CVFF
ledger commit hash (``dedup_key = sha256(topic/partition/offset/
ledgerCommitHash)``), so a replayed Kafka record never produces a second
silver row. Gold is a curated one-row-per-ledger-commitment snapshot derived
from silver. All writes pass through :class:`SegregatedDeltaWriter`, so the
fiduciary boundary is enforced at every layer.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError, TableNotFoundError

from blueeconomy_data_platform.ingest import MAX_COMMIT_ATTEMPTS, read_identity_rows
from blueeconomy_data_platform.segregation import (
    BoundaryViolationError,
    LakehouseScope,
    SegregatedDeltaWriter,
)

DEFAULT_HOT_RETENTION_DAYS = 30
DEFAULT_COLD_RETENTION_YEARS = 7
MAX_HOT_RETENTION_DAYS = 366
MAX_COLD_RETENTION_YEARS = 25
LEDGER_COMMIT_HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")

SILVER_TABLE_DESCRIPTION = (
    "CVFF segregated silver table deduplicated by Kafka offset and ledger hash"
)
GOLD_TABLE_DESCRIPTION = "CVFF segregated gold table curated per ledger commitment"

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
    "ledger_commit_hash",
)


@dataclass(frozen=True)
class RetentionPolicy:
    """Bronze retention policy: hot window plus extended cold-tier horizon."""

    hot_days: int = DEFAULT_HOT_RETENTION_DAYS
    cold_years: int = DEFAULT_COLD_RETENTION_YEARS

    def __post_init__(self) -> None:
        if not 1 <= self.hot_days <= MAX_HOT_RETENTION_DAYS:
            raise ValueError(f"hot retention must be between 1 and {MAX_HOT_RETENTION_DAYS} days")
        if not 1 <= self.cold_years <= MAX_COLD_RETENTION_YEARS:
            raise ValueError(
                f"cold retention must be between 1 and {MAX_COLD_RETENTION_YEARS} years"
            )
        if self.cold_years * 365 <= self.hot_days:
            raise ValueError("cold retention horizon must exceed the hot retention window")

    @property
    def hot_horizon(self) -> timedelta:
        return timedelta(days=self.hot_days)

    @property
    def cold_horizon(self) -> timedelta:
        return timedelta(days=self.cold_years * 365)

    def tier_for(self, occurred_at: datetime, reference: datetime) -> str:
        """Classify a record as ``hot``, ``cold`` or ``expired`` relative to a reference time."""
        occurred = occurred_at.astimezone(UTC)
        checked = reference.astimezone(UTC)
        if occurred > checked:
            raise ValueError("occurred_at must not be later than the retention reference time")
        age = checked - occurred
        if age <= self.hot_horizon:
            return "hot"
        if age <= self.cold_horizon:
            return "cold"
        return "expired"

    def as_table_description(self, scope_label: str = "CVFF") -> str:
        """Encode the policy in the Delta table description for operations evidence.

        The delta-rs kernel accepts only known ``delta.*`` table properties, so
        the retention horizons are committed in the table description, which is
        retained table metadata, rather than in rejected custom properties.
        """
        return (
            f"{scope_label} segregated bronze table (raw validated envelopes); "
            f"retention: hot={self.hot_days}d, cold={self.cold_years}y"
        )


@dataclass(frozen=True)
class KafkaRecordMetadata:
    """Kafka coordinates of one consumed cvff.* record."""

    topic: str
    partition: int
    offset: int

    def __post_init__(self) -> None:
        if not self.topic.startswith("cvff."):
            raise BoundaryViolationError(
                f"silver dedup metadata requires a cvff.* topic, got {self.topic!r}"
            )
        if (
            not isinstance(self.partition, int)
            or isinstance(self.partition, bool)
            or self.partition < 0
        ):
            raise ValueError("kafka partition must be a non-negative integer")
        if not isinstance(self.offset, int) or isinstance(self.offset, bool) or self.offset < 0:
            raise ValueError("kafka offset must be a non-negative integer")


def extract_ledger_commit_hash(event: dict[str, Any]) -> str:
    """Extract and validate the CVFF ``ledgerCommitHash`` from a bronze payload.

    Canonical-ingested bronze payloads carry the FHIR message entry resource
    with the envelope provenance block attached, so the commit hash is read
    from ``provenance.ledgerCommitHash``; a top-level ``ledgerCommitHash`` is
    still accepted for directly projected payloads. Anything else fails
    closed.
    """
    payload_json = event.get("payload_json")
    if not isinstance(payload_json, str):
        raise ValueError("bronze event is missing payload_json")
    payload = json.loads(payload_json)
    if not isinstance(payload, dict):
        raise ValueError("cvff payload must be a JSON object")
    ledger_hash = payload.get("ledgerCommitHash")
    if ledger_hash is None:
        provenance = payload.get("provenance")
        if isinstance(provenance, dict):
            ledger_hash = provenance.get("ledgerCommitHash")
    if not isinstance(ledger_hash, str) or not LEDGER_COMMIT_HASH_PATTERN.fullmatch(ledger_hash):
        raise ValueError(
            "cvff payload must carry a ledgerCommitHash of 64 lowercase hexadecimal characters"
        )
    return ledger_hash


def silver_dedup_key(metadata: KafkaRecordMetadata, ledger_commit_hash: str) -> str:
    """Composite silver dedup key: Kafka offset plus ledger commit hash."""
    material = f"{metadata.topic}/{metadata.partition}/{metadata.offset}/{ledger_commit_hash}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def build_silver_record(event: dict[str, Any], metadata: KafkaRecordMetadata) -> dict[str, Any]:
    """Project a validated bronze row into the deduplicated silver shape."""
    ledger_hash = extract_ledger_commit_hash(event)
    record = {column: event.get(column) for column in SILVER_IDENTITY_COLUMNS[:10]}
    if record["event_id"] is None:
        raise ValueError("bronze event is missing event_id")
    record.update(
        {
            "kafka_topic": metadata.topic,
            "kafka_partition": metadata.partition,
            "kafka_offset": metadata.offset,
            "ledger_commit_hash": ledger_hash,
            "dedup_key": silver_dedup_key(metadata, ledger_hash),
            "promoted_at": datetime.now(UTC),
        }
    )
    return record


def _table_exists(table_uri: str) -> bool:
    try:
        DeltaTable(table_uri, without_files=True)
    except TableNotFoundError:
        return False
    return True


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


def append_bronze(
    writer: SegregatedDeltaWriter,
    events: list[dict[str, Any]],
    kafka_topic: str | None = None,
    retention: RetentionPolicy | None = None,
) -> tuple[int, int, int]:
    """Append raw cvff events to the segregated bronze table (boundary-enforced).

    The retention policy (30 days hot / 7 years cold by default) is committed
    in the Delta table description at creation, so the horizons are retained
    table metadata, not an informal convention.
    """
    from blueeconomy_data_platform.ingest import append_events

    policy = retention or RetentionPolicy()
    table_uri = writer.guard_write("bronze", events, kafka_topic)
    return append_events(
        table_uri,
        events,
        table_description=policy.as_table_description(writer.scope.value.upper()),
    )


def append_silver(
    writer: SegregatedDeltaWriter,
    records: list[dict[str, Any]],
) -> tuple[int, int, int]:
    """Insert-only merge of silver rows keyed by the composite dedup key.

    Returns ``(table_version, records_written, records_already_present)``; a
    replayed Kafka record with an identical dedup key is counted as already
    present and is never duplicated.
    """
    if writer.scope is not LakehouseScope.CVFF:
        raise BoundaryViolationError("silver promotion is only defined for the cvff boundary")
    if not records:
        raise ValueError("refusing to promote an empty silver batch")
    keys = [str(record["dedup_key"]) for record in records]
    if len(set(keys)) != len(keys):
        raise ValueError("silver batch repeats a dedup_key")
    table_uri = writer.table_uri("silver")
    arrow_table = pa.Table.from_pylist(records)
    if not _table_exists(table_uri):
        write_deltalake(
            table_uri,
            arrow_table,
            mode="error",
            name="cvff_silver_events",
            description=SILVER_TABLE_DESCRIPTION,
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


def curate_gold(writer: SegregatedDeltaWriter) -> tuple[int, int]:
    """Rebuild the curated gold table from silver, one row per ledger commitment.

    Returns ``(table_version, row_count)``. The gold table is derived state and
    is overwritten atomically from the current silver content.
    """
    if writer.scope is not LakehouseScope.CVFF:
        raise BoundaryViolationError("gold curation is only defined for the cvff boundary")
    silver_uri = writer.table_uri("silver")
    if not _table_exists(silver_uri):
        raise ValueError("cannot curate gold before the silver table exists")
    rows = (
        DeltaTable(silver_uri)
        .to_pyarrow_table(
            columns=["ledger_commit_hash", "event_id", "occurred_at", "source_system"]
        )
        .to_pylist()
    )
    if not rows:
        raise ValueError("cannot curate gold from an empty silver table")

    curated: dict[str, dict[str, Any]] = {}
    for row in rows:
        ledger_hash = str(row["ledger_commit_hash"])
        occurred_at = row["occurred_at"]
        entry = curated.get(ledger_hash)
        if entry is None:
            curated[ledger_hash] = {
                "ledger_commit_hash": ledger_hash,
                "record_count": 1,
                "first_occurred_at": occurred_at,
                "last_occurred_at": occurred_at,
                "distinct_source_systems": {str(row["source_system"])},
                "event_ids": [str(row["event_id"])],
            }
            continue
        entry["record_count"] += 1
        entry["first_occurred_at"] = min(entry["first_occurred_at"], occurred_at)
        entry["last_occurred_at"] = max(entry["last_occurred_at"], occurred_at)
        entry["distinct_source_systems"].add(str(row["source_system"]))
        entry["event_ids"].append(str(row["event_id"]))

    curated_rows = [
        {
            "ledger_commit_hash": entry["ledger_commit_hash"],
            "record_count": entry["record_count"],
            "first_occurred_at": entry["first_occurred_at"],
            "last_occurred_at": entry["last_occurred_at"],
            "source_systems_json": json.dumps(sorted(entry["distinct_source_systems"])),
            "event_ids_json": json.dumps(sorted(entry["event_ids"])),
            "curated_at": datetime.now(UTC),
        }
        for _, entry in sorted(curated.items())
    ]
    gold_uri = writer.table_uri("gold")
    write_deltalake(
        gold_uri,
        pa.Table.from_pylist(curated_rows),
        mode="overwrite",
        name="cvff_gold_ledger_commitments",
        description=GOLD_TABLE_DESCRIPTION,
    )
    table = DeltaTable(gold_uri)
    return table.version(), len(curated_rows)


def retention_report(
    writer: SegregatedDeltaWriter, retention: RetentionPolicy, reference: datetime | None = None
) -> dict[str, int]:
    """Count bronze records per retention tier for operations evidence."""
    if writer.scope is not LakehouseScope.CVFF:
        raise BoundaryViolationError("retention reporting is only defined for the cvff boundary")
    bronze_uri = writer.table_uri("bronze")
    if not _table_exists(bronze_uri):
        raise ValueError("cannot report retention before the bronze table exists")
    checked = (reference or datetime.now(UTC)).astimezone(UTC)
    rows = DeltaTable(bronze_uri).to_pyarrow_table(columns=["occurred_at"]).to_pylist()
    counts = {"hot": 0, "cold": 0, "expired": 0}
    for row in rows:
        counts[retention.tier_for(row["occurred_at"], checked)] += 1
    return counts


__all__ = [
    "DEFAULT_COLD_RETENTION_YEARS",
    "DEFAULT_HOT_RETENTION_DAYS",
    "KafkaRecordMetadata",
    "RetentionPolicy",
    "append_bronze",
    "append_silver",
    "build_silver_record",
    "curate_gold",
    "extract_ledger_commit_hash",
    "retention_report",
    "silver_dedup_key",
]
