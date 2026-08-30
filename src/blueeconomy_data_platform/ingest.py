"""Append approved, real-source platform events to a governed Delta Lake table.

The command intentionally has no default input, table URI, source-system value or
synthetic fallback. It validates a real NDJSON source against the committed event
envelope schema before writing an append-only Delta table.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
import unicodedata
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError, TableNotFoundError
from jsonschema import Draft202012Validator, FormatChecker

from blueeconomy_data_platform.segregation import map_canonical_classification

MAX_RECORDS_PER_BATCH = 100_000
MAX_INPUT_BYTES = 256 * 1024 * 1024
MAX_LINE_BYTES = 2 * 1024 * 1024
MAX_PAYLOAD_JSON_BYTES = 1024 * 1024
MAX_COMMIT_ATTEMPTS = 3
EVENT_TABLE_DESCRIPTION = "Governed append-only Blue Economy Platform event envelope table"


@dataclass(frozen=True)
class IngestionReport:
    schema_version: str
    started_at: str
    completed_at: str
    input_sha256: str
    table_reference_sha256: str
    records_received: int
    records_written: int
    records_already_present: int
    source_systems: list[str]
    data_classifications: list[str]
    table_version: int


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate approved NDJSON events and append them to an append-only Delta Lake table."
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Approved real-source NDJSON input path. The command rejects absent or empty input.",
    )
    parser.add_argument(
        "--table-uri",
        required=True,
        help="Approved Delta Lake table URI. No default table location exists.",
    )
    parser.add_argument(
        "--schema",
        required=True,
        type=Path,
        help="Event-envelope JSON Schema path.",
    )
    parser.add_argument(
        "--report",
        required=True,
        type=Path,
        help="Non-secret JSON run-report output path.",
    )
    return parser.parse_args()


def load_schema(path: Path) -> Draft202012Validator:
    require_regular_file(path, "schema", MAX_LINE_BYTES)
    with path.open("r", encoding="utf-8") as handle:
        schema = json.load(handle, parse_constant=reject_non_finite_constant)
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


def read_and_validate_events(
    input_path: Path, validator: Draft202012Validator
) -> list[dict[str, Any]]:
    require_regular_file(input_path, "input", MAX_INPUT_BYTES)

    events: list[dict[str, Any]] = []
    event_ids: set[str] = set()
    with input_path.open("rb") as handle:
        line_number = 0
        while True:
            raw_line = handle.readline(MAX_LINE_BYTES + 1)
            if not raw_line:
                break
            line_number += 1
            if len(raw_line) > MAX_LINE_BYTES:
                raise ValueError(f"input line {line_number} exceeds {MAX_LINE_BYTES} bytes")
            if not raw_line.strip():
                continue
            try:
                line = raw_line.decode("utf-8")
            except UnicodeDecodeError as error:
                raise ValueError(f"input line {line_number} is not valid UTF-8") from error
            try:
                event = json.loads(line, parse_constant=reject_non_finite_constant)
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"input line {line_number} is not valid JSON: {error.msg}"
                ) from error
            if not isinstance(event, dict):
                raise ValueError(f"input line {line_number} must be a JSON object")
            validation_errors = sorted(
                validator.iter_errors(event), key=lambda item: list(item.path)
            )
            if validation_errors:
                messages = "; ".join(error.message for error in validation_errors)
                raise ValueError(
                    f"input line {line_number} fails event-envelope validation: {messages}"
                )
            event = map_canonical_envelope(event)
            event_id = event["event_id"]
            if event_id in event_ids:
                raise ValueError(f"input contains duplicate event_id {event_id!r}")
            event_ids.add(event_id)
            events.append(normalize_event(event))
            if len(events) > MAX_RECORDS_PER_BATCH:
                raise ValueError(
                    f"input exceeds the maximum batch size of {MAX_RECORDS_PER_BATCH} records"
                )

    if not events:
        raise ValueError("input contains no event records")
    return events


def map_canonical_envelope(document: dict[str, Any]) -> dict[str, Any]:
    """Project a schema-validated canonical envelope into the internal event shape.

    Producers emit the canonical platform contract (camelCase field names, a
    FHIR R4 message Bundle, a provenance block and the canonical
    classification vocabulary). The lakehouse retains the internal snake_case
    event model: the FHIR message entry's primary resource becomes the
    retained payload with the provenance block attached, and the canonical
    classification is mapped to the internal lowercase per-scope label.
    Anything unexpected fails closed.
    """
    event_id = require_canonical_text(document["eventId"], "eventId", 256)
    event_type = require_canonical_text(document["eventType"], "eventType", 128)
    producer = require_canonical_text(document["producer"], "producer", 256)
    bundle = document["fhir"]
    if not isinstance(bundle, dict):
        raise ValueError("fhir must be a FHIR R4 message Bundle object")
    entries = bundle.get("entry")
    if not isinstance(entries, list) or not entries:
        raise ValueError("fhir bundle must carry at least one message entry")
    first_entry = entries[0]
    if not isinstance(first_entry, dict):
        raise ValueError("fhir bundle entries must be objects")
    resource = first_entry.get("resource")
    if not isinstance(resource, dict) or not resource:
        raise ValueError("fhir message entry resource must be a non-empty object")
    if "provenance" in resource:
        raise ValueError("fhir message entry resource must not redefine provenance")
    provenance = document["provenance"]
    if not isinstance(provenance, dict):
        raise ValueError("provenance must be an object")
    ledger_commit_hash = require_canonical_text(
        provenance["ledgerCommitHash"], "provenance.ledgerCommitHash", 512
    )
    return {
        "event_id": event_id,
        "event_type": event_type,
        "producer": producer,
        "occurred_at": document["occurredAt"],
        # The canonical envelope carries only occurredAt; the lakehouse
        # records the same instant as the observation time.
        "recorded_at": document["occurredAt"],
        "data_classification": map_canonical_classification(document["classification"], event_type),
        "source_system": producer,
        "source_record_reference": ledger_commit_hash,
        "correlation_id": document.get("correlationId"),
        "payload": {**resource, "provenance": provenance},
        **(
            {"record_classification": document["recordClassification"]}
            if document.get("recordClassification") is not None
            else {}
        ),
    }


def normalize_event(event: dict[str, Any]) -> dict[str, Any]:
    """Create a stable Arrow-compatible row while retaining the original approved payload."""
    occurred_at = parse_timestamp(event["occurred_at"], "occurred_at")
    recorded_at = parse_timestamp(event["recorded_at"], "recorded_at")
    if occurred_at > recorded_at:
        raise ValueError("occurred_at must not be later than recorded_at")

    payload_json = canonical_json(event["payload"])
    if event["event_type"] == "maritime.position.v1":
        validate_maritime_position(event["payload"])
    if len(payload_json.encode("utf-8")) > MAX_PAYLOAD_JSON_BYTES:
        raise ValueError(f"payload exceeds {MAX_PAYLOAD_JSON_BYTES} canonical JSON bytes")

    correlation_id = event.get("correlation_id")
    if correlation_id is not None:
        correlation_id = require_canonical_text(correlation_id, "correlation_id", 256)

    record_classification = event.get("record_classification")
    if record_classification is not None:
        # Row-level clearance label, persisted as a column; unknown labels fail closed.
        from blueeconomy_data_platform.access_policy import Clearance

        record_classification = Clearance.from_label(
            require_canonical_text(record_classification, "record_classification", 32)
        ).label

    normalized = {
        "event_id": require_canonical_text(event["event_id"], "event_id", 256),
        "event_type": require_canonical_text(event["event_type"], "event_type", 128),
        "producer": require_canonical_text(event["producer"], "producer", 256),
        "occurred_at": occurred_at,
        "recorded_at": recorded_at,
        "data_classification": event["data_classification"],
        "source_system": require_canonical_text(event["source_system"], "source_system", 256),
        "source_record_reference": require_canonical_text(
            event["source_record_reference"], "source_record_reference", 512
        ),
        "correlation_id": correlation_id,
        "payload_json": payload_json,
        "ingested_at": datetime.now(UTC),
    }
    if record_classification is not None:
        normalized["record_classification"] = record_classification
    return normalized


def validate_maritime_position(payload: Any) -> None:
    if not isinstance(payload, dict):
        raise ValueError("maritime.position.v1 payload must be an object")
    required = {"asset_id", "latitude", "longitude", "speed_knots", "heading_degrees"}
    missing = required.difference(payload)
    if missing:
        raise ValueError(f"maritime.position.v1 payload is missing fields: {sorted(missing)}")
    asset_id = require_canonical_text(payload["asset_id"], "asset_id", 256)
    if not asset_id:
        raise ValueError("asset_id must be non-empty")
    latitude = payload["latitude"]
    longitude = payload["longitude"]
    speed_knots = payload["speed_knots"]
    heading_degrees = payload["heading_degrees"]
    if not all(
        isinstance(value, (int, float)) and not isinstance(value, bool)
        for value in (latitude, longitude, speed_knots, heading_degrees)
    ):
        raise ValueError("maritime position coordinates and motion values must be finite numbers")
    if not all(
        math.isfinite(float(value)) for value in (latitude, longitude, speed_knots, heading_degrees)
    ):
        raise ValueError("maritime position coordinates and motion values must be finite numbers")
    if not -90.0 <= float(latitude) <= 90.0:
        raise ValueError("latitude must be between -90 and 90 degrees")
    if not -180.0 <= float(longitude) <= 180.0:
        raise ValueError("longitude must be between -180 and 180 degrees")
    if float(speed_knots) < 0:
        raise ValueError("speed_knots must not be negative")
    if not 0.0 <= float(heading_degrees) < 360.0:
        raise ValueError("heading_degrees must be in the range [0, 360)")


def parse_timestamp(value: str, field_name: str) -> datetime:
    if not isinstance(value, str) or "T" not in value:
        raise ValueError(f"{field_name} must be an RFC 3339 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field_name} must be an RFC 3339 timestamp") from error
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include an offset or Z")
    return parsed.astimezone(UTC)


def canonical_json(value: dict[str, Any]) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def require_canonical_text(value: Any, field_name: str, limit: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value.encode("utf-8")) > limit
        or has_control_characters(value)
    ):
        raise ValueError(
            f"{field_name} must be canonical non-control text of at most {limit} UTF-8 bytes"
        )
    return value


def reject_non_finite_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number {value!r} is not permitted")


def has_control_characters(value: str) -> bool:
    return any(unicodedata.category(character).startswith("C") for character in value)


def delta_table_exists(table_uri: str) -> bool:
    try:
        DeltaTable(table_uri, without_files=True)
    except TableNotFoundError:
        return False
    return True


def validate_table_uri(table_uri: str) -> None:
    if not table_uri or table_uri != table_uri.strip() or has_control_characters(table_uri):
        raise ValueError("table URI must be canonical non-control text")
    parsed = urlsplit(table_uri)
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError(
            "table URI must not contain embedded credentials, query parameters or fragments"
        )


EVENT_IDENTITY_COLUMNS = (
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
)


def read_identity_rows(
    table: DeltaTable,
    columns: list[str],
    key_column: str,
    keys: list[str],
) -> list[dict[str, Any]]:
    """Read replay-guard identity rows, tolerant of merge-written files.

    deltalake 1.6.2 with pyarrow 25 raises ``ArrowNotImplementedError`` on
    filtered ``to_pyarrow_table`` reads of tables containing merge-written
    files. The replay guards must stay fail-closed regardless of file
    provenance, so on exactly that failure we fall back to an unfiltered
    read and filter the rows in Python. Any other error still propagates.
    """
    try:
        filtered: list[dict[str, Any]] = table.to_pyarrow_table(
            columns=columns,
            filters=[(key_column, "in", list(keys))],
        ).to_pylist()
        return filtered
    except pa.lib.ArrowNotImplementedError:
        wanted = {str(key) for key in keys}
        return [
            row
            for row in table.to_pyarrow_table(columns=columns).to_pylist()
            if str(row[key_column]) in wanted
        ]


# Row attributes excluded from immutable-replay identity comparison: they
# record processing instants or optional labels, not event content.
REPLAY_GUARD_EXCLUDED_COLUMNS = frozenset(
    {"ingested_at", "promoted_at", "record_classification", "executed_at", "computed_at"}
)


def replay_guard_columns(rows: list[dict[str, Any]]) -> list[str]:
    """Derive the immutable identity columns shared by every row in a batch."""
    if not rows:
        raise ValueError("cannot derive replay-guard columns from an empty batch")
    columns = [column for column in rows[0] if column not in REPLAY_GUARD_EXCLUDED_COLUMNS]
    for row in rows[1:]:
        derived = [c for c in row if c not in REPLAY_GUARD_EXCLUDED_COLUMNS]
        if derived != columns:
            raise ValueError("batch rows do not share an identical column shape")
    return columns


def reject_conflicting_event_replays(table: DeltaTable, events: list[dict[str, Any]]) -> None:
    reject_conflicting_replays(table, events, "event_id")


def reject_conflicting_replays(
    table: DeltaTable, rows: list[dict[str, Any]], key_column: str
) -> None:
    columns = replay_guard_columns(rows)
    keys = [str(row[key_column]) for row in rows]
    existing_rows = read_identity_rows(table, columns, key_column, keys)
    existing_by_key = {str(row[key_column]): row for row in existing_rows}
    conflicts: list[str] = []
    for row in rows:
        key = str(row[key_column])
        existing = existing_by_key.get(key)
        if existing is None:
            continue
        if any(existing[column] != row[column] for column in columns):
            conflicts.append(key)
    if conflicts:
        raise ValueError(
            f"{key_column} reuse conflicts with retained immutable content: "
            + ", ".join(sorted(conflicts))
        )


def append_rows(
    table_uri: str,
    rows: list[dict[str, Any]],
    key_column: str = "event_id",
    table_description: str | None = None,
    arrow_schema: pa.Schema | None = None,
    table_name: str = "blueeconomy_event_envelope",
) -> tuple[int, int, int]:
    """Insert-only merge of *rows* into an append-only Delta table.

    Idempotent on *key_column*: a replayed row with identical immutable
    content is counted as already present; a key reused with conflicting
    content fails closed. ``arrow_schema`` fixes the physical schema for
    tables with typed columns (for example vessel observations).
    """
    if not rows:
        raise ValueError("refusing to append an empty row batch")
    validate_table_uri(table_uri)
    if arrow_schema is not None:
        arrow_table = pa.Table.from_pylist(rows, schema=arrow_schema)
    else:
        arrow_table = pa.Table.from_pylist(rows)
    if not delta_table_exists(table_uri):
        description = EVENT_TABLE_DESCRIPTION
        if table_description is not None:
            if not table_description or table_description != table_description.strip():
                raise ValueError("table description must be canonical non-empty text")
            description = table_description
        write_deltalake(
            table_uri,
            arrow_table,
            mode="error",
            name=table_name,
            description=description,
            configuration={"delta.appendOnly": "true"},
        )
        return DeltaTable(table_uri).version(), len(rows), 0

    for attempt in range(1, MAX_COMMIT_ATTEMPTS + 1):
        table = DeltaTable(table_uri)
        if table.metadata().configuration.get("delta.appendOnly") != "true":
            raise ValueError("existing Delta table is not configured with delta.appendOnly=true")
        reject_conflicting_replays(table, rows, key_column)
        try:
            metrics = (
                table.merge(
                    source=arrow_table,
                    predicate=f"target.{key_column} = source.{key_column}",
                    source_alias="source",
                    target_alias="target",
                )
                .when_not_matched_insert_all()
                .execute()
            )
            records_written = int(metrics["num_target_rows_inserted"])
            retained = DeltaTable(table_uri)
            reject_conflicting_replays(retained, rows, key_column)
            return (
                retained.version(),
                records_written,
                len(rows) - records_written,
            )
        except CommitFailedError:
            if attempt == MAX_COMMIT_ATTEMPTS:
                raise
    raise RuntimeError("Delta commit retry loop exhausted unexpectedly")


def append_events(
    table_uri: str,
    events: list[dict[str, Any]],
    table_description: str | None = None,
) -> tuple[int, int, int]:
    # Phase-7 OTel span (no-op when telemetry is disabled). This is also the
    # S3/MinIO client span: Delta writes go through deltalake/object_store,
    # which is the only storage client (no boto3/httpx S3 client exists).
    from blueeconomy_data_platform.telemetry import get_tracer

    with get_tracer().start_as_current_span("lakehouse.bronze.append") as span:
        span.set_attribute("lakehouse.rows", len(events))
        span.set_attribute("storage.uri_scheme", table_uri.split(":", 1)[0])
        version, written, already_present = append_rows(
            table_uri, events, table_description=table_description
        )
        span.set_attribute("lakehouse.records_written", written)
        span.set_attribute("lakehouse.records_already_present", already_present)
        span.set_attribute("lakehouse.table_version", version)
        return version, written, already_present


def write_report(path: Path, report: IngestionReport) -> None:
    path.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(asdict(report), indent=2) + "\n", encoding="utf-8")
    temporary.chmod(0o640)
    temporary.replace(path)


def require_regular_file(path: Path, label: str, maximum_bytes: int) -> None:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise ValueError(f"{label} path does not identify a readable file") from error
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{label} path must be a regular file and not a symbolic link")
    if metadata.st_size <= 0 or metadata.st_size > maximum_bytes:
        raise ValueError(f"{label} file must contain between 1 and {maximum_bytes} bytes")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def reference_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def validate_output_path(input_path: Path, schema_path: Path, report_path: Path) -> None:
    report_resolved = report_path.resolve(strict=False)
    if report_resolved in {
        input_path.resolve(strict=False),
        schema_path.resolve(strict=False),
    }:
        raise ValueError("report path must not overwrite the input or schema file")


def main() -> None:
    arguments = parse_arguments()
    started_at = datetime.now(UTC)
    try:
        validate_output_path(arguments.input, arguments.schema, arguments.report)
        validator = load_schema(arguments.schema)
        events = read_and_validate_events(arguments.input, validator)
        table_version, records_written, records_already_present = append_events(
            arguments.table_uri, events
        )
        report = IngestionReport(
            schema_version="blueeconomy.lakehouse.ingestion-report.v2",
            started_at=started_at.isoformat(),
            completed_at=datetime.now(UTC).isoformat(),
            input_sha256=file_sha256(arguments.input),
            table_reference_sha256=reference_sha256(arguments.table_uri),
            records_received=len(events),
            records_written=records_written,
            records_already_present=records_already_present,
            source_systems=sorted({event["source_system"] for event in events}),
            data_classifications=sorted({event["data_classification"] for event in events}),
            table_version=table_version,
        )
        write_report(arguments.report, report)
        print(json.dumps(asdict(report), sort_keys=True))
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as error:
        print(f"blueeconomy-ingest-events: {error}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
