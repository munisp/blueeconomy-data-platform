"""Append approved, real-source platform events to a governed Delta Lake table.

The command intentionally has no default input, table URI, source-system value or
synthetic fallback. It validates a real NDJSON source against the committed event
envelope schema before writing an append-only Delta table.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from jsonschema import Draft202012Validator, FormatChecker

MAX_RECORDS_PER_BATCH = 100_000
EVENT_TABLE_DESCRIPTION = "Governed append-only Blue Economy Platform event envelope table"


@dataclass(frozen=True)
class IngestionReport:
    schema_version: str
    started_at: str
    completed_at: str
    input_path: str
    table_uri: str
    records_received: int
    records_written: int
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
    if not path.is_file():
        raise ValueError(f"schema path does not identify a readable file: {path}")
    with path.open("r", encoding="utf-8") as handle:
        schema = json.load(handle)
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


def read_and_validate_events(input_path: Path, validator: Draft202012Validator) -> list[dict[str, Any]]:
    if not input_path.is_file():
        raise ValueError(f"input path does not identify a readable file: {input_path}")

    events: list[dict[str, Any]] = []
    event_ids: set[str] = set()
    with input_path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"input line {line_number} is not valid JSON: {error.msg}") from error
            if not isinstance(event, dict):
                raise ValueError(f"input line {line_number} must be a JSON object")
            validation_errors = sorted(validator.iter_errors(event), key=lambda item: list(item.path))
            if validation_errors:
                messages = "; ".join(error.message for error in validation_errors)
                raise ValueError(f"input line {line_number} fails event-envelope validation: {messages}")
            event_id = event["event_id"]
            if event_id in event_ids:
                raise ValueError(f"input contains duplicate event_id {event_id!r}")
            event_ids.add(event_id)
            events.append(normalize_event(event))
            if len(events) > MAX_RECORDS_PER_BATCH:
                raise ValueError(f"input exceeds the maximum batch size of {MAX_RECORDS_PER_BATCH} records")

    if not events:
        raise ValueError("input contains no event records")
    return events


def normalize_event(event: dict[str, Any]) -> dict[str, Any]:
    """Create a stable Arrow-compatible row while retaining the original approved payload."""
    return {
        "event_id": event["event_id"],
        "event_type": event["event_type"],
        "producer": event["producer"],
        "occurred_at": parse_timestamp(event["occurred_at"], "occurred_at"),
        "recorded_at": parse_timestamp(event["recorded_at"], "recorded_at"),
        "data_classification": event["data_classification"],
        "source_system": event["source_system"],
        "source_record_reference": event["source_record_reference"],
        "correlation_id": event.get("correlation_id"),
        "payload_json": canonical_json(event["payload"]),
        "ingested_at": datetime.now(UTC),
    }


def parse_timestamp(value: str, field_name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field_name} must be an RFC 3339 timestamp") from error
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include an offset or Z")
    return parsed.astimezone(UTC)


def canonical_json(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def existing_duplicate_ids(table_uri: str, event_ids: set[str]) -> set[str]:
    if not delta_table_exists(table_uri):
        return set()
    table = DeltaTable(table_uri)
    configuration = table.metadata().configuration
    if configuration.get("delta.appendOnly") != "true":
        raise ValueError("existing Delta table is not configured with delta.appendOnly=true")

    duplicates: set[str] = set()
    sorted_ids = sorted(event_ids)
    for index in range(0, len(sorted_ids), 1_000):
        candidate_ids = sorted_ids[index : index + 1_000]
        existing = table.to_pyarrow_table(columns=["event_id"], filters=[("event_id", "in", candidate_ids)])
        duplicates.update(existing.column("event_id").to_pylist())
    return duplicates


def delta_table_exists(table_uri: str) -> bool:
    """Determine existence without accepting a URI that the process cannot read."""
    try:
        DeltaTable(table_uri, without_files=True)
    except Exception as error:  # Delta Lake supplies multiple backend-specific error types.
        message = str(error).lower()
        if "not a delta table" in message or "no such file" in message or "not found" in message:
            return False
        raise RuntimeError(f"cannot open Delta table URI {table_uri!r}: {error}") from error
    return True


def append_events(table_uri: str, events: list[dict[str, Any]]) -> int:
    duplicate_ids = existing_duplicate_ids(table_uri, {event["event_id"] for event in events})
    if duplicate_ids:
        sample = ", ".join(sorted(duplicate_ids)[:10])
        raise ValueError(f"refusing to append duplicate event_id values: {sample}")

    arrow_table = pa.Table.from_pylist(events)
    table_exists = delta_table_exists(table_uri)
    write_deltalake(
        table_uri,
        arrow_table,
        mode="append" if table_exists else "error",
        name="blueeconomy_event_envelope",
        description=EVENT_TABLE_DESCRIPTION,
        configuration={"delta.appendOnly": "true"} if not table_exists else None,
    )
    return DeltaTable(table_uri).version()


def write_report(path: Path, report: IngestionReport) -> None:
    path.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(asdict(report), indent=2) + "\n", encoding="utf-8")
    temporary.chmod(0o640)
    temporary.replace(path)


def main() -> None:
    arguments = parse_arguments()
    started_at = datetime.now(UTC)
    try:
        validator = load_schema(arguments.schema)
        events = read_and_validate_events(arguments.input, validator)
        table_version = append_events(arguments.table_uri, events)
        report = IngestionReport(
            schema_version="blueeconomy.lakehouse.ingestion-report.v1",
            started_at=started_at.isoformat(),
            completed_at=datetime.now(UTC).isoformat(),
            input_path=str(arguments.input),
            table_uri=arguments.table_uri,
            records_received=len(events),
            records_written=len(events),
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
