"""Export consignment traceability assembly (Workstream E, fisheries scope).

The gold-layer consignment view assembles one traceable record per export
consignment from the segregated fisheries bronze track: the catch event
(species, weight, time), the custody-transfer trail and the coldchain
temperature evidence, which is reduced to a SHA-256 digest so gold consumers
can verify integrity without reading the raw telemetry. Assembly fails closed:
a consignment without exactly one catch event, or with malformed payload
fields, is rejected rather than partially assembled.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from blueeconomy_data_platform.ingest import require_canonical_text
from blueeconomy_data_platform.segregation import (
    BoundaryViolationError,
    LakehouseScope,
    SegregatedDeltaWriter,
    require_scope_table_uri,
)

CONSIGNMENT_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
CATCH_EVENT_TYPE_PREFIX = "fisheries.catch."
CUSTODY_EVENT_TYPE_PREFIX = "fisheries.custody."
COLDCHAIN_EVENT_TYPE_PREFIX = "coldchain."
EXPORT_EVENT_TYPE_PREFIX = "export."
MIN_TEMPERATURE_CELSIUS = -80.0
MAX_TEMPERATURE_CELSIUS = 60.0

EXPORT_CONSIGNMENT_TABLE_NAME = "fisheries_gold_export_consignments"
EXPORT_CONSIGNMENT_TABLE_DESCRIPTION = (
    "Fisheries gold export consignments: per-consignment catch, custody trail "
    "and coldchain digest assembled from the segregated fisheries bronze track"
)

EXPORT_CONSIGNMENT_SCHEMA = pa.schema(
    [
        pa.field("consignment_id", pa.string(), nullable=False),
        pa.field("catch_event_id", pa.string(), nullable=False),
        pa.field("catch_occurred_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("species_code", pa.string(), nullable=False),
        pa.field("catch_weight_kg", pa.float64(), nullable=False),
        pa.field("custody_event_count", pa.int64(), nullable=False),
        pa.field("custody_event_ids_json", pa.string(), nullable=False),
        pa.field("coldchain_sample_count", pa.int64(), nullable=False),
        pa.field("min_temperature_celsius", pa.float64()),
        pa.field("max_temperature_celsius", pa.float64()),
        pa.field("coldchain_digest_sha256", pa.string(), nullable=False),
        pa.field("export_reference", pa.string()),
        pa.field("source_event_ids_json", pa.string(), nullable=False),
        pa.field("assembled_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


def _consignment_id(payload: dict[str, Any], event_id: object) -> str:
    consignment_id = payload.get("consignmentId")
    if not isinstance(consignment_id, str) or not CONSIGNMENT_ID_PATTERN.fullmatch(consignment_id):
        raise ValueError(
            f"fisheries event {event_id!r} must carry a consignmentId of 1-128 characters "
            "from [A-Za-z0-9._-], starting with a letter or digit"
        )
    return consignment_id


def _finite_number(value: Any, field: str, event_id: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"fisheries event {event_id!r} field {field} must be a finite number")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"fisheries event {event_id!r} field {field} must be a finite number")
    return number


def _parse_payload(event: dict[str, Any]) -> dict[str, Any]:
    payload_json = event.get("payload_json")
    if not isinstance(payload_json, str):
        raise ValueError(f"fisheries event {event.get('event_id')!r} is missing payload_json")
    payload = json.loads(payload_json)
    if not isinstance(payload, dict):
        raise ValueError(f"fisheries event {event.get('event_id')!r} payload must be an object")
    return payload


def _coldchain_digest(samples: list[dict[str, Any]]) -> str:
    canonical = json.dumps(
        [
            {
                "event_id": str(sample["event_id"]),
                "occurred_at": sample["occurred_at"].isoformat(),
                "temperature_celsius": sample["temperature_celsius"],
            }
            for sample in samples
        ],
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_consignment_records(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Assemble one traceable record per consignment from fisheries bronze events.

    Events are grouped by ``payload.consignmentId``; each group must contain
    exactly one ``fisheries.catch.*`` event. Custody (``fisheries.custody.*``),
    coldchain (``coldchain.*``) and export declaration (``export.*``) events
    are optional evidence attached to the same consignment. The coldchain
    digest commits to the ordered set of ``(event_id, occurred_at,
    temperature_celsius)`` samples so tampering with any sample changes it.
    """
    if not events:
        raise ValueError("refusing to assemble consignments from an empty event batch")
    groups: dict[str, dict[str, Any]] = {}
    for event in events:
        event_id = event.get("event_id")
        event_type = event.get("event_type")
        occurred_at = event.get("occurred_at")
        if not isinstance(event_id, str) or not event_id:
            raise ValueError("fisheries event is missing a string event_id")
        if not isinstance(event_type, str) or not event_type:
            raise ValueError(f"fisheries event {event_id!r} is missing a string event_type")
        if not isinstance(occurred_at, datetime):
            raise ValueError(f"fisheries event {event_id!r} is missing an occurred_at timestamp")
        payload = _parse_payload(event)
        consignment_id = _consignment_id(payload, event_id)
        group = groups.setdefault(
            consignment_id, {"catch": [], "custody": [], "coldchain": [], "export": []}
        )
        if event_type.startswith(CATCH_EVENT_TYPE_PREFIX):
            group["catch"].append((event, payload))
        elif event_type.startswith(CUSTODY_EVENT_TYPE_PREFIX):
            group["custody"].append(event)
        elif event_type.startswith(COLDCHAIN_EVENT_TYPE_PREFIX):
            temperature = _finite_number(
                payload.get("temperatureCelsius"), "temperatureCelsius", event_id
            )
            if not MIN_TEMPERATURE_CELSIUS <= temperature <= MAX_TEMPERATURE_CELSIUS:
                raise ValueError(
                    f"fisheries event {event_id!r} temperatureCelsius is outside the "
                    f"sanity range [{MIN_TEMPERATURE_CELSIUS}, {MAX_TEMPERATURE_CELSIUS}]"
                )
            group["coldchain"].append(
                {
                    "event_id": event_id,
                    "occurred_at": occurred_at.astimezone(UTC),
                    "temperature_celsius": temperature,
                }
            )
        elif event_type.startswith(EXPORT_EVENT_TYPE_PREFIX):
            group["export"].append((event, payload))
        else:
            raise ValueError(
                f"fisheries event {event_id!r} has event_type {event_type!r} outside the "
                "governed fisheries catch/custody/coldchain/export families"
            )

    assembled_at = datetime.now(UTC)
    records: list[dict[str, Any]] = []
    for consignment_id, group in sorted(groups.items()):
        catch_events = group["catch"]
        if len(catch_events) != 1:
            raise ValueError(
                f"consignment {consignment_id!r} must have exactly one fisheries.catch.* "
                f"event, found {len(catch_events)}"
            )
        catch_event, catch_payload = catch_events[0]
        catch_event_id = str(catch_event["event_id"])
        species_code = require_canonical_text(catch_payload.get("speciesCode"), "speciesCode", 64)
        catch_weight_kg = _finite_number(
            catch_payload.get("catchWeightKg"), "catchWeightKg", catch_event_id
        )
        if catch_weight_kg < 0:
            raise ValueError(
                f"fisheries event {catch_event_id!r} catchWeightKg must not be negative"
            )

        export_reference: str | None = None
        if group["export"]:
            export_event, export_payload = group["export"][-1]
            export_reference = require_canonical_text(
                export_payload.get("exportReference"), "exportReference", 256
            )

        coldchain_samples = sorted(
            group["coldchain"], key=lambda sample: (sample["occurred_at"], sample["event_id"])
        )
        temperatures = [sample["temperature_celsius"] for sample in coldchain_samples]
        custody_ids = sorted(str(event["event_id"]) for event in group["custody"])
        source_ids = sorted(
            [catch_event_id]
            + custody_ids
            + [str(sample["event_id"]) for sample in coldchain_samples]
            + [str(event["event_id"]) for event, _ in group["export"]]
        )
        records.append(
            {
                "consignment_id": consignment_id,
                "catch_event_id": catch_event_id,
                "catch_occurred_at": catch_event["occurred_at"].astimezone(UTC),
                "species_code": species_code,
                "catch_weight_kg": catch_weight_kg,
                "custody_event_count": len(custody_ids),
                "custody_event_ids_json": json.dumps(custody_ids),
                "coldchain_sample_count": len(coldchain_samples),
                "min_temperature_celsius": min(temperatures) if temperatures else None,
                "max_temperature_celsius": max(temperatures) if temperatures else None,
                "coldchain_digest_sha256": _coldchain_digest(coldchain_samples),
                "export_reference": export_reference,
                "source_event_ids_json": json.dumps(source_ids),
                "assembled_at": assembled_at,
            }
        )
    return records


def export_consignment_table_uri(writer: SegregatedDeltaWriter) -> str:
    """Resolve the gold consignment table URI under the writer's fisheries root."""
    gold_events_uri = writer.table_uri("gold")
    consignment_uri = f"{gold_events_uri.rsplit('/', 1)[0]}/export_consignments"
    require_scope_table_uri(writer.scope, consignment_uri)
    return consignment_uri


def assemble_export_consignment_gold(
    writer: SegregatedDeltaWriter,
) -> tuple[int, int]:
    """Rebuild the gold export-consignment table from the fisheries bronze track.

    Returns ``(table_version, consignment_count)``. The consignment table is
    derived state and is overwritten atomically from the current bronze
    content. Only a fisheries-scope writer may assemble consignments; any
    other scope fails closed at the boundary.
    """
    if writer.scope is not LakehouseScope.FISHERIES:
        raise BoundaryViolationError(
            "export consignment assembly is only defined for the fisheries boundary"
        )
    bronze_uri = writer.table_uri("bronze")
    try:
        DeltaTable(bronze_uri, without_files=True)
    except TableNotFoundError:
        raise ValueError(
            "cannot assemble export consignments before the fisheries bronze table exists"
        ) from None
    events = (
        DeltaTable(bronze_uri)
        .to_pyarrow_table(columns=["event_id", "event_type", "occurred_at", "payload_json"])
        .to_pylist()
    )
    records = build_consignment_records(events)
    table_uri = export_consignment_table_uri(writer)
    write_deltalake(
        table_uri,
        pa.Table.from_pylist(records, schema=EXPORT_CONSIGNMENT_SCHEMA),
        mode="overwrite",
        name=EXPORT_CONSIGNMENT_TABLE_NAME,
        description=EXPORT_CONSIGNMENT_TABLE_DESCRIPTION,
    )
    table = DeltaTable(table_uri)
    return table.version(), len(records)


__all__ = [
    "EXPORT_CONSIGNMENT_SCHEMA",
    "EXPORT_CONSIGNMENT_TABLE_NAME",
    "assemble_export_consignment_gold",
    "build_consignment_records",
    "export_consignment_table_uri",
]
