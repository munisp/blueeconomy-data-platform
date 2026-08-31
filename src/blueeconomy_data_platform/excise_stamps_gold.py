"""Platform scope gold assembly: ``platform_gold/excise_stamp_facts``.

Deterministic 1:1 projection of the verified tax-stamps lifecycle events
(``stamps.assessed.v1`` / ``stamps.approved.v1`` / ``stamps.issued.v1`` /
``stamps.activated.v1``) landed in the platform silver table once the
``stamps.`` topic prefix is admitted to the platform scope.

Fail-closed posture (mirrors the mrv gold doctrine): every silver event of a
stamps event type MUST map deterministically — malformed money
(``totalDutyKobo`` non-integral or negative), non-integral quantities or a
missing payload fails the run closed; no amount is ever guessed. The gold
table is derived state, rebuilt atomically (overwrite) from the current
silver content on every run, so replays are idempotent.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from blueeconomy_data_platform.envelope_payload import extract_domain_fields
from blueeconomy_data_platform.segregation import LakehouseScope, SegregatedDeltaWriter

ASSESSED_EVENT_TYPE = "stamps.assessed.v1"
APPROVED_EVENT_TYPE = "stamps.approved.v1"
ISSUED_EVENT_TYPE = "stamps.issued.v1"
ACTIVATED_EVENT_TYPE = "stamps.activated.v1"
STAMP_EVENT_TYPES = (
    ASSESSED_EVENT_TYPE,
    APPROVED_EVENT_TYPE,
    ISSUED_EVENT_TYPE,
    ACTIVATED_EVENT_TYPE,
)

GOLD_TABLE_NAME = "excise_stamp_facts"
GOLD_TABLE_DESCRIPTION = (
    "Platform gold excise_stamp_facts: deterministic 1:1 projection of verified "
    "tax-stamps lifecycle events (derived state, rebuilt atomically)"
)

EXCISE_STAMP_FACTS_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("event_type", pa.string()),
        ("occurred_at", pa.timestamp("us", tz="UTC")),
        ("assessment_id", pa.string()),
        ("declaration_ref", pa.string()),
        ("batch_id", pa.string()),
        ("total_duty_kobo", pa.int64()),
        ("quantity", pa.int64()),
        ("payload_json", pa.string()),
        ("curated_at", pa.timestamp("us", tz="UTC")),
    ]
)


def excise_stamps_gold_table_uri(writer: SegregatedDeltaWriter) -> str:
    return writer.table_uri("gold").rstrip("/") + "/" + GOLD_TABLE_NAME


def _require_int(fields: dict[str, Any], name: str, *, minimum: int = 0) -> int:
    """Money/quantities are integral minor units; anything else fails closed."""
    value = fields.get(name)
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise ValueError(f"stamps event {name} must be an integer >= {minimum}")
    return value


def build_excise_stamp_fact_rows(
    silver_events: list[dict[str, Any]], curated_at: datetime
) -> list[dict[str, Any]]:
    """Assemble gold rows from silver stamps events (pure, deterministic).

    1:1 projection ordered by ``(occurred_at, event_id)``; every stamps event
    must map or the run fails closed.
    """
    stamps_events = [
        event for event in silver_events if event.get("event_type") in STAMP_EVENT_TYPES
    ]
    rows: list[dict[str, Any]] = []
    for event in sorted(
        stamps_events, key=lambda item: (item.get("occurred_at") or datetime.min.replace(tzinfo=UTC), str(item.get("event_id")))
    ):
        payload_json = event.get("payload_json")
        if not isinstance(payload_json, str):
            raise ValueError("silver stamps event is missing payload_json")
        occurred_at = event.get("occurred_at")
        if not isinstance(occurred_at, datetime):
            raise ValueError("silver stamps event is missing occurred_at")
        event_id = event.get("event_id")
        if not isinstance(event_id, str):
            raise ValueError("silver stamps event is missing event_id")
        fields = extract_domain_fields(json.loads(payload_json))
        event_type = event["event_type"]
        total_duty_kobo: int | None = None
        quantity: int | None = None
        if event_type == ASSESSED_EVENT_TYPE:
            total_duty_kobo = _require_int(fields, "totalDutyKobo")
            quantity = _require_int(fields, "stampsRequired")
        elif event_type == ISSUED_EVENT_TYPE:
            quantity = _require_int(fields, "quantity", minimum=1)
        elif event_type == ACTIVATED_EVENT_TYPE:
            quantity = _require_int(fields, "activatedCount")
        rows.append(
            {
                "event_id": event_id,
                "event_type": event_type,
                "occurred_at": occurred_at.astimezone(UTC),
                "assessment_id": str(fields.get("assessmentId") or ""),
                "declaration_ref": str(fields.get("declarationRef") or ""),
                "batch_id": str(fields.get("batchId") or ""),
                "total_duty_kobo": total_duty_kobo,
                "quantity": quantity,
                "payload_json": payload_json,
                "curated_at": curated_at,
            }
        )
    return rows


def assemble_excise_stamps_gold(writer: SegregatedDeltaWriter) -> tuple[int, int]:
    """Rebuild the platform gold excise_stamp_facts table from silver.

    Returns ``(table_version, row_count)``.
    """
    if writer.scope is not LakehouseScope.PLATFORM:
        raise ValueError("excise_stamp_facts gold assembly is only defined for the platform boundary")
    silver_uri = writer.table_uri("silver")
    try:
        silver = DeltaTable(silver_uri)
    except TableNotFoundError:
        raise ValueError("cannot assemble excise stamps gold before the silver table exists") from None
    events = silver.to_pyarrow_table(
        columns=["event_id", "event_type", "occurred_at", "payload_json"]
    ).to_pylist()
    rows = build_excise_stamp_fact_rows(events, curated_at=datetime.now(UTC))
    gold_uri = excise_stamps_gold_table_uri(writer)
    write_deltalake(
        gold_uri,
        pa.Table.from_pylist(rows, schema=EXCISE_STAMP_FACTS_SCHEMA),
        mode="overwrite",
        name="platform_gold_excise_stamp_facts",
        description=GOLD_TABLE_DESCRIPTION,
    )
    return DeltaTable(gold_uri).version(), len(rows)


__all__ = [
    "ACTIVATED_EVENT_TYPE",
    "APPROVED_EVENT_TYPE",
    "ASSESSED_EVENT_TYPE",
    "EXCISE_STAMP_FACTS_SCHEMA",
    "GOLD_TABLE_NAME",
    "ISSUED_EVENT_TYPE",
    "STAMP_EVENT_TYPES",
    "assemble_excise_stamps_gold",
    "build_excise_stamp_fact_rows",
    "excise_stamps_gold_table_uri",
]
