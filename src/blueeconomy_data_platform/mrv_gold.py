"""MRV scope gold assembly: ``mrv_gold/vessel_annual`` (phase 8).

One gold row per ``(imo_number, calendar_year)`` assembled from the MRV
segregated silver table. Per the module's no-fabrication boundary, a gold
row may only be produced from a ``mrv.emissions-annual.v1`` event whose
report state is ``VERIFIED`` — no unverified report may produce a gold row
or a Statement of Compliance. The ``mrv.soc.v1`` events contribute the
issued SoC artifact SHA-256; a SoC referencing a report that is not verified
in silver fails the run closed.

The gold table is derived state: it is rebuilt atomically (overwrite) from
the current silver content on every run, so replays are idempotent.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from blueeconomy_data_platform.envelope_payload import extract_domain_fields
from blueeconomy_data_platform.segregation import (
    LakehouseScope,
    SegregatedDeltaWriter,
    scope_layer_table_uri,
)

ANNUAL_REPORT_EVENT_TYPE = "mrv.emissions-annual.v1"
SOC_EVENT_TYPE = "mrv.soc.v1"
VERIFIED_STATE = "VERIFIED"

GOLD_TABLE_NAME = "vessel_annual"
GOLD_TABLE_DESCRIPTION = (
    "MRV gold vessel_annual: one row per (imo_number, calendar_year) assembled "
    "from VERIFIED mrv.emissions-annual.v1 silver events plus the issued SoC "
    "artifact hash (derived state, rebuilt atomically)"
)

IMO_NUMBER_PATTERN = re.compile(r"^[0-9]{7}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
CII_RATINGS = frozenset({"A", "B", "C", "D", "E"})

MRV_GOLD_SCHEMA = pa.schema(
    [
        pa.field("imo_number", pa.string(), nullable=False),
        pa.field("calendar_year", pa.int32(), nullable=False),
        pa.field("report_id", pa.string(), nullable=False),
        pa.field("fuel_totals_json", pa.string(), nullable=False),
        pa.field("co2_tonnes", pa.float64()),
        pa.field("attained_cii", pa.float64()),
        pa.field("required_cii", pa.float64()),
        pa.field("cii_rating", pa.string()),
        pa.field("soc_artifact_sha256", pa.string()),
        pa.field("source_event_ids_json", pa.string(), nullable=False),
        pa.field("curated_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


def mrv_gold_table_uri(writer: SegregatedDeltaWriter) -> str:
    """Resolve the MRV gold vessel_annual table URI inside the scope boundary."""
    if writer.scope is not LakehouseScope.MRV:
        raise ValueError("vessel_annual gold assembly is only defined for the mrv boundary")
    roots = writer.roots
    return scope_layer_table_uri(roots.scope, _scope_root(roots.gold), "gold", GOLD_TABLE_NAME)


def _scope_root(gold_uri: str) -> str:
    # roots.gold is "<root>/mrv_gold/events"; the scope root is two levels up.
    return gold_uri.rsplit("/", 2)[0]


def _optional_float(fields: dict[str, Any], name: str) -> float | None:
    value = fields.get(name)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"mrv annual report field {name} must be a number when present")
    return float(value)


def _require_text(fields: dict[str, Any], name: str) -> str:
    value = fields.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"mrv annual report field {name} must be non-empty text")
    return value


def build_vessel_annual_rows(
    silver_events: list[dict[str, Any]], curated_at: datetime
) -> list[dict[str, Any]]:
    """Assemble gold rows from silver MRV events (pure function, deterministic).

    The latest VERIFIED annual report per ``(imo_number, calendar_year)`` —
    ordered by ``(occurred_at, event_id)`` — is authoritative. SoC artifact
    hashes join by report id; a SoC for a report that is not verified in
    silver fails closed.
    """
    verified: dict[tuple[str, int], dict[str, Any]] = {}
    soc_by_report: dict[str, tuple[str, str, datetime]] = {}
    for event in silver_events:
        event_type = event.get("event_type")
        if event_type not in {ANNUAL_REPORT_EVENT_TYPE, SOC_EVENT_TYPE}:
            continue
        payload_json = event.get("payload_json")
        if not isinstance(payload_json, str):
            raise ValueError("silver mrv event is missing payload_json")
        fields = extract_domain_fields(json.loads(payload_json))
        occurred_at = event.get("occurred_at")
        if not isinstance(occurred_at, datetime):
            raise ValueError("silver mrv event is missing occurred_at")
        occurred_at = occurred_at.astimezone(UTC)
        event_id = event.get("event_id")
        if not isinstance(event_id, str):
            raise ValueError("silver mrv event is missing event_id")
        if event_type == SOC_EVENT_TYPE:
            report_id = _require_text(fields, "reportId")
            artifact = _require_text(fields, "artifactSha256")
            if not SHA256_PATTERN.fullmatch(artifact):
                raise ValueError("mrv.soc.v1 artifactSha256 must be 64 lowercase hex characters")
            existing = soc_by_report.get(report_id)
            if existing is None or (occurred_at, event_id) > (existing[2], existing[1]):
                soc_by_report[report_id] = (artifact, event_id, occurred_at)
            continue
        if fields.get("state") != VERIFIED_STATE:
            continue
        imo_number = _require_text(fields, "imoNumber")
        if not IMO_NUMBER_PATTERN.fullmatch(imo_number):
            raise ValueError(f"mrv annual report imoNumber {imo_number!r} is not 7 digits")
        calendar_year = fields.get("calendarYear")
        if (
            not isinstance(calendar_year, int)
            or isinstance(calendar_year, bool)
            or not 2000 <= calendar_year <= 9999
        ):
            raise ValueError("mrv annual report calendarYear must be a four-digit year integer")
        key = (imo_number, calendar_year)
        existing_report = verified.get(key)
        if existing_report is None or (occurred_at, event_id) > (
            existing_report["occurred_at"],
            existing_report["event_id"],
        ):
            verified[key] = {
                "imo_number": imo_number,
                "calendar_year": calendar_year,
                "report_id": _require_text(fields, "reportId"),
                "fields": fields,
                "occurred_at": occurred_at,
                "event_id": event_id,
            }

    rows: list[dict[str, Any]] = []
    for (imo_number, calendar_year), entry in sorted(verified.items()):
        fields = entry["fields"]
        report_id = entry["report_id"]
        rating = fields.get("ciiRating")
        if rating is not None:
            if not isinstance(rating, str) or rating not in CII_RATINGS:
                raise ValueError(f"mrv annual report ciiRating {rating!r} is not A-E")
        totals = fields.get("totals")
        if not isinstance(totals, dict):
            raise ValueError("verified mrv annual report must carry a totals object")
        soc = soc_by_report.pop(report_id, None)
        rows.append(
            {
                "imo_number": imo_number,
                "calendar_year": calendar_year,
                "report_id": report_id,
                "fuel_totals_json": json.dumps(totals, sort_keys=True, separators=(",", ":")),
                "co2_tonnes": _optional_float(totals, "co2Tonnes"),
                "attained_cii": _optional_float(fields, "attainedCii"),
                "required_cii": _optional_float(fields, "requiredCii"),
                "cii_rating": rating,
                "soc_artifact_sha256": soc[0] if soc is not None else None,
                "source_event_ids_json": json.dumps(
                    sorted([entry["event_id"], *([soc[1]] if soc is not None else [])])
                ),
                "curated_at": curated_at,
            }
        )
    if soc_by_report:
        orphaned = sorted(soc_by_report)
        raise ValueError(
            "mrv.soc.v1 events reference reports that are not VERIFIED in silver "
            f"(no unverified report may produce an SoC): {', '.join(orphaned)}"
        )
    return rows


def assemble_mrv_vessel_annual_gold(writer: SegregatedDeltaWriter) -> tuple[int, int]:
    """Rebuild the MRV gold vessel_annual table from silver; derived state.

    Returns ``(table_version, row_count)``.
    """
    if writer.scope is not LakehouseScope.MRV:
        raise ValueError("vessel_annual gold assembly is only defined for the mrv boundary")
    # Phase-8 OTel span (no-op when telemetry is disabled); gold stage of the
    # mrv medallion DAG.
    from blueeconomy_data_platform.telemetry import get_tracer

    with get_tracer().start_as_current_span("lakehouse.gold.curate") as span:
        span.set_attribute("lakehouse.scope", writer.scope.value)
        span.set_attribute("lakehouse.table", GOLD_TABLE_NAME)
        version, row_count = _assemble(writer)
        span.set_attribute("lakehouse.rows", row_count)
        span.set_attribute("lakehouse.table_version", version)
        return version, row_count


def _assemble(writer: SegregatedDeltaWriter) -> tuple[int, int]:
    silver_uri = writer.table_uri("silver")
    try:
        silver = DeltaTable(silver_uri)
    except TableNotFoundError:
        raise ValueError("cannot assemble mrv gold before the silver table exists") from None
    events = silver.to_pyarrow_table(
        columns=["event_id", "event_type", "occurred_at", "payload_json"]
    ).to_pylist()
    if not events:
        raise ValueError("cannot assemble mrv gold from an empty silver table")
    rows = build_vessel_annual_rows(events, curated_at=datetime.now(UTC))
    gold_uri = mrv_gold_table_uri(writer)
    write_deltalake(
        gold_uri,
        pa.Table.from_pylist(rows, schema=MRV_GOLD_SCHEMA),
        mode="overwrite",
        name="mrv_gold_vessel_annual",
        description=GOLD_TABLE_DESCRIPTION,
    )
    return DeltaTable(gold_uri).version(), len(rows)


__all__ = [
    "ANNUAL_REPORT_EVENT_TYPE",
    "GOLD_TABLE_NAME",
    "MRV_GOLD_SCHEMA",
    "SOC_EVENT_TYPE",
    "VERIFIED_STATE",
    "assemble_mrv_vessel_annual_gold",
    "build_vessel_annual_rows",
    "mrv_gold_table_uri",
]
