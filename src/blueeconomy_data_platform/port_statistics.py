"""Statistics Port: reproducible port KPI gold rollups (phase 8, spec §1-§6).

Core invariant: **no fabricated figures.** Every KPI value is computed by a
gold-assembly run over the platform silver events table at a pinned Delta
table version; the run is recorded in ``platform_gold/port_kpi_runs`` with the
exact source table versions and the pinned query-definitions hash, and every
value row in ``platform_gold/port_kpi_values`` carries its ``run_id``. A KPI
with zero observations is emitted with ``value=null`` and a coverage note —
never omitted, never filled with an estimate, sample or seed value.

KPI definitions follow the UNCTAD lineage (UNCTAD 1976 Port Performance
Indicators; UNCTAD Review of Maritime Transport annual series): vessel calls,
turnaround time (median/P90), waiting time at anchorage, berth occupancy,
throughput, truck gate turnaround, booking lead time / slot utilisation and
declaration clearance time. Definitions are pinned in :data:`KPI_DEFINITIONS`;
``query_definitions_sha256`` is the SHA-256 of their canonical JSON rendering,
so any change to a definition is a provenance-visible event.

KPIs whose upstream feed does not yet exist ship as explicit gap rows keyed by
:data:`STATS_GAPS` (pattern: blueeconomy-mobile ``INTEGRATION_GAPS``).
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from blueeconomy_data_platform.envelope_payload import extract_domain_fields
from blueeconomy_data_platform.ingest import append_rows, canonical_json
from blueeconomy_data_platform.jcs import canonicalize
from blueeconomy_data_platform.segregation import (
    LakehouseScope,
    SegregatedDeltaWriter,
    require_scope_table_uri,
    scope_layer_table_uri,
)
from blueeconomy_data_platform.signature_verification import sign_document

PERIOD_PATTERN = re.compile(r"^[0-9]{4}-(0[1-9]|1[0-2])$")
UNLOCODE_PATTERN = re.compile(r"^[A-Z]{2}[A-Z0-9]{3}$")
RUN_ID_NAMESPACE = uuid.UUID("6f1b7e34-9c2a-4c6f-9f6d-2c1a5b8d7e90")

PORT_CALL_EVENT_TYPE = "ports.port-call.v1"
GATE_SCAN_EVENT_TYPE = "ports.gate.scan.v1"
BOOKING_EVENT_TYPE = "ports.booking.created.v1"

SILVER_TABLE_LABEL = "platform_silver/events"
FACTS_TABLE_NAME = "port_call_facts"
RUNS_TABLE_NAME = "port_kpi_runs"
VALUES_TABLE_NAME = "port_kpi_values"
REPORTS_DIR_NAME = "port_kpi_reports"

# Port-call lifecycle ordering: a call counts toward the KPI set once it has
# been ACCEPTED (spec MVP KPI table, "status >= ACCEPTED").
PORT_CALL_STATUSES = frozenset(
    {"DRAFT", "SUBMITTED", "ACCEPTED", "ARRIVED", "BERTHED", "DEPARTED", "COMPLETED", "REJECTED"}
)
ACCEPTED_OR_BEYOND = frozenset({"ACCEPTED", "ARRIVED", "BERTHED", "DEPARTED", "COMPLETED"})

NO_DATA_NOTE = "no source events in period"

# Quantile method: linear interpolation over sorted observations (Hyndman-Fan
# type 7), deterministic for identical input multisets.
P50 = "P50"
P90 = "P90"


@dataclass(frozen=True)
class StatsGap:
    """A documented statistics integration gap (stable id, needed upstream feed)."""

    gap_id: str
    description: str
    needed_upstream: str


STATS_GAPS: tuple[StatsGap, ...] = (
    StatsGap(
        gap_id="GAP-STATS-BERTH-REF",
        description=(
            "No authoritative berth reference data: berth-occupancy % is "
            "unavailable until the Ministry publishes berth counts per port."
        ),
        needed_upstream="Ministry berth reference dataset (berth count per port)",
    ),
    StatsGap(
        gap_id="GAP-STATS-TEU",
        description=(
            "Manifests record tonnage but not TEU: TEU throughput is "
            "unavailable; throughput is published in tonnes only."
        ),
        needed_upstream="manifest events carrying declared TEU",
    ),
    StatsGap(
        gap_id="GAP-STATS-SW-EVENTS",
        description=(
            "Singlewindow declaration lifecycle events are not yet bridged "
            "into the platform scope: declaration clearance time is pending."
        ),
        needed_upstream="singlewindow declaration events (submitted/cleared) bridge",
    ),
)

STATS_GAP_BY_ID = {gap.gap_id: gap for gap in STATS_GAPS}


@dataclass(frozen=True)
class KpiDefinition:
    """Pinned KPI definition (definition text carries its standards citation)."""

    kpi_id: str
    name: str
    definition: str
    unit: str
    definition_version: str
    source_event_types: tuple[str, ...]
    gap_id: str | None = None


KPI_DEFINITIONS: tuple[KpiDefinition, ...] = (
    KpiDefinition(
        kpi_id="vessel_calls",
        name="Vessel calls",
        definition=(
            "Count of distinct port calls whose latest lifecycle status is "
            "ACCEPTED or beyond, attributed to the period by arrival time, "
            "segmented by port and ship class (UNCTAD 1976 Port Performance "
            "Indicators; UNCTAD Review of Maritime Transport)."
        ),
        unit="calls",
        definition_version="1.0.0",
        source_event_types=(PORT_CALL_EVENT_TYPE,),
    ),
    KpiDefinition(
        kpi_id="vessel_turnaround_hours",
        name="Vessel turnaround time",
        definition=(
            "Hours from arrival to departure per port call (arrival to "
            "departure, UNCTAD/industry definition); median (P50) and P90 "
            "per port per period, Hyndman-Fan type-7 quantiles."
        ),
        unit="hours",
        definition_version="1.0.0",
        source_event_types=(PORT_CALL_EVENT_TYPE,),
    ),
    KpiDefinition(
        kpi_id="waiting_time_hours",
        name="Waiting time at anchorage",
        definition=(
            "Hours from arrival to berthing per port call where both "
            "timestamps exist (UNCTAD 1976 waiting time); median (P50) and "
            "P90 per port per period."
        ),
        unit="hours",
        definition_version="1.0.0",
        source_event_types=(PORT_CALL_EVENT_TYPE,),
    ),
    KpiDefinition(
        kpi_id="berth_occupancy_pct",
        name="Berth occupancy",
        definition=(
            "Occupied berth-hours / (berths x window hours) x 100 per port "
            "per period (UNCTAD RMT congestion threshold ~85%). BLOCKED by "
            "GAP-STATS-BERTH-REF: no authoritative berth reference data."
        ),
        unit="percent",
        definition_version="1.0.0",
        source_event_types=(PORT_CALL_EVENT_TYPE,),
        gap_id="GAP-STATS-BERTH-REF",
    ),
    KpiDefinition(
        kpi_id="throughput_tonnes",
        name="Throughput (tonnes)",
        definition=(
            "Sum of declared cargo tonnage over port calls in the period "
            "(UNCTAD RMT port throughput). TEU throughput is unavailable "
            "(GAP-STATS-TEU); tonnes only."
        ),
        unit="tonnes",
        definition_version="1.0.0",
        source_event_types=(PORT_CALL_EVENT_TYPE,),
    ),
    KpiDefinition(
        kpi_id="truck_gate_turnaround_minutes",
        name="Truck gate turnaround",
        definition=(
            "Minutes from gate-in scan to the next gate-out scan per truck "
            "and terminal; median per port per period (port-interop eCallUp "
            "gate scans)."
        ),
        unit="minutes",
        definition_version="1.0.0",
        source_event_types=(GATE_SCAN_EVENT_TYPE,),
    ),
    KpiDefinition(
        kpi_id="booking_lead_time_hours",
        name="Booking lead time",
        definition=(
            "Hours from booking creation to the booked slot window start; "
            "median per port per period (ports.booking.created.v1)."
        ),
        unit="hours",
        definition_version="1.0.0",
        source_event_types=(BOOKING_EVENT_TYPE,),
    ),
    KpiDefinition(
        kpi_id="slot_utilisation_pct",
        name="Slot utilisation",
        definition=(
            "Booked slots / offered slots x 100 per port per period, over "
            "booking events carrying both counters."
        ),
        unit="percent",
        definition_version="1.0.0",
        source_event_types=(BOOKING_EVENT_TYPE,),
    ),
    KpiDefinition(
        kpi_id="declaration_clearance_hours",
        name="Declaration clearance time",
        definition=(
            "Hours from declaration submission to clearance, median per "
            "lane per period. BLOCKED by GAP-STATS-SW-EVENTS: singlewindow "
            "declaration lifecycle events are not yet bridged into the "
            "platform scope."
        ),
        unit="hours",
        definition_version="1.0.0",
        source_event_types=(),
        gap_id="GAP-STATS-SW-EVENTS",
    ),
)

KPI_BY_ID = {definition.kpi_id: definition for definition in KPI_DEFINITIONS}


def query_definitions_sha256() -> str:
    """SHA-256 of the canonical JSON rendering of the pinned KPI registry."""
    material = [
        {
            "kpi_id": definition.kpi_id,
            "definition": definition.definition,
            "unit": definition.unit,
            "definition_version": definition.definition_version,
            "source_event_types": list(definition.source_event_types),
            "gap_id": definition.gap_id,
        }
        for definition in KPI_DEFINITIONS
    ]
    return hashlib.sha256(canonicalize(material).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Period handling (UTC month boundaries)
# ---------------------------------------------------------------------------


def parse_period(period: str) -> tuple[datetime, datetime]:
    """Resolve a ``YYYY-MM`` period to its UTC [start, end) bounds."""
    if not isinstance(period, str) or not PERIOD_PATTERN.fullmatch(period):
        raise ValueError(f"period {period!r} must match YYYY-MM")
    year = int(period[:4])
    month = int(period[5:7])
    start = datetime(year, month, 1, tzinfo=UTC)
    end = (
        datetime(year + 1, 1, 1, tzinfo=UTC)
        if month == 12
        else datetime(year, month + 1, 1, tzinfo=UTC)
    )
    return start, end


def _in_period(instant: datetime, start: datetime, end: datetime) -> bool:
    return start <= instant.astimezone(UTC) < end


# ---------------------------------------------------------------------------
# Silver row decoding
# ---------------------------------------------------------------------------


def _parse_instant(value: Any, field: str, event_type: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str) or "T" not in value:
        raise ValueError(f"{event_type} field {field} must be an RFC 3339 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{event_type} field {field} must be an RFC 3339 timestamp") from error
    if parsed.tzinfo is None:
        raise ValueError(f"{event_type} field {field} must include an offset or Z")
    return parsed.astimezone(UTC)


def _optional_number(fields: dict[str, Any], field: str, event_type: str) -> float | None:
    value = fields.get(field)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{event_type} field {field} must be a number when present")
    return float(value)


def _optional_text(fields: dict[str, Any], *names: str) -> str | None:
    for name in names:
        value = fields.get(name)
        if value is not None:
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"field {name} must be non-empty text when present")
            return value
    return None


def decode_silver_event(event: dict[str, Any]) -> tuple[str, dict[str, Any], datetime, str]:
    """Project one platform silver row to (event_type, domain fields, occurred_at, event_id)."""
    event_type = event.get("event_type")
    if not isinstance(event_type, str):
        raise ValueError("silver event is missing event_type")
    payload_json = event.get("payload_json")
    if not isinstance(payload_json, str):
        raise ValueError("silver event is missing payload_json")
    occurred_at = event.get("occurred_at")
    if not isinstance(occurred_at, datetime):
        raise ValueError("silver event is missing occurred_at")
    event_id = event.get("event_id")
    if not isinstance(event_id, str):
        raise ValueError("silver event is missing event_id")
    fields = extract_domain_fields(json.loads(payload_json))
    return event_type, fields, occurred_at.astimezone(UTC), event_id


# ---------------------------------------------------------------------------
# Fact assembly: port calls, gate scans, bookings
# ---------------------------------------------------------------------------


def _hash_vessel_ref(vessel_ref: str | None) -> str | None:
    """Vessel identity is hashed in facts; KPI rows are aggregates only (no PII)."""
    if vessel_ref is None:
        return None
    return hashlib.sha256(f"blueeconomy.stats.vessel-ref.v1|{vessel_ref}".encode()).hexdigest()


def assemble_port_call_facts(
    silver_events: list[dict[str, Any]], period_start: datetime, period_end: datetime
) -> list[dict[str, Any]]:
    """Build the ``port_call_facts`` rows for one period from silver events.

    Lifecycle events for the same ``portCallId`` are merged: the latest event
    (by ``occurred_at``, then ``event_id``) is authoritative for status, and
    each timestamp/tonnage field takes its most recent non-null value. A call
    is attributed to the period by its arrival time, falling back to the
    latest event time when no arrival was reported. Calls whose latest status
    is below ACCEPTED are retained in facts (they are real events) but never
    counted by the KPI set.
    """
    calls: dict[str, dict[str, Any]] = {}
    for event in silver_events:
        event_type, fields, occurred_at, event_id = decode_silver_event(event)
        if event_type != PORT_CALL_EVENT_TYPE:
            continue
        port_call_id = fields.get("portCallId")
        if not isinstance(port_call_id, str) or not port_call_id.strip():
            raise ValueError(f"{PORT_CALL_EVENT_TYPE} field portCallId must be non-empty text")
        port_code = fields.get("portCode")
        if not isinstance(port_code, str) or not UNLOCODE_PATTERN.fullmatch(port_code):
            raise ValueError(
                f"{PORT_CALL_EVENT_TYPE} field portCode must be a UN/LOCODE, got {port_code!r}"
            )
        status = fields.get("status")
        if not isinstance(status, str) or status not in PORT_CALL_STATUSES:
            raise ValueError(
                f"{PORT_CALL_EVENT_TYPE} field status {status!r} is outside the governed lifecycle"
            )
        entry = calls.setdefault(
            port_call_id,
            {
                "port_code": port_code,
                "order": (occurred_at, event_id),
                "status": status,
                "vessel_ref": None,
                "ship_class": None,
                "arrived_at": None,
                "berthed_at": None,
                "departed_at": None,
                "declared_tonnage": None,
                "source_event_ids": [],
            },
        )
        if entry["port_code"] != port_code:
            raise ValueError(f"port call {port_call_id!r} changed portCode across events")
        if (occurred_at, event_id) >= entry["order"]:
            entry["order"] = (occurred_at, event_id)
            entry["status"] = status
        vessel_ref = _optional_text(fields, "vesselRef")
        ship_class = _optional_text(fields, "shipClass")
        arrived = _parse_instant(fields.get("arrivedAt"), "arrivedAt", event_type)
        berthed = _parse_instant(fields.get("berthedAt"), "berthedAt", event_type)
        departed = _parse_instant(fields.get("departedAt"), "departedAt", event_type)
        tonnage = _optional_number(fields, "declaredTonnage", event_type)
        for key, value in (
            ("vessel_ref", vessel_ref),
            ("ship_class", ship_class),
            ("arrived_at", arrived),
            ("berthed_at", berthed),
            ("departed_at", departed),
            ("declared_tonnage", tonnage),
        ):
            if value is not None:
                entry[key] = value
        entry["source_event_ids"].append(event_id)

    facts: list[dict[str, Any]] = []
    for port_call_id, entry in sorted(calls.items()):
        attributable = entry["arrived_at"] or entry["order"][0]
        if not _in_period(attributable, period_start, period_end):
            continue
        facts.append(
            {
                "port_call_id": port_call_id,
                "port_code": entry["port_code"],
                "vessel_ref_hashed": _hash_vessel_ref(entry["vessel_ref"]),
                "ship_class": entry["ship_class"],
                "status": entry["status"],
                "arrived_at": entry["arrived_at"],
                "berthed_at": entry["berthed_at"],
                "departed_at": entry["departed_at"],
                "declared_tonnage": entry["declared_tonnage"],
                "source_event_ids_json": json.dumps(sorted(entry["source_event_ids"])),
            }
        )
    return facts


def extract_gate_scans(
    silver_events: list[dict[str, Any]], period_start: datetime, period_end: datetime
) -> list[dict[str, Any]]:
    """Project gate-scan events in the period to (truck, terminal, direction, at) rows."""
    scans: list[dict[str, Any]] = []
    for event in silver_events:
        event_type, fields, occurred_at, event_id = decode_silver_event(event)
        if event_type != GATE_SCAN_EVENT_TYPE or not _in_period(
            occurred_at, period_start, period_end
        ):
            continue
        truck_ref = _optional_text(fields, "truckRef", "truck_plate")
        if truck_ref is None:
            raise ValueError(f"{GATE_SCAN_EVENT_TYPE} must carry a truck reference")
        direction = fields.get("direction")
        if direction not in {"in", "out"}:
            raise ValueError(f"{GATE_SCAN_EVENT_TYPE} direction must be 'in' or 'out'")
        terminal = _optional_text(fields, "terminal", "terminalReference") or ""
        scans.append(
            {
                "truck_ref": truck_ref,
                "terminal": terminal,
                "port_code": _optional_text(fields, "portCode"),
                "direction": direction,
                "occurred_at": occurred_at,
                "event_id": event_id,
            }
        )
    return scans


def extract_bookings(
    silver_events: list[dict[str, Any]], period_start: datetime, period_end: datetime
) -> list[dict[str, Any]]:
    """Project booking-created events whose slot window starts in the period."""
    bookings: list[dict[str, Any]] = []
    for event in silver_events:
        event_type, fields, occurred_at, event_id = decode_silver_event(event)
        if event_type != BOOKING_EVENT_TYPE:
            continue
        created_at = _parse_instant(fields.get("createdAt"), "createdAt", event_type) or occurred_at
        window_start = _parse_instant(fields.get("slotWindowStart"), "slotWindowStart", event_type)
        if window_start is None or not _in_period(window_start, period_start, period_end):
            continue
        if window_start < created_at:
            raise ValueError(f"{BOOKING_EVENT_TYPE} slot window starts before creation")
        bookings.append(
            {
                "booking_id": _optional_text(fields, "bookingId") or event_id,
                "port_code": _optional_text(fields, "portCode"),
                "lead_time_hours": (window_start - created_at).total_seconds() / 3600.0,
                "booked_slots": _optional_number(fields, "bookedSlots", event_type),
                "offered_slots": _optional_number(fields, "offeredSlots", event_type),
                "event_id": event_id,
            }
        )
    return bookings


# ---------------------------------------------------------------------------
# KPI computation (pure functions over facts/scans/bookings)
# ---------------------------------------------------------------------------


def percentile_linear(values: list[float], quantile: float) -> float:
    """Hyndman-Fan type-7 quantile over a non-empty sorted-copy of *values*."""
    if not values:
        raise ValueError("quantile of an empty observation set is undefined")
    if not 0.0 < quantile < 1.0:
        raise ValueError("quantile must be within (0, 1)")
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = quantile * (len(ordered) - 1)
    lower = int(rank)
    fraction = rank - lower
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + fraction * (ordered[upper] - ordered[lower])


def _round6(value: float) -> float:
    return round(value, 6)


def _gate_durations_minutes(scans: list[dict[str, Any]]) -> dict[str | None, list[float]]:
    """Gate-out minus gate-in minutes per port, pairing in->out scans in order."""
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for scan in scans:
        grouped.setdefault((scan["truck_ref"], scan["terminal"]), []).append(scan)
    durations: dict[str | None, list[float]] = {}
    for _key, group in grouped.items():
        ordered = sorted(group, key=lambda scan: (scan["occurred_at"], scan["event_id"]))
        open_in: dict[str, Any] | None = None
        for scan in ordered:
            if scan["direction"] == "in":
                open_in = scan
            elif open_in is not None:
                minutes = (scan["occurred_at"] - open_in["occurred_at"]).total_seconds() / 60.0
                if minutes >= 0:
                    durations.setdefault(scan["port_code"], []).append(minutes)
                open_in = None
    return durations


def compute_kpi_observations(
    facts: list[dict[str, Any]],
    gate_scans: list[dict[str, Any]],
    bookings: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Compute per-KPI observation groups.

    Returns ``kpi_id -> [observation rows]`` where each observation row is
    ``{"port_code", "ship_class", "percentile", "value", "n_observations"}``
    with a non-null value. KPIs without observations are absent here; the
    emitter turns absence into explicit no-data rows. Gap KPIs never appear:
    their rows are always gap rows.
    """
    accepted = [fact for fact in facts if fact["status"] in ACCEPTED_OR_BEYOND]
    result: dict[str, list[dict[str, Any]]] = {}

    # vessel_calls: count by port and ship class (plus per-port aggregate).
    calls_by_port_class: dict[tuple[str | None, str | None], int] = {}
    for fact in accepted:
        key = (fact["port_code"], fact["ship_class"])
        calls_by_port_class[key] = calls_by_port_class.get(key, 0) + 1
        aggregate_key = (fact["port_code"], None)
        if fact["ship_class"] is not None:
            calls_by_port_class[aggregate_key] = calls_by_port_class.get(aggregate_key, 0) + 1
    result["vessel_calls"] = [
        {
            "port_code": port_code,
            "ship_class": ship_class,
            "percentile": None,
            "value": float(count),
            "n_observations": count,
        }
        for (port_code, ship_class), count in sorted(
            calls_by_port_class.items(), key=lambda item: (str(item[0][0]), str(item[0][1]))
        )
    ]

    # turnaround / waiting time per port (aggregate over ship classes).
    turnaround: dict[str | None, list[float]] = {}
    waiting: dict[str | None, list[float]] = {}
    for fact in accepted:
        port = fact["port_code"]
        arrived, berthed, departed = fact["arrived_at"], fact["berthed_at"], fact["departed_at"]
        if arrived is not None and departed is not None and departed >= arrived:
            turnaround.setdefault(port, []).append((departed - arrived).total_seconds() / 3600.0)
        if arrived is not None and berthed is not None and berthed >= arrived:
            waiting.setdefault(port, []).append((berthed - arrived).total_seconds() / 3600.0)
    result["vessel_turnaround_hours"] = [
        {
            "port_code": port,
            "ship_class": None,
            "percentile": percentile,
            "value": _round6(percentile_linear(durations, 0.5 if percentile == P50 else 0.9)),
            "n_observations": len(durations),
        }
        for port, durations in sorted(turnaround.items(), key=lambda item: str(item[0]))
        for percentile in (P50, P90)
    ]
    result["waiting_time_hours"] = [
        {
            "port_code": port,
            "ship_class": None,
            "percentile": percentile,
            "value": _round6(percentile_linear(durations, 0.5 if percentile == P50 else 0.9)),
            "n_observations": len(durations),
        }
        for port, durations in sorted(waiting.items(), key=lambda item: str(item[0]))
        for percentile in (P50, P90)
    ]

    # throughput: sum of declared tonnage per port.
    tonnage_by_port: dict[str | None, list[float]] = {}
    for fact in accepted:
        if fact["declared_tonnage"] is not None:
            tonnage_by_port.setdefault(fact["port_code"], []).append(fact["declared_tonnage"])
    result["throughput_tonnes"] = [
        {
            "port_code": port,
            "ship_class": None,
            "percentile": None,
            "value": _round6(sum(tonnages)),
            "n_observations": len(tonnages),
        }
        for port, tonnages in sorted(tonnage_by_port.items(), key=lambda item: str(item[0]))
    ]

    # truck gate turnaround: median minutes per port.
    gate_durations = _gate_durations_minutes(gate_scans)
    result["truck_gate_turnaround_minutes"] = [
        {
            "port_code": port,
            "ship_class": None,
            "percentile": P50,
            "value": _round6(percentile_linear(durations, 0.5)),
            "n_observations": len(durations),
        }
        for port, durations in sorted(gate_durations.items(), key=lambda item: str(item[0]))
    ]

    # booking lead time (median hours) and slot utilisation (%) per port.
    lead_by_port: dict[str | None, list[float]] = {}
    slots_by_port: dict[str | None, list[tuple[float, float]]] = {}
    for booking in bookings:
        lead_by_port.setdefault(booking["port_code"], []).append(booking["lead_time_hours"])
        if booking["booked_slots"] is not None and booking["offered_slots"] is not None:
            if booking["offered_slots"] <= 0 or booking["booked_slots"] < 0:
                raise ValueError("booking slot counters must be non-negative with offered > 0")
            slots_by_port.setdefault(booking["port_code"], []).append(
                (booking["booked_slots"], booking["offered_slots"])
            )
    result["booking_lead_time_hours"] = [
        {
            "port_code": port,
            "ship_class": None,
            "percentile": P50,
            "value": _round6(percentile_linear(lead_times, 0.5)),
            "n_observations": len(lead_times),
        }
        for port, lead_times in sorted(lead_by_port.items(), key=lambda item: str(item[0]))
    ]
    result["slot_utilisation_pct"] = [
        {
            "port_code": port,
            "ship_class": None,
            "percentile": None,
            "value": _round6(100.0 * sum(b for b, _ in slots) / sum(o for _, o in slots)),
            "n_observations": len(slots),
        }
        for port, slots in sorted(slots_by_port.items(), key=lambda item: str(item[0]))
    ]
    return result


# ---------------------------------------------------------------------------
# Value-row emission (no-data rows are first class)
# ---------------------------------------------------------------------------


def emit_value_rows(
    observations: dict[str, list[dict[str, Any]]],
    discovered_ports: list[str | None],
    period: str,
    run_id: str,
    source_table: str,
    table_version: int,
    query_hash: str,
    computed_at: datetime,
) -> list[dict[str, Any]]:
    """Emit exactly one value row per KPI x port x segment, no-data included.

    A KPI with zero observations for a port is emitted with ``value=null``
    and a coverage note; gap KPIs always emit gap rows citing their
    ``STATS_GAPS`` id. Nothing is silently omitted.
    """

    def base_row(definition: KpiDefinition, port_code: str | None) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "kpi_id": definition.kpi_id,
            "period": period,
            "port_code": port_code,
            "ship_class": None,
            "value": None,
            "unit": definition.unit,
            "n_observations": 0,
            "percentile": None,
            "coverage_note": None,
            "definition_version": definition.definition_version,
            "source_table": source_table,
            "table_version": table_version,
            "query_hash": query_hash,
            "computed_at": computed_at,
        }

    rows: list[dict[str, Any]] = []
    for definition in KPI_DEFINITIONS:
        for port_code in discovered_ports:
            if definition.gap_id is not None:
                gap = STATS_GAP_BY_ID[definition.gap_id]
                row = base_row(definition, port_code)
                row["coverage_note"] = f"{gap.gap_id}: {gap.description}"
                rows.append(row)
                continue
            emitted = [
                observation
                for observation in observations.get(definition.kpi_id, [])
                if observation["port_code"] == port_code
            ]
            if not emitted:
                row = base_row(definition, port_code)
                row["coverage_note"] = NO_DATA_NOTE
                rows.append(row)
                continue
            for observation in sorted(
                emitted,
                key=lambda item: (str(item["ship_class"]), str(item["percentile"])),
            ):
                row = base_row(definition, port_code)
                row.update(
                    {
                        "ship_class": observation["ship_class"],
                        "value": observation["value"],
                        "n_observations": observation["n_observations"],
                        "percentile": observation["percentile"],
                    }
                )
                rows.append(row)
    return rows


def discover_ports(
    facts: list[dict[str, Any]],
    gate_scans: list[dict[str, Any]],
    bookings: list[dict[str, Any]],
) -> list[str | None]:
    """All port codes observable in the period; ``[None]`` when nothing was seen."""
    ports = {fact["port_code"] for fact in facts}
    ports |= {scan["port_code"] for scan in gate_scans}
    ports |= {booking["port_code"] for booking in bookings}
    ports.discard(None)
    ordered = sorted(str(port) for port in ports)
    return list(ordered) if ordered else [None]


# ---------------------------------------------------------------------------
# Run orchestration: pinned-version read, gold writes, signed report artefact
# ---------------------------------------------------------------------------

PORT_CALL_FACTS_SCHEMA = pa.schema(
    [
        pa.field("port_call_id", pa.string(), nullable=False),
        pa.field("port_code", pa.string(), nullable=False),
        pa.field("vessel_ref_hashed", pa.string()),
        pa.field("ship_class", pa.string()),
        pa.field("status", pa.string(), nullable=False),
        pa.field("arrived_at", pa.timestamp("us", tz="UTC")),
        pa.field("berthed_at", pa.timestamp("us", tz="UTC")),
        pa.field("departed_at", pa.timestamp("us", tz="UTC")),
        pa.field("declared_tonnage", pa.float64()),
        pa.field("source_event_ids_json", pa.string(), nullable=False),
    ]
)

PORT_KPI_RUNS_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("period", pa.string(), nullable=False),
        pa.field("source_table", pa.string(), nullable=False),
        pa.field("table_version", pa.int64(), nullable=False),
        pa.field("query_hash", pa.string(), nullable=False),
        pa.field("definitions_version", pa.string(), nullable=False),
        pa.field("started_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("finished_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("input_event_count", pa.int64(), nullable=False),
        pa.field("report_path", pa.string(), nullable=False),
    ]
)

PORT_KPI_VALUES_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("kpi_id", pa.string(), nullable=False),
        pa.field("period", pa.string(), nullable=False),
        pa.field("port_code", pa.string()),
        pa.field("ship_class", pa.string()),
        pa.field("value", pa.float64()),
        pa.field("unit", pa.string(), nullable=False),
        pa.field("n_observations", pa.int64(), nullable=False),
        pa.field("percentile", pa.string()),
        pa.field("coverage_note", pa.string()),
        pa.field("definition_version", pa.string(), nullable=False),
        pa.field("source_table", pa.string(), nullable=False),
        pa.field("table_version", pa.int64(), nullable=False),
        pa.field("query_hash", pa.string(), nullable=False),
        pa.field("computed_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


def run_id_for(period: str, source_table: str, table_version: int, query_hash: str) -> str:
    """Deterministic run id: identical inputs reproduce the identical run."""
    material = f"{period}|{source_table}|{table_version}|{query_hash}"
    return str(uuid.uuid5(RUN_ID_NAMESPACE, material))


def read_silver_events_at_version(table_uri: str, table_version: int) -> list[dict[str, Any]]:
    """Read the silver events table pinned at ``table_version`` (time travel)."""
    table = DeltaTable(table_uri, version=table_version)
    return table.to_pyarrow_table().to_pylist()


def render_report_markdown(
    *,
    period: str,
    run_id: str,
    source_table: str,
    table_version: int,
    query_hash: str,
    input_event_count: int,
    value_rows: list[dict[str, Any]],
    generated_at: datetime,
) -> str:
    """Render the human-readable KPI report; no-data and gap rows are shown."""
    lines = [
        f"# Port KPI report — {period}",
        "",
        f"- run_id: `{run_id}`",
        f"- source table: `{source_table}` at Delta version `{table_version}`",
        f"- query definitions sha256: `{query_hash}`",
        f"- input silver events: `{input_event_count}`",
        f"- generated at (UTC): `{generated_at.isoformat().replace('+00:00', 'Z')}`",
        "",
        "| KPI | Port | Ship class | Percentile | Value | Unit | n | Note |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in value_rows:
        value = "" if row["value"] is None else f"{row['value']:.6g}"
        lines.append(
            "| {kpi} | {port} | {ship} | {pct} | {value} | {unit} | {n} | {note} |".format(
                kpi=row["kpi_id"],
                port=row["port_code"] or "-",
                ship=row["ship_class"] or "-",
                pct=row["percentile"] or "-",
                value=value,
                unit=row["unit"],
                n=row["n_observations"],
                note=row["coverage_note"] or "",
            )
        )
    return "\n".join(lines) + "\n"


def build_report_document(
    *,
    period: str,
    run_id: str,
    source_table: str,
    table_version: int,
    query_hash: str,
    input_event_count: int,
    value_rows: list[dict[str, Any]],
    generated_at: datetime,
    report_markdown: str,
) -> dict[str, Any]:
    """Assemble the signable report document (markdown + machine-readable rows)."""
    return {
        "document_type": "platform_port_kpi_report",
        "period": period,
        "run_id": run_id,
        "source": {
            "table": source_table,
            "table_version": table_version,
            "query_definitions_sha256": query_hash,
            "input_event_count": input_event_count,
        },
        "generated_at": generated_at.isoformat().replace("+00:00", "Z"),
        "report_markdown": report_markdown,
        "rows": [
            {
                "kpi_id": row["kpi_id"],
                "period": row["period"],
                "port_code": row["port_code"],
                "ship_class": row["ship_class"],
                "percentile": row["percentile"],
                "value": row["value"],
                "unit": row["unit"],
                "n_observations": row["n_observations"],
                "coverage_note": row["coverage_note"],
                "definition_version": row["definition_version"],
            }
            for row in value_rows
        ],
    }


def _write_gold_table(
    writer: SegregatedDeltaWriter,
    table_uri: str,
    table_name: str,
    schema: pa.Schema,
    rows: list[dict[str, Any]],
) -> None:
    if rows:
        writer.write(table_uri, pa.Table.from_pylist(rows, schema=schema), mode="append")
        return
    # Empty runs still create the table so downstream readers find the schema.
    writer.write(table_uri, schema.empty_table(), mode="append")


def assemble_port_kpi_run(
    scope: LakehouseScope,
    period: str,
    *,
    signing_key: Ed25519PrivateKey,
    key_id: str,
    silver_version: int | None = None,
    reports_dir: Path | None = None,
) -> dict[str, Any]:
    """Run one gold-assembly pass for *period* inside *scope*.

    Steps: pin the silver table version, read events at that version, build
    facts, compute KPI observations, emit value rows (no-data + gap rows
    included), write the runs/values/facts gold tables, and emit a signed
    report artefact (Ed25519 JWS, ``signature-algorithm: Ed25519``). The run
    row is written before the value rows so every value row's ``run_id``
    resolves against ``port_kpi_runs``.
    """
    period_start, period_end = parse_period(period)
    started_at = datetime.now(tz=UTC)
    query_hash = query_definitions_sha256()

    silver_uri = require_scope_table_uri(scope, "silver", "events")
    if silver_version is None:
        try:
            silver_version = DeltaTable(silver_uri).version()
        except TableNotFoundError:
            raise ValueError(
                f"silver events table does not exist at {silver_uri!r}; "
                "no KPI run is possible without source data"
            ) from None
    source_table = SILVER_TABLE_LABEL

    silver_events = read_silver_events_at_version(silver_uri, silver_version)
    facts = assemble_port_call_facts(silver_events, period_start, period_end)
    gate_scans = extract_gate_scans(silver_events, period_start, period_end)
    bookings = extract_bookings(silver_events, period_start, period_end)

    observations = compute_kpi_observations(facts, gate_scans, bookings)
    ports = discover_ports(facts, gate_scans, bookings)

    run_id = run_id_for(period, source_table, silver_version, query_hash)
    computed_at = datetime.now(tz=UTC)
    value_rows = emit_value_rows(
        observations,
        ports,
        period,
        run_id,
        source_table,
        silver_version,
        query_hash,
        computed_at,
    )

    generated_at = datetime.now(tz=UTC)
    report_markdown = render_report_markdown(
        period=period,
        run_id=run_id,
        source_table=source_table,
        table_version=silver_version,
        query_hash=query_hash,
        input_event_count=len(silver_events),
        value_rows=value_rows,
        generated_at=generated_at,
    )
    report_document = build_report_document(
        period=period,
        run_id=run_id,
        source_table=source_table,
        table_version=silver_version,
        query_hash=query_hash,
        input_event_count=len(silver_events),
        value_rows=value_rows,
        generated_at=generated_at,
        report_markdown=report_markdown,
    )
    signed_report = sign_document(report_document, signing_key, key_id=key_id)

    reports_root = reports_dir or (Path(scope.root_uri) / "gold" / REPORTS_DIR_NAME)
    reports_root.mkdir(parents=True, exist_ok=True)
    report_path = reports_root / f"port_kpi_{period}_{run_id}.json"
    report_path.write_text(canonical_json(signed_report) + "\n", encoding="utf-8")

    writer = SegregatedDeltaWriter(scope)
    gold_layer_uri = scope_layer_table_uri(scope, "gold", "")
    facts_uri = f"{gold_layer_uri}{FACTS_TABLE_NAME}"
    runs_uri = f"{gold_layer_uri}{RUNS_TABLE_NAME}"
    values_uri = f"{gold_layer_uri}{VALUES_TABLE_NAME}"

    run_row = {
        "run_id": run_id,
        "period": period,
        "source_table": source_table,
        "table_version": silver_version,
        "query_hash": query_hash,
        "definitions_version": "1.0.0",
        "started_at": started_at,
        "finished_at": datetime.now(tz=UTC),
        "input_event_count": len(silver_events),
        "report_path": str(report_path),
    }
    _write_gold_table(writer, facts_uri, FACTS_TABLE_NAME, PORT_CALL_FACTS_SCHEMA, facts)
    _write_gold_table(writer, runs_uri, RUNS_TABLE_NAME, PORT_KPI_RUNS_SCHEMA, [run_row])
    _write_gold_table(writer, values_uri, VALUES_TABLE_NAME, PORT_KPI_VALUES_SCHEMA, value_rows)

    return {
        "run_id": run_id,
        "period": period,
        "source_table": source_table,
        "table_version": silver_version,
        "query_hash": query_hash,
        "input_event_count": len(silver_events),
        "fact_count": len(facts),
        "value_row_count": len(value_rows),
        "report_path": str(report_path),
    }


def append_facts(
    scope: LakehouseScope, facts: list[dict[str, Any]]
) -> None:
    """Append pre-built fact rows (testing seam used by gold-assembly tests)."""
    gold_layer_uri = scope_layer_table_uri(scope, "gold", "")
    append_rows(f"{gold_layer_uri}{FACTS_TABLE_NAME}", PORT_CALL_FACTS_SCHEMA, facts)


__all__ = [
    "ACCEPTED_OR_BEYOND",
    "BOOKING_EVENT_TYPE",
    "FACTS_TABLE_NAME",
    "GATE_SCAN_EVENT_TYPE",
    "KPI_BY_ID",
    "KPI_DEFINITIONS",
    "NO_DATA_NOTE",
    "PERIOD_PATTERN",
    "PORT_CALL_EVENT_TYPE",
    "PORT_CALL_FACTS_SCHEMA",
    "PORT_CALL_STATUSES",
    "PORT_KPI_RUNS_SCHEMA",
    "PORT_KPI_VALUES_SCHEMA",
    "P50",
    "P90",
    "REPORTS_DIR_NAME",
    "RUNS_TABLE_NAME",
    "SILVER_TABLE_LABEL",
    "STATS_GAPS",
    "STATS_GAP_BY_ID",
    "UNLOCODE_PATTERN",
    "VALUES_TABLE_NAME",
    "KpiDefinition",
    "StatsGap",
    "append_facts",
    "assemble_port_call_facts",
    "assemble_port_kpi_run",
    "build_report_document",
    "compute_kpi_observations",
    "decode_silver_event",
    "discover_ports",
    "emit_value_rows",
    "extract_bookings",
    "extract_gate_scans",
    "parse_period",
    "percentile_linear",
    "query_definitions_sha256",
    "read_silver_events_at_version",
    "render_report_markdown",
    "run_id_for",
]
