"""Blue-Carbon scope gold assembly: ``bluecarbon_gold/public_registry``.

The public transparency projection is the only ``public`` artefact of the
bluecarbon scope (spec §2.2/§3.3): it is built exclusively from a fixed
SELECT-list of public fields, by construction — raw evidence URIs, monitoring
report detail, proponent PII and any confidential-classified field can never
reach the projection because the assembler copies only the allow-listed keys.

Content per registered project: identity, lifecycle state, ecosystem,
methodology, strata *centroids* (never the full evidence polygons), external
registry linkage (recorded, never asserted as verified), issued/retired/
cancelled totals per vintage, buffer balance and retirement beneficiaries.

The gold table is derived state: rebuilt atomically (overwrite) from the
current silver content on every run, so replays are idempotent.
"""

from __future__ import annotations

import json
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

PROJECT_EVENT_TYPE = "bluecarbon.project.v1"
CREDIT_BLOCK_EVENT_TYPE = "bluecarbon.credit-block.v1"
RETIREMENT_EVENT_TYPE = "bluecarbon.retirement.v1"

GOLD_TABLE_NAME = "public_registry"
GOLD_TABLE_DESCRIPTION = (
    "Blue-Carbon gold public_registry: clearance-filtered PUBLIC projection of "
    "registered projects, per-vintage credit totals, buffer balance and "
    "retirement beneficiaries (derived state, rebuilt atomically)"
)

# The complete public SELECT-list. Any source field outside this allow-list is
# dropped by construction; there is no copy-then-filter path.
PUBLIC_PROJECT_FIELDS = (
    "projectId",
    "projectName",
    "state",
    "ecosystem",
    "methodology",
    "countryCode",
    "strataCentroids",
    "externalRegistry",
    "externalProjectId",
)

BLUECARBON_GOLD_SCHEMA = pa.schema(
    [
        pa.field("project_id", pa.string(), nullable=False),
        pa.field("project_name", pa.string()),
        pa.field("state", pa.string(), nullable=False),
        pa.field("ecosystem", pa.string()),
        pa.field("methodology", pa.string()),
        pa.field("country_code", pa.string()),
        pa.field("strata_centroids_json", pa.string()),
        pa.field("external_registry", pa.string()),
        pa.field("external_project_id", pa.string()),
        pa.field("vintage_totals_json", pa.string(), nullable=False),
        pa.field("buffer_balance_kg_co2e", pa.float64(), nullable=False),
        pa.field("retirements_json", pa.string(), nullable=False),
        pa.field("source_event_ids_json", pa.string(), nullable=False),
        pa.field("curated_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

CREDIT_BLOCK_STATUSES = frozenset({"ACTIVE", "RETIRED", "CANCELLED"})


def bluecarbon_gold_table_uri(writer: SegregatedDeltaWriter) -> str:
    """Resolve the public_registry table URI inside the bluecarbon boundary."""
    if writer.scope is not LakehouseScope.BLUECARBON:
        raise ValueError("public_registry gold assembly requires the bluecarbon boundary")
    roots = writer.roots
    return scope_layer_table_uri(roots.scope, roots.gold.rsplit("/", 2)[0], "gold", GOLD_TABLE_NAME)


def _require_text(fields: dict[str, Any], name: str, event_type: str) -> str:
    value = fields.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{event_type} field {name} must be non-empty text")
    return value


def _optional_text(value: Any, name: str, event_type: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{event_type} field {name} must be non-empty text when present")
    return value


def _require_quantity(fields: dict[str, Any], name: str, event_type: str) -> float:
    value = fields.get(name)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ValueError(f"{event_type} field {name} must be a non-negative number")
    return float(value)


def _latest_events_by_key(
    silver_events: list[dict[str, Any]], event_type: str, key_field: str
) -> dict[str, tuple[dict[str, Any], str, datetime]]:
    """Latest event per key field, ordered by (occurred_at, event_id)."""
    latest: dict[str, tuple[dict[str, Any], str, datetime]] = {}
    for event in silver_events:
        if event.get("event_type") != event_type:
            continue
        payload_json = event.get("payload_json")
        if not isinstance(payload_json, str):
            raise ValueError(f"silver {event_type} event is missing payload_json")
        occurred_at = event.get("occurred_at")
        if not isinstance(occurred_at, datetime):
            raise ValueError(f"silver {event_type} event is missing occurred_at")
        event_id = event.get("event_id")
        if not isinstance(event_id, str):
            raise ValueError(f"silver {event_type} event is missing event_id")
        fields = extract_domain_fields(json.loads(payload_json))
        key = _require_text(fields, key_field, event_type)
        existing = latest.get(key)
        if existing is None or (occurred_at.astimezone(UTC), event_id) > (
            existing[2],
            existing[1],
        ):
            latest[key] = (fields, event_id, occurred_at.astimezone(UTC))
    return latest


def build_public_registry_rows(
    silver_events: list[dict[str, Any]], curated_at: datetime
) -> list[dict[str, Any]]:
    """Assemble the public projection from silver bluecarbon events.

    Pure function, deterministic for identical input rows. Only
    ``PUBLIC_PROJECT_FIELDS`` plus per-vintage movement totals and retirement
    beneficiary/purpose/quantity/artifact fields are copied out of silver.
    """
    projects = _latest_events_by_key(silver_events, PROJECT_EVENT_TYPE, "projectId")
    blocks = _latest_events_by_key(silver_events, CREDIT_BLOCK_EVENT_TYPE, "blockId")

    retirements_by_project: dict[str, list[dict[str, Any]]] = {}
    retirement_sources: dict[str, list[str]] = {}
    for event in silver_events:
        if event.get("event_type") != RETIREMENT_EVENT_TYPE:
            continue
        payload_json = event.get("payload_json")
        if not isinstance(payload_json, str):
            raise ValueError("silver retirement event is missing payload_json")
        fields = extract_domain_fields(json.loads(payload_json))
        project_id = _require_text(fields, "projectId", RETIREMENT_EVENT_TYPE)
        retirement = {
            "retirement_id": _require_text(fields, "retirementId", RETIREMENT_EVENT_TYPE),
            "serial": _optional_text(fields.get("serial"), "serial", RETIREMENT_EVENT_TYPE),
            "beneficiary": _require_text(fields, "beneficiary", RETIREMENT_EVENT_TYPE),
            "purpose": _require_text(fields, "purpose", RETIREMENT_EVENT_TYPE),
            "quantity_kg_co2e": _require_quantity(fields, "quantityKgCo2e", RETIREMENT_EVENT_TYPE),
            "artifact_sha256": _require_text(fields, "artifactSha256", RETIREMENT_EVENT_TYPE),
            "retired_at": _optional_text(
                fields.get("retiredAt"), "retiredAt", RETIREMENT_EVENT_TYPE
            ),
        }
        retirements_by_project.setdefault(project_id, []).append(retirement)
        event_id = event.get("event_id")
        if isinstance(event_id, str):
            retirement_sources.setdefault(project_id, []).append(event_id)

    vintage_totals: dict[str, dict[str, dict[str, float]]] = {}
    buffer_balances: dict[str, float] = {}
    block_sources: dict[str, list[str]] = {}
    for block_id, (fields, event_id, _occurred) in sorted(blocks.items()):
        project_id = _require_text(fields, "projectId", CREDIT_BLOCK_EVENT_TYPE)
        vintage = _require_text(fields, "vintage", CREDIT_BLOCK_EVENT_TYPE)
        status = _require_text(fields, "status", CREDIT_BLOCK_EVENT_TYPE)
        if status not in CREDIT_BLOCK_STATUSES:
            raise ValueError(
                f"{CREDIT_BLOCK_EVENT_TYPE} block {block_id!r} status {status!r} is not "
                f"one of {sorted(CREDIT_BLOCK_STATUSES)}"
            )
        quantity = _require_quantity(fields, "quantityKgCo2e", CREDIT_BLOCK_EVENT_TYPE)
        is_buffer = fields.get("bufferPool") is True
        totals = vintage_totals.setdefault(project_id, {}).setdefault(
            vintage, {"issued": 0.0, "retired": 0.0, "cancelled": 0.0}
        )
        totals["issued"] += quantity
        if status == "RETIRED":
            totals["retired"] += quantity
        elif status == "CANCELLED":
            totals["cancelled"] += quantity
        if is_buffer:
            buffer_balances[project_id] = buffer_balances.get(project_id, 0.0) + (
                quantity if status == "ACTIVE" else 0.0
            )
        block_sources.setdefault(project_id, []).append(event_id)

    rows: list[dict[str, Any]] = []
    for project_id, (source_fields, project_event_id, _occurred) in sorted(projects.items()):
        # Projection-by-construction: only allow-listed public fields are
        # copied out of the confidential/internal silver payload.
        fields = {name: source_fields.get(name) for name in PUBLIC_PROJECT_FIELDS}
        centroids = fields.get("strataCentroids")
        if centroids is not None and not isinstance(centroids, list):
            raise ValueError(f"{PROJECT_EVENT_TYPE} strataCentroids must be a list when present")
        totals_by_vintage = vintage_totals.get(project_id, {})
        vintage_rows = [
            {
                "vintage": vintage,
                "issued_kg_co2e": totals["issued"],
                "retired_kg_co2e": totals["retired"],
                "cancelled_kg_co2e": totals["cancelled"],
            }
            for vintage, totals in sorted(totals_by_vintage.items())
        ]
        retirements = sorted(
            retirements_by_project.get(project_id, []),
            key=lambda item: (item["retirement_id"]),
        )
        source_ids = [
            project_event_id,
            *block_sources.get(project_id, []),
            *retirement_sources.get(project_id, []),
        ]
        rows.append(
            {
                "project_id": project_id,
                "project_name": _optional_text(
                    fields.get("projectName"), "projectName", PROJECT_EVENT_TYPE
                ),
                "state": _require_text(fields, "state", PROJECT_EVENT_TYPE),
                "ecosystem": _optional_text(
                    fields.get("ecosystem"), "ecosystem", PROJECT_EVENT_TYPE
                ),
                "methodology": _optional_text(
                    fields.get("methodology"), "methodology", PROJECT_EVENT_TYPE
                ),
                "country_code": _optional_text(
                    fields.get("countryCode"), "countryCode", PROJECT_EVENT_TYPE
                ),
                "strata_centroids_json": (
                    json.dumps(centroids, sort_keys=True, separators=(",", ":"))
                    if centroids is not None
                    else None
                ),
                "external_registry": _optional_text(
                    fields.get("externalRegistry"), "externalRegistry", PROJECT_EVENT_TYPE
                ),
                "external_project_id": _optional_text(
                    fields.get("externalProjectId"), "externalProjectId", PROJECT_EVENT_TYPE
                ),
                "vintage_totals_json": json.dumps(vintage_rows, separators=(",", ":")),
                "buffer_balance_kg_co2e": buffer_balances.get(project_id, 0.0),
                "retirements_json": json.dumps(retirements, separators=(",", ":")),
                "source_event_ids_json": json.dumps(sorted(source_ids)),
                "curated_at": curated_at,
            }
        )
    return rows


def assemble_bluecarbon_public_registry_gold(writer: SegregatedDeltaWriter) -> tuple[int, int]:
    """Rebuild the bluecarbon public_registry gold table; derived state.

    Returns ``(table_version, row_count)``.
    """
    if writer.scope is not LakehouseScope.BLUECARBON:
        raise ValueError("public_registry gold assembly requires the bluecarbon boundary")
    # Phase-8 OTel span (no-op when telemetry is disabled); gold stage of the
    # bluecarbon medallion DAG.
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
        raise ValueError("cannot assemble bluecarbon gold before the silver table exists") from None
    events = silver.to_pyarrow_table(
        columns=["event_id", "event_type", "occurred_at", "payload_json"]
    ).to_pylist()
    if not events:
        raise ValueError("cannot assemble bluecarbon gold from an empty silver table")
    rows = build_public_registry_rows(events, curated_at=datetime.now(UTC))
    gold_uri = bluecarbon_gold_table_uri(writer)
    write_deltalake(
        gold_uri,
        pa.Table.from_pylist(rows, schema=BLUECARBON_GOLD_SCHEMA),
        mode="overwrite",
        name="bluecarbon_gold_public_registry",
        description=GOLD_TABLE_DESCRIPTION,
    )
    return DeltaTable(gold_uri).version(), len(rows)


__all__ = [
    "BLUECARBON_GOLD_SCHEMA",
    "CREDIT_BLOCK_EVENT_TYPE",
    "GOLD_TABLE_NAME",
    "PROJECT_EVENT_TYPE",
    "PUBLIC_PROJECT_FIELDS",
    "RETIREMENT_EVENT_TYPE",
    "assemble_bluecarbon_public_registry_gold",
    "bluecarbon_gold_table_uri",
    "build_public_registry_rows",
]
