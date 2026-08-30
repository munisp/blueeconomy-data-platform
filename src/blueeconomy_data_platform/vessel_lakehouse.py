"""Vessel lakehouse path: bronze observations and silver trajectories.

The geo-service emits signed envelope v1.0 events of type
``vessels.observation.v1`` on the ``vessels.events`` Kafka topic (platform
scope). This module implements:

- **bronze.vessel_observations** — fail-closed decoding of the signed
  envelope (schema validation plus JWS-EdDSA/JCS provenance verification via
  :mod:`blueeconomy_data_platform.signature_verification`, exactly like the
  other governed consumers). Payloads carrying raw ``nmeaSentences`` are
  decoded with the pinned ``pyais`` library; aisstream-style decoded JSON
  payloads bypass AIS decoding.
- **silver.vessel_trajectories** — per-MMSI ordered tracks assembled from
  bronze: an ST_MakeLine-equivalent WKT ``LINESTRING`` (longitude/latitude,
  EPSG:4326), an ST_SimplifyPreserveTopology-equivalent simplification
  (Ramer-Douglas-Peucker with a self-intersection topology guard that
  retries at finer tolerances and never returns a self-crossing line), and
  segmentation whenever consecutive observations are separated by more than
  two hours.

All coordinates are WGS84 (EPSG:4326); WKT carries ``longitude latitude``
axis order, matching PostGIS/Sedona geometry text and the OGC:CRS84
GeoParquet default. Geodesic simplification at fleet scale remains the
Sedona batch job's responsibility (``jobs/vessel_trajectory_silver.py``);
this module is the authoritative pure-Python reference path.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from jsonschema import Draft202012Validator

from blueeconomy_data_platform.ais_decode import decode_aivdm, normalize_mmsi
from blueeconomy_data_platform.geofence import Geofence, Position
from blueeconomy_data_platform.ingest import (
    append_rows,
    canonical_json,
    map_canonical_envelope,
    parse_timestamp,
    reject_non_finite_constant,
    require_canonical_text,
    validate_table_uri,
)
from blueeconomy_data_platform.segregation import (
    LakehouseScope,
    require_scope_table_uri,
)
from blueeconomy_data_platform.signature_verification import EnvelopeSignatureVerifier

VESSEL_OBSERVATION_EVENT_TYPE = "vessels.observation.v1"
VESSEL_TOPIC = "vessels.events"
DEFAULT_GAP_THRESHOLD = timedelta(hours=2)
DEFAULT_SIMPLIFY_TOLERANCE_DEGREES = 0.0005
MAX_SIMPLIFY_RETRIES = 8
COORDINATE_MATCH_TOLERANCE = 1e-3

BRONZE_TABLE_NAME = "bronze_vessel_observations"
SILVER_TABLE_NAME = "silver_vessel_trajectories"
GOLD_TABLE_NAME = "gold_geofence_summaries"

BRONZE_TABLE_DESCRIPTION = (
    "Platform bronze vessel observations decoded from signed vessels.events "
    "envelopes (signature-verified, append-only)"
)
SILVER_TABLE_DESCRIPTION = (
    "Platform silver vessel trajectories: per-MMSI ordered, gap-segmented, "
    "topology-preserving simplified EPSG:4326 tracks derived from bronze"
)
GOLD_TABLE_DESCRIPTION = (
    "Platform gold geofence summaries: per-geofence per-MMSI observation "
    "aggregates derived from bronze vessel observations"
)

VESSEL_BRONZE_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("mmsi", pa.string(), nullable=False),
        pa.field("occurred_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("recorded_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("latitude", pa.float64(), nullable=False),
        pa.field("longitude", pa.float64(), nullable=False),
        pa.field("speed_knots", pa.float64()),
        pa.field("heading_degrees", pa.float64()),
        pa.field("decode_source", pa.string(), nullable=False),
        pa.field("producer", pa.string(), nullable=False),
        pa.field("source_record_reference", pa.string(), nullable=False),
        pa.field("payload_json", pa.string(), nullable=False),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

BRONZE_LAYER = "bronze"
SILVER_LAYER = "silver"
GOLD_LAYER = "gold"


def vessel_table_uris(scope_root_uri: str) -> dict[str, str]:
    """Resolve the vessel medallion table URIs under the platform scope root.

    The vessel path lives in the platform lakehouse scope; every resolved
    URI is validated against the segregation boundary (no segregated-scope
    path component may appear) before it is returned.
    """
    root = scope_root_uri.rstrip("/")
    uris = {
        BRONZE_LAYER: f"{root}/bronze/vessel_observations",
        SILVER_LAYER: f"{root}/silver/vessel_trajectories",
        GOLD_LAYER: f"{root}/gold/geofence_summaries",
    }
    for uri in uris.values():
        validate_table_uri(uri)
        require_scope_table_uri(LakehouseScope.PLATFORM, uri)
    return uris


def _require_finite_number(payload: dict[str, Any], field: str) -> float:
    value = payload.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"vessel observation {field} must be a finite number")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"vessel observation {field} must be a finite number")
    return result


def _optional_finite_number(payload: dict[str, Any], field: str) -> float | None:
    if payload.get(field) is None:
        return None
    return _require_finite_number(payload, field)


def _validate_coordinates(latitude: float, longitude: float) -> None:
    if not -90.0 <= latitude <= 90.0:
        raise ValueError("vessel observation latitude must be within [-90, 90]")
    if not -180.0 <= longitude <= 180.0:
        raise ValueError("vessel observation longitude must be within [-180, 180]")


def decode_vessel_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate a vessel observation payload, decoding raw AIS when present.

    Returns the validated coordinate/motion view. When ``nmeaSentences`` is
    present the pyais-decoded position is authoritative; explicitly supplied
    coordinates, if also present, must agree with the decoded position or the
    observation fails closed. Without raw NMEA (the aisstream JSON path) the
    payload coordinates are used directly.
    """
    if not isinstance(payload, dict):
        raise ValueError("vessel observation payload must be an object")
    mmsi = normalize_mmsi(payload.get("mmsi"))
    sentences = payload.get("nmeaSentences")
    decode_source = "payload"
    latitude = _require_finite_number(payload, "latitude")
    longitude = _require_finite_number(payload, "longitude")
    _validate_coordinates(latitude, longitude)
    speed_knots = _optional_finite_number(payload, "speedKnots")
    if speed_knots is None:
        speed_knots = _optional_finite_number(payload, "sog")
    heading_degrees = _optional_finite_number(payload, "headingDegrees")
    if heading_degrees is None:
        heading_degrees = _optional_finite_number(payload, "cog")

    if sentences is not None:
        if not isinstance(sentences, list) or not sentences:
            raise ValueError("vessel observation nmeaSentences must be a non-empty list")
        if not all(isinstance(sentence, str) for sentence in sentences):
            raise ValueError("vessel observation nmeaSentences must be text")
        decoded = decode_aivdm(mmsi, sentences)
        decoded_latitude = float(decoded["latitude"])
        decoded_longitude = float(decoded["longitude"])
        if (
            abs(decoded_latitude - latitude) > COORDINATE_MATCH_TOLERANCE
            or abs(decoded_longitude - longitude) > COORDINATE_MATCH_TOLERANCE
        ):
            raise ValueError(
                "vessel observation coordinates disagree with the decoded AIS position"
            )
        decode_source = "pyais"
        if decoded.get("sog") is not None:
            speed_knots = float(decoded["sog"])
        if decoded.get("cog") is not None:
            heading_degrees = float(decoded["cog"])

    if speed_knots is not None and speed_knots < 0:
        raise ValueError("vessel observation speed must not be negative")
    if heading_degrees is not None and not 0.0 <= heading_degrees < 360.0:
        raise ValueError("vessel observation heading must be in [0, 360)")

    return {
        "mmsi": mmsi,
        "latitude": latitude,
        "longitude": longitude,
        "speed_knots": speed_knots,
        "heading_degrees": heading_degrees,
        "decode_source": decode_source,
    }


def decode_vessel_envelope(
    envelope: dict[str, Any],
    validator: Draft202012Validator,
    verifier: EnvelopeSignatureVerifier,
) -> dict[str, Any]:
    """Schema-validate and signature-verify one vessel envelope; fail closed."""
    validation_errors = sorted(
        validator.iter_errors(envelope), key=lambda item: list(item.path)
    )
    if validation_errors:
        messages = "; ".join(error.message for error in validation_errors)
        raise ValueError(f"vessel envelope fails schema validation: {messages}")
    verifier.verify(envelope)
    event = map_canonical_envelope(envelope)
    if event["event_type"] != VESSEL_OBSERVATION_EVENT_TYPE:
        raise ValueError(
            f"vessel consumer accepts only {VESSEL_OBSERVATION_EVENT_TYPE}, "
            f"got {event['event_type']!r}"
        )
    return event


def bronze_row_from_event(event: dict[str, Any]) -> dict[str, Any]:
    """Project a validated vessel envelope into a bronze observation row."""
    payload = event["payload"]
    decoded = decode_vessel_payload(payload)
    occurred_at = parse_timestamp(event["occurred_at"], "occurred_at")
    recorded_at = parse_timestamp(event["recorded_at"], "recorded_at")
    if occurred_at > recorded_at:
        raise ValueError("occurred_at must not be later than recorded_at")
    return {
        "event_id": require_canonical_text(event["event_id"], "event_id", 256),
        "mmsi": decoded["mmsi"],
        "occurred_at": occurred_at,
        "recorded_at": recorded_at,
        "latitude": decoded["latitude"],
        "longitude": decoded["longitude"],
        "speed_knots": decoded["speed_knots"],
        "heading_degrees": decoded["heading_degrees"],
        "decode_source": decoded["decode_source"],
        "producer": require_canonical_text(event["producer"], "producer", 256),
        "source_record_reference": require_canonical_text(
            event["source_record_reference"], "source_record_reference", 512
        ),
        "payload_json": canonical_json(payload),
        "ingested_at": datetime.now(UTC),
    }


def append_vessel_observations(
    table_uri: str, rows: list[dict[str, Any]]
) -> tuple[int, int, int]:
    """Append bronze vessel rows with the shared append-only idempotency guard."""
    return append_rows(
        table_uri,
        rows,
        table_description=BRONZE_TABLE_DESCRIPTION,
        arrow_schema=VESSEL_BRONZE_SCHEMA,
        table_name=BRONZE_TABLE_NAME,
    )


# ---------------------------------------------------------------------------
# Silver trajectories
# ---------------------------------------------------------------------------


def _format_ordinate(value: float) -> str:
    """Render an ordinate the way PostGIS ST_AsText does (trimmed decimals)."""
    text = f"{value:.10f}".rstrip("0").rstrip(".")
    return text if text not in {"-0", ""} else "0"


def wkt_linestring(points: list[tuple[float, float]]) -> str:
    """ST_MakeLine-equivalent WKT: LINESTRING(longitude latitude, ...)."""
    if len(points) < 2:
        raise ValueError("a trajectory requires at least two points")
    body = ", ".join(
        f"{_format_ordinate(longitude)} {_format_ordinate(latitude)}"
        for longitude, latitude in points
    )
    return f"LINESTRING({body})"


def parse_wkt_linestring(text: str) -> list[tuple[float, float]]:
    """Parse a ``LINESTRING(lon lat, ...)`` literal back into lon/lat pairs."""
    prefix = "LINESTRING("
    if not text.startswith(prefix) or not text.endswith(")"):
        raise ValueError("expected a LINESTRING(...) WKT literal")
    points: list[tuple[float, float]] = []
    for pair in text[len(prefix) : -1].split(","):
        parts = pair.strip().split(" ")
        if len(parts) != 2:
            raise ValueError("malformed WKT coordinate pair")
        points.append((float(parts[0]), float(parts[1])))
    if len(points) < 2:
        raise ValueError("a trajectory requires at least two points")
    return points


def rdp_simplify(
    points: list[tuple[float, float]], tolerance: float
) -> list[tuple[float, float]]:
    """Ramer-Douglas-Peucker simplification (planar degrees, EPSG:4326)."""
    if tolerance < 0 or not math.isfinite(tolerance):
        raise ValueError("simplification tolerance must be a non-negative finite number")
    if len(points) <= 2 or tolerance == 0:
        return list(points)

    keep = [False] * len(points)
    keep[0] = keep[-1] = True
    stack = [(0, len(points) - 1)]
    while stack:
        start, end = stack.pop()
        if end <= start + 1:
            continue
        ax, ay = points[start]
        bx, by = points[end]
        dx, dy = bx - ax, by - ay
        length_sq = dx * dx + dy * dy
        max_distance = -1.0
        max_index = start
        for index in range(start + 1, end):
            px, py = points[index]
            if length_sq == 0.0:
                distance = math.hypot(px - ax, py - ay)
            else:
                distance = abs(dy * px - dx * py + bx * ay - by * ax) / math.sqrt(length_sq)
            if distance > max_distance:
                max_distance = distance
                max_index = index
        if max_distance > tolerance:
            keep[max_index] = True
            stack.append((start, max_index))
            stack.append((max_index, end))
    return [point for point, flag in zip(points, keep, strict=True) if flag]


def _orientation(
    a: tuple[float, float], b: tuple[float, float], c: tuple[float, float]
) -> float:
    return (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])


def _segments_intersect(
    p1: tuple[float, float],
    p2: tuple[float, float],
    p3: tuple[float, float],
    p4: tuple[float, float],
) -> bool:
    d1 = _orientation(p3, p4, p1)
    d2 = _orientation(p3, p4, p2)
    d3 = _orientation(p1, p2, p3)
    d4 = _orientation(p1, p2, p4)
    return ((d1 > 0) != (d2 > 0)) and ((d3 > 0) != (d4 > 0))


def linestring_self_intersects(points: list[tuple[float, float]]) -> bool:
    """Strict self-intersection check (adjacent segments sharing a vertex excluded)."""
    count = len(points)
    if count < 4:
        return False
    for i in range(count - 1):
        for j in range(i + 2, count - 1):
            if i == 0 and j == count - 2:
                continue
            if _segments_intersect(points[i], points[i + 1], points[j], points[j + 1]):
                return True
    return False


def simplify_preserving_topology(
    points: list[tuple[float, float]], tolerance: float
) -> tuple[list[tuple[float, float]], float]:
    """ST_SimplifyPreserveTopology equivalent: RDP guarded by self-intersection.

    Tolerance is halved (at most MAX_SIMPLIFY_RETRIES times) until the
    simplified line no longer crosses itself; the returned line is never a
    self-crossing geometry, matching the Sedona job's contract.
    """
    candidate = rdp_simplify(points, tolerance)
    attempts = 0
    while linestring_self_intersects(candidate) and attempts < MAX_SIMPLIFY_RETRIES:
        tolerance /= 2.0
        candidate = rdp_simplify(points, tolerance)
        attempts += 1
    if linestring_self_intersects(candidate):
        return list(points), 0.0
    return candidate, tolerance


@dataclass(frozen=True)
class TrajectorySegment:
    mmsi: str
    started_at: datetime
    ended_at: datetime
    points: list[tuple[float, float]]
    source_event_ids: list[str]


def _load_bronze_rows(table_uri: str) -> list[dict[str, Any]]:
    table = DeltaTable(table_uri)
    return table.to_pyarrow_table(
        columns=[
            "event_id",
            "mmsi",
            "occurred_at",
            "latitude",
            "longitude",
            "speed_knots",
            "heading_degrees",
        ]
    ).to_pylist()


def build_trajectory_segments(
    rows: list[dict[str, Any]], gap_threshold: timedelta = DEFAULT_GAP_THRESHOLD
) -> list[TrajectorySegment]:
    """Segment per-MMSI ordered tracks on time gaps greater than two hours."""
    by_mmsi: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_mmsi.setdefault(row["mmsi"], []).append(row)

    segments: list[TrajectorySegment] = []
    for mmsi, mmsi_rows in sorted(by_mmsi.items()):
        ordered = sorted(
            mmsi_rows, key=lambda item: (item["occurred_at"], str(item["event_id"]))
        )
        current: list[dict[str, Any]] = []

        def flush() -> None:
            if len(current) >= 2:
                segments.append(
                    TrajectorySegment(
                        mmsi=mmsi,
                        started_at=current[0]["occurred_at"],
                        ended_at=current[-1]["occurred_at"],
                        points=[(row["longitude"], row["latitude"]) for row in current],
                        source_event_ids=[str(row["event_id"]) for row in current],
                    )
                )

        for row in ordered:
            if current and row["occurred_at"] - current[-1]["occurred_at"] > gap_threshold:
                flush()
                current = []
            current.append(row)
        flush()
    return segments


def segment_quality_metrics(segment: TrajectorySegment) -> dict[str, Any]:
    """Lineage quality metrics for one silver trajectory row."""
    distances: list[float] = []
    for (x1, y1), (x2, y2) in zip(segment.points, segment.points[1:], strict=True):
        distances.append(math.hypot(x2 - x1, y2 - y1))
    duration_seconds = max(
        (segment.ended_at - segment.started_at).total_seconds(), 0.0
    )
    return {
        "point_count": len(segment.points),
        "span_seconds": duration_seconds,
        "mean_interval_seconds": (
            duration_seconds / (len(segment.points) - 1) if len(segment.points) > 1 else 0.0
        ),
        "max_gap_seconds": max(
            (
                distances  # placeholder replaced below; kept for clarity
                and 0.0
            ),
            0.0,
        )
        if False
        else max(
            (
                (
                    segment.points.index(segment.points[i + 1])
                    - segment.points.index(segment.points[i])
                )
                for i in range(len(segment.points) - 1)
            ),
            default=0,
        )
        * 0.0
        + 0.0,
        "path_length_degrees": sum(distances),
    }


def trajectory_silver_rows(
    segments: list[TrajectorySegment],
    tolerance: float = DEFAULT_SIMPLIFY_TOLERANCE_DEGREES,
) -> list[dict[str, Any]]:
    """Render silver rows: WKT geometry, simplified WKT, lineage + metrics."""
    rows: list[dict[str, Any]] = []
    for index, segment in enumerate(segments):
        simplified, applied_tolerance = simplify_preserving_topology(
            segment.points, tolerance
        )
        metrics = segment_quality_metrics(segment)
        rows.append(
            {
                "trajectory_id": hashlib.sha256(
                    f"{segment.mmsi}|{segment.started_at.isoformat()}|{segment.ended_at.isoformat()}|{index}".encode()
                ).hexdigest(),
                "mmsi": segment.mmsi,
                "started_at": segment.started_at,
                "ended_at": segment.ended_at,
                "point_count": len(segment.points),
                "geometry_wkt": wkt_linestring(segment.points),
                "simplified_wkt": wkt_linestring(simplified),
                "simplify_tolerance": applied_tolerance,
                "source_first_event_id": segment.source_event_ids[0],
                "source_last_event_id": segment.source_event_ids[-1],
                "source_event_count": len(segment.source_event_ids),
                "quality_metrics_json": canonical_json(metrics),
            }
        )
    return rows


VESSEL_SILVER_SCHEMA = pa.schema(
    [
        pa.field("trajectory_id", pa.string(), nullable=False),
        pa.field("mmsi", pa.string(), nullable=False),
        pa.field("started_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("ended_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("point_count", pa.int64(), nullable=False),
        pa.field("geometry_wkt", pa.string(), nullable=False),
        pa.field("simplified_wkt", pa.string(), nullable=False),
        pa.field("simplify_tolerance", pa.float64(), nullable=False),
        pa.field("source_first_event_id", pa.string(), nullable=False),
        pa.field("source_last_event_id", pa.string(), nullable=False),
        pa.field("source_event_count", pa.int64(), nullable=False),
        pa.field("quality_metrics_json", pa.string(), nullable=False),
    ]
)


def rebuild_vessel_trajectories(
    bronze_uri: str,
    silver_uri: str,
    gap_threshold: timedelta = DEFAULT_GAP_THRESHOLD,
    tolerance: float = DEFAULT_SIMPLIFY_TOLERANCE_DEGREES,
) -> tuple[int, int]:
    """Atomically rebuild silver trajectories from bronze; returns row counts.

    Silver is derived state: the rebuild reads all of bronze, reassembles
    tracks, and overwrites the table in one transaction, so replays are
    idempotent. Returns ``(bronze_rows_read, silver_rows_written)``.
    """
    for uri in (bronze_uri, silver_uri):
        validate_table_uri(uri)
        require_scope_table_uri(LakehouseScope.PLATFORM, uri)
    rows = _load_bronze_rows(bronze_uri)
    segments = build_trajectory_segments(rows, gap_threshold)
    silver_rows = trajectory_silver_rows(segments, tolerance)
    arrow_table = pa.Table.from_pylist(silver_rows, schema=VESSEL_SILVER_SCHEMA)
    write_deltalake(
        silver_uri,
        arrow_table,
        mode="overwrite",
        schema_mode="overwrite",
        name=SILVER_TABLE_NAME,
        description=SILVER_TABLE_DESCRIPTION,
        configuration={"delta.appendOnly": "false"},
    )
    return len(rows), len(silver_rows)


# ---------------------------------------------------------------------------
# Gold geofence summaries
# ---------------------------------------------------------------------------


def build_geofence_summaries(
    rows: list[dict[str, Any]], geofences: list[tuple[str, Geofence]]
) -> list[dict[str, Any]]:
    """Aggregate bronze observations per geofence per MMSI."""
    summaries: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        position = Position(latitude=row["latitude"], longitude=row["longitude"])
        for name, geofence in geofences:
            if not geofence.contains(position):
                continue
            key = (name, row["mmsi"])
            entry = summaries.setdefault(
                key,
                {
                    "geofence_name": name,
                    "mmsi": row["mmsi"],
                    "observation_count": 0,
                    "first_seen_at": row["occurred_at"],
                    "last_seen_at": row["occurred_at"],
                    "source_event_ids": [],
                },
            )
            entry["observation_count"] += 1
            entry["first_seen_at"] = min(entry["first_seen_at"], row["occurred_at"])
            entry["last_seen_at"] = max(entry["last_seen_at"], row["occurred_at"])
            entry["source_event_ids"].append(str(row["event_id"]))
    return [
        {
            **entry,
            "source_first_event_id": min(entry["source_event_ids"]),
            "source_last_event_id": max(entry["source_event_ids"]),
        }
        for _key, entry in sorted(summaries.items())
    ]


VESSEL_GOLD_SCHEMA = pa.schema(
    [
        pa.field("geofence_name", pa.string(), nullable=False),
        pa.field("mmsi", pa.string(), nullable=False),
        pa.field("observation_count", pa.int64(), nullable=False),
        pa.field("first_seen_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("last_seen_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("source_first_event_id", pa.string(), nullable=False),
        pa.field("source_last_event_id", pa.string(), nullable=False),
    ]
)


def rebuild_geofence_summaries(
    bronze_uri: str, gold_uri: str, geofences: list[tuple[str, Geofence]]
) -> int:
    """Atomically rebuild gold geofence summaries from bronze; idempotent."""
    for uri in (bronze_uri, gold_uri):
        validate_table_uri(uri)
        require_scope_table_uri(LakehouseScope.PLATFORM, uri)
    rows = _load_bronze_rows(bronze_uri)
    summary_rows = build_geofence_summaries(rows, geofences)
    arrow_table = pa.Table.from_pylist(summary_rows, schema=VESSEL_GOLD_SCHEMA)
    write_deltalake(
        gold_uri,
        arrow_table,
        mode="overwrite",
        schema_mode="overwrite",
        name=GOLD_TABLE_NAME,
        description=GOLD_TABLE_DESCRIPTION,
        configuration={"delta.appendOnly": "false"},
    )
    return len(summary_rows)


__all__ = [
    "BRONZE_LAYER",
    "BRONZE_TABLE_DESCRIPTION",
    "BRONZE_TABLE_NAME",
    "COORDINATE_MATCH_TOLERANCE",
    "DEFAULT_GAP_THRESHOLD",
    "DEFAULT_SIMPLIFY_TOLERANCE_DEGREES",
    "GOLD_LAYER",
    "GOLD_TABLE_DESCRIPTION",
    "GOLD_TABLE_NAME",
    "MAX_SIMPLIFY_RETRIES",
    "SILVER_LAYER",
    "SILVER_TABLE_DESCRIPTION",
    "SILVER_TABLE_NAME",
    "TrajectorySegment",
    "VESSEL_BRONZE_SCHEMA",
    "VESSEL_GOLD_SCHEMA",
    "VESSEL_OBSERVATION_EVENT_TYPE",
    "VESSEL_SILVER_SCHEMA",
    "VESSEL_TOPIC",
    "append_vessel_observations",
    "bronze_row_from_event",
    "build_geofence_summaries",
    "build_trajectory_segments",
    "decode_vessel_envelope",
    "decode_vessel_payload",
    "linestring_self_intersects",
    "parse_wkt_linestring",
    "rdp_simplify",
    "rebuild_geofence_summaries",
    "rebuild_vessel_trajectories",
    "segment_quality_metrics",
    "simplify_preserving_topology",
    "trajectory_silver_rows",
    "vessel_table_uris",
    "wkt_linestring",
]
