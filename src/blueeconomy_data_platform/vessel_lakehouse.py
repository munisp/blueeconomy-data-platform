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
    heading_degrees = _optional_finite_number(payload, "headingDegrees")
    if speed_knots is not None and speed_knots < 0:
        raise ValueError("vessel observation speedKnots must not be negative")
    if heading_degrees is not None and not 0.0 <= heading_degrees < 360.0:
        raise ValueError("vessel observation headingDegrees must be within [0, 360)")
    if sentences is not None:
        report = decode_aivdm(sentences)
        if report.mmsi != mmsi:
            raise ValueError("payload mmsi does not match the decoded AIS mmsi")
        if (
            abs(report.latitude - latitude) > COORDINATE_MATCH_TOLERANCE
            or abs(report.longitude - longitude) > COORDINATE_MATCH_TOLERANCE
        ):
            raise ValueError(
                "payload coordinates disagree with the decoded AIS position (fail-closed)"
            )
        decode_source = "ais-nmea"
        latitude, longitude = report.latitude, report.longitude
        speed_knots = report.speed_knots if report.speed_knots is not None else speed_knots
        heading_degrees = (
            report.heading_degrees if report.heading_degrees is not None else heading_degrees
        )
    return {
        "mmsi": mmsi,
        "latitude": latitude,
        "longitude": longitude,
        "speed_knots": speed_knots,
        "heading_degrees": heading_degrees,
        "decode_source": decode_source,
    }


def decode_vessel_observation(
    document: dict[str, Any],
    validator: Draft202012Validator,
    verifier: EnvelopeSignatureVerifier,
) -> dict[str, Any]:
    """Decode one signed ``vessels.events`` envelope into a bronze row.

    Fail-closed exactly like the other governed consumers: the envelope must
    validate against the committed schema and its provenance signature must
    verify against the startup-loaded key directory before any field is
    trusted.
    """
    if not isinstance(document, dict):
        raise ValueError("vessel envelope must be a JSON object")
    errors = sorted(validator.iter_errors(document), key=lambda item: list(item.path))
    if errors:
        messages = "; ".join(error.message for error in errors)
        raise ValueError(f"vessel envelope fails event-envelope validation: {messages}")
    signed_view = json.loads(
        canonical_json(document),
        parse_int=float,
        parse_float=float,
        parse_constant=reject_non_finite_constant,
    )
    verifier.verify(signed_view)
    event = map_canonical_envelope(document)
    if event["event_type"] != VESSEL_OBSERVATION_EVENT_TYPE:
        raise ValueError(
            f"vessel observation event type must be {VESSEL_OBSERVATION_EVENT_TYPE!r}, "
            f"got {event['event_type']!r}"
        )
    payload = event["payload"]
    resource = {key: value for key, value in payload.items() if key != "provenance"}
    observation = decode_vessel_payload(resource)
    occurred_at = parse_timestamp(event["occurred_at"], "occurred_at")
    recorded_at = parse_timestamp(event["recorded_at"], "recorded_at")
    if occurred_at > recorded_at:
        raise ValueError("occurred_at must not be later than recorded_at")
    return {
        "event_id": require_canonical_text(event["event_id"], "event_id", 256),
        "mmsi": observation["mmsi"],
        "occurred_at": occurred_at,
        "recorded_at": recorded_at,
        "latitude": observation["latitude"],
        "longitude": observation["longitude"],
        "speed_knots": observation["speed_knots"],
        "heading_degrees": observation["heading_degrees"],
        "decode_source": observation["decode_source"],
        "producer": require_canonical_text(event["producer"], "producer", 256),
        "source_record_reference": require_canonical_text(
            event["source_record_reference"], "source_record_reference", 512
        ),
        "payload_json": canonical_json(payload),
        "ingested_at": datetime.now(UTC),
    }


def append_vessel_observations(table_uri: str, rows: list[dict[str, Any]]) -> tuple[int, int, int]:
    """Append validated bronze vessel observation rows (idempotent by event_id)."""
    return append_rows(
        table_uri,
        rows,
        key_column="event_id",
        table_description=BRONZE_TABLE_DESCRIPTION,
        arrow_schema=VESSEL_BRONZE_SCHEMA,
        table_name=BRONZE_TABLE_NAME,
    )


# ---------------------------------------------------------------------------
# Silver trajectory assembly (pure-Python reference path)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TrackPoint:
    occurred_at: datetime
    longitude: float
    latitude: float
    event_id: str


def _format_ordinate(value: float) -> str:
    text = f"{value:.7f}".rstrip("0").rstrip(".")
    return text if text not in {"-0", ""} else "0"


def point_wkt(point: TrackPoint) -> str:
    return f"POINT ({_format_ordinate(point.longitude)} {_format_ordinate(point.latitude)})"


def line_wkt(points: list[TrackPoint]) -> str:
    ordinates = ", ".join(
        f"{_format_ordinate(point.longitude)} {_format_ordinate(point.latitude)}"
        for point in points
    )
    return f"LINESTRING ({ordinates})"


def _perpendicular_distance(
    point: tuple[float, float], start: tuple[float, float], end: tuple[float, float]
) -> float:
    """Planar perpendicular distance in degree space (track-simplification metric).

    This is the standard planar RDP metric used by the reference path; the
    governed geodesic equivalents (ST_SimplifyPreserveTopology on geography)
    remain with Sedona/PostGIS in the batch deployment.
    """
    dx = end[0] - start[0]
    dy = end[1] - start[1]
    length_sq = dx * dx + dy * dy
    if length_sq == 0.0:
        return math.hypot(point[0] - start[0], point[1] - start[1])
    numerator = abs(dy * point[0] - dx * point[1] + end[0] * start[1] - end[1] * start[0])
    return numerator / math.sqrt(length_sq)


def rdp_simplify(
    coordinates: list[tuple[float, float]], tolerance: float
) -> list[tuple[float, float]]:
    """Ramer-Douglas-Peucker simplification of an ordered coordinate sequence."""
    if tolerance <= 0 or not math.isfinite(tolerance):
        raise ValueError("simplification tolerance must be a positive finite value")
    if len(coordinates) <= 2:
        return list(coordinates)
    keep = [False] * len(coordinates)
    keep[0] = keep[-1] = True
    stack: list[tuple[int, int]] = [(0, len(coordinates) - 1)]
    while stack:
        first, last = stack.pop()
        start, end = coordinates[first], coordinates[last]
        farthest_index = -1
        farthest_distance = -1.0
        for index in range(first + 1, last):
            distance = _perpendicular_distance(coordinates[index], start, end)
            if distance > farthest_distance:
                farthest_distance = distance
                farthest_index = index
        if farthest_distance > tolerance and farthest_index > 0:
            keep[farthest_index] = True
            stack.append((first, farthest_index))
            stack.append((farthest_index, last))
    return [coordinate for coordinate, kept in zip(coordinates, keep, strict=True) if kept]


def _orientation(a: tuple[float, float], b: tuple[float, float], c: tuple[float, float]) -> float:
    return (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])


def _segments_intersect(
    p1: tuple[float, float],
    p2: tuple[float, float],
    q1: tuple[float, float],
    q2: tuple[float, float],
) -> bool:
    """Proper intersection test (shared endpoints of adjacent segments excluded by caller)."""
    d1 = _orientation(q1, q2, p1)
    d2 = _orientation(q1, q2, p2)
    d3 = _orientation(p1, p2, q1)
    d4 = _orientation(p1, p2, q2)
    return ((d1 > 0) != (d2 > 0)) and ((d3 > 0) != (d4 > 0))


def is_simple_line(coordinates: list[tuple[float, float]]) -> bool:
    """Return True when the polyline has no proper self-intersections."""
    count = len(coordinates)
    if count < 4:
        return True
    for i in range(count - 1):
        for j in range(i + 2, count - 1):
            if _segments_intersect(
                coordinates[i], coordinates[i + 1], coordinates[j], coordinates[j + 1]
            ):
                return False
    return True


def simplify_preserving_topology(points: list[TrackPoint], tolerance: float) -> list[TrackPoint]:
    """ST_SimplifyPreserveTopology-equivalent track simplification.

    RDP simplifies the track; when the result would self-intersect the
    tolerance is halved and retried (bounded by ``MAX_SIMPLIFY_RETRIES``),
    and the unsimplified track is the final fallback — a simplified line
    that changes the track's topology is never returned.
    """
    if len(points) <= 2:
        return list(points)
    coordinates = [(point.longitude, point.latitude) for point in points]
    candidate = tolerance
    for _ in range(MAX_SIMPLIFY_RETRIES):
        simplified = rdp_simplify(coordinates, candidate)
        if len(simplified) <= 2 or is_simple_line(simplified):
            indices: list[int] = []
            cursor = 0
            for coordinate in simplified:
                for index in range(cursor, len(coordinates)):
                    if coordinates[index] == coordinate:
                        indices.append(index)
                        cursor = index + 1
                        break
            return [points[index] for index in indices]
        candidate /= 2.0
    return list(points)


def segment_track(
    points: list[TrackPoint], gap_threshold: timedelta = DEFAULT_GAP_THRESHOLD
) -> list[list[TrackPoint]]:
    """Split an ordered track whenever consecutive points exceed the gap threshold."""
    if gap_threshold <= timedelta(0):
        raise ValueError("gap threshold must be positive")
    if not points:
        return []
    segments: list[list[TrackPoint]] = [[points[0]]]
    for previous, current in zip(points, points[1:]):
        if current.occurred_at - previous.occurred_at > gap_threshold:
            segments.append([current])
        else:
            segments[-1].append(current)
    return segments


def _trajectory_id(mmsi: str, first_event_id: str, last_event_id: str) -> str:
    material = f"vessel-trajectory/{mmsi}/{first_event_id}/{last_event_id}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def assemble_vessel_trajectories(
    observations: list[dict[str, Any]],
    gap_threshold: timedelta = DEFAULT_GAP_THRESHOLD,
    simplify_tolerance: float = DEFAULT_SIMPLIFY_TOLERANCE_DEGREES,
) -> list[dict[str, Any]]:
    """Assemble silver trajectory rows from bronze vessel observation rows.

    Tracks are grouped by MMSI, ordered by ``(occurred_at, event_id)``,
    segmented on time gaps greater than two hours (by default), rendered as
    EPSG:4326 WKT lines (ST_MakeLine equivalent) and simplified with a
    topology guard (ST_SimplifyPreserveTopology equivalent). Every row
    carries its source event-id range and quality metrics for lineage.
    """
    tracks: dict[str, list[TrackPoint]] = {}
    for row in observations:
        mmsi = normalize_mmsi(row.get("mmsi"))
        occurred_at = row.get("occurred_at")
        if not isinstance(occurred_at, datetime):
            raise ValueError("bronze vessel observation is missing occurred_at")
        point = TrackPoint(
            occurred_at=occurred_at.astimezone(UTC),
            longitude=float(row["longitude"]),
            latitude=float(row["latitude"]),
            event_id=require_canonical_text(row.get("event_id"), "event_id", 256),
        )
        _validate_coordinates(point.latitude, point.longitude)
        tracks.setdefault(mmsi, []).append(point)

    assembled_at = datetime.now(UTC)
    silver_rows: list[dict[str, Any]] = []
    for mmsi in sorted(tracks):
        ordered = sorted(tracks[mmsi], key=lambda point: (point.occurred_at, point.event_id))
        deduplicated: list[TrackPoint] = []
        track_duplicates = 0
        for point in ordered:
            if (
                deduplicated
                and deduplicated[-1].occurred_at == point.occurred_at
                and deduplicated[-1].longitude == point.longitude
                and deduplicated[-1].latitude == point.latitude
            ):
                track_duplicates += 1
                continue
            deduplicated.append(point)
        for segment_index, segment in enumerate(segment_track(deduplicated, gap_threshold)):
            simplified = simplify_preserving_topology(segment, simplify_tolerance)
            first, last = segment[0], segment[-1]
            if len(segment) == 1:
                geometry_wkt = point_wkt(first)
                simplified_wkt = geometry_wkt
                geometry_type = "Point"
            else:
                geometry_wkt = line_wkt(segment)
                simplified_wkt = (
                    line_wkt(simplified) if len(simplified) > 1 else point_wkt(simplified[0])
                )
                geometry_type = "LineString"
            quality = {
                "point_count": len(segment),
                "simplified_point_count": len(simplified),
                "track_duplicate_points_dropped": track_duplicates,
                "gap_threshold_seconds": int(gap_threshold.total_seconds()),
                "simplify_tolerance_degrees": simplify_tolerance,
                "time_span_seconds": int((last.occurred_at - first.occurred_at).total_seconds()),
            }
            silver_rows.append(
                {
                    "trajectory_id": _trajectory_id(mmsi, first.event_id, last.event_id),
                    "mmsi": mmsi,
                    "segment_index": segment_index,
                    "started_at": first.occurred_at,
                    "ended_at": last.occurred_at,
                    "point_count": len(segment),
                    "geometry_type": geometry_type,
                    "geometry_wkt": geometry_wkt,
                    "simplified_wkt": simplified_wkt,
                    "crs": "EPSG:4326",
                    "source_first_event_id": first.event_id,
                    "source_last_event_id": last.event_id,
                    "quality_json": json.dumps(quality, sort_keys=True),
                    "assembled_at": assembled_at,
                }
            )
    return silver_rows


def read_bronze_observations(bronze_uri: str) -> list[dict[str, Any]]:
    table = DeltaTable(bronze_uri)
    rows: list[dict[str, Any]] = table.to_pyarrow_table().to_pylist()
    return rows


def rebuild_silver_trajectories(
    bronze_uri: str,
    silver_uri: str,
    gap_threshold: timedelta = DEFAULT_GAP_THRESHOLD,
    simplify_tolerance: float = DEFAULT_SIMPLIFY_TOLERANCE_DEGREES,
) -> tuple[int, int]:
    """Atomically rebuild silver.vessel_trajectories from bronze (derived state).

    Returns ``(table_version, row_count)``. Like the gold rollups, the silver
    trajectory table is fully derived from bronze and is overwritten
    atomically, so replays are idempotent.
    """
    validate_table_uri(bronze_uri)
    validate_table_uri(silver_uri)
    observations = read_bronze_observations(bronze_uri)
    if not observations:
        raise ValueError("cannot assemble trajectories from an empty bronze table")
    rows = assemble_vessel_trajectories(observations, gap_threshold, simplify_tolerance)
    write_deltalake(
        silver_uri,
        pa.Table.from_pylist(rows),
        mode="overwrite",
        name=SILVER_TABLE_NAME,
        description=SILVER_TABLE_DESCRIPTION,
    )
    return DeltaTable(silver_uri).version(), len(rows)


# ---------------------------------------------------------------------------
# Gold geofence summaries
# ---------------------------------------------------------------------------


def geofence_polygon_wkt(geofence: Geofence) -> str:
    """Render a validated geofence as WKT (longitude/latitude axis order)."""

    def ring_text(ring: Any) -> str:
        return (
            "("
            + ", ".join(
                f"{_format_ordinate(float(coordinate[0]))} {_format_ordinate(float(coordinate[1]))}"
                for coordinate in ring
            )
            + ")"
        )

    geometry = geofence.geometry
    coordinates = geometry["coordinates"]
    if geometry["type"] == "Polygon":
        return "POLYGON (" + ", ".join(ring_text(ring) for ring in coordinates) + ")"
    if geometry["type"] == "MultiPolygon":
        polygons = []
        for polygon in coordinates:
            polygons.append("(" + ", ".join(ring_text(ring) for ring in polygon) + ")")
        return "MULTIPOLYGON (" + ", ".join(polygons) + ")"
    raise ValueError(f"unsupported geofence geometry type: {geometry['type']}")


def build_geofence_summaries(
    observations: list[dict[str, Any]], geofences: list[Geofence]
) -> list[dict[str, Any]]:
    """Aggregate bronze observations into per-geofence per-MMSI gold summaries."""
    if not geofences:
        raise ValueError("at least one validated geofence is required")
    summarized_at = datetime.now(UTC)
    aggregates: dict[tuple[str, str], dict[str, Any]] = {}
    for row in observations:
        mmsi = normalize_mmsi(row.get("mmsi"))
        occurred_at = row.get("occurred_at")
        if not isinstance(occurred_at, datetime):
            raise ValueError("bronze vessel observation is missing occurred_at")
        occurred_at = occurred_at.astimezone(UTC)
        position = Position(latitude=float(row["latitude"]), longitude=float(row["longitude"]))
        for geofence in geofences:
            if not geofence.contains(position):
                continue
            key = (geofence.identifier, mmsi)
            entry = aggregates.get(key)
            if entry is None:
                aggregates[key] = {
                    "geofence_id": geofence.identifier,
                    "mmsi": mmsi,
                    "observation_count": 1,
                    "first_seen_at": occurred_at,
                    "last_seen_at": occurred_at,
                    "first_event_id": str(row["event_id"]),
                    "last_event_id": str(row["event_id"]),
                    "geometry_wkt": geofence_polygon_wkt(geofence),
                    "summarized_at": summarized_at,
                }
            else:
                entry["observation_count"] += 1
                if occurred_at < entry["first_seen_at"]:
                    entry["first_seen_at"] = occurred_at
                    entry["first_event_id"] = str(row["event_id"])
                if occurred_at > entry["last_seen_at"]:
                    entry["last_seen_at"] = occurred_at
                    entry["last_event_id"] = str(row["event_id"])
    return [aggregates[key] for key in sorted(aggregates)]


def rebuild_gold_geofence_summaries(
    bronze_uri: str, gold_uri: str, geofences: list[Geofence]
) -> tuple[int, int]:
    """Atomically rebuild gold geofence summaries from bronze observations."""
    validate_table_uri(bronze_uri)
    validate_table_uri(gold_uri)
    observations = read_bronze_observations(bronze_uri)
    if not observations:
        raise ValueError("cannot summarize an empty bronze table")
    rows = build_geofence_summaries(observations, geofences)
    if not rows:
        raise ValueError("no bronze observations fall inside the governed geofences")
    write_deltalake(
        gold_uri,
        pa.Table.from_pylist(rows),
        mode="overwrite",
        name=GOLD_TABLE_NAME,
        description=GOLD_TABLE_DESCRIPTION,
    )
    return DeltaTable(gold_uri).version(), len(rows)


__all__ = [
    "DEFAULT_GAP_THRESHOLD",
    "DEFAULT_SIMPLIFY_TOLERANCE_DEGREES",
    "VESSEL_BRONZE_SCHEMA",
    "VESSEL_OBSERVATION_EVENT_TYPE",
    "VESSEL_TOPIC",
    "TrackPoint",
    "append_vessel_observations",
    "assemble_vessel_trajectories",
    "build_geofence_summaries",
    "decode_vessel_observation",
    "decode_vessel_payload",
    "geofence_polygon_wkt",
    "is_simple_line",
    "line_wkt",
    "point_wkt",
    "rdp_simplify",
    "rebuild_gold_geofence_summaries",
    "rebuild_silver_trajectories",
    "segment_track",
    "simplify_preserving_topology",
    "vessel_table_uris",
]
