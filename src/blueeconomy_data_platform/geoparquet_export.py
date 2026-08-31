"""GeoParquet export of governed geo tables to object storage.

Exports ``silver.vessel_trajectories`` and gold geofence summaries as
GeoParquet 1.0 files (WKB geometry encoding, OGC:CRS84 — the WGS84
longitude/latitude default whose horizontal datum matches the EPSG:4326
pipeline CRS) through the :mod:`blueeconomy_data_platform.storage` backend
abstraction (``s3``, ``adls`` or the explicitly gated ``local-gated``
backend). Every export carries lineage metadata — input table reference,
input event-id range and quality metrics — embedded in the Parquet schema
metadata and mirrored to a ``.lineage.json`` sidecar object.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import struct
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from blueeconomy_data_platform.storage import (
    BACKEND_ADLS,
    BACKEND_LOCAL,
    BACKEND_S3,
    ENV_BACKEND,
    BACKEND_ALIASES,
    StorageConfigurationError,
    load_local_root,
    load_s3_config,
    validate_s3_uri,
)

GEOPARQUET_VERSION = "1.0.0"
GEOMETRY_COLUMN = "geometry"
LINEAGE_METADATA_KEY = b"blueeconomy.lineage"
LINEAGE_SCHEMA_VERSION = "blueeconomy.lakehouse.geoparquet-lineage.v1"
MAX_EXPORT_ROWS = 1_000_000

_EVENT_ID_COLUMNS = (
    "event_id",
    "source_first_event_id",
    "source_last_event_id",
    "first_event_id",
    "last_event_id",
)


# ---------------------------------------------------------------------------
# Minimal WKT (POINT/LINESTRING/POLYGON/MULTIPOLYGON) parsing and WKB encoding
# ---------------------------------------------------------------------------

_TOKEN_PATTERN = re.compile(r"[A-Za-z]+|-?(?:\d+\.?\d*(?:[eE][+-]?\d+)?|\.\d+)|[(),]")


def _tokenize_wkt(wkt: str) -> list[str]:
    tokens = _TOKEN_PATTERN.findall(wkt)
    if not tokens:
        raise ValueError("geometry WKT is empty or malformed")
    return tokens


class _WktParser:
    def __init__(self, wkt: str) -> None:
        self._tokens = _tokenize_wkt(wkt)
        self._index = 0

    def _peek(self) -> str:
        if self._index >= len(self._tokens):
            raise ValueError("unexpected end of WKT")
        return self._tokens[self._index]

    def _next(self) -> str:
        token = self._peek()
        self._index += 1
        return token

    def _expect(self, expected: str) -> None:
        token = self._next()
        if token != expected:
            raise ValueError(f"expected {expected!r} in WKT, found {token!r}")

    def _number(self) -> float:
        token = self._next()
        try:
            value = float(token)
        except ValueError as error:
            raise ValueError(f"malformed ordinate {token!r} in WKT") from error
        return value

    def _position(self) -> tuple[float, float]:
        x = self._number()
        y = self._number()
        return (x, y)

    def _position_list(self) -> list[tuple[float, float]]:
        self._expect("(")
        positions = [self._position()]
        while self._peek() == ",":
            self._next()
            positions.append(self._position())
        self._expect(")")
        return positions

    def _polygon(self) -> list[list[tuple[float, float]]]:
        self._expect("(")
        rings = [self._position_list()]
        while self._peek() == ",":
            self._next()
            rings.append(self._position_list())
        self._expect(")")
        return rings

    def parse(self) -> tuple[str, Any]:
        keyword = self._next().upper()
        if keyword == "POINT":
            return "Point", self._position_list()[0]
        if keyword == "LINESTRING":
            return "LineString", self._position_list()
        if keyword == "POLYGON":
            return "Polygon", self._polygon()
        if keyword == "MULTIPOLYGON":
            self._expect("(")
            polygons = [self._polygon()]
            while self._peek() == ",":
                self._next()
                polygons.append(self._polygon())
            self._expect(")")
            return "MultiPolygon", polygons
        raise ValueError(f"unsupported WKT geometry type {keyword!r}")

    def done(self) -> None:
        if self._index != len(self._tokens):
            raise ValueError("trailing tokens after WKT geometry")


def parse_wkt(wkt: str) -> tuple[str, Any]:
    parser = _WktParser(wkt)
    geometry = parser.parse()
    parser.done()
    return geometry


_WKB_TYPE_CODES = {"Point": 1, "LineString": 2, "Polygon": 3, "MultiPolygon": 6}


def _wkb_position(position: tuple[float, float]) -> bytes:
    return struct.pack("<dd", position[0], position[1])


def _wkb_linestring_body(positions: list[tuple[float, float]]) -> bytes:
    return struct.pack("<I", len(positions)) + b"".join(
        _wkb_position(position) for position in positions
    )


def _wkb_polygon_body(rings: list[list[tuple[float, float]]]) -> bytes:
    return struct.pack("<I", len(rings)) + b"".join(_wkb_linestring_body(ring) for ring in rings)


def wkb_encode(geometry_type: str, coordinates: Any) -> bytes:
    """Encode a parsed 2D geometry as little-endian WKB."""
    type_code = _WKB_TYPE_CODES[geometry_type]
    header = struct.pack("<BI", 1, type_code)
    if geometry_type == "Point":
        return header + _wkb_position(coordinates)
    if geometry_type == "LineString":
        return header + _wkb_linestring_body(coordinates)
    if geometry_type == "Polygon":
        return header + _wkb_polygon_body(coordinates)
    return (
        header
        + struct.pack("<I", len(coordinates))
        + b"".join(wkb_encode("Polygon", polygon) for polygon in coordinates)
    )


def _iter_positions(geometry_type: str, coordinates: Any) -> Any:
    if geometry_type == "Point":
        yield coordinates
    elif geometry_type == "LineString":
        yield from coordinates
    elif geometry_type == "Polygon":
        for ring in coordinates:
            yield from ring
    else:
        for polygon in coordinates:
            for ring in polygon:
                yield from ring


# ---------------------------------------------------------------------------
# Lineage and GeoParquet table assembly
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ExportLineage:
    """Lineage metadata embedded in every exported GeoParquet file."""

    input_table_reference_sha256: str
    input_table_version: int
    input_event_id_range: tuple[str, str] | None
    row_count: int
    quality_metrics: Mapping[str, Any]
    exported_at: str

    def as_json(self) -> str:
        document = {
            "schema_version": LINEAGE_SCHEMA_VERSION,
            "input_table_reference_sha256": self.input_table_reference_sha256,
            "input_table_version": self.input_table_version,
            "input_event_id_range": (
                list(self.input_event_id_range) if self.input_event_id_range else None
            ),
            "row_count": self.row_count,
            "quality_metrics": dict(self.quality_metrics),
            "exported_at": self.exported_at,
        }
        return json.dumps(document, sort_keys=True)


def _event_id_range(rows: list[dict[str, Any]]) -> tuple[str, str] | None:
    identifiers: list[str] = []
    for row in rows:
        for column in _EVENT_ID_COLUMNS:
            value = row.get(column)
            if isinstance(value, str) and value:
                identifiers.append(value)
    if not identifiers:
        return None
    return (min(identifiers), max(identifiers))


def _quality_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    metrics: dict[str, Any] = {"row_count": len(rows)}
    identifiers = {str(row["mmsi"]) for row in rows if isinstance(row.get("mmsi"), str)}
    if identifiers:
        metrics["distinct_mmsi"] = len(identifiers)
    total_points = 0
    simplified_points = 0
    quality_rows = 0
    for row in rows:
        quality_json = row.get("quality_json")
        if not isinstance(quality_json, str):
            continue
        quality = json.loads(quality_json)
        total_points += int(quality.get("point_count", 0))
        simplified_points += int(quality.get("simplified_point_count", 0))
        quality_rows += 1
    if quality_rows:
        metrics["segments_with_quality"] = quality_rows
        metrics["total_track_points"] = total_points
        metrics["simplified_track_points"] = simplified_points
        metrics["simplification_ratio"] = (
            round(simplified_points / total_points, 6) if total_points else 1.0
        )
    observations = sum(int(row.get("observation_count", 0)) for row in rows)
    if observations:
        metrics["geofence_observations"] = observations
    return metrics


def build_geoparquet_table(
    rows: list[dict[str, Any]], wkt_column: str, lineage: ExportLineage
) -> pa.Table:
    """Build the GeoParquet 1.0 Arrow table (WKB geometry + metadata)."""
    if not 1 <= len(rows) <= MAX_EXPORT_ROWS:
        raise ValueError(f"export requires between 1 and {MAX_EXPORT_ROWS} rows")
    geometry_types: set[str] = set()
    wkb_values: list[bytes] = []
    min_x = min_y = float("inf")
    max_x = max_y = float("-inf")
    retained_columns: dict[str, list[Any]] = {}
    for row in rows:
        wkt = row.get(wkt_column)
        if not isinstance(wkt, str) or not wkt:
            raise ValueError(f"row is missing its {wkt_column} geometry text")
        geometry_type, coordinates = parse_wkt(wkt)
        geometry_types.add(geometry_type)
        wkb_values.append(wkb_encode(geometry_type, coordinates))
        for x, y in _iter_positions(geometry_type, coordinates):
            min_x, min_y = min(min_x, x), min(min_y, y)
            max_x, max_y = max(max_x, x), max(max_y, y)
        for column, value in row.items():
            if column == wkt_column:
                continue
            retained_columns.setdefault(column, []).append(value)

    arrays: list[pa.Array] = []
    fields: list[pa.Field] = []
    for column in sorted(retained_columns):
        values = retained_columns[column]
        if len(values) != len(rows):
            raise ValueError(f"column {column!r} is not present in every exported row")
        arrays.append(pa.array(values))
        fields.append(pa.field(column, arrays[-1].type))
    arrays.append(pa.array(wkb_values, type=pa.binary()))
    fields.append(pa.field(GEOMETRY_COLUMN, pa.binary(), nullable=False))

    geo_metadata = {
        "version": GEOPARQUET_VERSION,
        "primary_column": GEOMETRY_COLUMN,
        "columns": {
            GEOMETRY_COLUMN: {
                "encoding": "WKB",
                "geometry_types": sorted(geometry_types),
                # null crs => GeoParquet default OGC:CRS84 (WGS84,
                # longitude/latitude axis order), the horizontal datum of the
                # EPSG:4326 pipeline CRS.
                "crs": None,
                "edges": "planar",
                "bbox": [min_x, min_y, max_x, max_y],
            }
        },
    }
    schema = pa.schema(fields).with_metadata(
        {
            b"geo": json.dumps(geo_metadata, sort_keys=True).encode("utf-8"),
            LINEAGE_METADATA_KEY: lineage.as_json().encode("utf-8"),
        }
    )
    return pa.Table.from_arrays(arrays, schema=schema)


# ---------------------------------------------------------------------------
# Backend-aware object writing (storage.py abstraction)
# ---------------------------------------------------------------------------


def resolve_export_filesystem(
    target_uri: str, env: Mapping[str, str] | None = None
) -> tuple[pafs.FileSystem, str]:
    """Resolve a target URI to a pyarrow filesystem and object path.

    The backend comes from the same environment contract as
    :mod:`blueeconomy_data_platform.storage` and fails closed identically;
    credentials are resolved by the pyarrow filesystem from the ambient
    environment, never from the URI.
    """
    environment = os.environ if env is None else env
    raw_backend = environment.get(ENV_BACKEND, "")
    backend = BACKEND_ALIASES.get(raw_backend, raw_backend)
    if not backend:
        raise StorageConfigurationError(
            f"{ENV_BACKEND} is not set; refusing to assume a storage backend"
        )
    if backend == BACKEND_S3:
        config = load_s3_config(environment)
        bucket, key = validate_s3_uri(target_uri)
        filesystem = pafs.S3FileSystem(
            region=config.region,
            endpoint_override=config.endpoint_url,
            scheme="https" if config.secure else "http",
        )
        return filesystem, f"{bucket}/{key}"
    if backend == BACKEND_ADLS:
        parsed = urlsplit(target_uri)
        if parsed.scheme != "abfs":
            raise StorageConfigurationError("ADLS export targets must use abfs:// URIs")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise StorageConfigurationError(
                "ADLS URIs must not contain credentials, query parameters or fragments"
            )
        match = re.fullmatch(r"([a-z0-9-]+)@([a-z0-9]{3,24})\..+", parsed.netloc)
        if match is None:
            raise StorageConfigurationError(
                "ADLS export target must be abfs://<filesystem>@<account>.<suffix>/<key>"
            )
        container, account = match.group(1), match.group(2)
        path = parsed.path.lstrip("/")
        if not path or ".." in path.split("/"):
            raise StorageConfigurationError("ADLS export target must name an object key")
        return pafs.AzureFileSystem(account), f"{container}/{path}"
    if backend == BACKEND_LOCAL:
        root = load_local_root(environment)
        if not target_uri.startswith("/"):
            raise StorageConfigurationError("local export targets must be absolute paths")
        resolved = os.path.realpath(target_uri)
        if os.path.commonpath([os.path.realpath(root), resolved]) != os.path.realpath(root):
            raise StorageConfigurationError(
                "local export targets must stay under BLUEECONOMY_LOCAL_LAKEHOUSE_ROOT"
            )
        return pafs.LocalFileSystem(), resolved
    raise StorageConfigurationError(
        f"{ENV_BACKEND}={raw_backend!r} is not a supported backend for GeoParquet export"
    )


def export_geoparquet(
    source_table_uri: str,
    wkt_column: str,
    target_uri: str,
    env: Mapping[str, str] | None = None,
) -> ExportLineage:
    """Export a governed Delta table to GeoParquet with lineage metadata."""
    try:
        source = DeltaTable(source_table_uri)
    except TableNotFoundError as error:
        raise ValueError(f"export source table {source_table_uri!r} does not exist") from error
    rows = source.to_pyarrow_table().to_pylist()
    lineage = ExportLineage(
        input_table_reference_sha256=hashlib.sha256(source_table_uri.encode("utf-8")).hexdigest(),
        input_table_version=source.version(),
        input_event_id_range=_event_id_range(rows),
        row_count=len(rows),
        quality_metrics=_quality_metrics(rows),
        exported_at=datetime.now(UTC).isoformat(),
    )
    table = build_geoparquet_table(rows, wkt_column, lineage)
    filesystem, path = resolve_export_filesystem(target_uri, env)
    parent = path.rsplit("/", 1)[0] if "/" in path else path
    filesystem.create_dir(parent, recursive=True)
    with filesystem.open_output_stream(path) as stream:
        pq.write_table(table, stream, compression="zstd")
    sidecar = path + ".lineage.json"
    with filesystem.open_output_stream(sidecar) as stream:
        stream.write(lineage.as_json().encode("utf-8") + b"\n")
    return lineage


__all__ = [
    "ExportLineage",
    "build_geoparquet_table",
    "export_geoparquet",
    "parse_wkt",
    "resolve_export_filesystem",
    "wkb_encode",
]
