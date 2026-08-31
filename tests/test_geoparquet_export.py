from __future__ import annotations

import json
import struct
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from blueeconomy_data_platform.geofence import Geofence
from blueeconomy_data_platform.geoparquet_export import (
    ExportLineage,
    build_geoparquet_table,
    export_geoparquet,
    parse_wkt,
    resolve_export_filesystem,
    wkb_encode,
)
from blueeconomy_data_platform.storage import StorageConfigurationError
from blueeconomy_data_platform.vessel_lakehouse import (
    append_vessel_observations,
    rebuild_gold_geofence_summaries,
    rebuild_silver_trajectories,
    vessel_table_uris,
)


def local_env(root: Path) -> dict[str, str]:
    return {
        "BLUEECONOMY_STORAGE_BACKEND": "local-gated",
        "BLUEECONOMY_ALLOW_LOCAL_STORAGE": "true",
        "BLUEECONOMY_LOCAL_LAKEHOUSE_ROOT": str(root),
    }


def test_parse_wkt_all_supported_types() -> None:
    assert parse_wkt("POINT (-70.5 42.1)") == ("Point", (-70.5, 42.1))
    geometry_type, coordinates = parse_wkt("LINESTRING (0 0, 1 1, 2 0.5)")
    assert geometry_type == "LineString"
    assert len(coordinates) == 3
    geometry_type, rings = parse_wkt("POLYGON ((0 0, 1 0, 1 1, 0 0))")
    assert geometry_type == "Polygon"
    assert rings[0][0] == rings[0][-1]
    geometry_type, polygons = parse_wkt(
        "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)), ((2 2, 3 2, 3 3, 2 2)))"
    )
    assert geometry_type == "MultiPolygon"
    assert len(polygons) == 2
    for bad in (
        "",
        "CIRCULARSTRING (0 0, 1 1)",
        "POINT (0)",
        "LINESTRING (0 0,)",
        "POINT (0 0) extra",
    ):
        with pytest.raises(ValueError):
            parse_wkt(bad)


def test_wkb_encode_point_little_endian() -> None:
    encoded = wkb_encode("Point", (-70.5, 42.1))
    assert encoded[0] == 1  # little endian
    assert struct.unpack("<I", encoded[1:5])[0] == 1  # WKB point type
    assert struct.unpack("<dd", encoded[5:21]) == (-70.5, 42.1)


def test_wkb_encode_linestring_and_polygon_structure() -> None:
    line = wkb_encode("LineString", [(0.0, 0.0), (1.0, 1.0)])
    assert struct.unpack("<I", line[1:5])[0] == 2
    assert struct.unpack("<I", line[5:9])[0] == 2  # two positions
    polygon = wkb_encode("Polygon", [[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)]])
    assert struct.unpack("<I", polygon[1:5])[0] == 3
    assert struct.unpack("<I", polygon[5:9])[0] == 1  # one ring
    multi = wkb_encode(
        "MultiPolygon",
        [
            [[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)]],
            [[(2.0, 2.0), (3.0, 2.0), (3.0, 3.0), (2.0, 2.0)]],
        ],
    )
    assert struct.unpack("<I", multi[1:5])[0] == 6
    assert struct.unpack("<I", multi[5:9])[0] == 2  # two polygons


def lineage(row_count: int = 2) -> ExportLineage:
    return ExportLineage(
        input_table_reference_sha256="a" * 64,
        input_table_version=3,
        input_event_id_range=("evt-a", "evt-z"),
        row_count=row_count,
        quality_metrics={"row_count": row_count},
        exported_at="2026-08-12T12:00:00+00:00",
    )


def test_build_geoparquet_table_metadata() -> None:
    rows = [
        {"trajectory_id": "t1", "mmsi": "366123456", "geometry_wkt": "LINESTRING (0 0, 1 1)"},
        {"trajectory_id": "t2", "mmsi": "244123456", "geometry_wkt": "POINT (2 3)"},
    ]
    table = build_geoparquet_table(rows, "geometry_wkt", lineage())
    metadata = table.schema.metadata
    geo = json.loads(metadata[b"geo"])
    assert geo["version"] == "1.0.0"
    assert geo["primary_column"] == "geometry"
    column = geo["columns"]["geometry"]
    assert column["encoding"] == "WKB"
    assert column["geometry_types"] == ["LineString", "Point"]
    assert column["crs"] is None  # GeoParquet default OGC:CRS84 (WGS84 lon/lat)
    assert column["bbox"] == [0.0, 0.0, 2.0, 3.0]
    embedded = json.loads(metadata[b"blueeconomy.lineage"])
    assert embedded["input_event_id_range"] == ["evt-a", "evt-z"]
    assert table.schema.field("geometry").type == "binary"
    assert "geometry_wkt" not in table.schema.names
    with pytest.raises(ValueError, match="missing"):
        build_geoparquet_table([{"trajectory_id": "t3"}], "geometry_wkt", lineage(1))


def observation(event_id: str, occurred: datetime, lat: float, lon: float) -> dict[str, object]:
    return {
        "event_id": event_id,
        "mmsi": "366123456",
        "occurred_at": occurred,
        "recorded_at": occurred,
        "latitude": lat,
        "longitude": lon,
        "speed_knots": 10.0,
        "heading_degrees": 90.0,
        "decode_source": "payload",
        "producer": "geo-service",
        "source_record_reference": f"ref-{event_id}",
        "payload_json": json.dumps({"mmsi": "366123456"}),
        "ingested_at": occurred,
    }


def seed_silver(tmp_path: Path) -> dict[str, str]:
    uris = vessel_table_uris(str(tmp_path / "platform"))
    base = datetime(2026, 8, 12, 10, 0, tzinfo=UTC)
    rows = [
        observation(
            f"evt-{index:012d}",
            base + timedelta(minutes=10 * index),
            42.1 + 0.01 * index,
            -70.5 + 0.01 * index,
        )
        for index in range(3)
    ]
    append_vessel_observations(uris["bronze"], rows)
    rebuild_silver_trajectories(uris["bronze"], uris["silver"])
    return uris


def test_export_silver_trajectories_geoparquet_local(tmp_path: Path) -> None:
    uris = seed_silver(tmp_path)
    target = str(tmp_path / "exports" / "silver" / "vessel_trajectories.parquet")
    result = export_geoparquet(uris["silver"], "geometry_wkt", target, env=local_env(tmp_path))
    assert result.row_count == 1
    assert result.input_event_id_range is not None
    first, last = result.input_event_id_range
    assert first <= last and first.startswith("evt-")
    assert result.quality_metrics["distinct_mmsi"] == 1
    assert result.quality_metrics["total_track_points"] == 3

    table = pq.read_table(target)
    geo = json.loads(table.schema.metadata[b"geo"])
    assert geo["columns"]["geometry"]["geometry_types"] == ["LineString"]
    embedded = json.loads(table.schema.metadata[b"blueeconomy.lineage"])
    assert embedded["schema_version"] == "blueeconomy.lakehouse.geoparquet-lineage.v1"
    assert embedded["row_count"] == 1
    geometry = table.column("geometry").to_pylist()[0]
    assert geometry[0] == 1 and struct.unpack("<I", geometry[1:5])[0] == 2  # WKB LineString
    sidecar = json.loads(
        (tmp_path / "exports" / "silver" / "vessel_trajectories.parquet.lineage.json").read_text()
    )
    assert sidecar["input_table_version"] == result.input_table_version
    assert sidecar["quality_metrics"]["simplification_ratio"] <= 1.0


def test_export_gold_geofence_summaries_geoparquet_local(tmp_path: Path) -> None:
    uris = seed_silver(tmp_path)
    geofence = Geofence(
        identifier="test-zone",
        geometry={
            "type": "Polygon",
            "coordinates": [
                [[-71.0, 42.0], [-70.0, 42.0], [-70.0, 42.5], [-71.0, 42.5], [-71.0, 42.0]]
            ],
        },
    )
    rebuild_gold_geofence_summaries(uris["bronze"], uris["gold"], [geofence])
    target = str(tmp_path / "exports" / "gold" / "geofence_summaries.parquet")
    result = export_geoparquet(uris["gold"], "geometry_wkt", target, env=local_env(tmp_path))
    assert result.row_count == 1
    assert result.quality_metrics["geofence_observations"] == 3
    table = pq.read_table(target)
    geo = json.loads(table.schema.metadata[b"geo"])
    assert geo["columns"]["geometry"]["geometry_types"] == ["Polygon"]
    assert table.column("geofence_id").to_pylist() == ["test-zone"]


def test_export_fails_closed_without_backend(tmp_path: Path) -> None:
    uris = seed_silver(tmp_path)
    target = str(tmp_path / "exports" / "x.parquet")
    with pytest.raises(StorageConfigurationError):
        export_geoparquet(uris["silver"], "geometry_wkt", target, env={})
    with pytest.raises(StorageConfigurationError, match="local"):
        export_geoparquet(
            uris["silver"],
            "geometry_wkt",
            target,
            env={
                "BLUEECONOMY_STORAGE_BACKEND": "local-gated",
                "BLUEECONOMY_LOCAL_LAKEHOUSE_ROOT": str(tmp_path),
            },
        )


def test_local_export_must_stay_under_gated_root(tmp_path: Path) -> None:
    root = tmp_path / "lakehouse"
    root.mkdir()
    with pytest.raises(StorageConfigurationError, match="BLUEECONOMY_LOCAL_LAKEHOUSE_ROOT"):
        resolve_export_filesystem(str(tmp_path / "outside" / "x.parquet"), env=local_env(root))
    filesystem, path = resolve_export_filesystem(
        str(root / "exports" / "x.parquet"), env=local_env(root)
    )
    assert path.endswith("exports/x.parquet")


def test_s3_target_resolution_uses_storage_contract() -> None:
    env = {
        "BLUEECONOMY_STORAGE_BACKEND": "s3",
        "BLUEECONOMY_S3_BUCKET": "blueeconomy-lakehouse",
        "BLUEECONOMY_S3_REGION": "us-east-1",
        "BLUEECONOMY_S3_SECURE": "true",
    }
    filesystem, path = resolve_export_filesystem(
        "s3://blueeconomy-lakehouse/exports/silver/vessel_trajectories.parquet", env=env
    )
    assert path == "blueeconomy-lakehouse/exports/silver/vessel_trajectories.parquet"
    with pytest.raises(StorageConfigurationError):
        resolve_export_filesystem("s3://blueeconomy-lakehouse/../escape", env=env)
    with pytest.raises(StorageConfigurationError):
        resolve_export_filesystem("https://example.com/x.parquet", env=env)
