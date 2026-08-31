from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from deltalake import DeltaTable
from jsonschema import Draft202012Validator
from pyais import encode_dict

from blueeconomy_data_platform.geofence import Geofence
from blueeconomy_data_platform.ingest import load_schema
from blueeconomy_data_platform.segregation import BoundaryViolationError
from blueeconomy_data_platform.signature_verification import SignatureVerificationError
from blueeconomy_data_platform.vessel_lakehouse import (
    TrackPoint,
    append_vessel_observations,
    assemble_vessel_trajectories,
    build_geofence_summaries,
    decode_vessel_observation,
    decode_vessel_payload,
    is_simple_line,
    rdp_simplify,
    rebuild_gold_geofence_summaries,
    rebuild_silver_trajectories,
    segment_track,
    simplify_preserving_topology,
    vessel_table_uris,
)
from signing_helpers import sign_envelope, single_use_verifier

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "event-envelope.schema.json"
PRODUCER = "geo-service"
KID = f"{PRODUCER}-0"
MMSI = "366123456"


def vessel_envelope(
    event_id: str,
    occurred_at: str,
    payload: dict[str, object] | None = None,
    kid: str = KID,
) -> dict[str, object]:
    resource: dict[str, object] = payload or {
        "mmsi": MMSI,
        "latitude": 42.1,
        "longitude": -70.5,
        "speedKnots": 12.3,
        "headingDegrees": 90.0,
    }
    envelope: dict[str, object] = {
        "envelopeVersion": "1.0",
        "eventId": event_id,
        "eventType": "vessels.observation.v1",
        "occurredAt": occurred_at,
        "producer": PRODUCER,
        "correlationId": f"corr-{event_id}",
        "fhir": {
            "resourceType": "Bundle",
            "type": "message",
            "entry": [{"resource": resource}],
        },
        "provenance": {
            "principalId": "svc-geo-service",
            "principalRole": "vessel-observation-producer",
            "signature": "f" * 64,
            "ledgerCommitHash": "e" * 64,
        },
        "classification": "INTERNAL",
    }
    return sign_envelope(envelope, kid)


def validator() -> Draft202012Validator:
    return load_schema(SCHEMA_PATH)


def test_decode_signed_vessel_observation() -> None:
    envelope = vessel_envelope("11111111-1111-4111-8111-111111111111", "2026-08-12T10:00:00Z")
    verifier = single_use_verifier(KID)
    row = decode_vessel_observation(envelope, validator(), verifier)
    assert row["event_id"] == "11111111-1111-4111-8111-111111111111"
    assert row["mmsi"] == MMSI
    assert row["latitude"] == pytest.approx(42.1)
    assert row["longitude"] == pytest.approx(-70.5)
    assert row["speed_knots"] == pytest.approx(12.3)
    assert row["decode_source"] == "payload"
    assert verifier.metrics.verified == 1


def test_decode_rejects_invalid_signature_fail_closed() -> None:
    envelope = vessel_envelope("22222222-2222-4222-8222-222222222222", "2026-08-12T10:00:00Z")
    assert isinstance(envelope["provenance"], dict)
    envelope["occurredAt"] = "2026-08-12T10:05:00Z"  # tamper after signing
    verifier = single_use_verifier(KID)
    with pytest.raises(SignatureVerificationError):
        decode_vessel_observation(envelope, validator(), verifier)
    assert verifier.metrics.verified == 0
    assert sum(verifier.metrics.rejected.values()) == 1


def test_decode_rejects_wrong_event_type_and_unknown_kid() -> None:
    envelope = vessel_envelope("33333333-3333-4333-8333-333333333333", "2026-08-12T10:00:00Z")
    envelope["eventType"] = "ports.gate.scan.v1"
    envelope = sign_envelope(envelope, KID)
    verifier = single_use_verifier(KID)
    with pytest.raises(ValueError, match="vessels.observation.v1"):
        decode_vessel_observation(envelope, validator(), verifier)
    other = vessel_envelope(
        "44444444-4444-4444-8444-444444444444", "2026-08-12T10:00:00Z", kid="unknown-producer-0"
    )
    with pytest.raises(SignatureVerificationError, match="unknown-kid"):
        decode_vessel_observation(other, validator(), verifier)


def test_decode_raw_nmea_payload_via_pyais() -> None:
    sentences = list(
        encode_dict(
            {
                "msg_type": 1,
                "repeat": 0,
                "mmsi": int(MMSI),
                "status": 0,
                "turn": 0,
                "speed": 11.7,
                "accuracy": 0,
                "lon": -70.5,
                "lat": 42.1,
                "course": 88.0,
                "heading": 87,
                "second": 20,
                "maneuver": 0,
                "raim": False,
                "radio": 0,
            },
            radio_channel="A",
            talker_id="AI",
            sentence_type="VDM",
        )
    )
    payload = {
        "mmsi": MMSI,
        "latitude": 42.1,
        "longitude": -70.5,
        "nmeaSentences": sentences,
    }
    decoded = decode_vessel_payload(payload)
    assert decoded["decode_source"] == "ais-nmea"
    assert decoded["latitude"] == pytest.approx(42.1, abs=1e-4)
    assert decoded["speed_knots"] == pytest.approx(11.7, abs=1e-4)
    assert decoded["heading_degrees"] == pytest.approx(87.0, abs=1e-4)


def test_payload_ais_mismatch_fails_closed() -> None:
    sentences = list(
        encode_dict(
            {
                "msg_type": 1,
                "repeat": 0,
                "mmsi": int(MMSI),
                "status": 0,
                "turn": 0,
                "speed": 1.0,
                "accuracy": 0,
                "lon": -70.5,
                "lat": 42.1,
                "course": 10.0,
                "heading": 10,
                "second": 20,
                "maneuver": 0,
                "raim": False,
                "radio": 0,
            },
            radio_channel="A",
            talker_id="AI",
            sentence_type="VDM",
        )
    )
    with pytest.raises(ValueError, match="disagree"):
        decode_vessel_payload(
            {"mmsi": MMSI, "latitude": 10.0, "longitude": 10.0, "nmeaSentences": sentences}
        )
    with pytest.raises(ValueError, match="mmsi"):
        decode_vessel_payload(
            {"mmsi": "111222333", "latitude": 42.1, "longitude": -70.5, "nmeaSentences": sentences}
        )


def test_decode_payload_validation_fail_closed() -> None:
    with pytest.raises(ValueError, match="mmsi"):
        decode_vessel_payload({"mmsi": "12345", "latitude": 1.0, "longitude": 1.0})
    with pytest.raises(ValueError, match="latitude"):
        decode_vessel_payload({"mmsi": MMSI, "latitude": 95.0, "longitude": 1.0})
    with pytest.raises(ValueError, match="speedKnots"):
        decode_vessel_payload({"mmsi": MMSI, "latitude": 1.0, "longitude": 1.0, "speedKnots": -1})
    with pytest.raises(ValueError, match="headingDegrees"):
        decode_vessel_payload(
            {"mmsi": MMSI, "latitude": 1.0, "longitude": 1.0, "headingDegrees": 360.0}
        )


def observation(
    event_id: str, occurred_at: datetime, lat: float, lon: float, mmsi: str = MMSI
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "mmsi": mmsi,
        "occurred_at": occurred_at,
        "latitude": lat,
        "longitude": lon,
    }


def test_segment_track_splits_on_two_hour_gaps() -> None:
    base = datetime(2026, 8, 12, 10, 0, tzinfo=UTC)
    points = [
        TrackPoint(base, -70.5, 42.1, "e1"),
        TrackPoint(base + timedelta(minutes=30), -70.4, 42.2, "e2"),
        TrackPoint(base + timedelta(hours=2, minutes=40), -70.3, 42.3, "e3"),
        TrackPoint(base + timedelta(hours=3, minutes=10), -70.2, 42.4, "e4"),
    ]
    segments = segment_track(points)
    assert [[point.event_id for point in segment] for segment in segments] == [
        ["e1", "e2"],
        ["e3", "e4"],
    ]
    assert len(segment_track(points, timedelta(hours=3))) == 1


def test_rdp_simplify_keeps_endpoints_and_reduces_noise() -> None:
    coordinates = [(0.0, 0.0), (0.5, 0.00001), (1.0, 0.0), (1.5, 0.5), (2.0, 1.0)]
    simplified = rdp_simplify(coordinates, 0.001)
    assert simplified[0] == coordinates[0]
    assert simplified[-1] == coordinates[-1]
    assert len(simplified) < len(coordinates)
    with pytest.raises(ValueError):
        rdp_simplify(coordinates, 0.0)


def test_simplify_preserving_topology_never_self_intersects() -> None:
    # A hairpin track whose aggressive simplification would self-cross.
    base = datetime(2026, 8, 12, tzinfo=UTC)
    coordinates = [
        (0.0, 0.0),
        (1.0, 0.0),
        (1.0, 1.0),
        (0.0, 1.0),
        (0.0, 0.0009),
        (-1.0, 0.0009),
        (-1.0, 2.0),
        (2.0, 2.0),
    ]
    points = [
        TrackPoint(base + timedelta(minutes=index), lon, lat, f"e{index}")
        for index, (lon, lat) in enumerate(coordinates)
    ]
    simplified = simplify_preserving_topology(points, 0.05)
    simplified_coordinates = [(point.longitude, point.latitude) for point in simplified]
    assert is_simple_line(simplified_coordinates)
    assert simplified[0].event_id == "e0"
    assert simplified[-1].event_id == f"e{len(coordinates) - 1}"


def test_is_simple_line_detects_self_intersection() -> None:
    assert not is_simple_line([(0.0, 0.0), (1.0, 1.0), (1.0, 0.0), (0.0, 1.0)])
    assert is_simple_line([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (2.0, 1.0)])


def test_assemble_trajectories_orders_segments_and_simplifies() -> None:
    base = datetime(2026, 8, 12, 10, 0, tzinfo=UTC)
    observations = [
        # Deliberately unordered input; assembly must order per mmsi.
        observation("e3", base + timedelta(minutes=20), 42.12, -70.48),
        observation("e1", base, 42.10, -70.50),
        observation("e5", base + timedelta(hours=3), 42.30, -70.30),
        observation("e2", base + timedelta(minutes=10), 42.11, -70.49),
        observation("e4", base + timedelta(minutes=30), 42.13, -70.47),
        observation("x1", base, 6.45, 3.40, mmsi="244123456"),
    ]
    rows = assemble_vessel_trajectories(observations)
    assert len(rows) == 3  # two segments for MMSI + one singleton track
    first = next(row for row in rows if row["mmsi"] == MMSI and row["segment_index"] == 0)
    assert first["geometry_wkt"].startswith("LINESTRING (")
    assert first["source_first_event_id"] == "e1"
    assert first["source_last_event_id"] == "e4"
    assert first["point_count"] == 4
    assert first["crs"] == "EPSG:4326"
    assert first["simplified_wkt"].startswith("LINESTRING (")
    quality = json.loads(first["quality_json"])
    assert quality["point_count"] == 4
    assert quality["gap_threshold_seconds"] == 7200
    second = next(row for row in rows if row["mmsi"] == MMSI and row["segment_index"] == 1)
    assert second["geometry_type"] == "Point"
    assert second["geometry_wkt"].startswith("POINT (")
    singleton = next(row for row in rows if row["mmsi"] == "244123456")
    assert singleton["point_count"] == 1


def test_append_vessel_observations_is_idempotent(tmp_path: Path) -> None:
    table_uri = str(tmp_path / "platform" / "bronze" / "vessel_observations")
    envelope = vessel_envelope("55555555-5555-4555-8555-555555555555", "2026-08-12T10:00:00Z")
    verifier = single_use_verifier(KID)
    row = decode_vessel_observation(envelope, validator(), verifier)
    version, written, present = append_vessel_observations(table_uri, [row])
    assert (written, present) == (1, 0)
    assert DeltaTable(table_uri).metadata().configuration["delta.appendOnly"] == "true"
    replayed = decode_vessel_observation(envelope, validator(), single_use_verifier(KID))
    _, written2, present2 = append_vessel_observations(table_uri, [replayed])
    assert (written2, present2) == (0, 1)
    conflicting = dict(replayed)
    conflicting["latitude"] = 1.0
    with pytest.raises(ValueError, match="conflicts with retained immutable content"):
        append_vessel_observations(table_uri, [conflicting])


def test_rebuild_silver_and_gold_from_bronze(tmp_path: Path) -> None:
    uris = vessel_table_uris(str(tmp_path / "platform"))
    base = datetime(2026, 8, 12, 10, 0, tzinfo=UTC)
    verifier = single_use_verifier(KID)
    rows = []
    positions = [(42.10, -70.50), (42.11, -70.49), (42.12, -70.48), (42.30, -70.30)]
    times = [
        base,
        base + timedelta(minutes=10),
        base + timedelta(minutes=20),
        base + timedelta(hours=3),
    ]
    for index, ((lat, lon), occurred) in enumerate(zip(positions, times, strict=True)):
        event_id = f"66666666-6666-4666-8666-{index:012d}"
        envelope = vessel_envelope(
            event_id,
            occurred.strftime("%Y-%m-%dT%H:%M:%SZ"),
            {"mmsi": MMSI, "latitude": lat, "longitude": lon},
        )
        rows.append(decode_vessel_observation(envelope, validator(), verifier))
    append_vessel_observations(uris["bronze"], rows)

    version, count = rebuild_silver_trajectories(uris["bronze"], uris["silver"])
    assert count == 2
    silver_rows = DeltaTable(uris["silver"]).to_pyarrow_table().to_pylist()
    assert {row["segment_index"] for row in silver_rows} == {0, 1}
    assert all(row["crs"] == "EPSG:4326" for row in silver_rows)
    # Rebuild is idempotent derived state.
    _, count2 = rebuild_silver_trajectories(uris["bronze"], uris["silver"])
    assert count2 == count

    geofence = Geofence(
        identifier="gulf-of-guinea-test",
        geometry={
            "type": "Polygon",
            "coordinates": [
                [[-71.0, 42.0], [-70.0, 42.0], [-70.0, 42.2], [-71.0, 42.2], [-71.0, 42.0]]
            ],
        },
    )
    _, gold_count = rebuild_gold_geofence_summaries(uris["bronze"], uris["gold"], [geofence])
    assert gold_count == 1
    gold_rows = DeltaTable(uris["gold"]).to_pyarrow_table().to_pylist()
    assert gold_rows[0]["observation_count"] == 3
    assert gold_rows[0]["mmsi"] == MMSI
    assert gold_rows[0]["geometry_wkt"].startswith("POLYGON (")


def test_geofence_summaries_empty_and_outside(tmp_path: Path) -> None:
    geofence = Geofence(
        identifier="tiny-box",
        geometry={
            "type": "Polygon",
            "coordinates": [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]],
        },
    )
    outside = [observation("e1", datetime(2026, 8, 12, tzinfo=UTC), 42.0, -70.0)]
    assert build_geofence_summaries(outside, [geofence]) == []
    with pytest.raises(ValueError, match="at least one"):
        build_geofence_summaries(outside, [])


def test_vessel_table_uris_enforce_platform_boundary(tmp_path: Path) -> None:
    uris = vessel_table_uris(str(tmp_path / "platform"))
    assert uris["bronze"].endswith("/platform/bronze/vessel_observations")
    assert uris["silver"].endswith("/platform/silver/vessel_trajectories")
    assert uris["gold"].endswith("/platform/gold/geofence_summaries")
    with pytest.raises(BoundaryViolationError):
        vessel_table_uris(str(tmp_path / "cvff"))
