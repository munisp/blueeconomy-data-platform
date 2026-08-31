from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from blueeconomy_data_platform.movement_analytics import (
    ENV_ALLOW_RUNTIME_DOWNLOAD,
    ENV_GEOLIBRE_WASM,
    MOVEMENT_ENGINE_UNAVAILABLE,
    MovementEngineUnavailableError,
    calculate_motion_statistics,
    reconstruct_tracks,
    resolve_movement_engine,
    trace_proximity_events,
)

OBSERVATIONS = [
    {
        "mmsi": "366123456",
        "latitude": 42.10,
        "longitude": -70.50,
        "occurred_at": "2026-08-12T10:00:00Z",
    },
    {
        "mmsi": "366123456",
        "latitude": 42.11,
        "longitude": -70.49,
        "occurred_at": "2026-08-12T10:10:00Z",
    },
    {
        "mmsi": "366123456",
        "latitude": 42.12,
        "longitude": -70.48,
        "occurred_at": "2026-08-12T10:20:00Z",
    },
    {
        "mmsi": "244123456",
        "latitude": 42.10,
        "longitude": -70.51,
        "occurred_at": "2026-08-12T10:05:00Z",
    },
    {
        "mmsi": "244123456",
        "latitude": 42.105,
        "longitude": -70.50,
        "occurred_at": "2026-08-12T10:15:00Z",
    },
]


def _engine_available() -> bool:
    try:
        resolve_movement_engine(env={})
    except MovementEngineUnavailableError:
        return False
    return True


ENGINE_AVAILABLE = _engine_available()
requires_engine = pytest.mark.skipif(
    not ENGINE_AVAILABLE,
    reason=(
        "geolibre WASI runtime not vendored in this environment "
        "(set GEOLIBRE_WASM or seed the binding cache to run the live integration)"
    ),
)


# ---------------------------------------------------------------------------
# Pure validation logic (always runs)
# ---------------------------------------------------------------------------


def test_observation_validation_fails_closed() -> None:
    with pytest.raises(ValueError, match="mmsi"):
        reconstruct_tracks(
            [
                {
                    "mmsi": "123",
                    "latitude": 1.0,
                    "longitude": 1.0,
                    "occurred_at": "2026-08-12T10:00:00Z",
                }
            ]
        )
    with pytest.raises(ValueError, match="latitude"):
        reconstruct_tracks(
            [
                {
                    "mmsi": "366123456",
                    "latitude": 95.0,
                    "longitude": 1.0,
                    "occurred_at": "2026-08-12T10:00:00Z",
                }
            ]
        )
    with pytest.raises(ValueError, match="occurred_at"):
        reconstruct_tracks(
            [{"mmsi": "366123456", "latitude": 1.0, "longitude": 1.0, "occurred_at": "not-a-time"}]
        )
    with pytest.raises(ValueError, match="1 to"):
        reconstruct_tracks([])
    with pytest.raises(ValueError, match="gap_hours"):
        reconstruct_tracks(OBSERVATIONS, gap_hours=0.0)


def test_motion_statistics_validation() -> None:
    with pytest.raises(ValueError, match="window"):
        calculate_motion_statistics(OBSERVATIONS, window=0)
    with pytest.raises(ValueError, match="longitude"):
        calculate_motion_statistics(
            [
                {
                    "mmsi": "366123456",
                    "latitude": 1.0,
                    "longitude": -200.0,
                    "occurred_at": "2026-08-12T10:00:00Z",
                }
            ]
        )


def test_proximity_validation() -> None:
    with pytest.raises(ValueError, match="search_distance"):
        trace_proximity_events(OBSERVATIONS, search_distance=0.0)
    with pytest.raises(ValueError, match="min_duration_seconds"):
        trace_proximity_events(OBSERVATIONS, search_distance=1.0, min_duration_seconds=-1.0)
    with pytest.raises(ValueError, match="mmsi"):
        trace_proximity_events(OBSERVATIONS, search_distance=1.0, entities=["bad"])


def test_engine_fails_closed_without_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # No vendored wasm, empty cache, downloads disallowed: MOVEMENT_ENGINE_UNAVAILABLE.
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    monkeypatch.delenv(ENV_GEOLIBRE_WASM, raising=False)
    monkeypatch.delenv(ENV_ALLOW_RUNTIME_DOWNLOAD, raising=False)
    with pytest.raises(MovementEngineUnavailableError, match=MOVEMENT_ENGINE_UNAVAILABLE):
        resolve_movement_engine(env={"XDG_CACHE_HOME": str(tmp_path)})
    with pytest.raises(MovementEngineUnavailableError):
        reconstruct_tracks(OBSERVATIONS, engine=None)


def test_engine_rejects_non_wasm_vendored_file(tmp_path: Path) -> None:
    fake = tmp_path / "not-a-module.wasm"
    fake.write_bytes(b"MZ not wasm")
    with pytest.raises(MovementEngineUnavailableError, match="not a WASM module"):
        resolve_movement_engine(env={ENV_GEOLIBRE_WASM: str(fake)})


# ---------------------------------------------------------------------------
# Live WASI integration (env-gated: requires the vendored runtime)
# ---------------------------------------------------------------------------


@requires_engine
def test_reconstruct_tracks_live() -> None:
    tracks = reconstruct_tracks(OBSERVATIONS)
    assert tracks["type"] == "FeatureCollection"
    assert len(tracks["features"]) == 2
    for feature in tracks["features"]:
        assert feature["geometry"]["type"] == "LineString"
        assert feature["properties"]["track_id"] in {"366123456", "244123456"}
        assert feature["properties"]["n_points"] >= 2


@requires_engine
def test_reconstruct_tracks_segments_on_gap_live() -> None:
    observations = OBSERVATIONS + [
        {
            "mmsi": "366123456",
            "latitude": 42.30,
            "longitude": -70.30,
            "occurred_at": "2026-08-12T13:00:00Z",
        },
        {
            "mmsi": "366123456",
            "latitude": 42.31,
            "longitude": -70.29,
            "occurred_at": "2026-08-12T13:10:00Z",
        },
    ]
    tracks = reconstruct_tracks(observations, gap_hours=2.0)
    segments = [
        feature
        for feature in tracks["features"]
        if feature["properties"]["track_id"] == "366123456"
    ]
    assert len(segments) == 2
    assert {feature["properties"]["segment"] for feature in segments} == {0, 1}


@requires_engine
def test_calculate_motion_statistics_live() -> None:
    statistics = calculate_motion_statistics(OBSERVATIONS)
    assert statistics["type"] == "FeatureCollection"
    assert len(statistics["features"]) == len(OBSERVATIONS)
    moving = [
        feature
        for feature in statistics["features"]
        if feature["properties"]["mmsi"] == "366123456"
    ]
    speeds = [feature["properties"]["speed"] for feature in moving]
    assert speeds[0] == 0  # first point of a track has no predecessor
    assert any(speed > 0 for speed in speeds[1:])


@requires_engine
def test_trace_proximity_events_live() -> None:
    events = trace_proximity_events(OBSERVATIONS, search_distance=5.0)
    assert events["type"] == "FeatureCollection"
    assert len(events["features"]) >= 1
    pair = events["features"][0]["properties"]
    assert {pair["track_a"], pair["track_b"]} == {"366123456", "244123456"}
    distant = trace_proximity_events(OBSERVATIONS, search_distance=1e-9)
    assert distant["features"] == []


@requires_engine
def test_engine_vendored_via_env(tmp_path: Path) -> None:
    engine = resolve_movement_engine(env={})
    vendored = tmp_path / "geolibre-cli.wasm"
    shutil.copy(engine.wasm_path, vendored)
    resolved = resolve_movement_engine(env={ENV_GEOLIBRE_WASM: str(vendored)})
    assert resolved.wasm_path == str(vendored)
    tracks = reconstruct_tracks(OBSERVATIONS[:2], engine=resolved)
    assert len(tracks["features"]) == 1
