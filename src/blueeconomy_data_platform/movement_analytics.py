"""Phase-5 movement analytics via the geolibre-wasm WASI engine.

Thin governed wrappers around the pinned ``geolibre-wasm`` Python binding,
which runs the GeoLibre movement tools (compiled to a single WASI module)
in-process. The wrappers:

- ``reconstruct_tracks`` — rebuild per-MMSI ordered track lines from point
  observations;
- ``calculate_motion_statistics`` — per-track speed/heading/distance
  statistics;
- ``trace_proximity_events`` — time-bounded proximity events between two
  track sets.

Fail-closed contract: when the binding or the WASI runtime is unavailable
the wrappers raise :class:`MovementEngineUnavailableError`
(``MOVEMENT_ENGINE_UNAVAILABLE``); there is no silent fallback to
approximated or fabricated results. Production paths must never download
the runtime at request time: the ``.wasm`` is vendored through the
``GEOLIBRE_WASM`` environment variable (or pre-seeded binding cache). A
one-time download is permitted only in development behind the explicit
``BLUEECONOMY_MOVEMENT_ALLOW_RUNTIME_DOWNLOAD=true`` gate.
"""

from __future__ import annotations

import datetime
import json
import math
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from blueeconomy_data_platform.ais_decode import normalize_mmsi
from blueeconomy_data_platform.ingest import parse_timestamp

MOVEMENT_ENGINE_UNAVAILABLE = "MOVEMENT_ENGINE_UNAVAILABLE"
ENV_GEOLIBRE_WASM = "GEOLIBRE_WASM"
ENV_ALLOW_RUNTIME_DOWNLOAD = "BLUEECONOMY_MOVEMENT_ALLOW_RUNTIME_DOWNLOAD"

TOOL_RECONSTRUCT_TRACKS = "reconstruct_tracks"
TOOL_MOTION_STATISTICS = "calculate_motion_statistics"
TOOL_PROXIMITY_EVENTS = "trace_proximity_events"

MAX_OBSERVATIONS_PER_CALL = 250_000
_WASM_MAGIC = b"\x00asm"


class MovementEngineUnavailableError(RuntimeError):
    """Raised when the WASI movement engine cannot be used; fail closed."""

    code = MOVEMENT_ENGINE_UNAVAILABLE

    def __init__(self, detail: str) -> None:
        super().__init__(f"{MOVEMENT_ENGINE_UNAVAILABLE}: {detail}")


class MovementEngineError(RuntimeError):
    """Raised when the WASI tool ran but failed or returned malformed output."""


def _import_binding() -> Any:
    try:
        import geolibre_wasm
    except ImportError as error:
        raise MovementEngineUnavailableError(
            "the pinned geolibre-wasm binding is not installed in this environment"
        ) from error
    return geolibre_wasm


def _check_wasm_module(path: Path, source: str) -> str:
    if not path.is_file() or path.is_symlink():
        raise MovementEngineUnavailableError(
            f"GEOLIBRE runtime from {source} ({path}) is not a regular non-symlink file"
        )
    with path.open("rb") as handle:
        magic = handle.read(4)
    if magic != _WASM_MAGIC:
        raise MovementEngineUnavailableError(
            f"GEOLIBRE runtime from {source} ({path}) is not a WASM module"
        )
    return str(path)


def _resolve_wasm_path(binding: Any, env: Mapping[str, str]) -> str:
    vendored = env.get(ENV_GEOLIBRE_WASM, "")
    if vendored:
        if vendored != vendored.strip():
            raise MovementEngineUnavailableError(f"{ENV_GEOLIBRE_WASM} must be canonical")
        return _check_wasm_module(Path(vendored), ENV_GEOLIBRE_WASM)
    cache = (
        Path(env.get("XDG_CACHE_HOME") or Path.home() / ".cache")
        / "geolibre"
        / f"geolibre-cli-{binding.RUNTIME_VERSION}.wasm"
    )
    if cache.is_file():
        return _check_wasm_module(cache, "binding cache")
    if env.get(ENV_ALLOW_RUNTIME_DOWNLOAD, "") == "true":
        # DEV-only one-time download; production paths must vendor the
        # runtime via GEOLIBRE_WASM and never reach this branch.
        downloaded = binding.download_runtime()
        return _check_wasm_module(Path(downloaded), "dev download")
    raise MovementEngineUnavailableError(
        f"no vendored WASI runtime found at {cache}; vendor geolibre-cli.wasm via "
        f"{ENV_GEOLIBRE_WASM} (production) or set {ENV_ALLOW_RUNTIME_DOWNLOAD}=true "
        "for a one-time development download"
    )


@dataclass(frozen=True)
class MovementEngine:
    """A resolved, tool-verified handle on the WASI movement engine."""

    binding: Any
    wasm_path: str

    def run_tool(self, tool: str, args: Sequence[str], inputs: Mapping[str, bytes]) -> Any:
        result = self.binding.run_tool(
            tool, args=args, input=dict(inputs), wasm_path=self.wasm_path
        )
        if result.exit_code != 0:
            raise MovementEngineError(
                f"geolibre tool {tool!r} exited with code {result.exit_code}: "
                + "; ".join(result.stdout[-5:])
            )
        return result


def resolve_movement_engine(
    env: Mapping[str, str] | None = None,
    required_tools: Sequence[str] = (
        TOOL_RECONSTRUCT_TRACKS,
        TOOL_MOTION_STATISTICS,
        TOOL_PROXIMITY_EVENTS,
    ),
) -> MovementEngine:
    """Resolve the engine, failing closed when binding, runtime or tools are absent."""
    environment = os.environ if env is None else env
    binding = _import_binding()
    wasm_path = _resolve_wasm_path(binding, environment)
    try:
        available = set(binding.list_tools(wasm_path=wasm_path))
    except Exception as error:
        raise MovementEngineUnavailableError(
            f"the WASI runtime at {wasm_path} could not enumerate its tools: {error}"
        ) from error
    missing = [tool for tool in required_tools if tool not in available]
    if missing:
        raise MovementEngineUnavailableError(
            f"the WASI runtime at {wasm_path} does not provide the required movement "
            f"tools {missing}; refusing to approximate them"
        )
    return MovementEngine(binding=binding, wasm_path=wasm_path)


# ---------------------------------------------------------------------------
# Input validation and GeoJSON marshalling
# ---------------------------------------------------------------------------


def _validate_observation(observation: Mapping[str, Any], index: int) -> dict[str, Any]:
    if not isinstance(observation, Mapping):
        raise ValueError(f"observation {index} must be an object")
    mmsi = normalize_mmsi(observation.get("mmsi"))
    latitude = observation.get("latitude")
    longitude = observation.get("longitude")
    for label, value, bounds in (
        ("latitude", latitude, (-90.0, 90.0)),
        ("longitude", longitude, (-180.0, 180.0)),
    ):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"observation {index} {label} must be a finite number")
        if not math.isfinite(float(value)) or not bounds[0] <= float(value) <= bounds[1]:
            raise ValueError(f"observation {index} {label} must be within {bounds}")
    occurred_raw = observation.get("occurred_at")
    if isinstance(occurred_raw, str):
        occurred_at = parse_timestamp(occurred_raw, "occurred_at").isoformat()
    elif isinstance(occurred_raw, datetime.datetime):
        occurred_at = occurred_raw.isoformat()
    else:
        raise ValueError(f"observation {index} occurred_at must be an RFC 3339 timestamp")
    return {
        "mmsi": mmsi,
        "latitude": float(latitude),  # type: ignore[arg-type]
        "longitude": float(longitude),  # type: ignore[arg-type]
        "occurred_at": occurred_at,
    }


def _observations_geojson(observations: Sequence[Mapping[str, Any]]) -> bytes:
    if not 1 <= len(observations) <= MAX_OBSERVATIONS_PER_CALL:
        raise ValueError(
            f"reconstruct_tracks requires 1 to {MAX_OBSERVATIONS_PER_CALL} observations"
        )
    features = []
    for index, observation in enumerate(observations):
        validated = _validate_observation(observation, index)
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [validated["longitude"], validated["latitude"]],
                },
                "properties": {
                    "mmsi": validated["mmsi"],
                    "occurred_at": validated["occurred_at"],
                },
            }
        )
    document = {"type": "FeatureCollection", "features": features}
    return json.dumps(document, separators=(",", ":")).encode("utf-8")


def _read_json_output(result: Any, name: str, tool: str) -> Any:
    payload = result.files.get(name)
    if payload is None:
        raise MovementEngineError(
            f"geolibre tool {tool!r} did not produce the expected {name} output"
        )
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise MovementEngineError(
            f"geolibre tool {tool!r} returned malformed {name} output"
        ) from error


# ---------------------------------------------------------------------------
# Governed wrappers (tool contracts per the pinned runtime's manifests)
# ---------------------------------------------------------------------------


def reconstruct_tracks(
    observations: Sequence[Mapping[str, Any]],
    engine: MovementEngine | None = None,
    gap_hours: float = 2.0,
) -> Any:
    """Reconstruct per-MMSI track line segments from point observations.

    Runs the WASI ``reconstruct_tracks`` tool: points are grouped by MMSI,
    ordered by ``occurred_at`` and split into a new segment whenever the
    time gap exceeds ``gap_hours`` (the platform's two-hour default). The
    returned GeoJSON carries per-segment lengths and speeds computed by the
    engine.
    """
    if not 0 < gap_hours <= 24 * 7:
        raise ValueError("gap_hours must be within (0, 168]")
    payload = _observations_geojson(observations)
    resolved = engine or resolve_movement_engine(required_tools=(TOOL_RECONSTRUCT_TRACKS,))
    result = resolved.run_tool(
        TOOL_RECONSTRUCT_TRACKS,
        args=[
            "--input=/work/observations.geojson",
            "--output=/work/tracks.geojson",
            "--track_field=mmsi",
            "--time_field=occurred_at",
            f"--time_gap={gap_hours * 3600.0}",
        ],
        inputs={"observations.geojson": payload},
    )
    return _read_json_output(result, "tracks.geojson", TOOL_RECONSTRUCT_TRACKS)


def calculate_motion_statistics(
    observations: Sequence[Mapping[str, Any]],
    engine: MovementEngine | None = None,
    window: int = 1,
) -> Any:
    """Annotate timestamped track points with motion statistics.

    Runs the WASI ``calculate_motion_statistics`` tool: per point, grouped
    by MMSI and ordered by time, it adds speed, acceleration, bearing,
    segment and cumulative distance, elapsed time and idle flag. ``window``
    is the trailing point count for the smoothed average speed (1 =
    instantaneous).
    """
    if not 1 <= window <= 1000:
        raise ValueError("window must be between 1 and 1000 points")
    payload = _observations_geojson(observations)
    resolved = engine or resolve_movement_engine(required_tools=(TOOL_MOTION_STATISTICS,))
    result = resolved.run_tool(
        TOOL_MOTION_STATISTICS,
        args=[
            "--input=/work/observations.geojson",
            "--output=/work/statistics.geojson",
            "--track_field=mmsi",
            "--time_field=occurred_at",
            f"--window={window}",
        ],
        inputs={"observations.geojson": payload},
    )
    return _read_json_output(result, "statistics.geojson", TOOL_MOTION_STATISTICS)


def trace_proximity_events(
    observations: Sequence[Mapping[str, Any]],
    search_distance: float,
    min_duration_seconds: float = 0.0,
    engine: MovementEngine | None = None,
    entities: Sequence[str] | None = None,
) -> Any:
    """Trace intervals where two movers were within ``search_distance``.

    Runs the WASI ``trace_proximity_events`` tool over the combined
    timestamped point layer. Distances are planar in the input CRS map
    units (degrees for WGS84 lon/lat data); callers needing metric
    thresholds must supply projected coordinates. ``entities`` optionally
    restricts the trace to downstream contacts of the given seed MMSIs
    (contact tracing). ``min_duration_seconds`` is the minimum time in
    proximity to count as an event.
    """
    if not math.isfinite(search_distance) or not 0 < search_distance <= 180.0:
        raise ValueError("search_distance must be a positive finite CRS-unit distance")
    if not math.isfinite(min_duration_seconds) or not 0 <= min_duration_seconds <= 86_400:
        raise ValueError("min_duration_seconds must be within [0, 86400]")
    payload = _observations_geojson(observations)
    args = [
        "--input=/work/observations.geojson",
        "--output=/work/proximity.geojson",
        "--track_field=mmsi",
        "--time_field=occurred_at",
        f"--search_distance={search_distance}",
        f"--min_duration={min_duration_seconds}",
    ]
    if entities:
        seeds = [normalize_mmsi(entity) for entity in entities]
        if not seeds:
            raise ValueError("entities must contain at least one MMSI when provided")
        args.append(f"--entities={','.join(seeds)}")
    resolved = engine or resolve_movement_engine(required_tools=(TOOL_PROXIMITY_EVENTS,))
    result = resolved.run_tool(
        TOOL_PROXIMITY_EVENTS, args=args, inputs={"observations.geojson": payload}
    )
    return _read_json_output(result, "proximity.geojson", TOOL_PROXIMITY_EVENTS)


__all__ = [
    "MOVEMENT_ENGINE_UNAVAILABLE",
    "MovementEngine",
    "MovementEngineError",
    "MovementEngineUnavailableError",
    "calculate_motion_statistics",
    "reconstruct_tracks",
    "resolve_movement_engine",
    "trace_proximity_events",
]
