from __future__ import annotations

import pytest

from blueeconomy_data_platform.geofence import Geofence, GeofenceValidationError, Position


SQUARE = {
    "type": "Polygon",
    "coordinates": [[[-1, -1], [1, -1], [1, 1], [-1, 1], [-1, -1]]],
}


def test_polygon_contains_inside_outside_and_boundary() -> None:
    geofence = Geofence(identifier="port-a", geometry=SQUARE)
    assert geofence.contains(Position(latitude=0, longitude=0))
    assert geofence.contains(Position(latitude=1, longitude=0))
    assert not geofence.contains(Position(latitude=2, longitude=0))


def test_polygon_hole_is_not_inside() -> None:
    geometry = {
        "type": "Polygon",
        "coordinates": [
            [[-2, -2], [2, -2], [2, 2], [-2, 2], [-2, -2]],
            [[-1, -1], [1, -1], [1, 1], [-1, 1], [-1, -1]],
        ],
    }
    geofence = Geofence(identifier="restricted-port", geometry=geometry)
    assert not geofence.contains(Position(latitude=0, longitude=0))
    assert geofence.contains(Position(latitude=1.5, longitude=0))


def test_multipolygon_contains_any_member() -> None:
    geofence = Geofence(
        identifier="corridor",
        geometry={
            "type": "MultiPolygon",
            "coordinates": [
                SQUARE["coordinates"],
                [[[10, 10], [12, 10], [12, 12], [10, 12], [10, 10]]],
            ],
        },
    )
    assert geofence.contains(Position(latitude=0, longitude=0))
    assert geofence.contains(Position(latitude=11, longitude=11))
    assert not geofence.contains(Position(latitude=5, longitude=5))


@pytest.mark.parametrize(
    "geometry",
    [
        {"type": "Point", "coordinates": [0, 0]},
        {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1]]]},
        {"type": "Polygon", "coordinates": [[[0, 0], [181, 0], [1, 1], [0, 0]]]},
    ],
)
def test_rejects_unsupported_or_malformed_geometry(geometry: dict[str, object]) -> None:
    with pytest.raises(GeofenceValidationError):
        Geofence(identifier="invalid", geometry=geometry)


def test_rejects_invalid_position() -> None:
    with pytest.raises(GeofenceValidationError):
        Position(latitude=91, longitude=0)
