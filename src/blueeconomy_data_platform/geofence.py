"""Strict GeoJSON geofence evaluation for validated maritime positions.

This module intentionally supports only Polygon and MultiPolygon geometries with
closed linear rings. Complex geodesic, antimeridian and projected-coordinate
operations remain deployment responsibilities for Sedona/PostGIS and are
rejected rather than approximated silently.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Any, Mapping, Sequence


class GeofenceValidationError(ValueError):
    """Raised when a geofence or position is malformed or unsupported."""


@dataclass(frozen=True)
class Position:
    latitude: float
    longitude: float

    def __post_init__(self) -> None:
        if not isfinite(self.latitude) or not -90 <= self.latitude <= 90:
            raise GeofenceValidationError("latitude must be finite and between -90 and 90")
        if not isfinite(self.longitude) or not -180 <= self.longitude <= 180:
            raise GeofenceValidationError("longitude must be finite and between -180 and 180")


@dataclass(frozen=True)
class Geofence:
    identifier: str
    geometry: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not self.identifier.strip():
            raise GeofenceValidationError("geofence identifier is required")
        _validate_geometry(self.geometry)

    def contains(self, position: Position) -> bool:
        geometry_type = self.geometry["type"]
        coordinates = self.geometry["coordinates"]
        if geometry_type == "Polygon":
            return _polygon_contains(coordinates, position)
        if geometry_type == "MultiPolygon":
            return any(_polygon_contains(polygon, position) for polygon in coordinates)
        raise GeofenceValidationError(f"unsupported geometry type: {geometry_type}")


def _validate_geometry(geometry: Mapping[str, Any]) -> None:
    if not isinstance(geometry, Mapping):
        raise GeofenceValidationError("geometry must be an object")
    geometry_type = geometry.get("type")
    if geometry_type not in {"Polygon", "MultiPolygon"}:
        raise GeofenceValidationError("only Polygon and MultiPolygon geometries are supported")
    coordinates = geometry.get("coordinates")
    if not isinstance(coordinates, list) or not coordinates:
        raise GeofenceValidationError("geometry coordinates must be a non-empty array")
    polygons = [coordinates] if geometry_type == "Polygon" else coordinates
    if geometry_type == "MultiPolygon" and any(
        not isinstance(polygon, list) for polygon in polygons
    ):
        raise GeofenceValidationError("MultiPolygon coordinates must contain polygon arrays")
    for polygon in polygons:
        if not polygon or not isinstance(polygon, list):
            raise GeofenceValidationError("polygon must contain at least one linear ring")
        for ring in polygon:
            _validate_ring(ring)


def _validate_ring(ring: Any) -> None:
    if not isinstance(ring, list) or len(ring) < 4 or ring[0] != ring[-1]:
        raise GeofenceValidationError("linear ring must have at least four closed positions")
    for coordinate in ring:
        if not isinstance(coordinate, list) or len(coordinate) != 2:
            raise GeofenceValidationError("GeoJSON positions must be [longitude, latitude]")
        longitude, latitude = coordinate
        Position(latitude=float(latitude), longitude=float(longitude))


def _polygon_contains(polygon: Sequence[Any], position: Position) -> bool:
    outer = polygon[0]
    if _ring_contains(outer, position):
        for hole in polygon[1:]:
            if _ring_contains(hole, position):
                return False
        return True
    return False


def _ring_contains(ring: Sequence[Any], position: Position) -> bool:
    x = position.longitude
    y = position.latitude
    inside = False
    for index in range(len(ring) - 1):
        x1, y1 = float(ring[index][0]), float(ring[index][1])
        x2, y2 = float(ring[index + 1][0]), float(ring[index + 1][1])
        if _point_on_segment(x, y, x1, y1, x2, y2):
            return True
        crosses = (y1 > y) != (y2 > y)
        if crosses:
            intersection_x = (x2 - x1) * (y - y1) / (y2 - y1) + x1
            if x < intersection_x:
                inside = not inside
    return inside


def _point_on_segment(x: float, y: float, x1: float, y1: float, x2: float, y2: float) -> bool:
    cross = (y - y1) * (x2 - x1) - (x - x1) * (y2 - y1)
    if abs(cross) > 1e-12:
        return False
    return (
        min(x1, x2) - 1e-12 <= x <= max(x1, x2) + 1e-12
        and min(y1, y2) - 1e-12 <= y <= max(y1, y2) + 1e-12
    )
