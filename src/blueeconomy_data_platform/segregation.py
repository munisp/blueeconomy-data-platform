"""Fail-closed fiduciary segregation boundaries for the platform lakehouse.

Workstream C (CVFF fintech) events are physically segregated from Workstream A
(ports.*) and Workstream B (ferries.*) events. Classification is not a routing
hint: :class:`SegregatedDeltaWriter` is initialized for exactly one lakehouse
scope and raises :class:`BoundaryViolationError` on any cross-boundary write
attempt, including events with the wrong classification, Kafka topics outside
the writer's namespace, or table URIs outside the writer's segregated root.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any
from urllib.parse import urlsplit

FIDUCIARY_SEGREGATED_CLASSIFICATION = "fiduciary_segregated"
PLATFORM_CLASSIFICATIONS = frozenset(
    {"public", "internal", "confidential", "restricted", "highly_restricted"}
)
CVFF_TOPIC_PREFIX = "cvff."
PLATFORM_TOPIC_PREFIXES = ("ports.", "ferries.")
CVFF_ROOT_COMPONENT_PATTERN = re.compile(r"^cvff($|[_-])")


class BoundaryViolationError(ValueError):
    """Raised when a write would cross the fiduciary segregation boundary."""


class LakehouseScope(Enum):
    """Physically segregated lakehouse scopes."""

    PLATFORM = "platform"
    CVFF = "cvff"

    @property
    def layer_prefix(self) -> str:
        return "cvff" if self is LakehouseScope.CVFF else "platform"


def scope_for_classification(data_classification: str) -> LakehouseScope:
    """Map an event classification to its mandatory lakehouse scope, failing closed."""
    if data_classification == FIDUCIARY_SEGREGATED_CLASSIFICATION:
        return LakehouseScope.CVFF
    if data_classification in PLATFORM_CLASSIFICATIONS:
        return LakehouseScope.PLATFORM
    raise BoundaryViolationError(
        f"data_classification {data_classification!r} is not mapped to any lakehouse scope"
    )


def scope_for_topic(topic: str) -> LakehouseScope:
    """Map a Kafka topic namespace to its mandatory lakehouse scope, failing closed."""
    if topic.startswith(CVFF_TOPIC_PREFIX):
        return LakehouseScope.CVFF
    if topic.startswith(PLATFORM_TOPIC_PREFIXES):
        return LakehouseScope.PLATFORM
    raise BoundaryViolationError(
        f"Kafka topic {topic!r} is outside the governed cvff.*/ports.*/ferries.* namespaces"
    )


def uri_declares_cvff_root(table_uri: str) -> bool:
    """Return True when any URI path component belongs to the segregated cvff root."""
    path = urlsplit(table_uri).path
    components = [component for component in path.split("/") if component]
    return any(CVFF_ROOT_COMPONENT_PATTERN.match(component) for component in components)


@dataclass(frozen=True)
class LakehouseRoots:
    """Resolved medallion table URIs for one segregated lakehouse scope."""

    scope: LakehouseScope
    bronze: str
    silver: str
    gold: str

    def for_layer(self, layer: str) -> str:
        try:
            return {"bronze": self.bronze, "silver": self.silver, "gold": self.gold}[layer]
        except KeyError:
            raise BoundaryViolationError(f"unknown medallion layer {layer!r}") from None


def build_lakehouse_roots(scope: LakehouseScope, scope_root_uri: str) -> LakehouseRoots:
    """Build medallion roots under one scope root and enforce the path boundary.

    The cvff scope root must end in a path component starting with ``cvff``
    (for example ``.../cvff``) and the platform scope root must not. This
    prevents a writer from being pointed at the other scope's storage.
    """
    if not scope_root_uri or scope_root_uri != scope_root_uri.strip():
        raise BoundaryViolationError("scope root URI must be canonical non-empty text")
    root = scope_root_uri.rstrip("/")
    terminal = root.rsplit("/", 1)[-1]
    declares_cvff = bool(CVFF_ROOT_COMPONENT_PATTERN.match(terminal))
    if scope is LakehouseScope.CVFF and not declares_cvff:
        raise BoundaryViolationError(
            "cvff scope root must terminate in a cvff* path component; "
            f"refusing to initialize a cvff writer against {scope_root_uri!r}"
        )
    if scope is LakehouseScope.PLATFORM and declares_cvff:
        raise BoundaryViolationError(
            "platform scope root must not terminate in a cvff* path component; "
            f"refusing to initialize a platform writer against {scope_root_uri!r}"
        )
    prefix = scope.layer_prefix
    return LakehouseRoots(
        scope=scope,
        bronze=f"{root}/{prefix}_bronze/events",
        silver=f"{root}/{prefix}_silver/events",
        gold=f"{root}/{prefix}_gold/events",
    )


def require_scope_table_uri(scope: LakehouseScope, table_uri: str) -> None:
    """Fail closed when a table URI does not belong to the scope's segregated root."""
    declares_cvff = uri_declares_cvff_root(table_uri)
    if scope is LakehouseScope.CVFF and not declares_cvff:
        raise BoundaryViolationError(
            f"cvff writer cannot target {table_uri!r}: no cvff* root component present"
        )
    if scope is LakehouseScope.PLATFORM and declares_cvff:
        raise BoundaryViolationError(
            f"platform writer cannot target {table_uri!r}: cvff root component present"
        )


def enforce_event_scope(events: list[dict[str, Any]], scope: LakehouseScope) -> None:
    """Fail closed if any event classification is outside the writer's scope."""
    for event in events:
        classification = event.get("data_classification")
        if not isinstance(classification, str):
            raise BoundaryViolationError("event is missing a string data_classification")
        event_scope = scope_for_classification(classification)
        if event_scope is not scope:
            raise BoundaryViolationError(
                f"{scope.value} writer cannot accept event {event.get('event_id')!r} "
                f"classified {classification!r}; it belongs to the {event_scope.value} boundary"
            )


def enforce_topic_scope(topic: str, scope: LakehouseScope) -> None:
    """Fail closed if a Kafka topic namespace is outside the writer's scope."""
    topic_scope = scope_for_topic(topic)
    if topic_scope is not scope:
        raise BoundaryViolationError(
            f"{scope.value} writer cannot consume topic {topic!r}; "
            f"it belongs to the {topic_scope.value} boundary"
        )


class SegregatedDeltaWriter:
    """Append-only Delta writer bound to one lakehouse scope.

    The writer resolves its medallion table URIs from a single scope root at
    initialization and refuses, at the write path, any event classification,
    Kafka topic or table URI that belongs to the other scope.
    """

    def __init__(self, scope: LakehouseScope, scope_root_uri: str) -> None:
        if not isinstance(scope, LakehouseScope):
            raise BoundaryViolationError("writer scope must be a LakehouseScope value")
        self._roots = build_lakehouse_roots(scope, scope_root_uri)

    @property
    def scope(self) -> LakehouseScope:
        return self._roots.scope

    @property
    def roots(self) -> LakehouseRoots:
        return self._roots

    def table_uri(self, layer: str) -> str:
        """Resolve a medallion layer URI owned by this writer's scope."""
        uri = self._roots.for_layer(layer)
        require_scope_table_uri(self._roots.scope, uri)
        return uri

    def guard_write(
        self,
        layer: str,
        events: list[dict[str, Any]],
        kafka_topic: str | None = None,
    ) -> str:
        """Validate a write batch against every boundary rule and return the target URI.

        Raises :class:`BoundaryViolationError` on any cross-boundary attempt;
        no record is written when the guard fails.
        """
        if not events:
            raise BoundaryViolationError("refusing to write an empty event batch")
        target_uri = self.table_uri(layer)
        if kafka_topic is not None:
            enforce_topic_scope(kafka_topic, self._roots.scope)
        enforce_event_scope(events, self._roots.scope)
        return target_uri
