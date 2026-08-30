"""Fail-closed fiduciary segregation boundaries for the platform lakehouse.

Workstream C (CVFF fintech) events are physically segregated from Workstream A
(ports.*) and Workstream B (ferries.*) events, and the phase-2 lakehouse scopes
extend the same model to Workstream D (seafarer credentials), Workstream E
(fisheries catch/coldchain/export) and Workstream F (classified ISR tracks).
Phase 8 adds the MRV emissions scope (``mrv.*`` topics) and the Blue-Carbon
registry scope (``bluecarbon.*`` topics) under the identical boundary rules.
Classification is not a routing hint: :class:`SegregatedDeltaWriter` is
initialized for exactly one lakehouse scope and raises
:class:`BoundaryViolationError` on any cross-boundary write attempt, including
events with the wrong classification, Kafka topics outside the writer's
namespace, or table URIs outside the writer's segregated root.
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
SEAFARER_CLASSIFICATION = "seafarer_confidential"
FISHERIES_CLASSIFICATION = "fisheries_operational"
ISR_CLASSIFICATION = "isr_classified"
MRV_CLASSIFICATION = "mrv_confidential"
BLUECARBON_CLASSIFICATION = "bluecarbon_internal"

CVFF_TOPIC_PREFIX = "cvff."
PLATFORM_TOPIC_PREFIXES = ("ports.", "ferries.", "vessels.")
SEAFARER_TOPIC_PREFIXES = ("seafarer.",)
FISHERIES_TOPIC_PREFIXES = ("fisheries.", "coldchain.", "export.")
ISR_TOPIC_PREFIXES = ("maritime.isr.", "maritime.behaviour.", "maritime.outcome.")
MRV_TOPIC_PREFIXES = ("mrv.",)
BLUECARBON_TOPIC_PREFIXES = ("bluecarbon.",)

CVFF_ROOT_COMPONENT_PATTERN = re.compile(r"^cvff($|[_-])")


class BoundaryViolationError(ValueError):
    """Raised when a write would cross the fiduciary segregation boundary."""


class LakehouseScope(Enum):
    """Physically segregated lakehouse scopes."""

    PLATFORM = "platform"
    CVFF = "cvff"
    SEAFARER = "seafarer"
    FISHERIES = "fisheries"
    ISR = "isr"
    MRV = "mrv"
    BLUECARBON = "bluecarbon"

    @property
    def layer_prefix(self) -> str:
        return self.value

    @property
    def root_component_pattern(self) -> re.Pattern[str]:
        """Pattern matching this scope's own storage root path components."""
        return re.compile(rf"^{self.value}($|[_-])")


SEGREGATED_SCOPES = frozenset(
    {
        LakehouseScope.CVFF,
        LakehouseScope.SEAFARER,
        LakehouseScope.FISHERIES,
        LakehouseScope.ISR,
        LakehouseScope.MRV,
        LakehouseScope.BLUECARBON,
    }
)

SCOPE_TOPIC_PREFIXES: dict[LakehouseScope, tuple[str, ...]] = {
    LakehouseScope.PLATFORM: PLATFORM_TOPIC_PREFIXES,
    LakehouseScope.CVFF: (CVFF_TOPIC_PREFIX,),
    LakehouseScope.SEAFARER: SEAFARER_TOPIC_PREFIXES,
    LakehouseScope.FISHERIES: FISHERIES_TOPIC_PREFIXES,
    LakehouseScope.ISR: ISR_TOPIC_PREFIXES,
    LakehouseScope.MRV: MRV_TOPIC_PREFIXES,
    LakehouseScope.BLUECARBON: BLUECARBON_TOPIC_PREFIXES,
}

SCOPE_CLASSIFICATIONS: dict[LakehouseScope, frozenset[str]] = {
    LakehouseScope.PLATFORM: PLATFORM_CLASSIFICATIONS,
    LakehouseScope.CVFF: frozenset({FIDUCIARY_SEGREGATED_CLASSIFICATION}),
    LakehouseScope.SEAFARER: frozenset({SEAFARER_CLASSIFICATION}),
    LakehouseScope.FISHERIES: frozenset({FISHERIES_CLASSIFICATION}),
    LakehouseScope.ISR: frozenset({ISR_CLASSIFICATION}),
    LakehouseScope.MRV: frozenset({MRV_CLASSIFICATION}),
    LakehouseScope.BLUECARBON: frozenset({BLUECARBON_CLASSIFICATION}),
}


def scope_for_classification(data_classification: str) -> LakehouseScope:
    """Map an event classification to its mandatory lakehouse scope, failing closed."""
    for scope, classifications in SCOPE_CLASSIFICATIONS.items():
        if data_classification in classifications:
            return scope
    raise BoundaryViolationError(
        f"data_classification {data_classification!r} is not mapped to any lakehouse scope"
    )


CANONICAL_CLASSIFICATIONS = frozenset(
    {"FIDUCIARY_SEGREGATED", "CONFIDENTIAL", "RESTRICTED", "INTERNAL", "PUBLIC"}
)

# Canonical (producer-facing) classifications mapped onto the platform scope's
# internal lowercase labels. The segregated scopes each retain their single
# internal label; the canonical vocabulary never crosses a scope boundary.
CANONICAL_TO_PLATFORM_LABEL: dict[str, str] = {
    "PUBLIC": "public",
    "INTERNAL": "internal",
    "CONFIDENTIAL": "confidential",
    "RESTRICTED": "restricted",
}


def scope_for_event_type(event_type: str) -> LakehouseScope:
    """Map an envelope ``eventType`` namespace to its lakehouse scope, failing closed.

    Envelope event types share the governed Kafka topic namespaces (for
    example ``cvff.disbursement.v1`` on ``cvff.*`` topics), so the same
    prefix boundary applies.
    """
    for scope, prefixes in SCOPE_TOPIC_PREFIXES.items():
        if event_type.startswith(prefixes):
            return scope
    raise BoundaryViolationError(
        f"eventType {event_type!r} is outside the governed event namespaces"
    )


def map_canonical_classification(classification: str, event_type: str) -> str:
    """Map a canonical envelope classification to its internal scope label.

    Producers emit the canonical platform vocabulary
    (``FIDUCIARY_SEGREGATED``/``CONFIDENTIAL``/``RESTRICTED``/``INTERNAL``/
    ``PUBLIC``); the lakehouse retains the internal lowercase per-scope
    labels. The internal label is derived from the classification and the
    event type's governed namespace so a classification can never be
    laundered across a segregation boundary; anything unrecognized fails
    closed.
    """
    if classification not in CANONICAL_CLASSIFICATIONS:
        raise BoundaryViolationError(
            f"canonical classification {classification!r} is not in the platform vocabulary"
        )
    if classification == "FIDUCIARY_SEGREGATED":
        if scope_for_event_type(event_type) is not LakehouseScope.CVFF:
            raise BoundaryViolationError(
                f"FIDUCIARY_SEGREGATED event type {event_type!r} is outside the cvff boundary"
            )
        return FIDUCIARY_SEGREGATED_CLASSIFICATION
    try:
        scope = scope_for_event_type(event_type)
    except BoundaryViolationError:
        # Platform-scope producers may use event types outside the segregated
        # namespaces (for example maritime.position.v1); the Kafka consumer's
        # topic boundary remains the governing check for platform writes.
        scope = LakehouseScope.PLATFORM
    if scope is LakehouseScope.CVFF:
        raise BoundaryViolationError(
            f"cvff event type {event_type!r} must be classified FIDUCIARY_SEGREGATED"
        )
    if scope is LakehouseScope.SEAFARER:
        return SEAFARER_CLASSIFICATION
    if scope is LakehouseScope.FISHERIES:
        return FISHERIES_CLASSIFICATION
    if scope is LakehouseScope.ISR:
        return ISR_CLASSIFICATION
    if scope is LakehouseScope.MRV:
        return MRV_CLASSIFICATION
    if scope is LakehouseScope.BLUECARBON:
        return BLUECARBON_CLASSIFICATION
    return CANONICAL_TO_PLATFORM_LABEL[classification]


def scope_for_topic(topic: str) -> LakehouseScope:
    """Map a Kafka topic namespace to its mandatory lakehouse scope, failing closed."""
    for scope, prefixes in SCOPE_TOPIC_PREFIXES.items():
        if topic.startswith(prefixes):
            return scope
    governed = "/".join(
        f"{prefix}*" for prefixes in SCOPE_TOPIC_PREFIXES.values() for prefix in prefixes
    )
    raise BoundaryViolationError(
        f"Kafka topic {topic!r} is outside the governed namespaces ({governed})"
    )


def scopes_declared_by_uri(table_uri: str) -> frozenset[LakehouseScope]:
    """Return the segregated scopes whose root component appears in a URI path."""
    path = urlsplit(table_uri).path
    components = [component for component in path.split("/") if component]
    return frozenset(
        scope
        for scope in SEGREGATED_SCOPES
        if any(scope.root_component_pattern.match(component) for component in components)
    )


def uri_declares_cvff_root(table_uri: str) -> bool:
    """Return True when any URI path component belongs to the segregated cvff root."""
    return LakehouseScope.CVFF in scopes_declared_by_uri(table_uri)


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

    A segregated scope root must end in a path component starting with that
    scope's prefix (for example ``.../cvff`` or ``.../isr``) and must not
    declare any other segregated scope; the platform scope root must not
    declare any segregated scope at all. This prevents a writer from being
    pointed at another scope's storage.
    """
    if not scope_root_uri or scope_root_uri != scope_root_uri.strip():
        raise BoundaryViolationError("scope root URI must be canonical non-empty text")
    root = scope_root_uri.rstrip("/")
    terminal = root.rsplit("/", 1)[-1]
    terminal_scopes = frozenset(
        candidate
        for candidate in SEGREGATED_SCOPES
        if candidate.root_component_pattern.match(terminal)
    )
    if scope is LakehouseScope.PLATFORM:
        if terminal_scopes:
            declared = sorted(candidate.value for candidate in terminal_scopes)
            raise BoundaryViolationError(
                "platform scope root must not terminate in a segregated scope path component "
                f"({', '.join(declared)}); refusing to initialize a platform writer against "
                f"{scope_root_uri!r}"
            )
    else:
        if terminal_scopes != frozenset({scope}):
            raise BoundaryViolationError(
                f"{scope.value} scope root must terminate in a {scope.value}* path component "
                "and no other segregated scope component; refusing to initialize a "
                f"{scope.value} writer against {scope_root_uri!r}"
            )
    prefix = scope.layer_prefix
    return LakehouseRoots(
        scope=scope,
        bronze=f"{root}/{prefix}_bronze/events",
        silver=f"{root}/{prefix}_silver/events",
        gold=f"{root}/{prefix}_gold/events",
    )


NAMED_TABLE_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


def scope_layer_table_uri(
    scope: LakehouseScope, scope_root_uri: str, layer: str, table_name: str
) -> str:
    """Resolve a named table URI inside a scope's medallion layer directory.

    Curated products (for example ``mrv_gold/vessel_annual`` or
    ``platform_gold/port_kpi_values``) live as named tables beside the
    layer's canonical ``events`` table. The resolved URI passes through the
    same boundary guard as every other scope table, so a named table can
    never escape the writer's segregated root.
    """
    if not NAMED_TABLE_PATTERN.fullmatch(table_name):
        raise BoundaryViolationError(f"named table {table_name!r} is not a governed table name")
    roots = build_lakehouse_roots(scope, scope_root_uri)
    base = roots.for_layer(layer).rsplit("/", 1)[0]
    uri = f"{base}/{table_name}"
    require_scope_table_uri(scope, uri)
    return uri


def require_scope_table_uri(scope: LakehouseScope, table_uri: str) -> None:
    """Fail closed when a table URI does not belong to the scope's segregated root."""
    declared = scopes_declared_by_uri(table_uri)
    if scope is LakehouseScope.PLATFORM:
        if declared:
            names = sorted(candidate.value for candidate in declared)
            raise BoundaryViolationError(
                f"platform writer cannot target {table_uri!r}: "
                f"segregated root component present ({', '.join(names)})"
            )
        return
    if declared != frozenset({scope}):
        raise BoundaryViolationError(
            f"{scope.value} writer cannot target {table_uri!r}: URI must declare the "
            f"{scope.value}* root component and no other segregated scope root"
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
    Kafka topic or table URI that belongs to another scope.
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
