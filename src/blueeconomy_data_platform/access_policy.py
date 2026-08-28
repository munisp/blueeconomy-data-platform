"""Fail-closed read-access policy for the segregated lakehouse schemas.

Keycloak-style role claims are mapped to the Delta Lake schemas each role may
read. The segregated CVFF schemas (``cvff_bronze``/``cvff_silver``/
``cvff_gold``) are readable only by CVFF-scoped roles: the independent
auditor, NIMASA approver and CBN observer, all read-only on the CVFF scope.
Phase-2 scopes add seafarer credential schemas (``seafarer_*``), fisheries
catch/coldchain/export schemas (``fisheries_*``) and the classified ISR
schemas (``isr_*``), each readable only by their own scoped roles.

Reads against schemas above the UNCLASSIFIED floor additionally require a
clearance claim (UNCLASSIFIED < RESTRICTED < CONFIDENTIAL < SECRET) that meets
or exceeds the schema classification floor. The ``insurer-aggregator`` role is
granted only the declassified ISR outcome aggregates (``isr_gold``, floor
CONFIDENTIAL); the raw and behavioural ISR tracks (``isr_bronze``,
``isr_silver``, floor SECRET) are never readable by it. Unknown roles, unknown
schemas and unknown or missing clearance claims are denied; no role in this
policy is ever granted write access — writes belong to governed service
principals only.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from enum import IntEnum

PLATFORM_SCHEMAS = frozenset({"platform_bronze", "platform_silver", "platform_gold"})
CVFF_SCHEMAS = frozenset({"cvff_bronze", "cvff_silver", "cvff_gold"})
SEAFARER_SCHEMAS = frozenset({"seafarer_bronze", "seafarer_silver", "seafarer_gold"})
FISHERIES_SCHEMAS = frozenset({"fisheries_bronze", "fisheries_silver", "fisheries_gold"})
ISR_SCHEMAS = frozenset({"isr_bronze", "isr_silver", "isr_gold"})
GOVERNED_SCHEMAS = (
    PLATFORM_SCHEMAS | CVFF_SCHEMAS | SEAFARER_SCHEMAS | FISHERIES_SCHEMAS | (ISR_SCHEMAS)
)

ROLE_NIMASA_APPROVER = "nimasa-approver"
ROLE_CBN_OBSERVER = "cbn-observer"
ROLE_INDEPENDENT_AUDITOR = "independent-auditor"
ROLE_FMMBE_OVERSIGHT = "fmmbe-oversight"
ROLE_SEAFARER_REGISTRY = "seafarer-registry"
ROLE_FISHERIES_OPERATIONS = "fisheries-operations"
ROLE_ISR_ANALYST = "isr-analyst"
ROLE_INSURER_AGGREGATOR = "insurer-aggregator"

CVFF_SCOPED_ROLES = frozenset({ROLE_INDEPENDENT_AUDITOR, ROLE_NIMASA_APPROVER, ROLE_CBN_OBSERVER})

# Read-only role-to-schema grants. Each segregated scope's schemas appear only
# under that scope's roles; insurer-aggregator is granted only the ISR gold
# outcome aggregates, never the bronze/silver tracks.
ROLE_READ_GRANTS: dict[str, frozenset[str]] = {
    ROLE_INDEPENDENT_AUDITOR: CVFF_SCHEMAS,
    ROLE_NIMASA_APPROVER: CVFF_SCHEMAS,
    ROLE_CBN_OBSERVER: CVFF_SCHEMAS,
    ROLE_FMMBE_OVERSIGHT: PLATFORM_SCHEMAS,
    ROLE_SEAFARER_REGISTRY: SEAFARER_SCHEMAS,
    ROLE_FISHERIES_OPERATIONS: FISHERIES_SCHEMAS,
    ROLE_ISR_ANALYST: ISR_SCHEMAS,
    ROLE_INSURER_AGGREGATOR: frozenset({"isr_gold"}),
}


class Clearance(IntEnum):
    """Personnel clearance levels, ordered from lowest to highest."""

    UNCLASSIFIED = 0
    RESTRICTED = 1
    CONFIDENTIAL = 2
    SECRET = 3

    @property
    def label(self) -> str:
        return self.name

    @classmethod
    def from_label(cls, label: str) -> Clearance:
        """Parse a canonical clearance label, failing closed on anything unknown."""
        if not isinstance(label, str) or not label or label != label.strip():
            raise ValueError("clearance labels must be canonical non-empty text")
        try:
            return cls[label.upper()]
        except KeyError:
            raise ValueError(
                f"clearance label {label!r} is not one of "
                f"{[level.name for level in cls]}; access fails closed"
            ) from None


# Minimum classification floor a principal's clearance must meet to read a
# schema. The insurer-visible ISR outcome aggregates carry the CONFIDENTIAL
# floor; the raw and behavioural ISR tracks carry the SECRET floor.
SCHEMA_CLASSIFICATION_FLOOR: dict[str, Clearance] = {
    **{schema: Clearance.UNCLASSIFIED for schema in PLATFORM_SCHEMAS},
    **{schema: Clearance.UNCLASSIFIED for schema in CVFF_SCHEMAS},
    **{schema: Clearance.RESTRICTED for schema in FISHERIES_SCHEMAS},
    **{schema: Clearance.CONFIDENTIAL for schema in SEAFARER_SCHEMAS},
    "isr_bronze": Clearance.SECRET,
    "isr_silver": Clearance.SECRET,
    "isr_gold": Clearance.CONFIDENTIAL,
}


class AccessDeniedError(PermissionError):
    """Raised when a role claim set is not granted the requested access."""


def normalize_roles(roles: Iterable[str]) -> frozenset[str]:
    """Validate role claims, failing closed on any unknown role."""
    normalized: set[str] = set()
    for role in roles:
        if not isinstance(role, str) or not role or role != role.strip():
            raise AccessDeniedError("role claims must be canonical non-empty text")
        if role not in ROLE_READ_GRANTS:
            raise AccessDeniedError(f"unknown role claim {role!r} is denied by default")
        normalized.add(role)
    return frozenset(normalized)


def normalize_clearance(clearance: Clearance | str | None) -> Clearance | None:
    """Validate a clearance claim, failing closed on any unknown value."""
    if clearance is None:
        return None
    if isinstance(clearance, Clearance):
        return clearance
    if isinstance(clearance, str):
        try:
            return Clearance.from_label(clearance)
        except ValueError as error:
            raise AccessDeniedError(str(error)) from error
    raise AccessDeniedError("clearance claims must be a Clearance level or its canonical label")


def allowed_schemas(roles: Iterable[str]) -> frozenset[str]:
    """Return the union of schemas readable by the supplied role claims.

    The union reflects role grants only; schemas above the UNCLASSIFIED floor
    still require a sufficient clearance claim at :func:`authorize_read` time.
    """
    granted: set[str] = set()
    for role in normalize_roles(roles):
        granted.update(ROLE_READ_GRANTS[role])
    return frozenset(granted)


def authorize_read(
    roles: Iterable[str], schema: str, clearance: Clearance | str | None = None
) -> None:
    """Fail closed unless a role may read the schema and clearance meets its floor.

    Grants are a union across recognized roles, so a principal holding both a
    CVFF-scoped role and a platform oversight role can read both scopes — but
    the CVFF schemas are granted only to CVFF-scoped roles, so no platform-only
    role combination can ever read across the fiduciary boundary. Schemas
    above the UNCLASSIFIED floor additionally require a clearance claim at or
    above the schema's classification floor; an unknown or absent clearance is
    denied.
    """
    if not isinstance(schema, str) or schema not in GOVERNED_SCHEMAS:
        raise AccessDeniedError(f"schema {schema!r} is not a governed lakehouse schema")
    normalized = normalize_roles(roles)
    if not normalized:
        raise AccessDeniedError("at least one recognized role claim is required")
    if schema not in allowed_schemas(normalized):
        raise AccessDeniedError(
            f"roles {sorted(normalized)} are not granted read access to schema {schema!r}"
        )
    floor = SCHEMA_CLASSIFICATION_FLOOR[schema]
    level = normalize_clearance(clearance)
    if floor > Clearance.UNCLASSIFIED:
        if level is None:
            raise AccessDeniedError(
                f"schema {schema!r} requires a clearance claim at or above {floor.label}"
            )
        if level < floor:
            raise AccessDeniedError(
                f"clearance {level.label} is below the {floor.label} floor of schema {schema!r}"
            )


def clearance_permits(clearance: Clearance | str, record_classification: str) -> bool:
    """Row-level predicate: True when the clearance meets a record's label.

    Used to filter rows carrying a persisted ``record_classification`` column.
    Unknown clearance claims and unknown record labels fail closed (deny).
    """
    try:
        level = Clearance.from_label(clearance) if isinstance(clearance, str) else clearance
        floor = Clearance.from_label(record_classification)
    except ValueError:
        return False
    return level >= floor


def filter_records_by_clearance(
    records: Iterable[Mapping[str, object]],
    clearance: Clearance | str,
    label_column: str = "record_classification",
) -> list[Mapping[str, object]]:
    """Return only records whose persisted classification label the clearance permits.

    Records without a valid label are withheld, so unlabelled classified data
    is never exposed by omission.
    """
    visible: list[Mapping[str, object]] = []
    for record in records:
        label = record.get(label_column)
        if isinstance(label, str) and clearance_permits(clearance, label):
            visible.append(record)
    return visible


def authorize_write(roles: Iterable[str], schema: str) -> None:
    """Governance roles are read-only; all write attempts fail closed."""
    normalize_roles(roles)
    raise AccessDeniedError(
        f"governance role claims are read-only; write access to schema {schema!r} is denied"
    )
