"""Fail-closed read-access policy for the segregated lakehouse schemas.

Keycloak-style role claims are mapped to the Delta Lake schemas each role may
read. The segregated CVFF schemas (``cvff_bronze``/``cvff_silver``/
``cvff_gold``) are readable only by CVFF-scoped roles: the independent
auditor, NIMASA approver and CBN observer, all read-only on the CVFF scope.
Unknown roles and unknown schemas are denied; no role in this policy is ever
granted write access — writes belong to governed service principals only.
"""

from __future__ import annotations

from collections.abc import Iterable

PLATFORM_SCHEMAS = frozenset({"platform_bronze", "platform_silver", "platform_gold"})
CVFF_SCHEMAS = frozenset({"cvff_bronze", "cvff_silver", "cvff_gold"})
GOVERNED_SCHEMAS = PLATFORM_SCHEMAS | CVFF_SCHEMAS

ROLE_NIMASA_APPROVER = "nimasa-approver"
ROLE_CBN_OBSERVER = "cbn-observer"
ROLE_INDEPENDENT_AUDITOR = "independent-auditor"
ROLE_FMMBE_OVERSIGHT = "fmmbe-oversight"

CVFF_SCOPED_ROLES = frozenset({ROLE_INDEPENDENT_AUDITOR, ROLE_NIMASA_APPROVER, ROLE_CBN_OBSERVER})

# Read-only role-to-schema grants. The CVFF schemas appear only under
# CVFF-scoped roles; platform schemas never appear under CVFF-scoped roles.
ROLE_READ_GRANTS: dict[str, frozenset[str]] = {
    ROLE_INDEPENDENT_AUDITOR: CVFF_SCHEMAS,
    ROLE_NIMASA_APPROVER: CVFF_SCHEMAS,
    ROLE_CBN_OBSERVER: CVFF_SCHEMAS,
    ROLE_FMMBE_OVERSIGHT: PLATFORM_SCHEMAS,
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


def allowed_schemas(roles: Iterable[str]) -> frozenset[str]:
    """Return the union of schemas readable by the supplied role claims."""
    granted: set[str] = set()
    for role in normalize_roles(roles):
        granted.update(ROLE_READ_GRANTS[role])
    return frozenset(granted)


def authorize_read(roles: Iterable[str], schema: str) -> None:
    """Fail closed unless at least one supplied role may read the requested schema.

    Grants are a union across recognized roles, so a principal holding both a
    CVFF-scoped role and a platform oversight role can read both scopes — but
    the CVFF schemas are granted only to CVFF-scoped roles, so no platform-only
    role combination can ever read across the fiduciary boundary.
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


def authorize_write(roles: Iterable[str], schema: str) -> None:
    """Governance roles are read-only; all write attempts fail closed."""
    normalize_roles(roles)
    raise AccessDeniedError(
        f"governance role claims are read-only; write access to schema {schema!r} is denied"
    )
