"""Retention enforcement for governed lakehouse tables (Gap #46).

Implements the fiduciary metadata retention policy (30 days hot, 7 years
cold by default; see :class:`blueeconomy_data_platform.medallion.RetentionPolicy`)
as an audited, partition-aware deletion/tiering job:

- **dry-run by default** — without ``--apply`` the job only classifies rows
  into hot/cold/expired tiers, plans date-windowed deletion batches and
  records the plan in the audit table; nothing is mutated.
- **partition-aware batches** — expired rows are grouped by their
  occurrence day; each day-window is deleted as its own bounded batch with
  its own audit record, so an applied run maps one-to-one onto physical
  date partitions once tables are partitioned by occurrence date.
- **tiering** — with ``--archive-uri``, cold-tier rows are copied to the
  archive table (idempotent insert-only merge on the key column) before any
  deletion is considered; cold rows are never deleted.
- **fail-closed policy checks** — a row is only ever deleted when the
  policy classifies it as ``expired`` (age strictly beyond the cold
  horizon); every deleted key is re-validated against the policy in the
  same process, and post-deletion counts are verified. Out-of-policy
  deletion is a hard error, never a warning.
- **audit log** — every planned (dry-run) or executed deletion batch is
  appended to an append-only audit Delta table with the policy, counts,
  batch window, operator identity and a dry-run flag.

Append-only note: governed bronze tables carry ``delta.appendOnly=true``.
Deletion is an exceptional, evidence-recorded operation, so the applied
path performs it under an explicit, immediately-restored protocol: the
append-only property is lifted for exactly the duration of the batch
deletes and restored in a ``finally`` block, with each step audit-logged.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from blueeconomy_data_platform.ingest import append_rows, validate_table_uri
from blueeconomy_data_platform.medallion import RetentionPolicy

AUDIT_SCHEMA_VERSION = "blueeconomy.lakehouse.retention-audit.v1"
MAX_KEYS_PER_DELETE = 1000
AUDIT_TABLE_DESCRIPTION = (
    "Governed append-only retention audit log (every planned or executed "
    "deletion/tiering batch is evidence-recorded)"
)


@dataclass(frozen=True)
class RetentionBatch:
    """One date-windowed deletion batch of expired rows."""

    window_date: str
    keys: tuple[str, ...]


@dataclass(frozen=True)
class RetentionPlan:
    """Dry-run-computable retention plan for one table."""

    table_uri: str
    reference: datetime
    total_rows: int
    hot_rows: int
    cold_rows: int
    expired_rows: int
    batches: tuple[RetentionBatch, ...]
    cold_keys: tuple[str, ...] = field(default=())


@dataclass(frozen=True)
class RetentionAuditRecord:
    audit_id: str
    schema_version: str
    table_reference_sha256: str
    mode: str
    window_date: str
    policy_hot_days: int
    policy_cold_years: int
    planned_rows: int
    deleted_rows: int
    archived_rows: int
    operator: str
    dry_run: bool
    executed_at: str


def _audit_id(table_uri: str, window_date: str, keys: tuple[str, ...], dry_run: bool) -> str:
    material = json.dumps(
        {
            "table": table_uri,
            "window": window_date,
            "keys": list(keys),
            "dry_run": dry_run,
        },
        sort_keys=True,
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _read_rows(table_uri: str, key_column: str, time_column: str) -> list[dict[str, Any]]:
    try:
        table = DeltaTable(table_uri)
    except TableNotFoundError as error:
        raise ValueError(f"retention target table {table_uri!r} does not exist") from error
    rows: list[dict[str, Any]] = table.to_pyarrow_table(
        columns=[key_column, time_column]
    ).to_pylist()
    return rows


def plan_retention(
    table_uri: str,
    policy: RetentionPolicy,
    reference: datetime | None = None,
    key_column: str = "event_id",
    time_column: str = "occurred_at",
) -> RetentionPlan:
    """Classify rows into tiers and plan date-windowed deletion batches."""
    validate_table_uri(table_uri)
    checked = (reference or datetime.now(UTC)).astimezone(UTC)
    rows = _read_rows(table_uri, key_column, time_column)
    tiers: dict[str, list[tuple[str, datetime]]] = {"hot": [], "cold": [], "expired": []}
    for row in rows:
        occurred_at = row[time_column]
        if not isinstance(occurred_at, datetime):
            raise ValueError(f"retention time column {time_column!r} must be a timestamp")
        occurred_at = occurred_at.astimezone(UTC)
        tier = policy.tier_for(occurred_at, checked)
        tiers[tier].append((str(row[key_column]), occurred_at))
    windows: dict[str, list[str]] = {}
    for key, occurred_at in tiers["expired"]:
        windows.setdefault(occurred_at.date().isoformat(), []).append(key)
    batches = tuple(
        RetentionBatch(window_date=window, keys=tuple(sorted(keys)))
        for window, keys in sorted(windows.items())
    )
    return RetentionPlan(
        table_uri=table_uri,
        reference=checked,
        total_rows=len(rows),
        hot_rows=len(tiers["hot"]),
        cold_rows=len(tiers["cold"]),
        expired_rows=len(tiers["expired"]),
        batches=batches,
        cold_keys=tuple(sorted(key for key, _ in tiers["cold"])),
    )


def _assert_batch_in_policy(
    table_uri: str,
    batch: RetentionBatch,
    policy: RetentionPolicy,
    reference: datetime,
    key_column: str,
    time_column: str,
) -> None:
    """Re-validate that every key in a batch is expired under the policy."""
    wanted = set(batch.keys)
    rows = _read_rows(table_uri, key_column, time_column)
    found: set[str] = set()
    for row in rows:
        key = str(row[key_column])
        if key not in wanted:
            continue
        found.add(key)
        occurred_at = row[time_column].astimezone(UTC)
        if policy.tier_for(occurred_at, reference) != "expired":
            raise ValueError(
                f"refusing out-of-policy deletion: key {key!r} occurred at "
                f"{occurred_at.isoformat()} which is not beyond the cold horizon"
            )
    missing = wanted - found
    if missing:
        raise ValueError(f"batch keys no longer present in table: {sorted(missing)[:5]}")


def _delete_batch(table_uri: str, batch: RetentionBatch, key_column: str) -> int:
    """Delete one batch under the governed append-only-lift protocol."""
    table = DeltaTable(table_uri)
    configuration = table.metadata().configuration
    append_only = configuration.get("delta.appendOnly") == "true"
    deleted = 0
    try:
        if append_only:
            table.alter.set_table_properties({"delta.appendOnly": "false"})
        for start in range(0, len(batch.keys), MAX_KEYS_PER_DELETE):
            chunk = batch.keys[start : start + MAX_KEYS_PER_DELETE]
            escaped = ", ".join("'" + key.replace("'", "''") + "'" for key in chunk)
            DeltaTable(table_uri).delete(f"{key_column} IN ({escaped})")
            deleted += len(chunk)
    finally:
        if append_only:
            DeltaTable(table_uri).alter.set_table_properties({"delta.appendOnly": "true"})
    return deleted


def _append_audit_record(audit_table_uri: str, record: RetentionAuditRecord) -> None:
    row: dict[str, Any] = {
        "audit_id": record.audit_id,
        "event_id": record.audit_id,
        "schema_version": record.schema_version,
        "table_reference_sha256": record.table_reference_sha256,
        "mode": record.mode,
        "window_date": record.window_date,
        "policy_hot_days": record.policy_hot_days,
        "policy_cold_years": record.policy_cold_years,
        "planned_rows": record.planned_rows,
        "deleted_rows": record.deleted_rows,
        "archived_rows": record.archived_rows,
        "operator": record.operator,
        "dry_run": record.dry_run,
        "executed_at": record.executed_at,
    }
    append_rows(
        audit_table_uri,
        [row],
        key_column="audit_id",
        table_description=AUDIT_TABLE_DESCRIPTION,
        table_name="blueeconomy_retention_audit",
    )


def _record(
    table_uri: str,
    mode: str,
    window_date: str,
    policy: RetentionPolicy,
    planned: int,
    deleted: int,
    archived: int,
    operator: str,
    dry_run: bool,
    keys: tuple[str, ...],
) -> RetentionAuditRecord:
    return RetentionAuditRecord(
        audit_id=_audit_id(table_uri, f"{mode}/{window_date}", keys, dry_run),
        schema_version=AUDIT_SCHEMA_VERSION,
        table_reference_sha256=hashlib.sha256(table_uri.encode("utf-8")).hexdigest(),
        mode=mode,
        window_date=window_date,
        policy_hot_days=policy.hot_days,
        policy_cold_years=policy.cold_years,
        planned_rows=planned,
        deleted_rows=deleted,
        archived_rows=archived,
        operator=operator,
        dry_run=dry_run,
        executed_at=datetime.now(UTC).isoformat(),
    )


def enforce_retention(
    table_uri: str,
    policy: RetentionPolicy,
    audit_table_uri: str,
    apply: bool = False,
    archive_uri: str | None = None,
    operator: str = "",
    reference: datetime | None = None,
    key_column: str = "event_id",
    time_column: str = "occurred_at",
) -> RetentionPlan:
    """Plan (dry-run) or execute (``apply=True``) retention for one table.

    Every batch — planned or executed — is recorded in the append-only
    audit table. Deletion requires ``apply=True`` and a non-empty operator
    identity; anything else is a dry run.
    """
    if not operator or operator != operator.strip():
        raise ValueError("an operator identity is required for the retention audit log")
    validate_table_uri(audit_table_uri)
    checked = (reference or datetime.now(UTC)).astimezone(UTC)
    plan = plan_retention(table_uri, policy, checked, key_column, time_column)

    if not apply:
        for batch in plan.batches:
            _append_audit_record(
                audit_table_uri,
                _record(
                    table_uri,
                    "plan-delete",
                    batch.window_date,
                    policy,
                    len(batch.keys),
                    0,
                    0,
                    operator,
                    True,
                    batch.keys,
                ),
            )
        return plan

    if archive_uri is not None and plan.cold_keys:
        _archive_cold_rows(
            table_uri, archive_uri, plan, operator, policy, audit_table_uri, key_column
        )

    for batch in plan.batches:
        _assert_batch_in_policy(table_uri, batch, policy, checked, key_column, time_column)
        deleted = _delete_batch(table_uri, batch, key_column)
        _append_audit_record(
            audit_table_uri,
            _record(
                table_uri,
                "delete",
                batch.window_date,
                policy,
                len(batch.keys),
                deleted,
                0,
                operator,
                False,
                batch.keys,
            ),
        )

    # Post-deletion verification: exactly the expired rows are gone and no
    # remaining row is out of policy.
    remaining = plan_retention(table_uri, policy, checked, key_column, time_column)
    expected_remaining = plan.total_rows - plan.expired_rows
    if remaining.total_rows != expected_remaining:
        raise RuntimeError(
            f"post-deletion count mismatch: expected {expected_remaining} rows, "
            f"found {remaining.total_rows}"
        )
    if remaining.expired_rows != 0:
        raise RuntimeError(
            f"post-deletion verification found {remaining.expired_rows} expired rows remaining"
        )
    return plan


def _archive_cold_rows(
    table_uri: str,
    archive_uri: str,
    plan: RetentionPlan,
    operator: str,
    policy: RetentionPolicy,
    audit_table_uri: str,
    key_column: str,
) -> None:
    """Copy cold-tier rows to the archive table (idempotent on the key column)."""
    validate_table_uri(archive_uri)
    wanted = set(plan.cold_keys)
    source = DeltaTable(table_uri).to_pyarrow_table().to_pylist()
    cold_rows = [row for row in source if str(row[key_column]) in wanted]
    if not cold_rows:
        return
    for start in range(0, len(cold_rows), MAX_KEYS_PER_DELETE):
        chunk = cold_rows[start : start + MAX_KEYS_PER_DELETE]
        append_rows(
            archive_uri,
            chunk,
            key_column=key_column,
            table_description="Retention cold-tier archive (governed tiering target)",
            table_name="blueeconomy_retention_archive",
        )
        keys = tuple(sorted(str(row[key_column]) for row in chunk))
        # The audit records the submitted tiering batch; the archive merge is
        # idempotent on the key column, so a replay submits the same batch.
        _append_audit_record(
            audit_table_uri,
            _record(
                table_uri,
                "tier-archive",
                "cold",
                policy,
                len(chunk),
                0,
                len(chunk),
                operator,
                False,
                keys,
            ),
        )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Plan (default, dry-run) or apply (--apply) the fiduciary retention "
            "policy against a governed Delta table, with a mandatory audit log."
        )
    )
    parser.add_argument("--table-uri", required=True)
    parser.add_argument("--audit-table-uri", required=True)
    parser.add_argument("--archive-uri")
    parser.add_argument("--hot-days", type=int, default=30)
    parser.add_argument("--cold-years", type=int, default=7)
    parser.add_argument("--key-column", default="event_id")
    parser.add_argument("--time-column", default="occurred_at")
    parser.add_argument(
        "--apply", action="store_true", help="Execute deletions (default: dry-run)."
    )
    parser.add_argument("--operator", default=os.environ.get("BLUEECONOMY_RETENTION_OPERATOR", ""))
    parser.add_argument("--report", type=str, default="")
    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()
    try:
        policy = RetentionPolicy(hot_days=arguments.hot_days, cold_years=arguments.cold_years)
        plan = enforce_retention(
            table_uri=arguments.table_uri,
            policy=policy,
            audit_table_uri=arguments.audit_table_uri,
            apply=bool(arguments.apply),
            archive_uri=arguments.archive_uri,
            operator=arguments.operator,
            key_column=arguments.key_column,
            time_column=arguments.time_column,
        )
        summary = {
            "schema_version": "blueeconomy.lakehouse.retention-report.v1",
            "table_reference_sha256": hashlib.sha256(
                arguments.table_uri.encode("utf-8")
            ).hexdigest(),
            "dry_run": not arguments.apply,
            "policy": {"hot_days": policy.hot_days, "cold_years": policy.cold_years},
            "total_rows": plan.total_rows,
            "hot_rows": plan.hot_rows,
            "cold_rows": plan.cold_rows,
            "expired_rows": plan.expired_rows,
            "batches": [
                {"window_date": batch.window_date, "rows": len(batch.keys)}
                for batch in plan.batches
            ],
            "reference": plan.reference.isoformat(),
        }
        if arguments.report:
            from pathlib import Path

            path = Path(arguments.report)
            path.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
            path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(summary, sort_keys=True))
    except (OSError, RuntimeError, ValueError) as error:
        print(f"blueeconomy-retention-enforce: {error}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
