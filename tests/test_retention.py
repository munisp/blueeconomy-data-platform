from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from deltalake import DeltaTable

from blueeconomy_data_platform.ingest import append_events
from blueeconomy_data_platform.medallion import RetentionPolicy
from blueeconomy_data_platform.retention import (
    enforce_retention,
    main,
    plan_retention,
)

REFERENCE = datetime(2026, 8, 12, 12, 0, tzinfo=UTC)
POLICY = RetentionPolicy(hot_days=30, cold_years=7)


def event(event_id: str, occurred_at: datetime) -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_type": "ports.gate.scan.v1",
        "producer": "test-producer",
        "occurred_at": occurred_at,
        "recorded_at": occurred_at,
        "data_classification": "internal",
        "source_system": "test-system",
        "source_record_reference": f"ref-{event_id}",
        "correlation_id": None,
        "payload_json": json.dumps({"ref": event_id}),
        "ingested_at": occurred_at,
    }


def seed_table(tmp_path: Path) -> str:
    table_uri = str(tmp_path / "platform" / "bronze" / "events")
    events = [
        event("hot-1", REFERENCE - timedelta(days=1)),
        event("hot-2", REFERENCE - timedelta(days=29)),
        event("cold-1", REFERENCE - timedelta(days=31)),
        event("cold-2", REFERENCE - timedelta(days=365 * 7 - 10)),
        event("expired-1", REFERENCE - timedelta(days=365 * 7 + 1)),
        event("expired-2", REFERENCE - timedelta(days=365 * 7 + 40)),
    ]
    append_events(table_uri, events)
    return table_uri


def audit_uri(tmp_path: Path) -> str:
    return str(tmp_path / "platform" / "audit" / "retention_audit")


def audit_rows(tmp_path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = DeltaTable(audit_uri(tmp_path)).to_pyarrow_table().to_pylist()
    return rows


def test_plan_classifies_hot_cold_expired(tmp_path: Path) -> None:
    table_uri = seed_table(tmp_path)
    plan = plan_retention(table_uri, POLICY, REFERENCE)
    assert (plan.total_rows, plan.hot_rows, plan.cold_rows, plan.expired_rows) == (6, 2, 2, 2)
    assert [batch.window_date for batch in plan.batches] == sorted(
        batch.window_date for batch in plan.batches
    )
    keys = {key for batch in plan.batches for key in batch.keys}
    assert keys == {"expired-1", "expired-2"}
    assert set(plan.cold_keys) == {"cold-1", "cold-2"}


def test_dry_run_is_default_and_mutates_nothing(tmp_path: Path) -> None:
    table_uri = seed_table(tmp_path)
    plan = enforce_retention(
        table_uri, POLICY, audit_uri(tmp_path), operator="retention-bot", reference=REFERENCE
    )
    assert DeltaTable(table_uri).to_pyarrow_table().num_rows == 6
    rows = audit_rows(tmp_path)
    assert len(rows) == len(plan.batches)
    assert all(row["dry_run"] is True for row in rows)
    assert all(row["mode"] == "plan-delete" for row in rows)
    assert all(row["deleted_rows"] == 0 for row in rows)
    assert all(row["policy_hot_days"] == 30 and row["policy_cold_years"] == 7 for row in rows)
    # Dry-run re-execution is idempotent on the audit identity.
    enforce_retention(
        table_uri, POLICY, audit_uri(tmp_path), operator="retention-bot", reference=REFERENCE
    )
    assert len(audit_rows(tmp_path)) == len(plan.batches)


def test_apply_deletes_only_expired_rows_and_restores_append_only(tmp_path: Path) -> None:
    table_uri = seed_table(tmp_path)
    plan = enforce_retention(
        table_uri,
        POLICY,
        audit_uri(tmp_path),
        apply=True,
        operator="retention-bot",
        reference=REFERENCE,
    )
    table = DeltaTable(table_uri)
    remaining = {row["event_id"] for row in table.to_pyarrow_table().to_pylist()}
    assert remaining == {"hot-1", "hot-2", "cold-1", "cold-2"}
    assert table.metadata().configuration["delta.appendOnly"] == "true"
    delete_records = [row for row in audit_rows(tmp_path) if row["mode"] == "delete"]
    assert len(delete_records) == len(plan.batches)
    assert sum(int(str(row["deleted_rows"])) for row in delete_records) == 2
    assert all(row["dry_run"] is False for row in delete_records)
    assert all(row["operator"] == "retention-bot" for row in delete_records)


def test_apply_never_deletes_hot_or_cold_rows(tmp_path: Path) -> None:
    # Even with --apply, nothing within the 7-year cold horizon is deleted.
    table_uri = str(tmp_path / "platform" / "bronze" / "events")
    events = [
        event("young-1", REFERENCE - timedelta(hours=1)),
        event("young-2", REFERENCE - timedelta(days=365 * 7 - 1)),
    ]
    append_events(table_uri, events)
    plan = enforce_retention(
        table_uri,
        POLICY,
        audit_uri(tmp_path),
        apply=True,
        operator="retention-bot",
        reference=REFERENCE,
    )
    assert plan.expired_rows == 0
    assert DeltaTable(table_uri).to_pyarrow_table().num_rows == 2
    assert not (tmp_path / "platform" / "audit" / "retention_audit").exists()


def test_apply_with_tiering_archives_cold_rows(tmp_path: Path) -> None:
    table_uri = seed_table(tmp_path)
    archive = str(tmp_path / "platform" / "archive" / "events_cold")
    enforce_retention(
        table_uri,
        POLICY,
        audit_uri(tmp_path),
        apply=True,
        archive_uri=archive,
        operator="retention-bot",
        reference=REFERENCE,
    )
    archived = {row["event_id"] for row in DeltaTable(archive).to_pyarrow_table().to_pylist()}
    assert archived == {"cold-1", "cold-2"}
    # Expired rows are deleted, never archived.
    remaining = {row["event_id"] for row in DeltaTable(table_uri).to_pyarrow_table().to_pylist()}
    assert remaining == {"hot-1", "hot-2", "cold-1", "cold-2"}
    modes = {row["mode"] for row in audit_rows(tmp_path)}
    assert modes == {"delete", "tier-archive"}
    # Re-applying is idempotent: archive merge and audit identity hold.
    enforce_retention(
        table_uri,
        POLICY,
        audit_uri(tmp_path),
        apply=True,
        archive_uri=archive,
        operator="retention-bot",
        reference=REFERENCE,
    )
    assert DeltaTable(archive).to_pyarrow_table().num_rows == 2


def test_operator_identity_is_mandatory(tmp_path: Path) -> None:
    table_uri = seed_table(tmp_path)
    with pytest.raises(ValueError, match="operator"):
        enforce_retention(
            table_uri, POLICY, audit_uri(tmp_path), operator="  ", reference=REFERENCE
        )


def test_retention_policy_bounds_are_enforced() -> None:
    with pytest.raises(ValueError):
        RetentionPolicy(hot_days=0)
    with pytest.raises(ValueError):
        RetentionPolicy(hot_days=30, cold_years=0)
    with pytest.raises(ValueError):
        RetentionPolicy(hot_days=4000, cold_years=7)


def test_cli_dry_run_and_apply(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    table_uri = seed_table(tmp_path)
    # The CLI evaluates against wall-clock now; derive expectations the same way.
    expected = plan_retention(table_uri, POLICY)
    report = tmp_path / "report.json"
    argv = [
        "blueeconomy-retention-enforce",
        "--table-uri",
        table_uri,
        "--audit-table-uri",
        audit_uri(tmp_path),
        "--operator",
        "retention-bot",
        "--report",
        str(report),
    ]
    monkeypatch.setattr("sys.argv", argv)
    main()
    summary = json.loads(report.read_text())
    assert summary["dry_run"] is True
    assert summary["expired_rows"] == expected.expired_rows
    assert summary["expired_rows"] > 0
    assert DeltaTable(table_uri).to_pyarrow_table().num_rows == 6
    monkeypatch.setattr("sys.argv", [*argv, "--apply"])
    main()
    assert DeltaTable(table_uri).to_pyarrow_table().num_rows == 6 - expected.expired_rows
    summary = json.loads(report.read_text())
    assert summary["dry_run"] is False
    assert summary["total_rows"] == 6  # plan reflects the pre-deletion table
