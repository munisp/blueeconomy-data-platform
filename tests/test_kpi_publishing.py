"""WP-8 KPI publishing tests — seeded, clearly test-scoped rows with known
expected percentiles; INSUFFICIENT_DATA below thresholds; tamper-evident digests.
All data below is synthetic test fixture data, never production data."""

from __future__ import annotations

from datetime import UTC, datetime

from blueeconomy_data_platform.kpi_publishing import (
    CLEARANCE_MIN_N,
    build_snapshot,
    compute_clearance_time_percentiles,
    compute_paper_visit_avoidance,
)

FROM = datetime(2026, 9, 1, tzinfo=UTC)
TO = datetime(2026, 9, 2, tzinfo=UTC)
NOW = TO


def test_clearance_percentiles_known_values() -> None:
    kpi = compute_clearance_time_percentiles([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], FROM, TO, NOW)
    assert kpi.status == "OK"
    assert kpi.provenance["n"] == 10
    assert kpi.value == {"p50": 5, "p90": 9, "p95": 10, "mean": 5.5}
    assert "declarations.submitted_at" in kpi.provenance["sources"]


def test_clearance_insufficient_data_never_zeros() -> None:
    kpi = compute_clearance_time_percentiles([2.0, 4.0], FROM, TO, NOW)
    assert kpi.status == "INSUFFICIENT_DATA"
    assert kpi.value is None
    assert kpi.provenance["n"] == 2
    assert kpi.min_sample_size == CLEARANCE_MIN_N


def test_paper_avoidance_methodology_labelled_estimate() -> None:
    kpi = compute_paper_visit_avoidance(10, FROM, TO, NOW)
    assert kpi.status == "OK"
    assert kpi.value == 5.0
    assert kpi.methodology is not None and "ESTIMATE" in kpi.methodology
    empty = compute_paper_visit_avoidance(0, FROM, TO, NOW)
    assert empty.status == "INSUFFICIENT_DATA"
    assert empty.value is None


def test_snapshot_digest_is_tamper_evident() -> None:
    kpi = compute_clearance_time_percentiles([1, 2, 3, 4, 5], FROM, TO, NOW)
    snap = build_snapshot([kpi], FROM, TO, NOW)
    digest = snap.sha256()
    assert len(digest) == 64
    # Deterministic
    assert build_snapshot([kpi], FROM, TO, NOW).sha256() == digest
    # Any mutation changes the digest
    tampered = compute_paper_visit_avoidance(11, FROM, TO, NOW)
    assert build_snapshot([tampered], FROM, TO, NOW).sha256() != digest
