"""WP-8 — Operational KPI publishing from governed platform event data.

Computes publication-grade operational KPIs from REAL event rows (the same
governed envelope table written by ``ingest.py``). Binding doctrine:

- No fabricated metrics: every KPI is derived from input rows only.
- Every KPI carries provenance: source topics/tables, sample size n, window,
  and computation timestamp.
- Below the per-KPI minimum sample the KPI reports ``INSUFFICIENT_DATA`` with
  ``value=None`` — never zeros-as-real.
- Estimates carry an explicit methodology label.
- Signed snapshot export: sha256 digest over a canonical (JCS-like, sorted-key,
  compact-separator) JSON serialization; signing itself is performed by the
  platform signer (singlewindow envelope v1.0 Ed25519 JWS) — this module never
  fabricates signatures.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any, Iterable

CLEARANCE_MIN_N = 5

PAPER_AVOIDANCE_METHODOLOGY = (
    "ESTIMATE: electronic_documents x 0.5 visits/document. Rule-of-thumb: a "
    "typical 2-document declaration pack otherwise requires one physical "
    "lodgement visit. Not a measured value."
)


@dataclass(frozen=True)
class Kpi:
    id: str
    label: str
    unit: str
    status: str  # "OK" | "INSUFFICIENT_DATA"
    min_sample_size: int
    value: Any
    window: dict[str, str]
    provenance: dict[str, Any]
    methodology: str | None = None
    classification: str = "PUBLIC"


@dataclass(frozen=True)
class KpiSnapshot:
    envelope_version: str
    classification: str
    producer: str
    generated_at: str
    window: dict[str, str]
    kpis: list[dict[str, Any]] = field(default_factory=list)

    def canonical_json(self) -> str:
        """Deterministic JSON (sorted keys, compact separators) for digesting."""
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


def _percentile(sorted_asc: list[float], p: float) -> float:
    """Nearest-rank percentile; p in (0, 100]."""
    if not sorted_asc:
        raise ValueError("percentile of empty set")
    rank = math.ceil(p / 100 * len(sorted_asc))
    return sorted_asc[min(len(sorted_asc), max(1, rank)) - 1]


def _window(from_: datetime, to: datetime) -> dict[str, str]:
    return {"from": from_.astimezone(UTC).isoformat(), "to": to.astimezone(UTC).isoformat()}


def _prov(sources: list[str], n: int, computed_at: datetime) -> dict[str, Any]:
    return {"sources": sources, "n": n, "computedAt": computed_at.astimezone(UTC).isoformat()}


def compute_clearance_time_percentiles(
    clearance_durations_hours: Iterable[float],
    from_: datetime,
    to: datetime,
    computed_at: datetime,
) -> Kpi:
    """Clearance time percentiles from declaration submitted/cleared timestamps."""
    hours = sorted(h for h in clearance_durations_hours if math.isfinite(h) and h >= 0)
    n = len(hours)
    ok = n >= CLEARANCE_MIN_N
    value = None
    if ok:
        value = {
            "p50": _percentile(hours, 50),
            "p90": _percentile(hours, 90),
            "p95": _percentile(hours, 95),
            "mean": sum(hours) / n,
        }
    return Kpi(
        id="clearance_time_hours",
        label="Declaration clearance time (submission to clearance)",
        unit="hours",
        status="OK" if ok else "INSUFFICIENT_DATA",
        min_sample_size=CLEARANCE_MIN_N,
        value=value,
        window=_window(from_, to),
        provenance=_prov(["declarations.submitted_at", "declarations.cleared_at"], n, computed_at),
    )


def compute_paper_visit_avoidance(
    electronic_document_count: int,
    from_: datetime,
    to: datetime,
    computed_at: datetime,
) -> Kpi:
    ok = electronic_document_count >= 1
    return Kpi(
        id="paper_visits_avoided",
        label="Estimated physical counter visits avoided (e-lodgement)",
        unit="visits (estimated)",
        status="OK" if ok else "INSUFFICIENT_DATA",
        min_sample_size=1,
        value=round(electronic_document_count * 0.5, 2) if ok else None,
        window=_window(from_, to),
        provenance=_prov(["declaration_documents.created_at"], electronic_document_count, computed_at),
        methodology=PAPER_AVOIDANCE_METHODOLOGY,
    )


def build_snapshot(kpis: list[Kpi], from_: datetime, to: datetime, computed_at: datetime) -> KpiSnapshot:
    return KpiSnapshot(
        envelope_version="1.0",
        classification="PUBLIC",
        producer="blueeconomy-data-platform",
        generated_at=computed_at.astimezone(UTC).isoformat(),
        window=_window(from_, to),
        kpis=[asdict(k) for k in kpis],
    )
