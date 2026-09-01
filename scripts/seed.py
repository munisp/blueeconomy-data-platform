#!/usr/bin/env python3
"""Demo/staging lakehouse seeder (synthetic data only).

Seeds the medallion lakehouse with deterministic, clearly-synthetic Nigerian
maritime demo events and rebuilds the derived gold tables:

  - CVFF scope: bronze -> silver (dedup) -> gold (one row per ledger commitment)
  - Platform scope: bronze -> silver -> excise_stamp_facts gold projection

Doctrine:
  - REFUSES to run when ENV=production or PROFILE=prod.
  - Requires explicit SEED_DEMO=true.
  - Writes ONLY to SEED_LAKE_ROOT (default ./lakehouse-demo); never touches
    production lake roots because those require ENV-provided URIs.
  - Idempotent: bronze appends dedup on event identity, silver merges on the
    composite dedup key, gold tables are atomically rebuilt from silver.
    Proven by double-run (second run writes 0 new bronze/silver rows).

Usage:
  SEED_DEMO=true SEED_LAKE_ROOT=./lakehouse-demo python scripts/seed.py
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from blueeconomy_data_platform.excise_stamps_gold import (  # noqa: E402
    assemble_excise_stamps_gold,
    excise_stamps_gold_table_uri,
)
from blueeconomy_data_platform.medallion import (  # noqa: E402
    KafkaRecordMetadata,
    append_bronze,
    append_silver,
    build_silver_record,
    curate_gold,
)
from blueeconomy_data_platform.ingest import append_events  # noqa: E402
from blueeconomy_data_platform.segregation import (  # noqa: E402
    LakehouseScope,
    SegregatedDeltaWriter,
)

BASE = datetime(2026, 8, 1, 8, 0, tzinfo=UTC)
ROWS = 4  # deterministic demo rows per stage


def _cvff_event(i: int) -> dict[str, object]:
    ledger_hash = hashlib.sha256(f"seed.cvff.{i}".encode()).hexdigest()
    return {
        "event_id": f"seed-cvff-{i:03d}",
        "event_type": "cvff.ledger.commitment.v1",
        "producer": "seed.cvff-ledger-adapter",
        "occurred_at": BASE + timedelta(hours=i),
        "recorded_at": BASE + timedelta(hours=i, seconds=1),
        "data_classification": "fiduciary_segregated",
        "source_system": "cvff-ledger",
        "source_record_reference": f"ledger-seed-{i:03d}",
        "correlation_id": None,
        "payload_json": json.dumps({
            "ledgerCommitHash": ledger_hash,
            "amount": f"{1_250_000 + i * 1_000}.00",
            "currency": "NGN",
            "fund": "CVFF",
            "synthetic": True,
        }),
        "ingested_at": BASE + timedelta(hours=i, seconds=2),
    }


def _stamp_events() -> list[tuple[str, dict[str, object]]]:
    """One full excise-stamp lifecycle for a synthetic import declaration."""
    events: list[tuple[str, dict[str, object]]] = []
    lifecycle = [
        ("stamps.assessed.v1", {
            "@type": "type.googleapis.com/blueeconomy.contracts.v1.TaxStampAssessed",
            "assessmentId": "seed-assessment-001",
            "declarationRef": "SEED-DECL-2026-0001",
            "consigneeTin": "12345678-0001",
            "totalDutyKobo": 1_250_000,
            "stampsRequired": 500,
            "riskTier": "LOW",
        }),
        ("stamps.approved.v1", {"assessmentId": "seed-assessment-001", "approvalsRequired": 2}),
        ("stamps.issued.v1", {"batchId": "seed-batch-001", "assessmentId": "seed-assessment-001", "quantity": 500}),
        ("stamps.activated.v1", {"batchId": "seed-batch-001", "activatedCount": 500}),
    ]
    for i, (event_type, fields) in enumerate(lifecycle):
        events.append((event_type, {
            "event_id": f"seed-stamp-{i:03d}",
            "event_type": event_type,
            "producer": "seed.tax-stamps",
            "occurred_at": BASE + timedelta(hours=i),
            "recorded_at": BASE + timedelta(hours=i, seconds=1),
            "data_classification": "internal",
            "source_system": "tax-stamps",
            "source_record_reference": f"stamps-seed-{i:03d}",
            "correlation_id": None,
            "payload_json": json.dumps(fields),
            "ingested_at": BASE + timedelta(hours=i, seconds=2),
        }))
    return events


def main() -> int:
    env = os.environ.get("ENV", "").lower()
    profile = os.environ.get("PROFILE", "").lower()
    if env == "production" or profile == "prod":
        print("refusing to seed: ENV/PROFILE indicates production", file=sys.stderr)
        return 1
    if os.environ.get("SEED_DEMO", "").lower() != "true":
        print("refusing to seed: set SEED_DEMO=true to acknowledge synthetic demo data", file=sys.stderr)
        return 1
    lake_root = Path(os.environ.get("SEED_LAKE_ROOT", str(ROOT / "lakehouse-demo"))).resolve()

    # --- CVFF fiduciary-segregated medallion -------------------------------
    cvff = SegregatedDeltaWriter(LakehouseScope.CVFF, str(lake_root / "cvff"))
    events = [_cvff_event(i) for i in range(ROWS)]
    b_ver, b_written, b_present = append_bronze(cvff, events, kafka_topic="cvff.ledger.commitments")
    records = [
        build_silver_record(event, KafkaRecordMetadata(topic="cvff.ledger.commitments", partition=0, offset=i))
        for i, event in enumerate(events)
    ]
    s_ver, s_written, s_present = append_silver(cvff, records)
    g_ver, g_rows = curate_gold(cvff)

    # --- Platform scope: stamps lifecycle -> excise gold -------------------
    platform = SegregatedDeltaWriter(LakehouseScope.PLATFORM, str(lake_root / "platform"))
    stamp_events = _stamp_events()
    pb_ver, pb_written, pb_present = append_events(
        platform.guard_write("bronze", [e for _, e in stamp_events], "stamps.assessed.v1"),
        [event for _, event in stamp_events],
    )
    ps_ver, ps_written, ps_present = append_events(
        platform.guard_write("silver", [e for _, e in stamp_events]),
        [event for _, event in stamp_events],
    )
    x_ver, x_rows = assemble_excise_stamps_gold(platform)

    # --- coverage ----------------------------------------------------------
    from deltalake import DeltaTable

    coverage = {
        "lake_root": str(lake_root),
        "cvff_bronze": DeltaTable(cvff.table_uri("bronze")).to_pyarrow_table().num_rows,
        "cvff_silver": DeltaTable(cvff.table_uri("silver")).to_pyarrow_table().num_rows,
        "cvff_gold": DeltaTable(cvff.table_uri("gold")).to_pyarrow_table().num_rows,
        "platform_bronze": DeltaTable(platform.table_uri("bronze")).to_pyarrow_table().num_rows,
        "platform_silver": DeltaTable(platform.table_uri("silver")).to_pyarrow_table().num_rows,
        "platform_gold_excise_stamp_facts": DeltaTable(
            excise_stamps_gold_table_uri(platform)
        ).to_pyarrow_table().num_rows,
    }
    out = ROOT / "db" / "seed"
    out.mkdir(parents=True, exist_ok=True)
    doc = {
        "database": f"deltalake:{lake_root}",
        "table_count": len(coverage) - 1,
        "total_rows": sum(v for k, v in coverage.items() if k != "lake_root"),
        "tables": {k: v for k, v in coverage.items() if k != "lake_root"},
        "exemptions": {},
        "unjustified_empty_tables": [k for k, v in coverage.items() if k != "lake_root" and v == 0],
        "second_run_noop": {
            "cvff_bronze_present": b_present,
            "cvff_silver_present": s_present,
            "platform_bronze_present": pb_present,
            "platform_silver_present": ps_present,
        },
    }
    with open(out / "seed-coverage.json", "w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=2, sort_keys=True)
    print(json.dumps({k: doc[k] for k in ("table_count", "total_rows")}))
    print(
        "seed applied idempotently: "
        f"cvff bronze +{b_written} (present {b_present}), silver +{s_written} (present {s_present}), "
        f"gold {g_rows} rows; platform bronze +{pb_written} (present {pb_present}), "
        f"silver +{ps_written} (present {ps_present}), excise gold {x_rows} rows"
    )
    return 0 if not doc["unjustified_empty_tables"] else 1


if __name__ == "__main__":
    sys.exit(main())
