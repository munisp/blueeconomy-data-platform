from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from deltalake import DeltaTable

from blueeconomy_data_platform.access_policy import AccessDeniedError
from blueeconomy_data_platform.export_consignment import read_export_consignments
from blueeconomy_data_platform.gold_assembly import load_config, main
from blueeconomy_data_platform.medallion import (
    KafkaRecordMetadata,
    append_bronze,
    append_silver,
    build_silver_record,
)
from blueeconomy_data_platform.segregation import LakehouseScope, SegregatedDeltaWriter

BASE_TIME = datetime(2026, 8, 12, 8, 0, tzinfo=UTC)
LEDGER_HASH = "d" * 64


def cvff_event(event_id: str) -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_type": "cvff.ledger.commitment.v1",
        "producer": "cvff-ledger-adapter",
        "occurred_at": BASE_TIME,
        "recorded_at": BASE_TIME,
        "data_classification": "fiduciary_segregated",
        "source_system": "cvff-ledger",
        "source_record_reference": f"ledger-{event_id}",
        "correlation_id": None,
        "payload_json": json.dumps({"ledgerCommitHash": LEDGER_HASH}),
        "ingested_at": BASE_TIME,
    }


def fisheries_event(
    event_id: str,
    event_type: str,
    consignment_id: str,
    payload: dict[str, object],
    label: str | None = None,
) -> dict[str, object]:
    event: dict[str, object] = {
        "event_id": event_id,
        "event_type": event_type,
        "producer": "fisheries-gateway",
        "occurred_at": BASE_TIME,
        "recorded_at": BASE_TIME,
        "data_classification": "fisheries_operational",
        "source_system": "fisheries-operations",
        "source_record_reference": f"src-{event_id}",
        "correlation_id": None,
        "payload_json": json.dumps({**payload, "consignmentId": consignment_id}),
        "ingested_at": BASE_TIME,
    }
    if label is not None:
        event["record_classification"] = label
    return event


def consignment_events(consignment_id: str, label: str | None = None) -> list[dict[str, object]]:
    return [
        fisheries_event(
            f"{consignment_id}-catch",
            "fisheries.catch.v1",
            consignment_id,
            {"speciesCode": "TUNA-YFT", "catchWeightKg": 125.5},
            label,
        ),
        fisheries_event(
            f"{consignment_id}-export",
            "export.declaration.v1",
            consignment_id,
            {"exportReference": "EXP-8899"},
            label,
        ),
    ]


def gold_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    scope: str,
    clearance: str | None = None,
) -> dict[str, Path]:
    paths = {
        "root": tmp_path / scope,
        "report": tmp_path / "evidence" / "gold-assembly-report.json",
        "export": tmp_path / "evidence" / "consignment-export.json",
    }
    monkeypatch.setenv("BLUEECONOMY_GOLD_SCOPE", scope)
    monkeypatch.setenv("BLUEECONOMY_GOLD_SCOPE_ROOT_URI", str(paths["root"]))
    monkeypatch.setenv("BLUEECONOMY_GOLD_REPORT", str(paths["report"]))
    monkeypatch.setenv("BLUEECONOMY_GOLD_EXPORT_PATH", str(paths["export"]))
    if clearance is not None:
        monkeypatch.setenv("BLUEECONOMY_GOLD_CLEARANCE", clearance)
    else:
        monkeypatch.delenv("BLUEECONOMY_GOLD_CLEARANCE", raising=False)
    return paths


def test_load_config_fails_closed() -> None:
    with pytest.raises(ValueError, match="BLUEECONOMY_GOLD_SCOPE must be set"):
        load_config({})
    with pytest.raises(ValueError, match="not a governed lakehouse scope"):
        load_config({"BLUEECONOMY_GOLD_SCOPE": "root"})
    with pytest.raises(ValueError, match="no gold assembly is defined"):
        load_config({"BLUEECONOMY_GOLD_SCOPE": "isr"})
    with pytest.raises(ValueError, match="BLUEECONOMY_GOLD_SCOPE_ROOT_URI must be set"):
        load_config({"BLUEECONOMY_GOLD_SCOPE": "cvff"})
    base = {
        "BLUEECONOMY_GOLD_SCOPE": "fisheries",
        "BLUEECONOMY_GOLD_SCOPE_ROOT_URI": "/lakehouse/fisheries",
        "BLUEECONOMY_GOLD_REPORT": "/evidence/report.json",
    }
    with pytest.raises(ValueError, match="BLUEECONOMY_GOLD_EXPORT_PATH must be set"):
        load_config(base)
    with pytest.raises(ValueError, match="clearance label"):
        load_config(
            {
                **base,
                "BLUEECONOMY_GOLD_EXPORT_PATH": "/evidence/export.json",
                "BLUEECONOMY_GOLD_CLEARANCE": "TOP-SECRET",
            }
        )
    with pytest.raises(ValueError, match="must not overwrite"):
        load_config({**base, "BLUEECONOMY_GOLD_EXPORT_PATH": "/evidence/report.json"})
    config = load_config({**base, "BLUEECONOMY_GOLD_EXPORT_PATH": "/evidence/export.json"})
    # The fail-closed default is the most restrictive clearance.
    assert config.clearance.label == "UNCLASSIFIED"


def test_cvff_gold_rollup_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    writer = SegregatedDeltaWriter(LakehouseScope.CVFF, str(tmp_path / "cvff"))
    records = [
        build_silver_record(
            cvff_event("evt-1"), KafkaRecordMetadata("cvff.ledger.commitments", 0, 1)
        ),
        build_silver_record(
            cvff_event("evt-2"), KafkaRecordMetadata("cvff.ledger.commitments", 0, 2)
        ),
    ]
    append_silver(writer, records)
    paths = gold_env(monkeypatch, tmp_path, "cvff")
    main()
    report = json.loads(paths["report"].read_text(encoding="utf-8"))
    assert report["lakehouse_scope"] == "cvff"
    assert report["assembly"] == "cvff-silver-gold-ledger-commitments"
    assert report["gold_rows"] == 1
    assert report["exported_rows"] is None
    assert DeltaTable(writer.table_uri("gold")).to_pyarrow_table().num_rows == 1


def test_fisheries_gold_assembly_and_clearance_filtered_export(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = SegregatedDeltaWriter(LakehouseScope.FISHERIES, str(tmp_path / "fisheries"))
    events = [
        *consignment_events("CONS-2026-0001", label="RESTRICTED"),
        # Unlabelled sources default to the highest restriction (SECRET).
        *consignment_events("CONS-2026-0002"),
    ]
    append_bronze(writer, events, kafka_topic="fisheries.catch.v1")
    paths = gold_env(monkeypatch, tmp_path, "fisheries", clearance="RESTRICTED")
    main()

    report = json.loads(paths["report"].read_text(encoding="utf-8"))
    assert report["assembly"] == "fisheries-gold-export-consignments"
    assert report["gold_rows"] == 2
    assert report["exported_rows"] == 1

    exported = json.loads(paths["export"].read_text(encoding="utf-8"))
    assert [row["consignment_id"] for row in exported] == ["CONS-2026-0001"]
    assert exported[0]["record_classification"] == "RESTRICTED"

    # The serving read enforces the same filter against the gold table.
    visible = read_export_consignments(writer, "SECRET")
    assert {row["consignment_id"] for row in visible} == {"CONS-2026-0001", "CONS-2026-0002"}
    with pytest.raises(AccessDeniedError):
        read_export_consignments(writer, None)


def test_fisheries_export_defaults_to_most_restrictive_clearance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = SegregatedDeltaWriter(LakehouseScope.FISHERIES, str(tmp_path / "fisheries"))
    append_bronze(
        writer,
        consignment_events("CONS-2026-0003", label="RESTRICTED"),
        kafka_topic="fisheries.catch.v1",
    )
    paths = gold_env(monkeypatch, tmp_path, "fisheries")
    main()
    report = json.loads(paths["report"].read_text(encoding="utf-8"))
    assert report["clearance"] == "UNCLASSIFIED"
    assert report["gold_rows"] == 1
    assert report["exported_rows"] == 0
    assert json.loads(paths["export"].read_text(encoding="utf-8")) == []


def test_missing_scope_root_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    gold_env(monkeypatch, tmp_path, "cvff")
    monkeypatch.setenv("BLUEECONOMY_GOLD_SCOPE_ROOT_URI", str(tmp_path / "fisheries"))
    with pytest.raises(SystemExit):
        main()
    assert "cvff scope root must terminate" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Phase-8 dispatch: platform statistics, mrv, bluecarbon
# ---------------------------------------------------------------------------

MRV_TEST_SIGNING_KID = "blueeconomy-data-platform-test-0"
MRV_TEST_SIGNING_SEED = "61" * 32  # fixture-only seed material; never a real secret


def _fixture_seed_hex(kid: str) -> str:
    import hashlib

    from signing_helpers import SEED_DOMAIN

    return hashlib.sha256((SEED_DOMAIN + kid).encode("utf-8")).hexdigest()


def test_platform_scope_requires_period_and_signing_env() -> None:
    base = {
        "BLUEECONOMY_GOLD_SCOPE": "platform",
        "BLUEECONOMY_GOLD_SCOPE_ROOT_URI": "/lakehouse/platform",
        "BLUEECONOMY_GOLD_REPORT": "/evidence/report.json",
    }
    with pytest.raises(ValueError, match="BLUEECONOMY_GOLD_STATS_PERIOD must be set"):
        load_config(base)
    with pytest.raises(ValueError, match="YYYY-MM"):
        load_config({**base, "BLUEECONOMY_GOLD_STATS_PERIOD": "Sept-2026"})
    with pytest.raises(ValueError, match="BLUEECONOMY_SIGNING_KEY_SEED"):
        load_config({**base, "BLUEECONOMY_GOLD_STATS_PERIOD": "2026-09"})
    with pytest.raises(ValueError, match="BLUEECONOMY_SIGNING_KID"):
        load_config(
            {
                **base,
                "BLUEECONOMY_GOLD_STATS_PERIOD": "2026-09",
                "BLUEECONOMY_SIGNING_KEY_SEED": MRV_TEST_SIGNING_SEED,
            }
        )
    config = load_config(
        {
            **base,
            "BLUEECONOMY_GOLD_STATS_PERIOD": "2026-09",
            "BLUEECONOMY_SIGNING_KEY_SEED": MRV_TEST_SIGNING_SEED,
            "BLUEECONOMY_SIGNING_KID": MRV_TEST_SIGNING_KID,
        }
    )
    assert config.stats_period == "2026-09"


def test_platform_gold_port_statistics_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from test_port_statistics import period_fixture_events, write_silver

    root = tmp_path / "platform"
    write_silver(root, period_fixture_events())
    paths = gold_env(monkeypatch, tmp_path, "platform")
    monkeypatch.setenv("BLUEECONOMY_GOLD_STATS_PERIOD", "2026-09")
    monkeypatch.setenv("BLUEECONOMY_SIGNING_KEY_SEED", _fixture_seed_hex(MRV_TEST_SIGNING_KID))
    monkeypatch.setenv("BLUEECONOMY_SIGNING_KID", MRV_TEST_SIGNING_KID)
    main()
    report = json.loads(paths["report"].read_text(encoding="utf-8"))
    assert report["assembly"] == "platform-gold-port-statistics"
    assert report["gold_rows"] > 0
    assert report["stats_report_sha256"] is not None

    # The signed artefact verifies against the fixture key directory.
    from blueeconomy_data_platform.signature_verification import (
        EnvelopeSignatureVerifier,
        load_key_directory,
    )
    from signing_helpers import FIXTURE_KEY_DIRECTORY

    runs = DeltaTable(str(root / "platform_gold" / "port_kpi_runs")).to_pyarrow_table().to_pylist()
    assert len(runs) == 1
    artefact_path = root / "platform_gold" / "port_kpi_reports" / f"{runs[0]['run_id']}.json"
    verifier = EnvelopeSignatureVerifier(load_key_directory(FIXTURE_KEY_DIRECTORY))
    assert verifier.verify(json.loads(artefact_path.read_text(encoding="utf-8"))) == (
        MRV_TEST_SIGNING_KID
    )


def test_mrv_gold_assembly_dispatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from blueeconomy_data_platform.mrv_gold import mrv_gold_table_uri
    from blueeconomy_data_platform.scope_medallion import (
        ScopeKafkaRecordMetadata,
        append_scope_silver,
        build_silver_record,
    )

    writer = SegregatedDeltaWriter(LakehouseScope.MRV, str(tmp_path / "mrv"))
    event = {
        "event_id": "evt-ver",
        "event_type": "mrv.emissions-annual.v1",
        "producer": "blueeconomy-geo-service",
        "occurred_at": BASE_TIME,
        "recorded_at": BASE_TIME,
        "data_classification": "mrv_confidential",
        "source_system": "mrv-api",
        "source_record_reference": "src-1",
        "correlation_id": None,
        "payload_json": json.dumps(
            {
                "reportId": "rep-1",
                "imoNumber": "9081716",
                "calendarYear": 2026,
                "state": "VERIFIED",
                "totals": {"co2Tonnes": 1284.375},
            }
        ),
        "ingested_at": BASE_TIME,
    }
    append_scope_silver(
        writer,
        [
            build_silver_record(
                event, ScopeKafkaRecordMetadata(LakehouseScope.MRV, "mrv.annual-reports", 0, 0)
            )
        ],
    )
    paths = gold_env(monkeypatch, tmp_path, "mrv")
    main()
    report = json.loads(paths["report"].read_text(encoding="utf-8"))
    assert report["assembly"] == "mrv-gold-vessel-annual"
    assert report["gold_rows"] == 1
    assert DeltaTable(mrv_gold_table_uri(writer)).to_pyarrow_table().num_rows == 1


def test_bluecarbon_gold_assembly_dispatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from blueeconomy_data_platform.bluecarbon_gold import bluecarbon_gold_table_uri
    from blueeconomy_data_platform.scope_medallion import (
        ScopeKafkaRecordMetadata,
        append_scope_silver,
        build_silver_record,
    )

    writer = SegregatedDeltaWriter(LakehouseScope.BLUECARBON, str(tmp_path / "bluecarbon"))
    event = {
        "event_id": "evt-p1",
        "event_type": "bluecarbon.project.v1",
        "producer": "financial-controls",
        "occurred_at": BASE_TIME,
        "recorded_at": BASE_TIME,
        "data_classification": "bluecarbon_internal",
        "source_system": "bluecarbon-api",
        "source_record_reference": "src-1",
        "correlation_id": None,
        "payload_json": json.dumps(
            {"projectId": "NG-BC-2026-0001", "state": "REGISTERED", "methodology": "VM0033 v2.1"}
        ),
        "ingested_at": BASE_TIME,
    }
    append_scope_silver(
        writer,
        [
            build_silver_record(
                event,
                ScopeKafkaRecordMetadata(LakehouseScope.BLUECARBON, "bluecarbon.projects", 0, 0),
            )
        ],
    )
    paths = gold_env(monkeypatch, tmp_path, "bluecarbon")
    main()
    report = json.loads(paths["report"].read_text(encoding="utf-8"))
    assert report["assembly"] == "bluecarbon-gold-public-registry"
    assert report["gold_rows"] == 1
    assert DeltaTable(bluecarbon_gold_table_uri(writer)).to_pyarrow_table().num_rows == 1
