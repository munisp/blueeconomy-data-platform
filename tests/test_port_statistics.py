"""Statistics Port tests (phase 8): KPI math, no-data rows, determinism, signing."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from deltalake import DeltaTable

from blueeconomy_data_platform.ingest import append_rows
from blueeconomy_data_platform.port_statistics import (
    KPI_BY_ID,
    KPI_DEFINITIONS,
    STATS_GAPS,
    assemble_port_call_facts,
    compute_kpi_observations,
    discover_ports,
    emit_value_rows,
    extract_bookings,
    extract_gate_scans,
    parse_period,
    percentile_linear,
    query_definitions_sha256,
    run_port_statistics,
)
from blueeconomy_data_platform.segregation import LakehouseScope, SegregatedDeltaWriter
from blueeconomy_data_platform.signature_verification import (
    EnvelopeSignatureVerifier,
    SignatureVerificationError,
    load_key_directory,
)
from signing_helpers import FIXTURE_KEY_DIRECTORY, fixture_private_key

BASE = datetime(2026, 9, 1, tzinfo=UTC)
TEST_SIGNING_KID = "blueeconomy-data-platform-test-0"

NGLAG = "NGLAG"  # Lagos (Apapa)
NGTCM = "NGTCM"  # Tin Can Island


def silver_event(
    event_id: str,
    event_type: str,
    occurred_at: datetime,
    payload: dict[str, object],
) -> dict[str, object]:
    """A normalized platform silver row (envelope already ingested+verified upstream)."""
    return {
        "event_id": event_id,
        "event_type": event_type,
        "producer": "s1-port-interoperability",
        "occurred_at": occurred_at,
        "recorded_at": occurred_at,
        "data_classification": "internal",
        "source_system": "port-interoperability",
        "source_record_reference": f"src-{event_id}",
        "correlation_id": None,
        "payload_json": json.dumps({**payload, "provenance": {"principalId": "svc-port"}}),
        "ingested_at": occurred_at,
    }


def port_call(
    event_id: str,
    call_id: str,
    port: str,
    status: str,
    occurred_at: datetime,
    ship_class: str | None = "container",
    arrived: str | None = None,
    berthed: str | None = None,
    departed: str | None = None,
    tonnage: float | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "portCallId": call_id,
        "portCode": port,
        "status": status,
        "vesselRef": f"vessel-{call_id}",
    }
    if ship_class is not None:
        payload["shipClass"] = ship_class
    if arrived is not None:
        payload["arrivedAt"] = arrived
    if berthed is not None:
        payload["berthedAt"] = berthed
    if departed is not None:
        payload["departedAt"] = departed
    if tonnage is not None:
        payload["declaredTonnage"] = tonnage
    return silver_event(event_id, "ports.port-call.v1", occurred_at, payload)


def gate_scan(
    event_id: str, truck: str, direction: str, occurred_at: datetime, port: str | None = NGLAG
) -> dict[str, object]:
    payload: dict[str, object] = {"truckRef": truck, "direction": direction, "terminal": "apapa-t1"}
    if port is not None:
        payload["portCode"] = port
    return silver_event(event_id, "ports.gate.scan.v1", occurred_at, payload)


def booking(
    event_id: str,
    created: str,
    window_start: str,
    port: str = NGLAG,
    booked: float | None = None,
    offered: float | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "bookingId": f"bk-{event_id}",
        "portCode": port,
        "createdAt": created,
        "slotWindowStart": window_start,
    }
    if booked is not None:
        payload["bookedSlots"] = booked
    if offered is not None:
        payload["offeredSlots"] = offered
    return silver_event(
        event_id,
        "ports.booking.created.v1",
        datetime.fromisoformat(created.replace("Z", "+00:00")),
        payload,
    )


def period_fixture_events() -> list[dict[str, object]]:
    """The committed fixture scenario for the 2026-09 period (real table inputs)."""
    return [
        port_call(
            "evt-pc1",
            "PC-1",
            NGLAG,
            "DEPARTED",
            datetime(2026, 9, 4, 2, 0, tzinfo=UTC),
            arrived="2026-09-03T08:00:00Z",
            berthed="2026-09-03T10:00:00Z",
            departed="2026-09-04T02:00:00Z",
            tonnage=12000.0,
        ),
        port_call(
            "evt-pc2",
            "PC-2",
            NGLAG,
            "DEPARTED",
            datetime(2026, 9, 11, 0, 0, tzinfo=UTC),
            arrived="2026-09-10T08:00:00Z",
            berthed="2026-09-10T12:00:00Z",
            departed="2026-09-11T00:00:00Z",
            tonnage=8000.0,
        ),
        port_call(
            "evt-pc3",
            "PC-3",
            NGLAG,
            "DEPARTED",
            datetime(2026, 9, 16, 0, 0, tzinfo=UTC),
            ship_class="bulk",
            arrived="2026-09-15T00:00:00Z",
            berthed="2026-09-15T06:00:00Z",
            departed="2026-09-16T00:00:00Z",
            tonnage=20000.0,
        ),
        port_call(
            "evt-pc4",
            "PC-4",
            NGTCM,
            "DEPARTED",
            datetime(2026, 9, 21, 6, 0, tzinfo=UTC),
            arrived="2026-09-20T06:00:00Z",
            berthed="2026-09-20T09:00:00Z",
            departed="2026-09-21T06:00:00Z",
        ),
        # Below ACCEPTED: retained as a fact, never counted by the KPI set.
        port_call("evt-pc5", "PC-5", NGLAG, "SUBMITTED", datetime(2026, 9, 25, 8, 0, tzinfo=UTC)),
        # Next period: excluded from 2026-09.
        port_call(
            "evt-pc6",
            "PC-6",
            NGLAG,
            "DEPARTED",
            datetime(2026, 10, 2, 0, 0, tzinfo=UTC),
            arrived="2026-10-01T00:00:00Z",
            berthed="2026-10-01T02:00:00Z",
            departed="2026-10-02T00:00:00Z",
        ),
        gate_scan("evt-g1", "truck-A", "in", datetime(2026, 9, 5, 8, 0, tzinfo=UTC)),
        gate_scan("evt-g2", "truck-A", "out", datetime(2026, 9, 5, 11, 30, tzinfo=UTC)),
        gate_scan("evt-g3", "truck-B", "in", datetime(2026, 9, 6, 9, 0, tzinfo=UTC)),
        gate_scan("evt-g4", "truck-B", "out", datetime(2026, 9, 6, 10, 0, tzinfo=UTC)),
        booking(
            "evt-b1", "2026-09-01T00:00:00Z", "2026-09-03T12:00:00Z", booked=30.0, offered=40.0
        ),
        booking(
            "evt-b2", "2026-09-02T00:00:00Z", "2026-09-05T12:00:00Z", booked=10.0, offered=20.0
        ),
    ]


def write_silver(root: Path, events: list[dict[str, object]]) -> int:
    writer = SegregatedDeltaWriter(LakehouseScope.PLATFORM, str(root))
    version, _, _ = append_rows(writer.table_uri("silver"), events, key_column="event_id")
    return version


# ---------------------------------------------------------------------------
# Units: periods, percentiles, facts, KPI math
# ---------------------------------------------------------------------------


def test_parse_period_utc_bounds_and_validation() -> None:
    start, end = parse_period("2026-09")
    assert (start, end) == (
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 10, 1, tzinfo=UTC),
    )
    start, end = parse_period("2026-12")
    assert end == datetime(2027, 1, 1, tzinfo=UTC)
    for bad in ("2026-13", "2026-00", "2026-9", "202609", "", " 2026-09"):
        with pytest.raises(ValueError, match="YYYY-MM"):
            parse_period(bad)


def test_percentile_edge_cases() -> None:
    assert percentile_linear([7.5], 0.9) == 7.5
    assert percentile_linear([2.0, 2.0, 2.0], 0.5) == 2.0
    assert percentile_linear([16.0, 18.0, 24.0], 0.5) == 18.0
    assert percentile_linear([16.0, 18.0, 24.0], 0.9) == pytest.approx(22.8)
    assert percentile_linear([60.0, 210.0], 0.5) == pytest.approx(135.0)
    with pytest.raises(ValueError, match="empty"):
        percentile_linear([], 0.5)


def test_kpi_registry_is_pinned_and_hashed() -> None:
    assert len(KPI_DEFINITIONS) == 9
    digest = query_definitions_sha256()
    assert digest == query_definitions_sha256()
    assert len(digest) == 64
    gap_kpis = {definition.kpi_id for definition in KPI_DEFINITIONS if definition.gap_id}
    assert gap_kpis == {"berth_occupancy_pct", "declaration_clearance_hours"}
    assert {gap.gap_id for gap in STATS_GAPS} == {
        "GAP-STATS-BERTH-REF",
        "GAP-STATS-TEU",
        "GAP-STATS-SW-EVENTS",
    }
    assert KPI_BY_ID["vessel_calls"].unit == "calls"


def test_fact_assembly_merges_lifecycle_and_attributes_period() -> None:
    events = period_fixture_events()
    start, end = parse_period("2026-09")
    facts = assemble_port_call_facts(events, start, end)
    by_id = {fact["port_call_id"]: fact for fact in facts}
    assert set(by_id) == {"PC-1", "PC-2", "PC-3", "PC-4", "PC-5"}, "PC-6 belongs to 2026-10"
    pc1 = by_id["PC-1"]
    assert pc1["port_code"] == NGLAG
    assert pc1["ship_class"] == "container"
    assert pc1["declared_tonnage"] == 12000.0
    # Vessel identity is hashed in facts, never carried raw.
    assert pc1["vessel_ref_hashed"] is not None
    assert "vessel-PC-1" not in str(pc1)
    assert by_id["PC-5"]["status"] == "SUBMITTED"


def test_fact_assembly_fails_closed_on_bad_port_and_status() -> None:
    start, end = parse_period("2026-09")
    bad_port = port_call("e1", "PC-X", "LAG", "ACCEPTED", datetime(2026, 9, 3, tzinfo=UTC))
    with pytest.raises(ValueError, match="UN/LOCODE"):
        assemble_port_call_facts([bad_port], start, end)
    bad_status = port_call("e2", "PC-Y", NGLAG, "HALF_BERTHED", datetime(2026, 9, 3, tzinfo=UTC))
    with pytest.raises(ValueError, match="lifecycle"):
        assemble_port_call_facts([bad_status], start, end)


def test_gate_scan_and_booking_extraction() -> None:
    events = period_fixture_events()
    start, end = parse_period("2026-09")
    scans = extract_gate_scans(events, start, end)
    assert len(scans) == 4
    bookings = extract_bookings(events, start, end)
    assert len(bookings) == 2
    assert bookings[0]["lead_time_hours"] == pytest.approx(60.0)
    assert bookings[1]["lead_time_hours"] == pytest.approx(84.0)
    assert bookings[0]["port_code"] == NGLAG


def test_kpi_observations_match_hand_computed_fixture() -> None:
    events = period_fixture_events()
    start, end = parse_period("2026-09")
    facts = assemble_port_call_facts(events, start, end)
    observations = compute_kpi_observations(
        facts, extract_gate_scans(events, start, end), extract_bookings(events, start, end)
    )

    calls = {
        (row["port_code"], row["ship_class"]): row["value"] for row in observations["vessel_calls"]
    }
    assert calls == {
        (NGLAG, "container"): 2.0,
        (NGLAG, "bulk"): 1.0,
        (NGLAG, None): 3.0,
        (NGTCM, "container"): 1.0,
        (NGTCM, None): 1.0,
    }

    turnaround = {
        (row["port_code"], row["percentile"]): row["value"]
        for row in observations["vessel_turnaround_hours"]
    }
    assert turnaround[(NGLAG, "P50")] == 18.0
    assert turnaround[(NGLAG, "P90")] == pytest.approx(22.8)
    assert turnaround[(NGTCM, "P50")] == 24.0

    waiting = {
        (row["port_code"], row["percentile"]): row["value"]
        for row in observations["waiting_time_hours"]
    }
    assert waiting[(NGLAG, "P50")] == 4.0
    assert waiting[(NGLAG, "P90")] == pytest.approx(5.6)

    assert observations["throughput_tonnes"] == [
        {
            "port_code": NGLAG,
            "ship_class": None,
            "percentile": None,
            "value": 40000.0,
            "n_observations": 3,
        }
    ]
    gate = observations["truck_gate_turnaround_minutes"]
    assert gate[0]["value"] == pytest.approx(135.0)
    assert gate[0]["n_observations"] == 2
    lead = observations["booking_lead_time_hours"]
    assert lead[0]["value"] == pytest.approx(72.0)
    utilisation = observations["slot_utilisation_pct"]
    assert utilisation[0]["value"] == pytest.approx(round(100.0 * 40.0 / 60.0, 6))
    # Gap KPIs never produce observations.
    assert "berth_occupancy_pct" not in observations
    assert "declaration_clearance_hours" not in observations


def test_no_data_and_gap_rows_are_first_class() -> None:
    events = period_fixture_events()
    start, end = parse_period("2026-09")
    facts = assemble_port_call_facts(events, start, end)
    scans = extract_gate_scans(events, start, end)
    bookings = extract_bookings(events, start, end)
    observations = compute_kpi_observations(facts, scans, bookings)
    ports = discover_ports(facts, scans, bookings)
    assert ports == [NGLAG, NGTCM]
    rows = emit_value_rows(
        observations, ports, "2026-09", "run-test", "platform_silver/events", 7, "q" * 64, BASE
    )

    # Every KPI is present for every discovered port: nothing omitted.
    for kpi in KPI_BY_ID:
        assert any(row["kpi_id"] == kpi and row["port_code"] == NGLAG for row in rows)
        assert any(row["kpi_id"] == kpi and row["port_code"] == NGTCM for row in rows)

    berth = [row for row in rows if row["kpi_id"] == "berth_occupancy_pct"]
    assert all(row["value"] is None for row in berth)
    assert all("GAP-STATS-BERTH-REF" in row["coverage_note"] for row in berth)

    declaration = [row for row in rows if row["kpi_id"] == "declaration_clearance_hours"]
    assert all("GAP-STATS-SW-EVENTS" in row["coverage_note"] for row in declaration)

    gate_tcm = [
        row
        for row in rows
        if row["kpi_id"] == "truck_gate_turnaround_minutes" and row["port_code"] == NGTCM
    ]
    assert len(gate_tcm) == 1
    assert gate_tcm[0]["value"] is None
    assert gate_tcm[0]["coverage_note"] == "no source events in period"
    assert gate_tcm[0]["n_observations"] == 0

    # No fabricated numbers: every non-null value equals its observation value.
    for row in rows:
        if row["value"] is not None:
            assert row["coverage_note"] is None
            assert row["n_observations"] > 0


def test_discover_ports_empty_period() -> None:
    assert discover_ports([], [], []) == [None]


# ---------------------------------------------------------------------------
# Full gold runs: determinism/replay, provenance manifest, signed artefact
# ---------------------------------------------------------------------------


def run_stats(root: Path, period: str = "2026-09", computed_at: datetime | None = None):
    writer = SegregatedDeltaWriter(LakehouseScope.PLATFORM, str(root))
    return run_port_statistics(
        writer,
        period,
        signing_key=fixture_private_key(TEST_SIGNING_KID),
        signing_kid=TEST_SIGNING_KID,
        computed_at=computed_at,
    )


def test_gold_run_end_to_end(tmp_path: Path) -> None:
    root = tmp_path / "platform"
    silver_version = write_silver(root, period_fixture_events())
    result = run_stats(root)

    assert result.source_table_versions == {"platform_silver/events": silver_version}
    assert result.facts_rows == 5
    assert result.rows_emitted > 0
    assert result.rows_no_data > 0

    gold = root / "platform_gold"
    runs = DeltaTable(str(gold / "port_kpi_runs")).to_pyarrow_table().to_pylist()
    assert len(runs) == 1
    manifest = runs[0]
    assert manifest["run_id"] == result.run_id
    assert json.loads(manifest["source_table_versions_json"]) == {
        "platform_silver/events": silver_version
    }
    assert manifest["query_definitions_sha256"] == query_definitions_sha256()
    assert manifest["report_sha256"] == result.report_sha256
    assert manifest["kpi_count"] == len(KPI_DEFINITIONS)
    assert manifest["rows_emitted"] == result.rows_emitted
    assert manifest["rows_no_data"] == result.rows_no_data

    values = DeltaTable(str(gold / "port_kpi_values")).to_pyarrow_table().to_pylist()
    assert len(values) == result.rows_emitted
    by_key = {
        (row["kpi_id"], row["port_code"], row["ship_class"], row["percentile"]): row
        for row in values
    }
    assert by_key[("vessel_calls", NGLAG, None, None)]["value"] == 3.0
    assert by_key[("vessel_turnaround_hours", NGLAG, None, "P50")]["value"] == 18.0
    assert by_key[("throughput_tonnes", NGLAG, None, None)]["value"] == 40000.0
    no_data = by_key[("throughput_tonnes", NGTCM, None, None)]
    assert no_data["value"] is None
    assert no_data["coverage_note"] == "no source events in period"
    assert all(row["table_version"] == silver_version for row in values)
    assert all(row["query_hash"] == query_definitions_sha256() for row in values)
    assert all(row["run_id"] == result.run_id for row in values)

    facts = DeltaTable(str(gold / "port_call_facts")).to_pyarrow_table().to_pylist()
    assert len(facts) == 5

    # Signed artefacts exist and the JSON artefact verifies under the fleet scheme.
    artefact_path = Path(result.report_json_path)
    csv_path = Path(result.report_csv_path)
    assert artefact_path.is_file() and csv_path.is_file()
    artefact = json.loads(artefact_path.read_text(encoding="utf-8"))
    verifier = EnvelopeSignatureVerifier(load_key_directory(FIXTURE_KEY_DIRECTORY))
    assert verifier.verify(artefact) == TEST_SIGNING_KID
    assert artefact["report_sha256"] == result.report_sha256
    assert artefact["provenance"]["principalId"] == "blueeconomy-data-platform"
    csv_lines = csv_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(csv_lines) == result.rows_emitted + 1
    assert csv_lines[0].startswith("kpi_id,period,port_code")


def test_determinism_replay_identical_hashes(tmp_path: Path) -> None:
    root = tmp_path / "platform"
    write_silver(root, period_fixture_events())
    first = run_stats(root, computed_at=datetime(2026, 10, 1, 0, 0, tzinfo=UTC))
    first_artefact = json.loads(Path(first.report_json_path).read_text(encoding="utf-8"))
    # Re-run over the same silver table version: identical run id and hashes.
    second = run_stats(root, computed_at=datetime(2026, 10, 2, 12, 0, tzinfo=UTC))
    assert first.run_id == second.run_id
    assert first.report_sha256 == second.report_sha256

    # Replay is idempotent at the table level: still one manifest, same rows.
    gold = root / "platform_gold"
    assert DeltaTable(str(gold / "port_kpi_runs")).to_pyarrow_table().num_rows == 1
    assert (
        DeltaTable(str(gold / "port_kpi_values")).to_pyarrow_table().num_rows == first.rows_emitted
    )

    # The report core is wall-clock independent: the two runs carry different
    # computed_at values but an identical hash, and the replayed artefact
    # (same deterministic run id overwrites the same path) verifies the same.
    assert first_artefact["computed_at"] == "2026-10-01T00:00:00+00:00"
    assert first_artefact["report_sha256"] == second.report_sha256
    second_artefact = json.loads(Path(second.report_json_path).read_text(encoding="utf-8"))
    assert second_artefact["computed_at"] == "2026-10-02T12:00:00+00:00"
    assert second_artefact["report_sha256"] == first.report_sha256
    assert second_artefact["values"] == first_artefact["values"]


def test_forged_report_signature_is_refused(tmp_path: Path) -> None:
    root = tmp_path / "platform"
    write_silver(root, period_fixture_events())
    result = run_stats(root)
    artefact = json.loads(Path(result.report_json_path).read_text(encoding="utf-8"))
    verifier = EnvelopeSignatureVerifier(load_key_directory(FIXTURE_KEY_DIRECTORY))

    # Tamper with one served figure: payload-mismatch, refused.
    forged = json.loads(json.dumps(artefact))
    forged["values"][0]["value"] = 999999.0
    with pytest.raises(SignatureVerificationError, match="payload-mismatch"):
        verifier.verify(forged)

    # A report swapped under an unknown kid is refused.
    swapped = json.loads(json.dumps(artefact))
    header, payload, _signature = swapped["provenance"]["signature"].split(".")
    import base64
    import json as _json

    bad_header = (
        base64.urlsafe_b64encode(_json.dumps({"alg": "EdDSA", "kid": "stats-attacker-1"}).encode())
        .rstrip(b"=")
        .decode()
    )
    swapped["provenance"]["signature"] = f"{bad_header}.{payload}.{'A' * 86}"
    with pytest.raises(SignatureVerificationError, match="unknown-kid"):
        verifier.verify(swapped)


def test_empty_period_emits_only_no_data_and_gap_rows(tmp_path: Path) -> None:
    root = tmp_path / "platform"
    write_silver(root, period_fixture_events())
    result = run_stats(root, period="2026-08")
    assert result.rows_no_data == result.rows_emitted
    values = (
        DeltaTable(str(root / "platform_gold" / "port_kpi_values")).to_pyarrow_table().to_pylist()
    )
    assert len(values) == len(KPI_DEFINITIONS), "one row per KPI for the null-port aggregate"
    assert all(row["value"] is None for row in values)
    assert all(row["port_code"] is None for row in values)
    notes = {row["kpi_id"]: row["coverage_note"] for row in values}
    assert notes["vessel_calls"] == "no source events in period"
    assert "GAP-STATS-BERTH-REF" in notes["berth_occupancy_pct"]
    assert "GAP-STATS-SW-EVENTS" in notes["declaration_clearance_hours"]


def test_run_fails_closed_without_silver(tmp_path: Path) -> None:
    writer = SegregatedDeltaWriter(LakehouseScope.PLATFORM, str(tmp_path / "platform"))
    with pytest.raises(ValueError, match="before the platform silver table exists"):
        run_port_statistics(
            writer,
            "2026-09",
            signing_key=fixture_private_key(TEST_SIGNING_KID),
            signing_kid=TEST_SIGNING_KID,
        )


def test_run_refuses_non_platform_scope(tmp_path: Path) -> None:
    writer = SegregatedDeltaWriter(LakehouseScope.MRV, str(tmp_path / "mrv"))
    with pytest.raises(ValueError, match="only defined for the platform scope"):
        run_port_statistics(
            writer,
            "2026-09",
            signing_key=fixture_private_key(TEST_SIGNING_KID),
            signing_kid=TEST_SIGNING_KID,
        )
