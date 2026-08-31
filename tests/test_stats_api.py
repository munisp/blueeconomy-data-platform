"""blueeconomy-stats-api contract tests (phase 8): read-only, fail-closed, honest."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import threading
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

import pytest
from deltalake import DeltaTable

from blueeconomy_data_platform.port_statistics import run_port_statistics
from blueeconomy_data_platform.segregation import LakehouseScope, SegregatedDeltaWriter
from blueeconomy_data_platform.stats_api import StatsApiConfig, make_handler, serve
from signing_helpers import fixture_private_key
from test_port_statistics import period_fixture_events, write_silver

TEST_SIGNING_KID = "blueeconomy-data-platform-test-0"
TOKEN_SECRET = "stats-api-test-secret"


def mint_token(roles: list[str], secret: str = TOKEN_SECRET, exp: float | None = None) -> str:
    def b64(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")

    header = b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    claims: dict[str, object] = {"sub": "ministry-reader", "roles": roles}
    if exp is not None:
        claims["exp"] = exp
    payload = b64(json.dumps(claims).encode())
    signature = b64(
        hmac.new(secret.encode(), f"{header}.{payload}".encode(), hashlib.sha256).digest()
    )
    return f"{header}.{payload}.{signature}"


@pytest.fixture()
def stats_server(tmp_path: Path):
    root = tmp_path / "platform"
    write_silver(root, period_fixture_events())
    writer = SegregatedDeltaWriter(LakehouseScope.PLATFORM, str(root))
    run_result = run_port_statistics(
        writer,
        "2026-09",
        signing_key=fixture_private_key(TEST_SIGNING_KID),
        signing_kid=TEST_SIGNING_KID,
        computed_at=datetime(2026, 10, 1, tzinfo=UTC),
    )
    config = StatsApiConfig(str(root / "platform_gold"), 0, TOKEN_SECRET)
    server = serve(config)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_address[1]}"
    yield base_url, run_result, root
    server.shutdown()
    thread.join(timeout=5)
    server.server_close()


def get(base_url: str, path: str, token: str | None = None) -> tuple[int, str]:
    request = urllib.request.Request(base_url + path)
    if token is not None:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(request) as response:
            return response.status, response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode("utf-8")


def test_health_is_unauthenticated_liveness(stats_server) -> None:
    base_url, _, _ = stats_server
    status, body = get(base_url, "/v1/stats/health")
    assert status == 200
    assert json.loads(body) == {"status": "ok"}


def test_data_routes_require_a_valid_token(stats_server) -> None:
    base_url, _, _ = stats_server
    for route in ("/v1/stats/kpis", "/v1/stats/runs", "/v1/stats/values"):
        status, body = get(base_url, route)
        assert status == 401, route
        assert "error" in json.loads(body)
    # Wrong secret.
    status, _ = get(
        base_url, "/v1/stats/kpis", token=mint_token(["stats-reader"], secret="wrong-secret")
    )
    assert status == 401
    # Expired token.
    status, _ = get(
        base_url, "/v1/stats/kpis", token=mint_token(["stats-reader"], exp=time.time() - 60)
    )
    assert status == 401
    # Right token, missing role -> 403.
    status, _ = get(base_url, "/v1/stats/kpis", token=mint_token(["someone-else"]))
    assert status == 403


def test_kpi_registry_endpoint(stats_server) -> None:
    base_url, _, _ = stats_server
    status, body = get(base_url, "/v1/stats/kpis", token=mint_token(["stats-reader"]))
    assert status == 200
    document = json.loads(body)
    assert {kpi["kpi_id"] for kpi in document["kpis"]} == {
        "vessel_calls",
        "vessel_turnaround_hours",
        "waiting_time_hours",
        "berth_occupancy_pct",
        "throughput_tonnes",
        "truck_gate_turnaround_minutes",
        "booking_lead_time_hours",
        "slot_utilisation_pct",
        "declaration_clearance_hours",
    }
    assert {gap["gap_id"] for gap in document["gaps"]} == {
        "GAP-STATS-BERTH-REF",
        "GAP-STATS-TEU",
        "GAP-STATS-SW-EVENTS",
    }


def test_runs_endpoint_serves_provenance_ledger(stats_server) -> None:
    base_url, run_result, _ = stats_server
    status, body = get(base_url, "/v1/stats/runs", token=mint_token(["stats-reader"]))
    assert status == 200
    runs = json.loads(body)["runs"]
    assert len(runs) == 1
    manifest = runs[0]
    assert manifest["run_id"] == run_result.run_id
    assert manifest["report_sha256"] == run_result.report_sha256
    assert manifest["source_table_versions"] == {"platform_silver/events": 0}
    assert manifest["period"] == "2026-09"


def test_values_endpoint_serves_exactly_the_gold_rows(stats_server) -> None:
    base_url, run_result, root = stats_server
    token = mint_token(["stats-reader"])
    status, body = get(base_url, "/v1/stats/values?period=2026-09", token=token)
    assert status == 200
    served = json.loads(body)["values"]

    gold_values = (
        DeltaTable(str(root / "platform_gold" / "port_kpi_values")).to_pyarrow_table().to_pylist()
    )
    # Honesty regression: the API can only return numbers that exist in the
    # gold table for this run — every served row matches a stored row exactly.
    stored = {
        (row["kpi_id"], row["port_code"], row["ship_class"], row["percentile"]): row["value"]
        for row in gold_values
    }
    assert len(served) == len(stored)
    for item in served:
        key = (item["kpi_id"], item["port_code"], item["ship_class"], item["percentile"])
        assert stored[key] == item["value"]
        assert item["run_id"] == run_result.run_id
        assert item["source_table_versions"] == {"platform_silver/events": 0}

    # Equality filters work and never invent rows.
    status, body = get(
        base_url,
        "/v1/stats/values?kpi_id=vessel_calls&port_code=NGLAG&period=2026-09",
        token=token,
    )
    assert status == 200
    filtered = json.loads(body)["values"]
    assert {row["value"] for row in filtered} == {2.0, 1.0, 3.0}
    status, body = get(base_url, "/v1/stats/values?period=2031-01", token=token)
    assert status == 200
    assert json.loads(body)["values"] == []


def test_values_endpoint_rejects_malformed_params(stats_server) -> None:
    base_url, _, _ = stats_server
    token = mint_token(["stats-reader"])
    for query in (
        "period=2026-13",
        "period=September",
        "port_code=Lagos",
        "kpi_id=made_up_kpi",
        "kpi_id=vessel_calls&kpi_id=vessel_calls",
        "bogus=1",
    ):
        status, body = get(base_url, f"/v1/stats/values?{query}", token=token)
        assert status == 400, query
        assert "error" in json.loads(body)


def test_report_endpoint_serves_the_exact_signed_artefact(stats_server) -> None:
    base_url, run_result, _ = stats_server
    token = mint_token(["stats-reader"])
    status, body = get(base_url, f"/v1/stats/report/{run_result.run_id}?format=json", token=token)
    assert status == 200
    artefact = json.loads(body)
    assert artefact["report_sha256"] == run_result.report_sha256
    assert artefact["run_id"] == run_result.run_id
    assert artefact["provenance"]["signature"]

    on_disk = Path(run_result.report_json_path).read_text(encoding="utf-8")
    assert json.loads(on_disk) == artefact

    status, body = get(base_url, f"/v1/stats/report/{run_result.run_id}?format=csv", token=token)
    assert status == 200
    assert body.splitlines()[0].startswith("kpi_id,period,port_code")

    status, _ = get(base_url, "/v1/stats/report/00000000-0000-0000-0000-000000000000", token=token)
    assert status == 404
    status, _ = get(base_url, "/v1/stats/report/..%2F..%2Fetc", token=token)
    assert status == 400
    status, _ = get(base_url, f"/v1/stats/report/{run_result.run_id}?format=xml", token=token)
    assert status == 400


def test_missing_tables_fail_closed_503(tmp_path: Path) -> None:
    root = tmp_path / "platform"
    root.mkdir()
    config = StatsApiConfig(str(root / "platform_gold"), 0, TOKEN_SECRET)
    server = serve(config)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        token = mint_token(["stats-reader"])
        status, body = get(base_url, "/v1/stats/runs", token=token)
        assert status == 503
        detail = json.loads(body)["error"]
        assert "no statistics run has been published" in detail
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_config_fails_closed_on_missing_env() -> None:
    with pytest.raises(ValueError, match="missing required configuration"):
        StatsApiConfig.from_env({})
    with pytest.raises(ValueError, match="STATS_API_PORT"):
        StatsApiConfig.from_env(
            {
                "STATS_API_TABLE_ROOT": "/lakehouse/platform/platform_gold",
                "STATS_API_PORT": "abc",
                "STATS_API_BEARER_TOKEN_SECRET": "s",
            }
        )
    with pytest.raises(ValueError, match="STATS_API_BEARER_TOKEN_SECRET"):
        StatsApiConfig.from_env(
            {
                "STATS_API_TABLE_ROOT": "/lakehouse/platform/platform_gold",
                "STATS_API_PORT": "8080",
                "STATS_API_BEARER_TOKEN_SECRET": "  ",
            }
        )
    # The table root must stay inside the platform boundary.
    with pytest.raises(ValueError, match="segregated root"):
        StatsApiConfig("/lakehouse/cvff/cvff_gold", 8080, TOKEN_SECRET)


def test_make_handler_is_bound_to_config(stats_server) -> None:
    base_url, _, _ = stats_server
    assert callable(make_handler)
