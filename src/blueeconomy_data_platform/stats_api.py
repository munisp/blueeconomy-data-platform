"""``blueeconomy-stats-api``: read-only serving layer for port statistics.

Serves the precomputed gold KPI tables (``port_kpi_runs`` manifest +
``port_kpi_values``) and the signed per-run report artefacts. It never
computes aggregates at request time — responses are exact gold rows,
filtered by equality on the query parameters — so every served figure traces
to a batch run recorded in the provenance ledger (spec §2 serving layer).

Configuration is env-only and fail-closed:

- ``STATS_API_TABLE_ROOT`` (required): the platform scope gold directory
  (for example ``s3://bucket/platform/platform_gold`` or a local path).
- ``STATS_API_PORT`` (required): listen port.
- ``STATS_API_BEARER_TOKEN_SECRET`` (required): HS256 gateway-token secret
  (port-interop gateway-token pattern). Data endpoints require a bearer JWT
  whose ``roles`` claim includes ``stats-reader``; ``/v1/stats/health`` is
  unauthenticated liveness only.

OTel is a no-op unless ``OTEL_EXPORTER_OTLP_ENDPOINT`` is set; the service
boots and serves with telemetry off.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import re
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Mapping
from urllib.parse import parse_qs, urlsplit

from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from blueeconomy_data_platform.port_statistics import (
    KPI_BY_ID,
    KPI_DEFINITIONS,
    PERIOD_PATTERN,
    REPORTS_DIR_NAME,
    RUNS_TABLE_NAME,
    STATS_GAPS,
    UNLOCODE_PATTERN,
    VALUES_TABLE_NAME,
)
from blueeconomy_data_platform.segregation import LakehouseScope, require_scope_table_uri

ENV_TABLE_ROOT = "STATS_API_TABLE_ROOT"
ENV_PORT = "STATS_API_PORT"
ENV_TOKEN_SECRET = "STATS_API_BEARER_TOKEN_SECRET"

REQUIRED_ROLE = "stats-reader"
RUN_ID_PATTERN = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

ROUTE_HEALTH = "/v1/stats/health"
ROUTE_KPIS = "/v1/stats/kpis"
ROUTE_RUNS = "/v1/stats/runs"
ROUTE_VALUES = "/v1/stats/values"
ROUTE_REPORT_PREFIX = "/v1/stats/report/"


class StatsApiError(Exception):
    """Truthful API failure with an HTTP status; never an empty success."""

    def __init__(self, status: HTTPStatus, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.detail = detail


class StatsApiConfig:
    """Resolved, fail-closed service configuration."""

    def __init__(self, table_root: str, port: int, token_secret: str) -> None:
        if not table_root or table_root != table_root.strip():
            raise ValueError(f"{ENV_TABLE_ROOT} must be set to canonical non-empty text")
        root = table_root.rstrip("/")
        require_scope_table_uri(LakehouseScope.PLATFORM, root)
        # Port 0 is accepted so tests can bind an ephemeral port; production
        # configuration through from_env requires an explicit port.
        if not 0 <= port <= 65535:
            raise ValueError(f"{ENV_PORT} must be a TCP port number")
        if not token_secret or token_secret != token_secret.strip():
            raise ValueError(f"{ENV_TOKEN_SECRET} must be set (fail-closed)")
        self.table_root = root
        self.port = port
        self.token_secret = token_secret.encode("utf-8")

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> StatsApiConfig:
        missing = [
            name for name in (ENV_TABLE_ROOT, ENV_PORT, ENV_TOKEN_SECRET) if not env.get(name)
        ]
        if missing:
            raise ValueError(f"missing required configuration: {', '.join(missing)}")
        try:
            port = int(env[ENV_PORT])
        except ValueError:
            raise ValueError(f"{ENV_PORT} must be a TCP port number") from None
        if port == 0:
            raise ValueError(f"{ENV_PORT} must be an explicit TCP port number")
        return cls(env[ENV_TABLE_ROOT], port, env[ENV_TOKEN_SECRET])

    def table_uri(self, name: str) -> str:
        uri = f"{self.table_root}/{name}"
        require_scope_table_uri(LakehouseScope.PLATFORM, uri)
        return uri

    def reports_dir(self) -> str:
        uri = f"{self.table_root}/{REPORTS_DIR_NAME}"
        require_scope_table_uri(LakehouseScope.PLATFORM, uri)
        return uri


def _b64url_decode(segment: str) -> bytes:
    if not segment or "=" in segment or not re.fullmatch(r"[A-Za-z0-9_-]+", segment):
        raise StatsApiError(HTTPStatus.UNAUTHORIZED, "malformed bearer token")
    try:
        return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))
    except (binascii.Error, ValueError):
        raise StatsApiError(HTTPStatus.UNAUTHORIZED, "malformed bearer token") from None


def authenticate(config: StatsApiConfig, authorization: str | None) -> None:
    """Verify the HS256 gateway token and the stats-reader role (fail-closed)."""
    if authorization is None or not authorization.startswith("Bearer "):
        raise StatsApiError(HTTPStatus.UNAUTHORIZED, "bearer token required")
    segments = authorization[len("Bearer ") :].split(".")
    if len(segments) != 3:
        raise StatsApiError(HTTPStatus.UNAUTHORIZED, "malformed bearer token")
    encoded_header, encoded_payload, encoded_signature = segments
    header_bytes = _b64url_decode(encoded_header)
    payload_bytes = _b64url_decode(encoded_payload)
    signature = _b64url_decode(encoded_signature)
    try:
        header = json.loads(header_bytes.decode("utf-8"))
        claims = json.loads(payload_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise StatsApiError(HTTPStatus.UNAUTHORIZED, "malformed bearer token") from None
    if not isinstance(header, dict) or header.get("alg") != "HS256":
        raise StatsApiError(HTTPStatus.UNAUTHORIZED, "unsupported token algorithm")
    expected = hmac.new(
        config.token_secret, f"{encoded_header}.{encoded_payload}".encode("ascii"), hashlib.sha256
    ).digest()
    if not hmac.compare_digest(expected, signature):
        raise StatsApiError(HTTPStatus.UNAUTHORIZED, "token signature does not verify")
    if not isinstance(claims, dict):
        raise StatsApiError(HTTPStatus.UNAUTHORIZED, "token claims must be a JSON object")
    expires = claims.get("exp")
    if expires is not None:
        if isinstance(expires, bool) or not isinstance(expires, (int, float)):
            raise StatsApiError(HTTPStatus.UNAUTHORIZED, "token exp claim is malformed")
        if float(expires) <= time.time():
            raise StatsApiError(HTTPStatus.UNAUTHORIZED, "token is expired")
    roles = claims.get("roles")
    if not isinstance(roles, list) or REQUIRED_ROLE not in roles:
        raise StatsApiError(HTTPStatus.FORBIDDEN, f"insufficient role: {REQUIRED_ROLE} is required")


def _read_gold_table(config: StatsApiConfig, name: str) -> list[dict[str, Any]]:
    try:
        table = DeltaTable(config.table_uri(name))
    except TableNotFoundError:
        raise StatsApiError(
            HTTPStatus.SERVICE_UNAVAILABLE,
            f"gold table {name} does not exist yet; no statistics run has been published",
        ) from None
    except Exception as error:
        raise StatsApiError(
            HTTPStatus.SERVICE_UNAVAILABLE, f"gold table {name} is unreadable: {error}"
        ) from error
    rows: list[dict[str, Any]] = table.to_pyarrow_table().to_pylist()
    return rows


def kpi_registry_document() -> dict[str, Any]:
    """The static KPI registry (definitions are pinned, never computed here)."""
    return {
        "kpis": [
            {
                "kpi_id": definition.kpi_id,
                "name": definition.name,
                "definition": definition.definition,
                "unit": definition.unit,
                "definition_version": definition.definition_version,
                "gap_id": definition.gap_id,
            }
            for definition in KPI_DEFINITIONS
        ],
        "gaps": [
            {
                "gap_id": gap.gap_id,
                "description": gap.description,
                "needed_upstream": gap.needed_upstream,
            }
            for gap in STATS_GAPS
        ],
    }


def run_manifest(row: dict[str, Any]) -> dict[str, Any]:
    """Project a gold run row to its API manifest shape."""
    return {
        "run_id": row["run_id"],
        "computed_at": row["computed_at"].isoformat(),
        "period": row["period"],
        "period_start": row["period_start"].isoformat(),
        "period_end": row["period_end"].isoformat(),
        "scope": row["scope"],
        "source_table_versions": json.loads(row["source_table_versions_json"]),
        "query_definitions_sha256": row["query_definitions_sha256"],
        "kpi_count": row["kpi_count"],
        "rows_emitted": row["rows_emitted"],
        "rows_no_data": row["rows_no_data"],
        "report_sha256": row["report_sha256"],
    }


def list_runs(config: StatsApiConfig) -> dict[str, Any]:
    rows = _read_gold_table(config, RUNS_TABLE_NAME)
    rows.sort(key=lambda row: (row["computed_at"], row["run_id"]), reverse=True)
    return {"runs": [run_manifest(row) for row in rows]}


def query_values(config: StatsApiConfig, query: dict[str, list[str]]) -> dict[str, Any]:
    """Serve precomputed KPI rows filtered by equality; no ad-hoc aggregation."""
    filters: dict[str, str] = {}
    for parameter in ("kpi_id", "port_code", "period"):
        values = query.get(parameter)
        if values is None:
            continue
        if len(values) != 1:
            raise StatsApiError(HTTPStatus.BAD_REQUEST, f"parameter {parameter} repeats")
        filters[parameter] = values[0]
    unknown = sorted(set(query) - {"kpi_id", "port_code", "period"})
    if unknown:
        raise StatsApiError(
            HTTPStatus.BAD_REQUEST, f"unknown query parameters: {', '.join(unknown)}"
        )
    if "kpi_id" in filters and filters["kpi_id"] not in KPI_BY_ID:
        raise StatsApiError(HTTPStatus.BAD_REQUEST, "unknown kpi_id")
    if "period" in filters and not PERIOD_PATTERN.fullmatch(filters["period"]):
        raise StatsApiError(HTTPStatus.BAD_REQUEST, "period must match YYYY-MM")
    if "port_code" in filters and not UNLOCODE_PATTERN.fullmatch(filters["port_code"]):
        raise StatsApiError(HTTPStatus.BAD_REQUEST, "port_code must be a UN/LOCODE")

    runs_by_id = {row["run_id"]: row for row in _read_gold_table(config, RUNS_TABLE_NAME)}
    items: list[dict[str, Any]] = []
    for row in _read_gold_table(config, VALUES_TABLE_NAME):
        if any(row[key] != value for key, value in filters.items()):
            continue
        manifest = runs_by_id.get(row["run_id"])
        if manifest is None:
            # A value row without its provenance manifest can never be served.
            raise StatsApiError(
                HTTPStatus.SERVICE_UNAVAILABLE,
                f"value row references unknown run_id {row['run_id']!r}",
            )
        items.append(
            {
                "run_id": row["run_id"],
                "kpi_id": row["kpi_id"],
                "period": row["period"],
                "port_code": row["port_code"],
                "ship_class": row["ship_class"],
                "value": row["value"],
                "unit": row["unit"],
                "n_observations": row["n_observations"],
                "percentile": row["percentile"],
                "coverage_note": row["coverage_note"],
                "definition_version": row["definition_version"],
                "source_table": row["source_table"],
                "table_version": row["table_version"],
                "query_hash": row["query_hash"],
                "computed_at": row["computed_at"].isoformat(),
                "source_table_versions": json.loads(manifest["source_table_versions_json"]),
            }
        )
    items.sort(
        key=lambda item: (
            item["kpi_id"],
            str(item["port_code"]),
            str(item["ship_class"]),
            str(item["percentile"]),
        )
    )
    return {"values": items}


def get_report(config: StatsApiConfig, run_id: str, query: dict[str, list[str]]) -> tuple[str, str]:
    """Return (content_type, body) for the exact signed artefact of one run."""
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise StatsApiError(HTTPStatus.BAD_REQUEST, "run_id must be a UUID")
    formats = query.get("format", ["json"])
    if len(formats) != 1 or formats[0] not in {"json", "csv"}:
        raise StatsApiError(HTTPStatus.BAD_REQUEST, "format must be json or csv")
    from pathlib import Path

    path = Path(config.reports_dir()) / f"{run_id}.{formats[0]}"
    try:
        if not path.is_file() or path.is_symlink():
            raise StatsApiError(HTTPStatus.NOT_FOUND, f"no report artefact for run_id {run_id!r}")
        body = path.read_text(encoding="utf-8")
    except OSError as error:
        raise StatsApiError(
            HTTPStatus.SERVICE_UNAVAILABLE, f"report artefact is unreadable: {error}"
        ) from error
    content_type = "application/json" if formats[0] == "json" else "text/csv"
    return content_type, body


# ---------------------------------------------------------------------------
# HTTP plumbing (stdlib, dependency-light per repo style)
# ---------------------------------------------------------------------------


def make_handler(config: StatsApiConfig) -> type[BaseHTTPRequestHandler]:
    """Build the request handler bound to a resolved configuration."""

    class StatsHandler(BaseHTTPRequestHandler):
        server_version = "blueeconomy-stats-api/0.1.0"

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            import logging

            logging.getLogger("blueeconomy_data_platform.stats_api").info(
                "%s %s", self.address_string(), format % args
            )

        def _respond(
            self, status: HTTPStatus, body: str, content_type: str = "application/json"
        ) -> None:
            from blueeconomy_data_platform.telemetry import get_meter

            get_meter().create_counter(
                "stats_api_requests_total", description="stats API requests by route and status"
            ).add(1, {"route": self._route_label(), "code": str(status.value)})
            encoded = body.encode("utf-8")
            self.send_response(status.value)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def _route_label(self) -> str:
            path = urlsplit(self.path).path
            if path.startswith(ROUTE_REPORT_PREFIX):
                return ROUTE_REPORT_PREFIX + "{run_id}"
            return path

        def _respond_json(self, status: HTTPStatus, document: dict[str, Any]) -> None:
            self._respond(status, json.dumps(document, sort_keys=True) + "\n")

        def do_GET(self) -> None:  # noqa: N802 (stdlib handler contract)
            from blueeconomy_data_platform.telemetry import get_tracer

            with get_tracer().start_as_current_span("stats_api.request") as span:
                span.set_attribute("http.route", self._route_label())
                try:
                    self._handle_get()
                except StatsApiError as error:
                    span.set_attribute("http.status_code", error.status.value)
                    self._respond_json(error.status, {"error": error.detail})
                except Exception as error:  # fail closed, truthful 503
                    span.set_attribute("http.status_code", HTTPStatus.SERVICE_UNAVAILABLE.value)
                    self._respond_json(
                        HTTPStatus.SERVICE_UNAVAILABLE,
                        {"error": f"stats API could not serve the request: {error}"},
                    )

        def _handle_get(self) -> None:
            split = urlsplit(self.path)
            path = split.path.rstrip("/") or "/"
            if path == ROUTE_HEALTH:
                # Liveness only: no data claims (spec §4).
                self._respond_json(HTTPStatus.OK, {"status": "ok"})
                return
            authenticate(config, self.headers.get("Authorization"))
            query = parse_qs(split.query, keep_blank_values=False)
            if path == ROUTE_KPIS:
                self._respond_json(HTTPStatus.OK, kpi_registry_document())
            elif path == ROUTE_RUNS:
                self._respond_json(HTTPStatus.OK, list_runs(config))
            elif path == ROUTE_VALUES:
                self._respond_json(HTTPStatus.OK, query_values(config, query))
            elif path.startswith(ROUTE_REPORT_PREFIX):
                run_id = path[len(ROUTE_REPORT_PREFIX) :]
                content_type, body = get_report(config, run_id, query)
                self._respond(HTTPStatus.OK, body, content_type)
            else:
                raise StatsApiError(HTTPStatus.NOT_FOUND, f"unknown route {path!r}")

    return StatsHandler


def serve(config: StatsApiConfig) -> ThreadingHTTPServer:
    """Create (but do not start) the HTTP server; fail closed on bad config."""
    return ThreadingHTTPServer(("0.0.0.0", config.port), make_handler(config))


def main() -> None:
    import os
    import sys

    from blueeconomy_data_platform.telemetry import init_telemetry, shutdown_telemetry

    init_telemetry(service_name="blueeconomy-stats-api", version="0.1.0")
    try:
        config = StatsApiConfig.from_env(os.environ)
        server = serve(config)
        print(f"blueeconomy-stats-api listening on :{config.port}", file=sys.stderr)
        server.serve_forever()
    except (OSError, ValueError) as error:
        print(f"blueeconomy-stats-api: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        shutdown_telemetry()


if __name__ == "__main__":
    main()
