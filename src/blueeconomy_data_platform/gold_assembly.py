"""Scheduled gold-layer assembly entry point (silver-to-gold rollups and export).

``blueeconomy-gold-assembly`` runs one governed gold-assembly pass for a
single segregated lakehouse scope. Configuration is supplied entirely
through environment variables and fails closed on anything missing or
invalid:

- ``BLUEECONOMY_GOLD_SCOPE`` (required): ``cvff`` runs the silver-to-gold
  ledger-commitment rollup; ``fisheries`` runs the export-consignment gold
  assembly followed by a clearance-filtered export read; ``platform`` runs
  the port-KPI statistics rollup (phase 8); ``mrv`` runs the MRV
  ``vessel_annual`` gold assembly; ``bluecarbon`` runs the Blue-Carbon
  ``public_registry`` gold projection. No other scope has a defined gold
  rollup, so any other value fails closed.
- ``BLUEECONOMY_GOLD_SCOPE_ROOT_URI`` (required): the segregated scope root
  URI. :class:`SegregatedDeltaWriter` enforces the scope boundary on it.
- ``BLUEECONOMY_GOLD_REPORT`` (required): non-secret JSON run-report path.
- ``BLUEECONOMY_GOLD_EXPORT_PATH`` (required for the ``fisheries`` scope):
  JSON export of the consignment rows visible at the configured clearance.
- ``BLUEECONOMY_GOLD_STATS_PERIOD`` (required for the ``platform`` scope):
  the ``YYYY-MM`` computation period for the port-KPI statistics rollup.
- ``BLUEECONOMY_SIGNING_KEY_SEED`` / ``BLUEECONOMY_SIGNING_KID`` (required
  for the ``platform`` scope): env-only Ed25519 report-signing key and kid
  for the signed statistics report artefact (envelope v1.0 JWS scheme).
- ``BLUEECONOMY_GOLD_CLEARANCE`` (optional): clearance claim for the export
  read path. It defaults to ``UNCLASSIFIED`` — the most restrictive
  clearance — so an unconfigured run exports only consignments whose source
  events were all explicitly labelled UNCLASSIFIED; unlabelled or
  higher-classified source data is withheld.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Mapping

from blueeconomy_data_platform.access_policy import AccessDeniedError, Clearance
from blueeconomy_data_platform.export_consignment import (
    assemble_export_consignment_gold,
    read_export_consignments,
)
from blueeconomy_data_platform.bluecarbon_gold import assemble_bluecarbon_public_registry_gold
from blueeconomy_data_platform.medallion import curate_gold
from blueeconomy_data_platform.mrv_gold import assemble_mrv_vessel_annual_gold
from blueeconomy_data_platform.port_statistics import (
    PortStatisticsRunResult,
    parse_period,
    run_port_statistics,
)
from blueeconomy_data_platform.segregation import LakehouseScope, SegregatedDeltaWriter
from blueeconomy_data_platform.signature_verification import load_signing_key_from_env

ENV_SCOPE = "BLUEECONOMY_GOLD_SCOPE"
ENV_SCOPE_ROOT_URI = "BLUEECONOMY_GOLD_SCOPE_ROOT_URI"
ENV_REPORT = "BLUEECONOMY_GOLD_REPORT"
ENV_EXPORT_PATH = "BLUEECONOMY_GOLD_EXPORT_PATH"
ENV_CLEARANCE = "BLUEECONOMY_GOLD_CLEARANCE"
ENV_STATS_PERIOD = "BLUEECONOMY_GOLD_STATS_PERIOD"

# Scopes with a defined gold-layer rollup; every other scope fails closed.
GOLD_ASSEMBLY_SCOPES = frozenset(
    {
        LakehouseScope.CVFF,
        LakehouseScope.FISHERIES,
        LakehouseScope.PLATFORM,
        LakehouseScope.MRV,
        LakehouseScope.BLUECARBON,
    }
)


@dataclass(frozen=True)
class GoldAssemblyConfig:
    scope: LakehouseScope
    scope_root_uri: str
    clearance: Clearance
    export_path: Path | None
    report_path: Path
    stats_period: str | None


@dataclass(frozen=True)
class GoldAssemblyReport:
    schema_version: str
    started_at: str
    completed_at: str
    lakehouse_scope: str
    assembly: str
    clearance: str
    gold_table_version: int
    gold_rows: int
    exported_rows: int | None
    stats_run_id: str | None
    stats_report_sha256: str | None


def _require_env(environment: Mapping[str, str], name: str) -> str:
    value = environment.get(name, "")
    if not value or value != value.strip():
        raise ValueError(f"{name} must be set to canonical non-empty text")
    return value


def load_config(environment: Mapping[str, str]) -> GoldAssemblyConfig:
    """Resolve the gold-assembly configuration, failing closed on any defect."""
    scope_name = _require_env(environment, ENV_SCOPE)
    try:
        scope = LakehouseScope(scope_name)
    except ValueError:
        raise ValueError(f"{ENV_SCOPE} {scope_name!r} is not a governed lakehouse scope") from None
    if scope not in GOLD_ASSEMBLY_SCOPES:
        raise ValueError(
            f"no gold assembly is defined for the {scope.value!r} scope; "
            f"supported scopes: {sorted(item.value for item in GOLD_ASSEMBLY_SCOPES)}"
        )
    scope_root_uri = _require_env(environment, ENV_SCOPE_ROOT_URI)
    report_path = Path(_require_env(environment, ENV_REPORT))

    clearance_label = environment.get(ENV_CLEARANCE, Clearance.UNCLASSIFIED.label)
    clearance = Clearance.from_label(clearance_label)

    export_path: Path | None = None
    if scope is LakehouseScope.FISHERIES:
        export_path = Path(_require_env(environment, ENV_EXPORT_PATH))
        if export_path.resolve(strict=False) == report_path.resolve(strict=False):
            raise ValueError("export path must not overwrite the report path")
    stats_period: str | None = None
    if scope is LakehouseScope.PLATFORM:
        stats_period = _require_env(environment, ENV_STATS_PERIOD)
        # Fail closed at configuration time on a malformed period or a
        # missing/malformed report-signing key (secrets are env-only).
        parse_period(stats_period)
        load_signing_key_from_env(environment)
    return GoldAssemblyConfig(
        scope=scope,
        scope_root_uri=scope_root_uri,
        clearance=clearance,
        export_path=export_path,
        report_path=report_path,
        stats_period=stats_period,
    )


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    raise TypeError(f"gold export value of type {type(value).__name__} is not JSON serializable")


def _write_json_atomic(path: Path, document: object) -> None:
    path.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(document, indent=2, default=_json_default) + "\n", encoding="utf-8"
    )
    temporary.chmod(0o640)
    temporary.replace(path)


def run(config: GoldAssemblyConfig) -> GoldAssemblyReport:
    """Execute one gold-assembly pass for the configured scope."""
    # Phase-7 OTel span (no-op when telemetry is disabled); the gold stage
    # of the medallion DAG (curate_gold nests the silver->gold write spans).
    from blueeconomy_data_platform.telemetry import get_tracer

    with get_tracer().start_as_current_span("lakehouse.pipeline.gold_assembly") as span:
        span.set_attribute("lakehouse.scope", config.scope.value)
        report = _run(config)
        span.set_attribute("lakehouse.rows", report.gold_rows)
        span.set_attribute("lakehouse.table_version", report.gold_table_version)
        return report


def _run(config: GoldAssemblyConfig) -> GoldAssemblyReport:
    started_at = datetime.now(UTC)
    writer = SegregatedDeltaWriter(config.scope, config.scope_root_uri)
    exported_rows: int | None = None
    stats_result: PortStatisticsRunResult | None = None
    if config.scope is LakehouseScope.CVFF:
        assembly = "cvff-silver-gold-ledger-commitments"
        table_version, gold_rows = curate_gold(writer)
    elif config.scope is LakehouseScope.MRV:
        assembly = "mrv-gold-vessel-annual"
        table_version, gold_rows = assemble_mrv_vessel_annual_gold(writer)
    elif config.scope is LakehouseScope.BLUECARBON:
        assembly = "bluecarbon-gold-public-registry"
        table_version, gold_rows = assemble_bluecarbon_public_registry_gold(writer)
    elif config.scope is LakehouseScope.PLATFORM:
        assembly = "platform-gold-port-statistics"
        if config.stats_period is None:
            raise ValueError(f"{ENV_STATS_PERIOD} is required for the platform scope")
        signing_key, signing_kid = load_signing_key_from_env()
        stats_result = run_port_statistics(writer, config.stats_period, signing_key, signing_kid)
        table_version, gold_rows = stats_result.values_table_version, stats_result.rows_emitted
    else:
        assembly = "fisheries-gold-export-consignments"
        table_version, gold_rows = assemble_export_consignment_gold(writer)
        if config.export_path is None:
            raise ValueError(f"{ENV_EXPORT_PATH} is required for the fisheries scope")
        visible = read_export_consignments(writer, config.clearance)
        _write_json_atomic(config.export_path, visible)
        exported_rows = len(visible)
    return GoldAssemblyReport(
        schema_version="blueeconomy.lakehouse.gold-assembly-report.v1",
        started_at=started_at.isoformat(),
        completed_at=datetime.now(UTC).isoformat(),
        lakehouse_scope=config.scope.value,
        assembly=assembly,
        clearance=config.clearance.label,
        gold_table_version=table_version,
        gold_rows=gold_rows,
        exported_rows=exported_rows,
        stats_run_id=stats_result.run_id if stats_result is not None else None,
        stats_report_sha256=stats_result.report_sha256 if stats_result is not None else None,
    )


def main() -> None:
    # OTel (Phase-7): no-op unless OTEL_EXPORTER_OTLP_ENDPOINT is set.
    from blueeconomy_data_platform.telemetry import init_telemetry, shutdown_telemetry

    init_telemetry(service_name="blueeconomy-data-platform-gold-assembly", version="0.1.0")
    try:
        config = load_config(os.environ)
        report = run(config)
        _write_json_atomic(config.report_path, asdict(report))
        print(json.dumps(asdict(report), sort_keys=True))
    except (AccessDeniedError, OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"blueeconomy-gold-assembly: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        shutdown_telemetry()


if __name__ == "__main__":
    main()
