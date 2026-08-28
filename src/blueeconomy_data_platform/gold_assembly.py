"""Scheduled gold-layer assembly entry point (silver-to-gold rollups and export).

``blueeconomy-gold-assembly`` runs one governed gold-assembly pass for a
single segregated lakehouse scope. Configuration is supplied entirely
through environment variables and fails closed on anything missing or
invalid:

- ``BLUEECONOMY_GOLD_SCOPE`` (required): ``cvff`` runs the silver-to-gold
  ledger-commitment rollup; ``fisheries`` runs the export-consignment gold
  assembly followed by a clearance-filtered export read. No other scope has
  a defined gold rollup, so any other value fails closed.
- ``BLUEECONOMY_GOLD_SCOPE_ROOT_URI`` (required): the segregated scope root
  URI. :class:`SegregatedDeltaWriter` enforces the scope boundary on it.
- ``BLUEECONOMY_GOLD_REPORT`` (required): non-secret JSON run-report path.
- ``BLUEECONOMY_GOLD_EXPORT_PATH`` (required for the ``fisheries`` scope):
  JSON export of the consignment rows visible at the configured clearance.
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
from blueeconomy_data_platform.medallion import curate_gold
from blueeconomy_data_platform.segregation import LakehouseScope, SegregatedDeltaWriter

ENV_SCOPE = "BLUEECONOMY_GOLD_SCOPE"
ENV_SCOPE_ROOT_URI = "BLUEECONOMY_GOLD_SCOPE_ROOT_URI"
ENV_REPORT = "BLUEECONOMY_GOLD_REPORT"
ENV_EXPORT_PATH = "BLUEECONOMY_GOLD_EXPORT_PATH"
ENV_CLEARANCE = "BLUEECONOMY_GOLD_CLEARANCE"

# Scopes with a defined gold-layer rollup; every other scope fails closed.
GOLD_ASSEMBLY_SCOPES = frozenset({LakehouseScope.CVFF, LakehouseScope.FISHERIES})


@dataclass(frozen=True)
class GoldAssemblyConfig:
    scope: LakehouseScope
    scope_root_uri: str
    clearance: Clearance
    export_path: Path | None
    report_path: Path


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
    return GoldAssemblyConfig(
        scope=scope,
        scope_root_uri=scope_root_uri,
        clearance=clearance,
        export_path=export_path,
        report_path=report_path,
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
    started_at = datetime.now(UTC)
    writer = SegregatedDeltaWriter(config.scope, config.scope_root_uri)
    exported_rows: int | None = None
    if config.scope is LakehouseScope.CVFF:
        assembly = "cvff-silver-gold-ledger-commitments"
        table_version, gold_rows = curate_gold(writer)
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
    )


def main() -> None:
    try:
        config = load_config(os.environ)
        report = run(config)
        _write_json_atomic(config.report_path, asdict(report))
        print(json.dumps(asdict(report), sort_keys=True))
    except (AccessDeniedError, OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"blueeconomy-gold-assembly: {error}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
