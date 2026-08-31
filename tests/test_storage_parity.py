"""Cross-backend parity: one medallion write path over every storage backend.

The adls and s3 backends are exercised at the configuration/guard layer (no
cloud credentials exist in test); the gated local backend simulates the same
path end-to-end with real Delta writes, proving segregation, medallion and
storage layers consume the same backend-neutral abstraction.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from deltalake import DeltaTable

from blueeconomy_data_platform.medallion import (
    append_bronze,
    append_silver,
    build_silver_record,
    curate_gold,
    KafkaRecordMetadata,
)
from blueeconomy_data_platform.segregation import LakehouseScope, SegregatedDeltaWriter
from blueeconomy_data_platform.storage import (
    resolve_lakehouse_root,
    resolve_storage_options,
    validate_s3_uri,
)

LEDGER_HASH = "c" * 64

ADLS_ENV = {
    "BLUEECONOMY_STORAGE_BACKEND": "adls",
    "BLUEECONOMY_AZURE_CLOUD": "AzureUSGovernment",
    "BLUEECONOMY_STORAGE_ACCOUNT": "blueeconomystore",
    "BLUEECONOMY_STORAGE_FILESYSTEM": "lakehouse",
}
S3_ENV = {
    "BLUEECONOMY_STORAGE_BACKEND": "s3",
    "BLUEECONOMY_S3_BUCKET": "blueeconomy-lakehouse",
    "BLUEECONOMY_S3_REGION": "us-east-1",
    "BLUEECONOMY_S3_SECURE": "true",
    "BLUEECONOMY_S3_ENDPOINT_URL": "https://minio.storage.example:9000",
}


def cvff_event(event_id: str) -> dict[str, object]:
    occurred = datetime(2026, 8, 12, tzinfo=UTC)
    return {
        "event_id": event_id,
        "event_type": "cvff.ledger.commitment.v1",
        "producer": "cvff-ledger-adapter",
        "occurred_at": occurred,
        "recorded_at": occurred,
        "data_classification": "fiduciary_segregated",
        "source_system": "cvff-ledger",
        "source_record_reference": f"ledger-{event_id}",
        "correlation_id": None,
        "payload_json": json.dumps({"ledgerCommitHash": LEDGER_HASH}),
        "ingested_at": occurred,
    }


def test_scope_root_parity_across_backends(tmp_path: Path) -> None:
    local_env = {
        "BLUEECONOMY_STORAGE_BACKEND": "local-gated",
        "BLUEECONOMY_ALLOW_LOCAL_STORAGE": "true",
        "BLUEECONOMY_LOCAL_LAKEHOUSE_ROOT": str(tmp_path),
    }
    adls_root = resolve_lakehouse_root(LakehouseScope.CVFF, ADLS_ENV)
    s3_root = resolve_lakehouse_root(LakehouseScope.CVFF, S3_ENV)
    local_root = resolve_lakehouse_root(LakehouseScope.CVFF, local_env)
    assert adls_root == ("abfs://lakehouse@blueeconomystore.dfs.core.usgovcloudapi.net/cvff")
    assert s3_root == "s3://blueeconomy-lakehouse/cvff"
    assert local_root == f"{tmp_path}/cvff"
    validate_s3_uri(f"{s3_root}/cvff_bronze/events")

    # The segregation writer applies identical boundary rules on every root.
    for root in (adls_root, s3_root, local_root):
        writer = SegregatedDeltaWriter(LakehouseScope.CVFF, root)
        uri = writer.guard_write("bronze", [cvff_event("evt-parity")], "cvff.ledger.commitments")
        assert uri == f"{root}/cvff_bronze/events"

    # Storage options stay credential-free and backend-scoped.
    assert resolve_storage_options(ADLS_ENV) == {}
    assert resolve_storage_options(S3_ENV) == {
        "AWS_REGION": "us-east-1",
        "AWS_ENDPOINT_URL": "https://minio.storage.example:9000",
    }
    assert resolve_storage_options(local_env) == {}


def test_medallion_write_path_parity_on_resolved_backend_root(tmp_path: Path) -> None:
    local_env = {
        "BLUEECONOMY_STORAGE_BACKEND": "local-gated",
        "BLUEECONOMY_ALLOW_LOCAL_STORAGE": "true",
        "BLUEECONOMY_LOCAL_LAKEHOUSE_ROOT": str(tmp_path),
    }
    scope_root = resolve_lakehouse_root(LakehouseScope.CVFF, local_env)
    writer = SegregatedDeltaWriter(LakehouseScope.CVFF, scope_root)

    events = [cvff_event("evt-parity-1"), cvff_event("evt-parity-2")]
    _version, written, present = append_bronze(writer, events, "cvff.ledger.commitments")
    assert (written, present) == (2, 0)

    records = [
        build_silver_record(
            event, KafkaRecordMetadata(topic="cvff.ledger.commitments", partition=0, offset=offset)
        )
        for offset, event in enumerate(events, start=10)
    ]
    _version, written, present = append_silver(writer, records)
    assert (written, present) == (2, 0)
    _version, written, present = append_silver(writer, records)
    assert (written, present) == (0, 2)

    _version, row_count = curate_gold(writer)
    assert row_count == 1
    gold = DeltaTable(writer.table_uri("gold")).to_pyarrow_table().to_pylist()
    assert gold[0]["ledger_commit_hash"] == LEDGER_HASH
    assert gold[0]["record_count"] == 2

    # The resolved root is under the gated local root and declares the cvff scope.
    assert Path(writer.table_uri("bronze")).is_relative_to(tmp_path)
