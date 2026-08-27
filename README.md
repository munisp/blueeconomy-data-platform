# Blue Economy Data Platform

This repository contains governed ingestion components for the platform lakehouse. It uses open-source **Delta Lake** and **Parquet** libraries for controlled event storage. Apache Spark, Flink, DataFusion, Ray and Sedona remain separately governed execution engines and are introduced only for an approved workload and measured operational need.

## Implemented component: governed event ingestion

`blueeconomy-ingest-events` accepts an explicitly supplied **real-source** NDJSON file, validates every record against the committed event-envelope schema, requires provenance and classification, canonicalises the retained payload JSON, and writes to a Delta table configured with `delta.appendOnly=true`. Existing tables use an insert-only Delta merge keyed by `event_id`; a retried event is counted as already present and is not duplicated.

The file command has no default input, endpoint, source system, table URI, credential or synthetic fallback. `blueeconomy-ingest-kafka` adds a bounded Kafka consumer that disables automatic commits, persists a schema-validated batch to Delta, then commits each consumed partition offset synchronously. A write followed by an offset failure is safely replayable because Delta insertion remains keyed by `event_id`. Neither command is represented as a live maritime, IoT, port, payment or agency integration until authorised non-production records traverse an approved Ministry environment and the evidence is accepted.

## Required execution inputs

| Input | Requirement |
|---|---|
| `--input` | Approved regular NDJSON file, not a symlink. Empty, oversized, malformed or noncanonical input fails closed. |
| `--schema` | The committed [`event-envelope.schema.json`](schemas/event-envelope.schema.json). |
| `--table-uri` | Approved writable Delta location. Credentials, query parameters and fragments must not be embedded in the URI. |
| `--report` | Non-secret evidence path distinct from the input and schema files. |

```bash
blueeconomy-ingest-events \
  --input /approved/input/events.ndjson \
  --schema schemas/event-envelope.schema.json \
  --table-uri /approved/lakehouse/bronze/events \
  --report /approved/evidence/lakehouse-ingestion-report.json
```

The v2 file-ingestion report contains record counts, already-present counts, source systems, classifications, table version, the input-file SHA-256 and a SHA-256 reference for the table location. It does not disclose the raw input path, table URI or payload. The Kafka report similarly hashes broker and consumer-group references, records confirmed partition offsets and excludes credentials and message payloads.

For Kafka ingestion, `--bootstrap-servers`, `--topic`, `--group-id`, `--security-protocol`, `--max-messages`, `--lakehouse-scope`, `--table-uri`, `--schema` and `--report` are mandatory. `PLAINTEXT` is accepted only for explicit loopback integration runs; non-local execution requires `SSL` or `SASL_SSL`, CA material and environment-injected SASL credentials where applicable. The proven local Apache Kafka-to-Delta path is documented in [`integration/kafka-delta`](integration/kafka-delta/README.md).

## S2 GeoJSON geofence evaluation

`blueeconomy_data_platform.geofence` provides a real, dependency-free evaluator for validated WGS84 GeoJSON `Polygon` and `MultiPolygon` boundaries. It validates finite longitude/latitude ranges, closed linear rings, holes, boundary points and multipolygon membership, and rejects unsupported geometry rather than silently approximating it. It is suitable for deterministic local event classification and test evidence. It does not replace Sedona/PostGIS geospatial storage, geodesic/antimeridian handling or a Ministry-approved geofence source; those remain target-deployment requirements.

## Integrity and operational boundaries

The implementation enforces bounded files, bounded lines, bounded canonical payloads, finite JSON values, canonical text identifiers, `occurred_at <= recorded_at`, unique IDs within a batch and Delta insert-only idempotency. New-table creation and existing-table merge use the actual pinned Delta Lake library.

This remains an ingestion subsystem rather than a complete data platform. A real local Apache Kafka broker now verifies topic consumption, offset commits and idempotent Delta replay. External-agency readiness still requires Ministry object storage and IAM, encryption keys, catalog/lineage, retention/legal hold, source agreements, schema compatibility, TLS/SASL Kafka clusters and ACLs, Flink delivery, dead-letter/replay policy, single-writer or conflict-retry operating rules, backup/recovery, quality ownership and an approved integration registry.

## Reproducible local verification

Use Python 3.12. Install the hash-locked development graph and run the saved verification script:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --require-hashes -r requirements-dev.lock
chmod +x scripts/verify-local.sh
./scripts/verify-local.sh
```

The script runs Ruff formatting/linting, strict mypy, pytest with a real local Delta table, Bandit, `pip-audit`, schema validation and wheel/source-distribution builds.

## Data authority

The schema requires `public`, `internal`, `confidential`, `restricted` or `highly_restricted` classification plus source-system and source-record references. These fields do not grant permission to ingest a source. The accountable data owner must approve lawful purpose, data-sharing terms, minimisation, retention, correction, access, export and incident handling before target deployment.

## Fiduciary segregation (Workstream C / CVFF)

Workstream C (CVFF fintech) runs on a **physically segregated Delta Lake schema** on Azure Government ADLS Gen2. Workstream C events carry the `fiduciary_segregated` classification and arrive on `cvff.*` Kafka topics; Workstream A (`ports.*`) and Workstream B (`ferries.*`) events remain in the platform scope. There is no shared schema and no shared storage root between the scopes:

| Scope | Kafka namespaces | Delta tables |
|---|---|---|
| platform | `ports.*`, `ferries.*` | `<root>/platform/platform_bronze/events`, `platform_silver/events`, `platform_gold/events` |
| cvff | `cvff.*` | `<root>/cvff/cvff_bronze/events`, `cvff_silver/events`, `cvff_gold/events` |

### Boundary guarantees

Segregation is enforced at the write path, not by routing convention:

- `blueeconomy_data_platform.segregation.SegregatedDeltaWriter` is initialized for exactly one scope and one scope root. A cvff writer cannot be pointed at a non-`cvff*` root, and a platform writer cannot be pointed at a `cvff*` root — initialization fails closed.
- `guard_write` raises `BoundaryViolationError` before any record is written when an event classification, Kafka topic or table URI belongs to the other scope. A platform writer cannot write a `fiduciary_segregated` record and a cvff writer cannot write any platform-classified record.
- Classification and topic mappings fail closed: an unrecognized `data_classification` or a topic outside the `cvff.*`/`ports.*`/`ferries.*` namespaces is rejected, never defaulted.
- `blueeconomy-ingest-kafka` requires `--lakehouse-scope platform|cvff`; the topic namespace, the event classifications in the batch and the target table URI must all match the declared scope before consumption is persisted.

### CVFF medallion layers

`blueeconomy_data_platform.medallion` implements the segregated cvff medallion pipeline on top of `SegregatedDeltaWriter`:

- **Bronze** — raw validated envelopes, append-only. The retention policy (default 30 days hot, 7 years cold, bounds-validated) is committed in the Delta table description at creation (the delta-rs kernel rejects custom table properties, so the horizons live in retained table metadata); `RetentionPolicy.tier_for` and `retention_report` classify records as `hot`, `cold` or `expired` for the operations runbook.
- **Silver** — deduplicated on the composite key `sha256(kafka_topic/kafka_partition/kafka_offset/ledgerCommitHash)`. The `ledgerCommitHash` (64 lowercase hex) is required in the cvff payload. Replayed Kafka records with an identical dedup key are counted as already present and never duplicated; a dedup key reused with conflicting content fails closed.
- **Gold** — a curated one-row-per-`ledgerCommitHash` snapshot (record count, occurrence window, source systems, event IDs) atomically overwritten from silver.

### Segregated read access

`blueeconomy_data_platform.access_policy` maps Keycloak-style role claims to readable schemas. The cvff schemas are readable **only** by cvff-scoped roles, all read-only on the CVFF scope:

| Role claim | Readable schemas |
|---|---|
| `independent-auditor` | `cvff_bronze`, `cvff_silver`, `cvff_gold` |
| `nimasa-approver` | `cvff_bronze`, `cvff_silver`, `cvff_gold` |
| `cbn-observer` | `cvff_bronze`, `cvff_silver`, `cvff_gold` |
| `fmmbe-oversight` | `platform_bronze`, `platform_silver`, `platform_gold` |

Unknown roles, unknown schemas and empty claim sets are denied. No governance role can write; writes belong to governed service principals. This module is the platform-side policy of record: deployment must bind the same grants to Keycloak client scopes and ADLS Gen2 ACLs so the storage layer independently denies what the policy denies.

### Azure Government storage configuration

Lakehouse roots are resolved from the environment only; no endpoint, account or credential is hardcoded, and no AWS-specific assumptions exist anywhere in the codebase. `blueeconomy_data_platform.storage.resolve_lakehouse_root` fails closed unless configuration is complete:

| Variable | Requirement |
|---|---|
| `BLUEECONOMY_STORAGE_BACKEND` | `adls-gen2` for deployment; `local` only behind the explicit `BLUEECONOMY_ALLOW_LOCAL_STORAGE=true` development gate. |
| `BLUEECONOMY_AZURE_CLOUD` | `AzureUSGovernment` (endpoint suffix `dfs.core.usgovcloudapi.net`) for the CVFF deployment, or `AzureCloud`. No other cloud is accepted. |
| `BLUEECONOMY_STORAGE_ACCOUNT` | ADLS Gen2 account name (3–24 lowercase alphanumeric). |
| `BLUEECONOMY_STORAGE_FILESYSTEM` | ADLS Gen2 filesystem (container) name. |
| `BLUEECONOMY_LOCAL_LAKEHOUSE_ROOT` | Absolute path; required only for the gated local backend. |

The resolved URI is `abfs://<filesystem>@<account>.<cloud-suffix>/<scope>` — for Azure Government, `abfs://<filesystem>@<account>.dfs.core.usgovcloudapi.net/cvff`. Credentials are never embedded in URIs; authentication is environment-injected at deployment (managed identity / workload identity against `login.microsoftonline.us`). Azure Government deployment additionally requires an approved ADLS Gen2 account in the US Gov region, Keycloak role bindings matching the table above, and per-scope ACLs on the `cvff/` and `platform/` roots.

### Segregation runbook

1. Provision one ADLS Gen2 filesystem per environment in Azure Government; apply deny-all default ACLs, then grant the cvff writer service principal `rwx` on `/cvff/**` only and the platform writer on `/platform/**` only.
2. Create Kafka topics under the governed namespaces (`cvff.*`, `ports.*`, `ferries.*`) with ACLs that let each consumer group subscribe only to its scope's namespace.
3. Run consumers with the matching scope, e.g. `blueeconomy-ingest-kafka --lakehouse-scope cvff --topic cvff.ledger.commitments --table-uri <cvff bronze URI> ...`. A scope/topic/table/classification mismatch aborts before any write.
4. Promote bronze→silver with `medallion.build_silver_record` + `medallion.append_silver`; replays are idempotent on the dedup key. Rebuild gold with `medallion.curate_gold`.
5. Retention: evaluate `medallion.retention_report` daily. Move `cold` records to the archive tier per the retention policy; `expired` records require legal-hold review before deletion. Bronze/silver tables are append-only; deletions are exceptional, evidence-recorded operations.
6. Grant auditor/NIMASA/CBN read access by assigning the Keycloak roles above; verify with `access_policy.authorize_read` before issuing credentials, and confirm ADLS ACLs independently deny cross-scope reads.
