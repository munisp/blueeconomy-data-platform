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

The committed schema is the canonical platform event envelope (`blueeconomy.contracts.v1.EventEnvelope`, envelopeVersion `1.0`): camelCase `eventId`/`eventType`/`occurredAt`/`producer`/`correlationId`, the domain resource carried as the first entry of a FHIR R4 message `Bundle` under `fhir`, a `provenance` block (`principalId`, `principalRole`, `signature`, `ledgerCommitHash`) and the canonical `classification` vocabulary (`FIDUCIARY_SEGREGATED`, `CONFIDENTIAL`, `RESTRICTED`, `INTERNAL`, `PUBLIC`), with an optional per-record `recordClassification` clearance label (mandatory for ISR scope ingestion). At ingestion the canonical classification is mapped onto the internal lowercase per-scope labels (`fiduciary_segregated`, `seafarer_confidential`, `fisheries_operational`, `isr_classified` or the platform labels) from the classification and the event type's governed namespace, so a classification can never be laundered across a segregation boundary; anything outside the canonical contract fails closed. These fields do not grant permission to ingest a source. The accountable data owner must approve lawful purpose, data-sharing terms, minimisation, retention, correction, access, export and incident handling before target deployment.

## Fiduciary segregation (Workstream C / CVFF)

Workstream C (CVFF fintech) runs on a **physically segregated Delta Lake schema**, deployable on Azure Government ADLS Gen2 or any S3-compatible object storage (see [Cloud-agnostic storage configuration](#cloud-agnostic-storage-configuration)). Workstream C events carry the `fiduciary_segregated` classification and arrive on `cvff.*` Kafka topics; Workstream A (`ports.*`) and Workstream B (`ferries.*`) events remain in the platform scope. Phase 2 adds the Workstream D (seafarer credentials), E (fisheries catch/coldchain/export) and F (classified ISR) scopes under the same segregation model. There is no shared schema and no shared storage root between the scopes:

| Scope | Kafka namespaces | Event classification | Delta tables |
|---|---|---|---|
| platform | `ports.*`, `ferries.*` | `public`, `internal`, `confidential`, `restricted`, `highly_restricted` | `<root>/platform/platform_bronze/events`, `platform_silver/events`, `platform_gold/events` |
| cvff | `cvff.*` | `fiduciary_segregated` | `<root>/cvff/cvff_bronze/events`, `cvff_silver/events`, `cvff_gold/events` |
| seafarer | `seafarer.*` | `seafarer_confidential` (CONFIDENTIAL credentials) | `<root>/seafarer/seafarer_bronze/events`, `seafarer_silver/events`, `seafarer_gold/events` |
| fisheries | `fisheries.*`, `coldchain.*`, `export.*` | `fisheries_operational` | `<root>/fisheries/fisheries_bronze/events`, `fisheries_silver/events`, `fisheries_gold/events` |
| isr | `maritime.isr.*`, `maritime.behaviour.*`, `maritime.outcome.*` | `isr_classified` (CLASSIFIED — highest bar) | `<root>/isr/isr_bronze/events`, `isr_silver/events`, `isr_gold/events` |

The phase-2 scopes (Workstreams D, E, F) follow the same boundary model as cvff: each scope root must terminate in its own `<scope>*` path component and no other, every scope has its own medallion tables, and `SegregatedDeltaWriter`/`guard_write` enforce classification, topic and table-URI boundaries identically for every scope.

### Boundary guarantees

Segregation is enforced at the write path, not by routing convention:

- `blueeconomy_data_platform.segregation.SegregatedDeltaWriter` is initialized for exactly one scope and one scope root. A cvff writer cannot be pointed at a non-`cvff*` root, and a platform writer cannot be pointed at a `cvff*` root — initialization fails closed.
- `guard_write` raises `BoundaryViolationError` before any record is written when an event classification, Kafka topic or table URI belongs to the other scope. A platform writer cannot write a `fiduciary_segregated` record and a cvff writer cannot write any platform-classified record.
- Classification and topic mappings fail closed: an unrecognized `data_classification` or a topic outside the `cvff.*`/`ports.*`/`ferries.*` namespaces is rejected, never defaulted.
- `blueeconomy-ingest-kafka` requires `--lakehouse-scope platform|cvff|seafarer|fisheries|isr`; the topic namespace, the event classifications in the batch and the target table URI must all match the declared scope before consumption is persisted.
- Classification-labelled ingestion: ISR-scope records must additionally carry a per-record `record_classification` clearance label (`UNCLASSIFIED`, `RESTRICTED`, `CONFIDENTIAL` or `SECRET`). An ISR record without the label is rejected before any write; the validated label is persisted as a `record_classification` column so readers apply row-level clearance filtering (`access_policy.clearance_permits` / `filter_records_by_clearance`).

### CVFF medallion layers

`blueeconomy_data_platform.medallion` implements the segregated cvff medallion pipeline on top of `SegregatedDeltaWriter`:

- **Bronze** — raw validated envelopes, append-only. The retention policy (default 30 days hot, 7 years cold, bounds-validated) is committed in the Delta table description at creation (the delta-rs kernel rejects custom table properties, so the horizons live in retained table metadata); `RetentionPolicy.tier_for` and `retention_report` classify records as `hot`, `cold` or `expired` for the operations runbook.
- **Silver** — deduplicated on the composite key `sha256(kafka_topic/kafka_partition/kafka_offset/ledgerCommitHash)`. The `ledgerCommitHash` (64 lowercase hex) is required in the cvff payload. Replayed Kafka records with an identical dedup key are counted as already present and never duplicated; a dedup key reused with conflicting content fails closed.
- **Gold** — a curated one-row-per-`ledgerCommitHash` snapshot (record count, occurrence window, source systems, event IDs) atomically overwritten from silver.

### Segregated read access

`blueeconomy_data_platform.access_policy` maps Keycloak-style role claims to readable schemas. Each segregated scope's schemas are readable **only** by that scope's roles:

| Role claim | Readable schemas | Required clearance |
|---|---|---|
| `independent-auditor` | `cvff_bronze`, `cvff_silver`, `cvff_gold` | — (UNCLASSIFIED floor) |
| `nimasa-approver` | `cvff_bronze`, `cvff_silver`, `cvff_gold` | — (UNCLASSIFIED floor) |
| `cbn-observer` | `cvff_bronze`, `cvff_silver`, `cvff_gold` | — (UNCLASSIFIED floor) |
| `fmmbe-oversight` | `platform_bronze`, `platform_silver`, `platform_gold` | — (UNCLASSIFIED floor) |
| `seafarer-registry` | `seafarer_bronze`, `seafarer_silver`, `seafarer_gold` | CONFIDENTIAL |
| `fisheries-operations` | `fisheries_bronze`, `fisheries_silver`, `fisheries_gold` | RESTRICTED |
| `isr-analyst` | `isr_bronze`, `isr_silver`, `isr_gold` | SECRET (`isr_gold`: CONFIDENTIAL) |
| `insurer-aggregator` | `isr_gold` only | CONFIDENTIAL |

#### Clearance model (Workstream F)

Clearance levels are strictly ordered: `UNCLASSIFIED < RESTRICTED < CONFIDENTIAL < SECRET`. For any schema above the UNCLASSIFIED floor, `authorize_read(roles, schema, clearance=...)` requires a clearance claim at or above the schema's classification floor; a missing or unknown clearance is denied (fail closed). The `insurer-aggregator` role is deliberately granted **only** the declassified ISR outcome aggregates (`isr_gold`, derived from `maritime.outcome.*` events) and can never read the raw or behavioural ISR tracks (`isr_bronze`/`isr_silver`), no matter what clearance it presents. Row-level filtering uses the persisted `record_classification` column: `clearance_permits(clearance, label)` is the predicate and `filter_records_by_clearance` withholds every row whose label is missing or unknown.

Unknown roles, unknown schemas and empty claim sets are denied. No governance role can write; writes belong to governed service principals. This module is the platform-side policy of record: deployment must bind the same grants to Keycloak client scopes and storage-layer ACLs so the storage layer independently denies what the policy denies.

### Export consignment traceability (Workstream E)

`blueeconomy_data_platform.export_consignment` builds the fisheries gold-layer consignment view. `build_consignment_records` groups fisheries bronze events by `payload.consignmentId` — exactly one `fisheries.catch.*` event (species code, catch weight), the `fisheries.custody.*` transfer trail, `coldchain.*` temperature samples (range-validated, reduced to a tamper-evident SHA-256 digest of the ordered `(event_id, occurred_at, temperature_celsius)` samples) and the optional `export.*` declaration reference. `assemble_export_consignment_gold` rebuilds `<root>/fisheries/fisheries_gold/export_consignments` atomically from bronze under the declared `EXPORT_CONSIGNMENT_SCHEMA` (pyarrow). Assembly fails closed on missing/duplicate catch events, malformed consignment IDs, implausible temperatures or ungoverned event families, and only a fisheries-scope writer may assemble. Every assembled consignment carries a `record_classification` clearance floor equal to the most restrictive label among its source events — an unlabelled source defaults to SECRET, the highest restriction — and `read_export_consignments(writer, clearance)` is the serving read path: every row passes through `access_policy.filter_records_by_clearance`, so rows above the claimed clearance (or with missing/unknown labels) are withheld and a missing or unknown clearance claim is denied.

### Gold-layer assembly and scheduling

`blueeconomy-gold-assembly` is the scheduled orchestration entry point for the gold layer. One invocation runs one governed assembly pass for a single segregated scope, configured entirely through environment variables (fail closed on anything missing or invalid):

| Variable | Requirement |
|---|---|
| `BLUEECONOMY_GOLD_SCOPE` | `cvff` runs the silver→gold ledger-commitment rollup; `fisheries` runs the export-consignment gold assembly plus the clearance-filtered export. Any other scope fails closed. |
| `BLUEECONOMY_GOLD_SCOPE_ROOT_URI` | The segregated scope root URI; `SegregatedDeltaWriter` enforces the scope boundary on it. |
| `BLUEECONOMY_GOLD_REPORT` | Non-secret JSON run-report path (counts, table version, clearance; no payload data). |
| `BLUEECONOMY_GOLD_EXPORT_PATH` | Required for `fisheries`: JSON export of the consignment rows visible at the configured clearance. |
| `BLUEECONOMY_GOLD_CLEARANCE` | Optional clearance claim for the export read; defaults to `UNCLASSIFIED`, the most restrictive clearance, so an unconfigured run exports nothing above the UNCLASSIFIED floor. |

```bash
BLUEECONOMY_GOLD_SCOPE=fisheries \
BLUEECONOMY_GOLD_SCOPE_ROOT_URI=s3://approved-lakehouse/fisheries \
BLUEECONOMY_GOLD_REPORT=/approved/evidence/gold-assembly-report.json \
BLUEECONOMY_GOLD_EXPORT_PATH=/approved/evidence/consignment-export.json \
BLUEECONOMY_GOLD_CLEARANCE=RESTRICTED \
blueeconomy-gold-assembly
```

Scheduling: run one invocation per scope on a bounded cadence, after the ingestion window closes. A cron entry (for example `*/15 * * * *` per scope with a per-scope lock) is sufficient for batch cadences; on the platform's Temporal deployment, model each scope as a scheduled workflow with a single activity per invocation — the command is idempotent (gold tables are atomically rebuilt derived state), so a retried or overlapping run never duplicates or corrupts gold content. The run report is the operations evidence for each scheduled pass.

### Cloud-agnostic storage configuration

The platform is not Azure-locked. `blueeconomy_data_platform.storage` is the only module containing cloud specifics; segregation, medallion and access layers consume resolved URIs and never branch on a provider. Lakehouse roots are resolved from the environment only; no endpoint, account, bucket or credential is hardcoded. `resolve_lakehouse_root` fails closed unless configuration is complete:

| Variable | Requirement |
|---|---|
| `BLUEECONOMY_STORAGE_BACKEND` | `adls`, `s3`, or `local-gated` (the latter only behind the explicit `BLUEECONOMY_ALLOW_LOCAL_STORAGE=true` development gate). The historical values `adls-gen2` and `local` remain accepted as aliases. |
| `BLUEECONOMY_AZURE_CLOUD` | ADLS only: `AzureUSGovernment` (endpoint suffix `dfs.core.usgovcloudapi.net`) for the CVFF deployment, or `AzureCloud`. No other cloud is accepted. |
| `BLUEECONOMY_STORAGE_ACCOUNT` | ADLS only: ADLS Gen2 account name (3–24 lowercase alphanumeric). |
| `BLUEECONOMY_STORAGE_FILESYSTEM` | ADLS only: ADLS Gen2 filesystem (container) name. |
| `BLUEECONOMY_S3_BUCKET` | S3 only: bucket name (3–63 lowercase letters/digits/dots/hyphens, no IP-address form, no consecutive dots). |
| `BLUEECONOMY_S3_REGION` | S3 only: region identifier such as `us-east-1` or `us-gov-west-1`. |
| `BLUEECONOMY_S3_ENDPOINT_URL` | S3 only, optional: custom endpoint for MinIO/Ceph/GCS-interop, e.g. `https://minio.storage.example:9000`. No credentials, path or query allowed; the scheme must match `BLUEECONOMY_S3_SECURE`. |
| `BLUEECONOMY_S3_SECURE` | S3 only: exactly `true` or `false`. `false` (plain HTTP) is permitted only against an explicit custom endpoint; AWS S3 transport is always TLS. |
| `BLUEECONOMY_LOCAL_LAKEHOUSE_ROOT` | Absolute path; required only for the gated local backend. |

Resolved URIs are `abfs://<filesystem>@<account>.<cloud-suffix>/<scope>` for ADLS and `s3://<bucket>/<scope>` for S3-compatible storage. Credentials are never embedded in URIs and never returned by the storage module; authentication is environment-injected at deployment (managed/workload identity against `login.microsoftonline.us` for ADLS; the standard `AWS_*` credential chain for S3-compatible backends). `resolve_storage_options` maps the validated configuration to non-secret deltalake/object_store options (`AWS_REGION`, `AWS_ENDPOINT_URL`, `AWS_ALLOW_HTTP`) so writers stay provider-neutral, and `validate_s3_uri` enforces bucket/key rules (fail closed on embedded credentials, empty or `..` segments, control characters).

#### Neutral reference deployment: MinIO on any Kubernetes

The cloud-neutral reference deployment runs [MinIO](https://min.io/) (or any S3-compatible service — Ceph RGW, GCS with S3 interoperability, AWS S3) on any Kubernetes cluster:

1. Deploy MinIO (operator or Helm chart) with TLS enabled; create one bucket per environment (e.g. `blueeconomy-lakehouse`) and one IAM policy per scope prefix (`cvff/*`, `platform/*`, `seafarer/*`, `fisheries/*`, `isr/*`) bound to per-scope service accounts.
2. Inject credentials through Kubernetes Secrets into the standard `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` environment variables of the ingestion workloads — never into URIs, ConfigMaps or images.
3. Configure the workloads with `BLUEECONOMY_STORAGE_BACKEND=s3`, `BLUEECONOMY_S3_BUCKET`, `BLUEECONOMY_S3_REGION` (MinIO conventionally `us-east-1`), `BLUEECONOMY_S3_ENDPOINT_URL=https://<minio-service>:9000` and `BLUEECONOMY_S3_SECURE=true`.
4. The same binaries run unchanged on AWS GovCloud S3 (drop the endpoint variable), Azure Government ADLS Gen2 (`BLUEECONOMY_STORAGE_BACKEND=adls` with the Azure coordinates above) or the gated local backend for conformance runs. Azure Government deployment additionally requires an approved ADLS Gen2 account in the US Gov region, Keycloak role bindings matching the table above, and per-scope ACLs on the scope roots.

### Segregation runbook

1. Provision one filesystem/bucket per environment on the selected backend; apply deny-all default ACLs, then grant each scope's writer service principal access to its own root only (`/cvff/**`, `/platform/**`, `/seafarer/**`, `/fisheries/**`, `/isr/**`).
2. Create Kafka topics under the governed namespaces (`cvff.*`, `ports.*`, `ferries.*`, `seafarer.*`, `fisheries.*`, `coldchain.*`, `export.*`, `maritime.isr.*`, `maritime.behaviour.*`, `maritime.outcome.*`) with ACLs that let each consumer group subscribe only to its scope's namespace.
3. Run consumers with the matching scope, e.g. `blueeconomy-ingest-kafka --lakehouse-scope cvff --topic cvff.ledger.commitments --table-uri <cvff bronze URI> ...`. A scope/topic/table/classification mismatch aborts before any write.
4. Promote bronze→silver with `medallion.build_silver_record` + `medallion.append_silver`; replays are idempotent on the dedup key. Rebuild gold with `medallion.curate_gold`.
5. Retention: evaluate `medallion.retention_report` daily. Move `cold` records to the archive tier per the retention policy; `expired` records require legal-hold review before deletion. Bronze/silver tables are append-only; deletions are exceptional, evidence-recorded operations.
6. Grant auditor/NIMASA/CBN read access by assigning the Keycloak roles above; verify with `access_policy.authorize_read` before issuing credentials, and confirm ADLS ACLs independently deny cross-scope reads.
