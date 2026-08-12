# Blue Economy Data Platform

This repository contains governed ingestion components for the platform lakehouse. It uses open-source **Delta Lake** and **Parquet** libraries for controlled event storage. Apache Spark, Flink, DataFusion, Ray and Sedona remain separately governed execution engines and are introduced only for an approved workload and measured operational need.

## Implemented component: governed event ingestion

`blueeconomy-ingest-events` accepts an explicitly supplied **real-source** NDJSON file, validates every record against the committed event-envelope schema, requires provenance and classification, canonicalises the retained payload JSON, and writes to a Delta table configured with `delta.appendOnly=true`. Existing tables use an insert-only Delta merge keyed by `event_id`; a retried event is counted as already present and is not duplicated.

The command has no default input, endpoint, source system, table URI, credential or synthetic fallback. It is not represented as a live maritime, IoT, Kafka, port, payment or partner integration until authorised non-production records traverse an approved Ministry environment and the evidence is accepted.

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

The v2 run report contains record counts, already-present counts, source systems, classifications, table version, the input-file SHA-256 and a SHA-256 reference for the table location. It does not disclose the raw input path, table URI or payload.

## Integrity and operational boundaries

The implementation enforces bounded files, bounded lines, bounded canonical payloads, finite JSON values, canonical text identifiers, `occurred_at <= recorded_at`, unique IDs within a batch and Delta insert-only idempotency. New-table creation and existing-table merge use the actual pinned Delta Lake library.

This remains an ingestion component rather than a complete data platform. External-agency readiness additionally requires Ministry object storage and IAM, encryption keys, catalog/lineage, retention/legal hold, source agreements, schema compatibility, Kafka/Flink delivery, dead-letter/replay policy, single-writer or conflict-retry operating rules, backup/recovery, quality ownership and an approved integration registry.

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
