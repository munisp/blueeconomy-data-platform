# Blue Economy Data Platform

This repository contains governed ingestion and processing components for the platform lakehouse. It uses open-source **Delta Lake** and **Parquet** libraries for immutable event storage and will integrate Apache Spark, Flink, DataFusion, Ray and Sedona only where an approved workload and measured operational need exists.

## Implemented component: governed event ingestion

`blueeconomy-ingest-events` accepts an approved **real-source** NDJSON event file, validates every record against the committed event-envelope schema, rejects missing provenance/classification and duplicate source event IDs, and appends the accepted records to a Delta Lake table configured as append-only. The command has no default input, endpoint, source system, table URI or synthetic fallback.

The component is deliberately not represented as a live maritime, IoT, Kafka, port, payment or partner integration until it has processed authorised non-production records from the approved integration registry and the resulting evidence is reviewed.

## Required execution inputs

| Input | Requirement |
|---|---|
| `--input` | Approved real-source NDJSON file. Empty or invalid input is rejected. |
| `--schema` | The committed [`event-envelope.schema.json`](schemas/event-envelope.schema.json). |
| `--table-uri` | An approved, writable Delta Lake location. There is no default local or cloud location. |
| `--report` | An approved non-secret run-report location. |

```bash
blueeconomy-ingest-events \
  --input /approved/input/events.ndjson \
  --schema schemas/event-envelope.schema.json \
  --table-uri /approved/lakehouse/bronze/events \
  --report /approved/evidence/lakehouse-ingestion-report.json
```

The command stores canonicalised payload JSON alongside the controlled envelope and configures a newly created Delta table with `delta.appendOnly=true`. If a table already exists without that setting, it refuses to append. This is an ingestion control; it does not replace data-retention, encryption, object-store IAM, catalog/lineage, source consent or legal data-governance controls.

## Data classifications and safety

The schema requires an explicit classification of `public`, `internal`, `confidential`, `restricted` or `highly_restricted`. It requires a source system and source-record reference, but it does not prescribe permission to ingest any source. An approved data owner must confirm the source agreement, purpose, retention, minimisation and access policy before execution.
