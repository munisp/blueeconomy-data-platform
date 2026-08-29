"""Consume signed ``vessels.events`` envelopes into bronze.vessel_observations.

Mirrors ``blueeconomy-ingest-kafka`` for the vessel path: bounded consumer,
no automatic commits, schema validation, fail-closed JWS-EdDSA/JCS
provenance-signature verification against the startup key directory
(``KEY_DIRECTORY_PATH``), mandatory dead-letter quarantine (topic plus
append-only quarantine table) for poison messages, and synchronous offset
commits only after the bronze Delta write and every quarantine write are
durable. The consumer is bound to the platform lakehouse scope and to the
``vessels.`` topic namespace.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from confluent_kafka import Consumer, KafkaException
from jsonschema import Draft202012Validator

from blueeconomy_data_platform.dlq import DeadLetterQueue, DeadLetterSink
from blueeconomy_data_platform.ingest import (
    MAX_LINE_BYTES,
    load_schema,
    reject_non_finite_constant,
    require_canonical_text,
)
from blueeconomy_data_platform.kafka_ingest import (
    TOPIC_PATTERN,
    build_dlq_producer,
    collect_messages,
    commit_messages,
    validate_report_path,
    validate_transport,
    write_kafka_report,
    KafkaIngestionReport,
)
from blueeconomy_data_platform.segregation import (
    LakehouseScope,
    enforce_topic_scope,
    require_scope_table_uri,
)
from blueeconomy_data_platform.signature_verification import (
    EnvelopeSignatureVerifier,
    load_key_directory_from_env,
)
from blueeconomy_data_platform.vessel_lakehouse import (
    VESSEL_OBSERVATION_EVENT_TYPE,
    append_vessel_observations,
    decode_vessel_observation,
)

VESSEL_BRONZE_PATH_SUFFIX = "/bronze/vessel_observations"
DLQ_QUARANTINE_PATH_SUFFIX = "/bronze/vessel_observations_dlq"


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Consume signed vessels.events envelopes into the platform bronze "
            "vessel_observations Delta table."
        )
    )
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--group-id", required=True)
    parser.add_argument(
        "--security-protocol",
        required=True,
        choices=("SSL", "SASL_SSL", "PLAINTEXT"),
    )
    parser.add_argument("--ssl-ca-location", type=Path)
    parser.add_argument("--sasl-mechanism", choices=("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"))
    parser.add_argument("--allow-insecure-localhost", action="store_true")
    parser.add_argument("--max-messages", required=True, type=int)
    parser.add_argument("--idle-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--table-uri", required=True)
    parser.add_argument("--dlq-topic", required=True)
    parser.add_argument("--dlq-table-uri", required=True)
    parser.add_argument("--schema", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    return parser.parse_args()


def validate_vessel_paths(table_uri: str, dlq_table_uri: str) -> None:
    scope = LakehouseScope.PLATFORM
    require_scope_table_uri(scope, table_uri)
    require_scope_table_uri(scope, dlq_table_uri)
    if not table_uri.rstrip("/").endswith(VESSEL_BRONZE_PATH_SUFFIX):
        raise ValueError(
            f"table-uri must terminate in {VESSEL_BRONZE_PATH_SUFFIX} "
            "(the governed bronze vessel_observations path)"
        )
    if not dlq_table_uri.rstrip("/").endswith(DLQ_QUARANTINE_PATH_SUFFIX):
        raise ValueError(
            f"dlq-table-uri must terminate in {DLQ_QUARANTINE_PATH_SUFFIX} "
            "(the governed vessel DLQ quarantine path)"
        )


def reference_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def main() -> None:
    arguments = parse_arguments()
    started_at = datetime.now(UTC)
    consumer: Consumer | None = None
    try:
        validate_report_path(arguments.schema, arguments.report)
        scope = LakehouseScope.PLATFORM
        enforce_topic_scope(arguments.topic, scope)
        if not arguments.topic.startswith("vessels."):
            raise ValueError("the vessel consumer only subscribes to vessels.* topics")
        validate_vessel_paths(arguments.table_uri, arguments.dlq_table_uri)
        dlq_topic = require_canonical_text(arguments.dlq_topic, "dlq_topic", 249)
        if not TOPIC_PATTERN.fullmatch(dlq_topic):
            raise ValueError("dlq_topic is not a valid Kafka topic name")
        if dlq_topic == arguments.topic:
            raise ValueError("dlq_topic must differ from the consumed topic")
        if not dlq_topic.startswith("vessels."):
            raise ValueError("the vessel DLQ topic must live in the vessels.* namespace")
        configuration = validate_transport(arguments)
        validator = load_schema(arguments.schema)
        verifier = EnvelopeSignatureVerifier(load_key_directory_from_env())
        dlq: DeadLetterSink = DeadLetterQueue(
            producer=build_dlq_producer(configuration),
            dlq_topic=dlq_topic,
            quarantine_table_uri=arguments.dlq_table_uri,
            consumer_group=arguments.group_id,
        )
        consumer = Consumer(configuration)
        consumer.subscribe([arguments.topic])
        rows, messages, dlq_reason_counts = collect_messages(
            consumer,
            lambda value: decode_message(value, validator, verifier),
            arguments.max_messages,
            arguments.idle_timeout_seconds,
            dlq,
        )
        if rows:
            table_version, records_written, records_already_present = append_vessel_observations(
                arguments.table_uri, rows
            )
        else:
            table_version, records_written, records_already_present = (-1, 0, 0)
        committed_offsets = commit_messages(consumer, messages)
        report = KafkaIngestionReport(
            schema_version="blueeconomy.lakehouse.vessel-ingestion-report.v1",
            started_at=started_at.isoformat(),
            completed_at=datetime.now(UTC).isoformat(),
            bootstrap_reference_sha256=reference_sha256(arguments.bootstrap_servers),
            consumer_group_sha256=reference_sha256(arguments.group_id),
            topic=arguments.topic,
            lakehouse_scope=scope.value,
            messages_received=len(messages),
            records_written=records_written,
            records_already_present=records_already_present,
            dlq_topic=dlq_topic,
            messages_quarantined=sum(dlq_reason_counts.values()),
            dlq_reason_counts=dict(sorted(dlq_reason_counts.items())),
            table_reference_sha256=reference_sha256(arguments.table_uri),
            table_version=table_version,
            committed_offsets=committed_offsets,
            source_systems=sorted({str(row["producer"]) for row in rows}),
            data_classifications=[VESSEL_OBSERVATION_EVENT_TYPE],
        )
        write_kafka_report(arguments.report, report)
        print(json.dumps(asdict(report), sort_keys=True))
    except (KafkaException, OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"blueeconomy-ingest-vessels: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        if consumer is not None:
            consumer.close()


def decode_message(
    value: bytes | None,
    validator: Draft202012Validator,
    verifier: EnvelopeSignatureVerifier,
) -> dict[str, object]:
    """Decode one raw Kafka message into a bronze vessel observation row."""
    if value is None or len(value) == 0 or len(value) > MAX_LINE_BYTES:
        raise ValueError(f"Kafka message value must contain 1 to {MAX_LINE_BYTES} bytes")
    try:
        document = json.loads(value.decode("utf-8"), parse_constant=reject_non_finite_constant)
    except UnicodeDecodeError as error:
        raise ValueError("Kafka message value is not valid UTF-8") from error
    except json.JSONDecodeError as error:
        raise ValueError(f"Kafka message value is not valid JSON: {error.msg}") from error
    if not isinstance(document, dict):
        raise ValueError("Kafka message value must be a JSON object")
    return decode_vessel_observation(document, validator, verifier)


if __name__ == "__main__":
    main()
