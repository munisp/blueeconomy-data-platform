"""Consume governed event envelopes from Kafka and commit offsets after Delta persistence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from jsonschema import Draft202012Validator

from blueeconomy_data_platform.ingest import (
    MAX_LINE_BYTES,
    append_events,
    load_schema,
    map_canonical_envelope,
    normalize_event,
    reject_non_finite_constant,
    require_canonical_text,
)
from blueeconomy_data_platform.segregation import (
    LakehouseScope,
    enforce_event_scope,
    enforce_topic_scope,
    require_scope_table_uri,
)

TOPIC_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,248}$")
LOCAL_BOOTSTRAP = re.compile(r"^(localhost|127\.0\.0\.1|\[::1\]):[0-9]{1,5}$")
MAX_MESSAGES_PER_RUN = 100_000


@dataclass(frozen=True)
class KafkaIngestionReport:
    schema_version: str
    started_at: str
    completed_at: str
    bootstrap_reference_sha256: str
    consumer_group_sha256: str
    topic: str
    lakehouse_scope: str
    messages_received: int
    records_written: int
    records_already_present: int
    table_reference_sha256: str
    table_version: int
    committed_offsets: dict[str, int]
    source_systems: list[str]
    data_classifications: list[str]


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume approved Kafka event envelopes into an append-only Delta table."
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
    parser.add_argument(
        "--lakehouse-scope",
        required=True,
        choices=tuple(scope.value for scope in LakehouseScope),
        help="Segregated lakehouse scope this consumer is authorized to write.",
    )
    parser.add_argument("--table-uri", required=True)
    parser.add_argument("--schema", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    return parser.parse_args()


def validate_transport(arguments: argparse.Namespace) -> dict[str, Any]:
    bootstrap = require_canonical_text(arguments.bootstrap_servers, "bootstrap_servers", 2048)
    topic = require_canonical_text(arguments.topic, "topic", 249)
    group_id = require_canonical_text(arguments.group_id, "group_id", 255)
    if not TOPIC_PATTERN.fullmatch(topic):
        raise ValueError("topic is not a valid Kafka topic name")
    if not 1 <= arguments.max_messages <= MAX_MESSAGES_PER_RUN:
        raise ValueError(f"max_messages must be between 1 and {MAX_MESSAGES_PER_RUN}")
    if not 0.1 <= arguments.idle_timeout_seconds <= 300:
        raise ValueError("idle_timeout_seconds must be between 0.1 and 300 seconds")

    configuration: dict[str, Any] = {
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "security.protocol": arguments.security_protocol,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
        "allow.auto.create.topics": False,
        "client.id": "blueeconomy-data-platform",
    }
    if arguments.security_protocol == "PLAINTEXT":
        bootstrap_hosts = [item.strip() for item in bootstrap.split(",")]
        if not arguments.allow_insecure_localhost or not all(
            LOCAL_BOOTSTRAP.fullmatch(item) for item in bootstrap_hosts
        ):
            raise ValueError("PLAINTEXT is restricted to explicit localhost integration runs")
    else:
        if arguments.allow_insecure_localhost:
            raise ValueError("allow_insecure_localhost cannot be used with encrypted transport")
        if arguments.ssl_ca_location is None:
            raise ValueError("ssl_ca_location is required for SSL and SASL_SSL")
        if not arguments.ssl_ca_location.is_file() or arguments.ssl_ca_location.is_symlink():
            raise ValueError("ssl_ca_location must be a regular non-symlink file")
        configuration["ssl.ca.location"] = str(arguments.ssl_ca_location)

    if arguments.security_protocol == "SASL_SSL":
        if arguments.sasl_mechanism is None:
            raise ValueError("sasl_mechanism is required for SASL_SSL")
        username = os.environ.get("BLUEECONOMY_KAFKA_SASL_USERNAME", "")
        password = os.environ.get("BLUEECONOMY_KAFKA_SASL_PASSWORD", "")
        if not username or not password:
            raise ValueError(
                "Kafka SASL credentials must be supplied through environment variables"
            )
        configuration.update(
            {
                "sasl.mechanism": arguments.sasl_mechanism,
                "sasl.username": username,
                "sasl.password": password,
            }
        )
    elif arguments.sasl_mechanism is not None:
        raise ValueError("sasl_mechanism is only valid with SASL_SSL")

    return configuration


def decode_event(value: bytes | None, validator: Draft202012Validator) -> dict[str, Any]:
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
    errors = sorted(validator.iter_errors(document), key=lambda item: list(item.path))
    if errors:
        messages = "; ".join(error.message for error in errors)
        raise ValueError(f"Kafka event fails event-envelope validation: {messages}")
    return normalize_event(map_canonical_envelope(document))


def collect_messages(
    consumer: Consumer,
    validator: Draft202012Validator,
    maximum: int,
    idle_timeout_seconds: float,
) -> tuple[list[dict[str, Any]], list[Message]]:
    events: list[dict[str, Any]] = []
    messages: list[Message] = []
    event_ids: set[str] = set()
    deadline = time.monotonic() + idle_timeout_seconds
    while len(messages) < maximum:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        message = consumer.poll(min(1.0, remaining))
        if message is None:
            continue
        message_error = message.error()
        if message_error is not None:
            if message_error.code() == KafkaError._PARTITION_EOF:
                continue
            raise KafkaException(message_error)
        event = decode_event(message.value(), validator)
        event_id = event["event_id"]
        if event_id in event_ids:
            raise ValueError(f"Kafka batch repeats event_id {event_id!r}")
        event_ids.add(event_id)
        events.append(event)
        messages.append(message)
        deadline = time.monotonic() + idle_timeout_seconds
    if not messages:
        raise ValueError("no Kafka messages were received before the idle timeout")
    return events, messages


def enforce_record_classification(events: list[dict[str, Any]], scope: LakehouseScope) -> None:
    """Fail closed when a classified-scope record lacks its per-record clearance label.

    Every event written to the ISR scope must carry a validated
    ``record_classification`` label (enforced by the event envelope and
    :func:`blueeconomy_data_platform.ingest.normalize_event`); the label is
    persisted as a column so readers apply row-level clearance filtering.
    """
    if scope is not LakehouseScope.ISR:
        return
    for event in events:
        if not isinstance(event.get("record_classification"), str):
            raise ValueError(
                f"isr scope record {event.get('event_id')!r} is missing its "
                "record_classification label; unlabelled classified records are rejected"
            )


def commit_messages(consumer: Consumer, messages: list[Message]) -> dict[str, int]:
    offsets: dict[str, int] = {}
    latest_by_partition: dict[tuple[str, int], Message] = {}
    for message in messages:
        topic = message.topic()
        partition = message.partition()
        if topic is None or partition is None:
            raise RuntimeError("Kafka message did not contain a topic and partition")
        latest_by_partition[(topic, partition)] = message
    for (topic, partition), message in sorted(latest_by_partition.items()):
        committed = consumer.commit(message=message, asynchronous=False)
        if not committed:
            raise RuntimeError(f"Kafka did not return a committed offset for {topic}:{partition}")
        message_offset = message.offset()
        if message_offset is None:
            raise RuntimeError(f"Kafka message did not contain an offset for {topic}:{partition}")
        expected = message_offset + 1
        matching = [
            offset
            for offset in committed
            if offset.topic == topic and offset.partition == partition
        ]
        if len(matching) != 1 or matching[0].error is not None or matching[0].offset != expected:
            raise RuntimeError(f"Kafka offset commit was not confirmed for {topic}:{partition}")
        offsets[f"{topic}:{partition}"] = expected
    return offsets


def reference_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def validate_report_path(schema_path: Path, report_path: Path) -> None:
    if schema_path.resolve(strict=False) == report_path.resolve(strict=False):
        raise ValueError("report path must not overwrite the schema file")


def write_kafka_report(path: Path, report: KafkaIngestionReport) -> None:
    path.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(asdict(report), indent=2) + "\n", encoding="utf-8")
    temporary.chmod(0o640)
    temporary.replace(path)


def main() -> None:
    arguments = parse_arguments()
    started_at = datetime.now(UTC)
    consumer: Consumer | None = None
    try:
        validate_report_path(arguments.schema, arguments.report)
        scope = LakehouseScope(arguments.lakehouse_scope)
        enforce_topic_scope(arguments.topic, scope)
        require_scope_table_uri(scope, arguments.table_uri)
        configuration = validate_transport(arguments)
        validator = load_schema(arguments.schema)
        consumer = Consumer(configuration)
        consumer.subscribe([arguments.topic])
        events, messages = collect_messages(
            consumer,
            validator,
            arguments.max_messages,
            arguments.idle_timeout_seconds,
        )
        enforce_event_scope(events, scope)
        enforce_record_classification(events, scope)
        table_version, records_written, records_already_present = append_events(
            arguments.table_uri, events
        )
        committed_offsets = commit_messages(consumer, messages)
        report = KafkaIngestionReport(
            schema_version="blueeconomy.lakehouse.kafka-ingestion-report.v1",
            started_at=started_at.isoformat(),
            completed_at=datetime.now(UTC).isoformat(),
            bootstrap_reference_sha256=reference_sha256(arguments.bootstrap_servers),
            consumer_group_sha256=reference_sha256(arguments.group_id),
            topic=arguments.topic,
            lakehouse_scope=scope.value,
            messages_received=len(messages),
            records_written=records_written,
            records_already_present=records_already_present,
            table_reference_sha256=reference_sha256(arguments.table_uri),
            table_version=table_version,
            committed_offsets=committed_offsets,
            source_systems=sorted({event["source_system"] for event in events}),
            data_classifications=sorted({event["data_classification"] for event in events}),
        )
        write_kafka_report(arguments.report, report)
        print(json.dumps(asdict(report), sort_keys=True))
    except (KafkaException, OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"blueeconomy-ingest-kafka: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        if consumer is not None:
            consumer.close()


if __name__ == "__main__":
    main()
