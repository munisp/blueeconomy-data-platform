#!/usr/bin/env bash
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
integration="$root/integration/kafka-delta"
results="$integration/results"
work="$(mktemp -d)"
compose=(sudo docker compose -f "$integration/compose.yaml")
cleanup() {
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

for command in docker jq python3 sudo; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "required command missing: $command" >&2
    exit 1
  }
done

rm -rf "$results"
mkdir -p "$results"
"${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
"${compose[@]}" up -d
for _ in $(seq 1 120); do
  if "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server 127.0.0.1:59092 >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server 127.0.0.1:59092 >/dev/null

readonly topic="ports.events.local"
"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 127.0.0.1:59092 \
  --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1

cat > "$work/event.ndjson" <<'JSON'
{"event_id":"local-kafka-event-0001","event_type":"safety.telemetry.validated","producer":"blueeconomy-waterway-safety","occurred_at":"2026-08-12T12:00:00Z","recorded_at":"2026-08-12T12:00:01Z","data_classification":"internal","source_system":"local-kafka-conformance","source_record_reference":"local-kafka-record-0001","correlation_id":"local-kafka-correlation-0001","payload":{"device_id":"device-local-conformance","gateway_id":"gateway-local-conformance","source_sequence":1,"payload_sha256":"277089d91c0bdf4f2e6862ba7e4a07605119431f5d13f726dd352b06f1b206a9","payload_byte_count":5}}
JSON
"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server 127.0.0.1:59092 --topic "$topic" < "$work/event.ndjson"

export PYTHONPATH="$root/src"
table="$work/delta-table"
for ordinal in first second; do
  group="blueeconomy-data-platform-local-$ordinal"
  python3 -m blueeconomy_data_platform.kafka_ingest \
    --bootstrap-servers 127.0.0.1:59092 \
    --topic "$topic" \
    --group-id "$group" \
    --security-protocol PLAINTEXT \
    --allow-insecure-localhost \
    --max-messages 1 \
    --idle-timeout-seconds 20 \
    --lakehouse-scope platform \
    --table-uri "$table" \
    --schema "$root/schemas/event-envelope.schema.json" \
    --report "$work/$ordinal-report.json" \
    > "$results/$ordinal.stdout.json"
  cp "$work/$ordinal-report.json" "$results/$ordinal-report.json"
  "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server 127.0.0.1:59092 --describe --group "$group" \
    > "$results/$ordinal-consumer-group.txt"
done

python3 "$integration/inspect_result.py" \
  --table "$table" \
  --first-report "$work/first-report.json" \
  --second-report "$work/second-report.json" \
  --output "$results/result.json"
printf '%s\n' "$(git -C "$root" rev-parse HEAD)" > "$results/data-platform.commit"
sudo docker image inspect apache/kafka:4.3.1 --format '{{index .RepoDigests 0}}' > "$results/kafka-image-digest.txt"
cat "$results/result.json"
