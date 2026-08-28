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

export PYTHONPATH="$root/src"
export WORK_DIR="$work"
# Sign the sample envelope under the fleet provenance scheme (JWS EdDSA over
# the RFC 8785 canonical envelope) with the deterministic fixture key, and
# point the consumer at the matching key directory (fail-closed startup).
python3 - <<'PY'
import json
import os
import sys

sys.path.insert(0, os.path.join(os.environ["PYTHONPATH"], "..", "tests"))
from signing_helpers import fixture_kid_for_producer, fixture_private_key, sign_envelope
from blueeconomy_data_platform.signature_verification import export_public_key_for_test

work = os.environ["WORK_DIR"]
envelope = {"envelopeVersion":"1.0","eventId":"0a1b2c3d-4e5f-4061-8273-849506a7b8c9","eventType":"safety.telemetry.validated","occurredAt":"2026-08-12T12:00:00Z","producer":"blueeconomy-waterway-safety","correlationId":"local-kafka-correlation-0001","fhir":{"resourceType":"Bundle","type":"message","entry":[{"resource":{"device_id":"device-local-conformance","gateway_id":"gateway-local-conformance","source_sequence":1,"payload_sha256":"277089d91c0bdf4f2e6862ba7e4a07605119431f5d13f726dd352b06f1b206a9","payload_byte_count":5}}]},"provenance":{"principalId":"svc-waterway-safety","principalRole":"telemetry-gateway","signature":"","ledgerCommitHash":"277089d91c0bdf4f2e6862ba7e4a07605119431f5d13f726dd352b06f1b206a9"},"classification":"INTERNAL"}
signed = sign_envelope(envelope)
with open(os.path.join(work, "event.ndjson"), "w", encoding="utf-8") as handle:
    handle.write(json.dumps(signed, separators=(",", ":")) + "\n")
kid = fixture_kid_for_producer(envelope["producer"])
with open(os.path.join(work, "key-directory.json"), "w", encoding="utf-8") as handle:
    json.dump({kid: export_public_key_for_test(fixture_private_key(kid).public_key())}, handle)
PY
export KEY_DIRECTORY_PATH="$work/key-directory.json"
"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server 127.0.0.1:59092 --topic "$topic" < "$work/event.ndjson"
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
