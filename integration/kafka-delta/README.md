# Apache Kafka to Delta Lake Integration

This integration starts the official `apache/kafka:4.3.1` image in single-node KRaft mode and runs the actual `confluent-kafka` consumer against it. It is a local conformance environment, not a Ministry or agency deployment.

## Execution

```bash
./integration/kafka-delta/run-local.sh
```

The runner creates one controlled topic, publishes one governed event envelope and invokes two independent consumer groups. The first group writes one row to Delta and commits offset `1`. The second group replays the same event, commits its own offset `1`, and proves Delta `event_id` idempotency by retaining one row and reporting the replay as already present.

| Evidence | Generated file |
|---|---|
| Combined assertions | `integration/kafka-delta/results/result.json` |
| Per-run ingestion evidence | `first-report.json`, `second-report.json` |
| Broker offset state | `first-consumer-group.txt`, `second-consumer-group.txt` |
| Exact broker artifact | `kafka-image-digest.txt` |

The production CLI rejects PLAINTEXT unless `--allow-insecure-localhost` is explicitly set and every bootstrap address is loopback. Target environments must use `SSL` or `SASL_SSL`, trusted CA material, broker ACLs, approved topics, retention/partition policy, secrets injection, monitoring and disaster-recovery evidence.
