# Rust — Kafka: Partitions & configs (admin)

Grow a topic's partitions (increase-only, ≤256), read its config, and prove an over-cap request is
rejected with `INVALID_PARTITIONS`.

## 1. Prerequisites

- Rust 1.75+ + Cargo; `rdkafka` 0.37 (librdkafka) on `tokio`, via the workspace.
- A running KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`).
- **`CONNECTORS_KAFKA_ENABLE=true`** on the broker (gotcha #1).

## 2. How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
cargo run -p partitions-and-configs
```

## 3. Expected Output

```text
[kubemq-kafka] admin/partitions-and-configs bootstrap=localhost:9092 (no-auth; connector must be enabled: CONNECTORS_KAFKA_ENABLE=true)
topic 'kafka-ex-admin-parts' ready with 2 partitions
grew 'kafka-ex-admin-parts' to 4 partitions
config for 'kafka-ex-admin-parts': <k> entries readable via DescribeConfigs
over-cap request (300 > 256) correctly rejected: INVALID_PARTITIONS
partitions-and-configs OK: increase-only growth honored, over-cap rejected
```

## 4. What's Happening

CreatePartitions (key 37) grows the topic 2 → 4 (partition growth is increase-only, capped at 256).
DescribeConfigs (key 32) reads the topic config. A request for 300 partitions exceeds the cap and is
rejected with `INVALID_PARTITIONS`.

## 5. Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| CreatePartitions(37), DescribeConfigs(32) | n/a (admin) | kafka-ex-admin-parts / 2→4 | n/a | n/a | n/a | n/a | over-cap → INVALID_PARTITIONS |

## 6. Related Examples

`../../../{go,java,javascript,csharp}/admin/partitions-and-configs`,
`../../../{python,ruby}/admin/partitions_and_configs`. Guide: `../../../../docs/guides/admin-and-topics.md`.

## 7. Gotcha callout

**🟡 Partial surfaces (spec §2.4):**
- **IncrementalAlterConfigs** — the connector honors only a subset of topic configs (`retention.*`) and
  no-ops the rest; this example reads config with DescribeConfigs rather than mutate it.
- **DeleteRecords** — the `rdkafka` admin API does **not** expose DeleteRecords; the connector supports
  only low-end log truncation. For that path use the Java `kafka-clients` admin (`deleteRecords`) —
  see `../../../java/admin/partitions-and-configs`.

## 8. Auth

Auth is off by default. See `../../../../docs/guides/security-sasl-tls.md`.
