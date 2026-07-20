# Rust — Kafka: Consume from beginning vs latest

Show how `auto.offset.reset` = `earliest` vs `latest` changes where a brand-new consumer group starts.

## 1. Prerequisites

- Rust 1.75+ + Cargo; `rdkafka` 0.37 (librdkafka) on `tokio`, via the workspace.
- A running KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`).
- **`CONNECTORS_KAFKA_ENABLE=true`** on the broker (gotcha #1).

## 2. How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
cargo run -p from-beginning-latest
```

## 3. Expected Output

```text
[kubemq-kafka] consume/from-beginning-latest bootstrap=localhost:9092 (no-auth; connector must be enabled: CONNECTORS_KAFKA_ENABLE=true)
seeded 3 records
earliest group saw 3 records: ["seed-0", "seed-1", "seed-2"]
latest group saw 2 records: ["post-0", "post-1"]
from-beginning-latest OK: earliest saw seeded, latest saw only post-join
```

## 4. What's Happening

Three records are seeded. The `earliest` group has no committed offset, so it resets to the log start and
sees all three. The `latest` group subscribes and joins first (pinning its start at the current
high-water mark); the two records produced afterwards are the only ones it delivers.

## 5. Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Fetch(1), OffsetFetch(9) | acks=all, read_uncommitted | kafka-ex-consume-reset / 1 | two fresh groups | auto.offset.reset earliest\|latest | none | librdkafka CRC32 | latest pins at HWM on join |

## 6. Related Examples

`../../../{go,java,javascript,csharp}/consume/from-beginning-latest`,
`../../../{python,ruby}/consume/from_beginning_latest`. Guide: `../../../../docs/guides/consuming-and-groups.md`.

## 7. Gotcha callout

`auto.offset.reset` only applies when the group has **no committed offset**. Once a group commits, it
resumes from the commit regardless of this setting — see `consumer-groups/commit-and-lag`.

## 8. Auth

Auth is off by default. See `../../../../docs/guides/security-sasl-tls.md`.
