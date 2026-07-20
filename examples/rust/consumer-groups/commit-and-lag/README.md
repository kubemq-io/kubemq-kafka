# Rust — Kafka: Commit offsets & measure lag

Manually commit offsets, resume a new consumer from the committed position, and compute consumer lag as
`HWM − committed`.

## 1. Prerequisites

- Rust 1.75+ + Cargo; `rdkafka` 0.37 (librdkafka) on `tokio`, via the workspace.
- A running KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`).
- **`CONNECTORS_KAFKA_ENABLE=true`** on the broker (gotcha #1).

## 2. How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
cargo run -p commit-and-lag
```

## 3. Expected Output

```text
[kubemq-kafka] consumer-groups/commit-and-lag bootstrap=localhost:9092 (no-auth; connector must be enabled: CONNECTORS_KAFKA_ENABLE=true)
produced 8 records
[c1] read offset=<n> body='msg-0'
...
[c1] committed through offset <n>
HWM=8 committed=4 lag=4
[c2] resumed offset=4 body='msg-4'
...
commit-and-lag OK: resumed from committed offset 4, lag was 4
```

## 4. What's Happening

Consumer #1 reads the first four records and `commit_message(..., Sync)` persists the group offset via
OffsetCommit (key 8). Lag is computed from `fetch_watermarks` (HWM) minus the committed offset. Consumer
#2, joining the same group, does an OffsetFetch (key 9) and resumes at `msg-4` — never re-reading the
committed records.

## 5. Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| OffsetCommit(8), OffsetFetch(9), ListOffsets(2) | acks=all, read_uncommitted | kafka-ex-cg-commit / 1 | shared group, resumed | manual sync commit | none | librdkafka CRC32 | lag = HWM − committed |

## 6. Related Examples

`../../../{go,java,javascript,csharp}/consumer-groups/commit-and-lag`,
`../../../{python,ruby}/consumer-groups/commit_and_lag`. Guide: `../../../../docs/guides/consuming-and-groups.md`.

## 7. Gotcha callout

`commit_message` commits `message.offset() + 1` (the next offset to read), so resuming reads the record
*after* the committed one. The server also exports `kubemq_kafka_consumer_group_lag` as a Prometheus
metric — the same quantity computed here client-side.

## 8. Auth

Auth is off by default. See `../../../../docs/guides/security-sasl-tls.md`.
