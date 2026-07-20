# Rust — Kafka: List offsets & retention

Query earliest/latest offsets and by-timestamp offsets (ListOffsets), and round-trip topic retention
config.

## 1. Prerequisites

- Rust 1.75+ + Cargo; `rdkafka` 0.37 (librdkafka) on `tokio`, via the workspace.
- A running KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`).
- **`CONNECTORS_KAFKA_ENABLE=true`** on the broker (gotcha #1).

## 2. How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
cargo run -p list-and-retention
```

## 3. Expected Output

```text
[kubemq-kafka] offsets/list-and-retention bootstrap=localhost:9092 (no-auth; connector must be enabled: CONNECTORS_KAFKA_ENABLE=true)
topic 'kafka-ex-offsets-list' ready (retention.ms=3600000, retention.bytes=1048576)
produced 6 records
watermarks: earliest=0 latest(HWM)=6
offsets_for_times(boundary) -> offset 3
retention.ms present in DescribeConfigs — config round-trip OK
list-and-retention OK: watermarks + by-timestamp + retention config all verified
```

## 4. What's Happening

`fetch_watermarks` issues ListOffsets (key 2): earliest tracks the log start (0), latest is the
high-water mark (6). `offsets_for_times` resolves a mid-run timestamp to the first record produced after
it (offset 3). Retention is set at CreateTopics and read back with DescribeConfigs — mapping to the
native channel's MaxAge/MaxBytes.

## 5. Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| ListOffsets(2), DescribeConfigs(32) | acks=all, read_uncommitted | kafka-ex-offsets-list / 1 | query-only | earliest/latest/by-ts | none | librdkafka CRC32 | retention → channel MaxAge/MaxBytes |

## 6. Related Examples

`../../../{go,java,javascript,csharp}/offsets/list-and-retention`,
`../../../{python,ruby}/offsets/list_and_retention`. Concept: `../../../../docs/concepts/topics-partitions-offsets.md`.

## 7. Gotcha callout

Time-based retention **expiry** is not asserted here — it would require waiting out `retention.ms`. The
example verifies the config **round-trips**; actual expiry maps to the channel MaxAge and is covered in
the concepts docs.

## 8. Auth

Auth is off by default. See `../../../../docs/guides/security-sasl-tls.md`.
