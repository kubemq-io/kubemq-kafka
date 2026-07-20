# Rust — Kafka: Seek by offset and timestamp

Reposition a consumer to an explicit log offset (`seek`) and to a wall-clock timestamp
(`offsets_for_times` = ListOffsets by-timestamp).

## 1. Prerequisites

- Rust 1.75+ + Cargo; `rdkafka` 0.37 (librdkafka) on `tokio`, via the workspace.
- A running KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`).
- **`CONNECTORS_KAFKA_ENABLE=true`** on the broker (gotcha #1).

## 2. How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
cargo run -p seek-offsets-timestamps
```

## 3. Expected Output

```text
[kubemq-kafka] consume/seek-offsets-timestamps bootstrap=localhost:9092 (no-auth; connector must be enabled: CONNECTORS_KAFKA_ENABLE=true)
produced 5 records; boundary timestamp = <ms> (between rec-2 and rec-3)
seek(offset=2) -> next record offset=2 body='rec-2'
offsets_for_times(boundary) -> offset 3
seek(by-timestamp) -> next record offset=3 body='rec-3'
seek-offsets-timestamps OK: offset seek and timestamp seek both landed correctly
```

## 4. What's Happening

Five records are produced with a wall-clock boundary marked between the third and fourth. The consumer
assigns the partition, `seek`s to offset 2 (next Fetch returns `rec-2`), then resolves the boundary
timestamp to an offset via `offsets_for_times` (ListOffsets, timestamp `-3`), which returns the first
record at/after the boundary — `rec-3`.

## 5. Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Produce(0), ListOffsets(2), Fetch(1) | acks=all, read_uncommitted | kafka-ex-consume-seek / 1 | assign (no subscribe) | explicit seek + by-timestamp | none | librdkafka CRC32 | timestamp resolves to first record ≥ ts |

## 6. Related Examples

`../../../{go,java,javascript,csharp}/consume/seek-offsets-timestamps`,
`../../../{python,ruby}/consume/seek_offsets_timestamps`. Concept: `../../../../docs/concepts/topics-partitions-offsets.md`.

## 7. Gotcha callout

`offsets_for_times` returns the offset of the **first record whose timestamp ≥ the query time**; if no
record is that recent it returns `-1` (end). Message timestamps here are producer CreateTime.

## 8. Auth

Auth is off by default. See `../../../../docs/guides/security-sasl-tls.md`.
