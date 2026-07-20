# javascript — Kafka: List Offsets and Retention

Create a topic with `retention.ms` / `retention.bytes` (mapped to the channel's MaxAge / MaxBytes),
produce a numbered sequence, and query offsets three ways: earliest (log-start), latest (HWM), and
by-timestamp (ListOffsets, key 2). The Kafka topic `kafka-ex-offsets` maps onto the Events-Store
channel `kafka.kafka-ex-offsets`.

## Prerequisites

- Node.js 18+ and `npm install` in `examples/javascript/` (pins `kafkajs` `^2.2.4` — v2+, murmur2
  `DefaultPartitioner`).
- A running KubeMQ server with the Kafka connector **enabled** (`CONNECTORS_KAFKA_ENABLE=true` — the
  connector is **disabled by default**, gotcha #1), reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). For external clients, set `CONNECTORS_KAFKA_ADVERTISED_HOST` (gotcha #2).

## How to Run

```bash
cd examples/javascript
npm install
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
npx tsx offsets/list-and-retention/index.ts
```

## Expected Output

```
Connecting to KubeMQ Kafka connector at localhost:9092 (topic "kafka-ex-offsets")
Created topic with retention.ms=86400000, retention.bytes=1048576
Produced 5 records
fetchTopicOffsets -> low(earliest)=0 high(latest/HWM)=5 offset=5
fetchTopicOffsetsByTimestamp(1752000002000) -> offset 2 (expect 2 = record o-2)

ListOffsets proven: earliest=0, latest=HWM, by-timestamp resolves the first record >= ts; retention config accepted
```

> The by-timestamp target is `Date.now()`-based, so the numeric timestamp varies per run; the resolved
> offset (2) is deterministic.

## What's Happening

- The topic is created with `retention.ms=86400000` and `retention.bytes=1048576`, which map to the
  Events-Store channel's MaxAge / MaxBytes (§2.2).
- Five records are produced with monotonically increasing timestamps.
- `admin.fetchTopicOffsets` returns `low` (earliest, 0) and `high` (latest / HWM, 5) — a ListOffsets
  query for the earliest/latest special timestamps.
- `admin.fetchTopicOffsetsByTimestamp(target)` (ListOffsets by-timestamp, key 2) returns the offset of
  the first record with `timestamp >= target` — here offset 2 (`o-2`).
- Mirrors connector behavior in `connectors/kafka/` (ListOffsets earliest/latest/by-ts; see
  `connectors/kafka/listoffsets_test.go`).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| ListOffsets (2), CreateTopics (19), Produce (0), DeleteTopics (20) | read_uncommitted | `kafka-ex-offsets` / 1 partition | none | earliest = log-start (0); latest = HWM (N); by-ts = first offset with `ts >= target` | none | murmur2 (DefaultPartitioner) | `retention.ms` / `retention.bytes` → channel MaxAge / MaxBytes (§2.2) |

## Related Examples

- Same variant, other languages: [`../../../go/offsets/list-and-retention`](../../../go/offsets/list-and-retention),
  [`../../../java/offsets/list-and-retention`](../../../java/offsets/list-and-retention),
  [`../../../csharp/offsets/list-and-retention`](../../../csharp/offsets/list-and-retention),
  [`../../../rust/offsets/list-and-retention`](../../../rust/offsets/list-and-retention),
  [`../../../python/offsets/list_and_retention`](../../../python/offsets/list_and_retention),
  [`../../../ruby/offsets/list_and_retention`](../../../ruby/offsets/list_and_retention).
- Doc: [`../../../../docs/concepts/topics-partitions-offsets.md`](../../../../docs/concepts/topics-partitions-offsets.md),
  [`../../../../docs/reference/configuration.md`](../../../../docs/reference/configuration.md).
- Next: [`../../transactions/eos-commit-abort/`](../../transactions/eos-commit-abort/).

> **Gotcha — offsets are STAN Sequences.** The earliest offset tracks the log-start (advances as
> retention or `DeleteRecords` truncates the low end); the latest offset is the HWM. Both are exposed
> via ListOffsets, not stored client-side.

> **Auth.** The dev default is no SASL over plain TCP (`:9092`). For a secured connector, configure
> SASL/PLAIN or SASL/SCRAM (and TLS on `:9093`). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
