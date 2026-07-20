# javascript — Kafka: Seek by Offset and Timestamp

Reposition a consumer with `consumer.seek({ offset })` and with a timestamp lookup
(`admin.fetchTopicOffsetsByTimestamp` → ListOffsets by-timestamp, key 2), and prove both land on the
expected record. The Kafka topic `kafka-ex-consume-seek` maps onto the Events-Store channel
`kafka.kafka-ex-consume-seek`.

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
npx tsx consume/seek-offsets-timestamps/index.ts
```

## Expected Output

```
Connecting to KubeMQ Kafka connector at localhost:9092 (topic "kafka-ex-consume-seek")
Produced 6 records with timestamps 1752000000000..1752000005000
seek(offset=3) -> first delivered: "record-3"
fetchTopicOffsetsByTimestamp(1752000004000) -> offset 4
seek(ts-offset=4) -> first delivered: "record-4"

Seek proven: seek-by-offset and ListOffsets-by-timestamp both land on the expected record
```

> The timestamp values are `Date.now()`-based, so the exact numbers vary per run; the resolved offsets
> (3 and 4) are deterministic.

## What's Happening

- Six records are produced with explicit, monotonically increasing timestamps so the by-timestamp
  lookup is deterministic.
- `consumer.seek({ topic, partition, offset: '3' })` repositions the running consumer; the next record
  delivered is exactly `record-3`. kafkajs requires the `run` loop to be live before `seek` takes
  effect, so the program starts `run()`, waits briefly, then seeks.
- `admin.fetchTopicOffsetsByTimestamp(targetTs)` issues a ListOffsets by-timestamp (key 2) and returns
  the offset of the first record with `timestamp >= target`; seeking there lands on `record-4`.
- Mirrors connector behavior in `connectors/kafka/` (ListOffsets by-timestamp; see
  `connectors/kafka/listoffsets_test.go`).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Produce (0), Fetch (1), ListOffsets (2), CreateTopics (19), DeleteTopics (20) | read_uncommitted | `kafka-ex-consume-seek` / 1 partition | two fresh seek groups | seek by absolute offset; by-timestamp → first offset with `ts >= target` | none | murmur2 (DefaultPartitioner) | `run` loop must be live before `seek`; by-ts resolves via ListOffsets(key 2) |

## Related Examples

- Same variant, other languages: [`../../../go/consume/seek-offsets-timestamps`](../../../go/consume/seek-offsets-timestamps),
  [`../../../java/consume/seek-offsets-timestamps`](../../../java/consume/seek-offsets-timestamps),
  [`../../../csharp/consume/seek-offsets-timestamps`](../../../csharp/consume/seek-offsets-timestamps),
  [`../../../rust/consume/seek-offsets-timestamps`](../../../rust/consume/seek-offsets-timestamps),
  [`../../../python/consume/seek_offsets_timestamps`](../../../python/consume/seek_offsets_timestamps),
  [`../../../ruby/consume/seek_offsets_timestamps`](../../../ruby/consume/seek_offsets_timestamps).
- Doc: [`../../../../docs/guides/consuming-and-groups.md`](../../../../docs/guides/consuming-and-groups.md),
  [`../../../../docs/reference/channel-mapping.md`](../../../../docs/reference/channel-mapping.md).
- Next: [`../../offsets/list-and-retention/`](../../offsets/list-and-retention/).

> **Gotcha — seek needs a live run loop.** In kafkajs, `consumer.seek(...)` is only honored while the
> consumer group's `run` loop is active; call `run()` first, then `seek()`.

> **Auth.** The dev default is no SASL over plain TCP (`:9092`). For a secured connector, configure
> SASL/PLAIN or SASL/SCRAM (and TLS on `:9093`). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
