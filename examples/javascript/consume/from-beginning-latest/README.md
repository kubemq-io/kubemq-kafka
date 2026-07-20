# javascript — Kafka: Consume From Beginning vs Latest

Prove the two `auto.offset.reset` positions: a consumer subscribed `{ fromBeginning: true }`
(earliest) replays pre-existing records, while `{ fromBeginning: false }` (latest) sees only records
produced after it joined. The Kafka topic `kafka-ex-consume-reset` maps onto the Events-Store channel
`kafka.kafka-ex-consume-reset`.

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
npx tsx consume/from-beginning-latest/index.ts
```

## Expected Output

```
Connecting to KubeMQ Kafka connector at localhost:9092 (topic "kafka-ex-consume-reset")
Produced 3 pre-existing records: [pre-1, pre-2, pre-3]
Produced 2 post-subscribe records: [post-1, post-2]
earliest consumer saw: [pre-1, pre-2, pre-3, post-1, post-2]
latest   consumer saw: [post-1, post-2]

Offset reset proven: fromBeginning=true replays history; fromBeginning=false starts at the log end
```

## What's Happening

- The producer writes three records before either consumer subscribes.
- The earliest consumer (`fromBeginning: true` == `auto.offset.reset=earliest`) replays the full log,
  so it sees the three pre-existing records plus the two produced afterward.
- The latest consumer (`fromBeginning: false` == `auto.offset.reset=latest`) starts at the log end, so
  it only sees the two post-subscribe records — never the pre-existing ones.
- Each consumer's `consumer.run({ eachMessage })` is a background Fetch long-poll (key 1); the program
  polls the collected arrays to a deadline, then `stop()`/`disconnect()`.
- Mirrors connector behavior in `connectors/kafka/` (Fetch bounded-read; see `connectors/kafka/fetch_test.go`).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Produce (0), Fetch (1), CreateTopics (19), DeleteTopics (20), FindCoordinator (10) | read_uncommitted | `kafka-ex-consume-reset` / 1 partition | two fresh groups (earliest, latest) | `fromBeginning` → `auto.offset.reset` earliest/latest | none | murmur2 (DefaultPartitioner) | earliest replays history; latest starts at the log end |

## Related Examples

- Same variant, other languages: [`../../../go/consume/from-beginning-latest`](../../../go/consume/from-beginning-latest),
  [`../../../java/consume/from-beginning-latest`](../../../java/consume/from-beginning-latest),
  [`../../../csharp/consume/from-beginning-latest`](../../../csharp/consume/from-beginning-latest),
  [`../../../rust/consume/from-beginning-latest`](../../../rust/consume/from-beginning-latest),
  [`../../../python/consume/from_beginning_latest`](../../../python/consume/from_beginning_latest),
  [`../../../ruby/consume/from_beginning_latest`](../../../ruby/consume/from_beginning_latest).
- Doc: [`../../../../docs/guides/consuming-and-groups.md`](../../../../docs/guides/consuming-and-groups.md),
  [`../../../../docs/concepts/topics-partitions-offsets.md`](../../../../docs/concepts/topics-partitions-offsets.md).
- Next: [`../seek-offsets-timestamps/`](../seek-offsets-timestamps/).

> **Gotcha — `consumer.run` is non-blocking.** kafkajs starts a background poll loop that never resolves
> to completion; a self-asserting example must poll a result buffer to a deadline and then explicitly
> `stop()`/`disconnect()`.

> **Auth.** The dev default is no SASL over plain TCP (`:9092`). For a secured connector, configure
> SASL/PLAIN or SASL/SCRAM (and TLS on `:9093`). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
