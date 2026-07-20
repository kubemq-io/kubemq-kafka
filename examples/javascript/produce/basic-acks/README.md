# javascript — Kafka: Produce Basic Acks

Produce the same payload at `acks=all` (-1), `acks=1` (leader), and `acks=0` (fire-and-forget)
against the KubeMQ Kafka connector, fetch the guaranteed records back, and prove an oversized
record is rejected with `MESSAGE_TOO_LARGE`. The Kafka topic `kafka-ex-produce-acks` maps onto the
Events-Store channel `kafka.kafka-ex-produce-acks` (channel prefix `kafka.`; offset = STAN Sequence).

## Prerequisites

- Node.js 18+ and `npm install` in `examples/javascript/` (pins `kafkajs` `^2.2.4` — v2+ so the
  `DefaultPartitioner` is **murmur2**, Java/franz-go compatible, NOT the librdkafka CRC32).
- A running KubeMQ server with the Kafka connector **enabled** (`CONNECTORS_KAFKA_ENABLE=true` — the
  connector is **disabled by default**, gotcha #1), reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). For external clients, set `CONNECTORS_KAFKA_ADVERTISED_HOST` or the
  client connects then hangs (gotcha #2).

## How to Run

```bash
cd examples/javascript
npm install
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
npx tsx produce/basic-acks/index.ts
```

## Expected Output

```
Connecting to KubeMQ Kafka connector at localhost:9092 (topic "kafka-ex-produce-acks" -> channel "kafka.kafka-ex-produce-acks")
Produce acks=all    -> partition=0 baseOffset=0
Produce acks=leader -> partition=0 baseOffset=1
Produce acks=none   -> partition=0 baseOffset=(none, acks=0)
Fetch          -> read back 3 record(s); acks>=1 records present
Produce 2MiB   -> rejected: MESSAGE_TOO_LARGE

Round-trip complete: acks 0/1/all produced, acks>=1 read back, oversized rejected OK
```

## What's Happening

- `admin.createTopics` registers `kafka.kafka-ex-produce-acks` (auto-create is also on via Metadata/Produce).
- `producer.send({ acks })` issues a Produce (key 0, RecordBatch v2); acks `-1/1/0` = all/leader/none.
- acks>=1 returns a `baseOffset` (the STAN Sequence); acks=0 returns none (fire-and-forget).
- A short-lived consumer Fetches (key 1, bounded-read long-poll) the records back from the beginning.
- The 2 MiB record exceeds `MaxMessageBytes` (default 1 MiB, §2.7) → `MESSAGE_TOO_LARGE`.
- Mirrors connector behavior in `connectors/kafka/` (Produce path; see `connectors/kafka/produce_test.go`).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Produce (0), Fetch (1), CreateTopics (19), DeleteTopics (20), Metadata (3) | acks 0/1/all; read_uncommitted | `kafka-ex-produce-acks` / 1 partition | ephemeral verify group | offset = STAN Sequence, starts at 0 | none | murmur2 (DefaultPartitioner) | oversized → `MESSAGE_TOO_LARGE`; gotcha #3 |

## Related Examples

- Same variant, other languages: [`../../../go/produce/basic-acks`](../../../go/produce/basic-acks),
  [`../../../java/produce/basic-acks`](../../../java/produce/basic-acks),
  [`../../../csharp/produce/basic-acks`](../../../csharp/produce/basic-acks),
  [`../../../rust/produce/basic-acks`](../../../rust/produce/basic-acks),
  [`../../../python/produce/basic_acks`](../../../python/produce/basic_acks),
  [`../../../ruby/produce/basic_acks`](../../../ruby/produce/basic_acks).
- Doc: [`../../../../docs/guides/producing.md`](../../../../docs/guides/producing.md).
- Next: [`../idempotent/`](../idempotent/), [`../compression-and-keys/`](../compression-and-keys/).

> **Gotcha #3 — `acks>=1` on multi-node.** On a clustered broker, `acks=0` targeting a follower can be
> silently dropped. Use `acks=1` (leader) or `acks=all` for any delivery guarantee.

> **Auth.** The dev default is no SASL over plain TCP (`:9092`). For a secured connector, configure
> SASL/PLAIN or SASL/SCRAM (and TLS on `:9093`). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
