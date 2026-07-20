# javascript — Kafka: Compression and Keys

Produce keyed records with `CompressionTypes.None` and `CompressionTypes.GZIP` against a
multi-partition topic, then read them back to prove (a) gzip round-trips through the connector and
(b) a given key always lands on the **same** partition — the one the **murmur2** `DefaultPartitioner`
computes (Java/franz-go compatible, NOT librdkafka CRC32). The Kafka topic `kafka-ex-produce-comp`
maps onto the Events-Store channel `kafka.kafka-ex-produce-comp`.

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
npx tsx produce/compression-and-keys/index.ts
```

## Expected Output

```
Connecting to KubeMQ Kafka connector at localhost:9092 (topic "kafka-ex-produce-comp", 3 partitions)
Produced 5 keyed records with compression=none across 3 partition(s)
Produced 5 keyed records with compression=gzip across 3 partition(s)
key user-1 -> partition(s) [2]
key user-2 -> partition(s) [0]
key user-3 -> partition(s) [1]
key user-4 -> partition(s) [2]
key user-5 -> partition(s) [0]

Compression + keyed partitioning proven: gzip round-tripped; each key stable on its murmur2 partition
```

> The exact partition each key lands on is fixed by murmur2 over the key bytes (the numbers above are
> illustrative). Each key appears on exactly **one** partition across both the `none` and `gzip` sends.

## What's Happening

- `producer.send({ compression, messages: [{ key, value }] })` issues a Produce (key 0) with the
  RecordBatch compression codec set to `none` then `gzip`.
- The murmur2 `DefaultPartitioner` maps each key to a partition; the same key always resolves to the
  same partition regardless of codec, so the read-back groups every key onto exactly one partition.
- kafkajs bundles gzip; **snappy / lz4 / zstd** require optional peer codecs registered on
  `CompressionCodecs` (documented in the source header) — this runnable path uses `none` + `gzip`.
- Mirrors connector behavior in `connectors/kafka/` (Produce with compressed RecordBatch v2; see
  `connectors/kafka/produce_test.go`).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Produce (0), Fetch (1), CreateTopics (19), DeleteTopics (20) | acks default | `kafka-ex-produce-comp` / 3 partitions | ephemeral verify group | offset = STAN Sequence | none, gzip (built-in); snappy/lz4/zstd opt-in | **murmur2** (DefaultPartitioner) | keyed records land on the murmur2 partition — same as Java/franz-go, **not** CRC32 (gotcha #4) |

## Related Examples

- Same variant, other languages: [`../../../go/produce/compression-and-keys`](../../../go/produce/compression-and-keys),
  [`../../../java/produce/compression-and-keys`](../../../java/produce/compression-and-keys),
  [`../../../csharp/produce/compression-and-keys`](../../../csharp/produce/compression-and-keys),
  [`../../../rust/produce/compression-and-keys`](../../../rust/produce/compression-and-keys),
  [`../../../python/produce/compression_and_keys`](../../../python/produce/compression_and_keys),
  [`../../../ruby/produce/compression_and_keys`](../../../ruby/produce/compression_and_keys).
- Doc: [`../../../../docs/concepts/cross-client-partitioning.md`](../../../../docs/concepts/cross-client-partitioning.md),
  [`../../../../docs/guides/producing.md`](../../../../docs/guides/producing.md).
- Next: [`../../consume/from-beginning-latest/`](../../consume/from-beginning-latest/).

> **Gotcha #4 — cross-client partitioner divergence.** kafkajs v2+ `DefaultPartitioner` is **murmur2**
> (same as Java `kafka-clients` and franz-go). The four librdkafka-based clients (confluent-kafka,
> Confluent.Kafka, rdkafka-ruby, rust rdkafka) default to **CRC32**, so the same key can land on a
> different partition across clients. Expect the murmur2 partition here.

> **Auth.** The dev default is no SASL over plain TCP (`:9092`). For a secured connector, configure
> SASL/PLAIN or SASL/SCRAM (and TLS on `:9093`). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
