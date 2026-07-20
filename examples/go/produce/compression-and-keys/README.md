# Go — Kafka: Produce Compression and Keys

Every compression codec round-trips, and a keyed record lands on the **murmur2**
partition franz-go/Java/kafkajs all agree on:
`Produce(none|gzip|snappy|lz4|zstd) → Fetch → Produce(key×10) → assert partition`.

## Prerequisites

- Go 1.24+ and `github.com/twmb/franz-go v1.21.4` (pinned in `../../go.mod`).
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). **The connector is DISABLED by default — start the
  broker with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1). For any non-same-host
  client, also set `CONNECTORS_KAFKA_ADVERTISED_HOST` or the client connects then
  hangs (gotcha #2).

## How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
go run ./produce/compression-and-keys
```

## Expected Output

```
[kubemq-kafka] produce/compression-and-keys | bootstrap=localhost:9092 partitioner=murmur2(franz-go)
CreateTopic: kafka-ex-produce-comp-<8hex> (partitions=3)
Produce(none): partition=<p> offset=<o> round-trip ok
Produce(gzip): partition=<p> offset=<o> round-trip ok
Produce(snappy): partition=<p> offset=<o> round-trip ok
Produce(lz4): partition=<p> offset=<o> round-trip ok
Produce(zstd): partition=<p> offset=<o> round-trip ok
Keyed: key "customer-42" -> partition 0 for all 10 copies (murmur2 expected 0)
DeleteTopic: ok
PASS: all codecs round-trip + murmur2 keyed partitioning verified
```

> The topic is suffixed with 8 random hex chars so concurrent runs of the other
> language examples against the same connector do not collide.

## What's Happening

The program creates a 3-partition topic and produces one record under each of the
five codecs (`none`, `gzip`, `snappy`, `lz4`, `zstd`), reading each back and
asserting the key and value survive the compress → RecordBatch → decompress trip
byte-for-byte. It then sends the **same key** (`customer-42`) 10 times and asserts
every copy lands on the partition franz-go's default **murmur2** partitioner
computes (`(murmur2(key) & 0x7fffffff) % 3 = 0`). This is the load-bearing point of
gotcha #4: a librdkafka client (confluent-kafka, Confluent.Kafka, rdkafka) hashes
the same key with **CRC32** and would pick a different partition.

The wire flow is `Metadata → CreateTopics → Produce (compressed RecordBatch v2) →
Fetch`, mirroring connector behavior in `connectors/kafka/produce_test.go`.

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Produce(0), Fetch(1), CreateTopics(19), DeleteTopics(20), Metadata(3) | acks=all; read_uncommitted | 3 partitions | none | offset per partition, independent spaces | none/gzip/snappy/lz4/zstd | **murmur2** (franz-go) | **gotcha #4** — same key hashes to a DIFFERENT partition on librdkafka (CRC32); keyed record asserted on the murmur2 partition; all codecs must round-trip |

## Related Examples

- Same variant in other languages: `../../../python/produce/compression_and_keys`,
  `../../../javascript/produce/compression-and-keys`,
  `../../../java/produce/compression-and-keys`,
  `../../../csharp/produce/compression-and-keys`,
  `../../../ruby/produce/compression_and_keys`,
  `../../../rust/produce/compression-and-keys`.
- Docs: `../../../../docs/concepts/cross-client-partitioning.md`.
- Related: [`../basic-acks`](../basic-acks), [`../idempotent`](../idempotent).

> **Gotcha #4 — cross-client partitioner divergence.** franz-go, Java
> `kafka-clients`, and kafkajs (≥2.0) default to **murmur2**; the four
> librdkafka-based clients (confluent-kafka, Confluent.Kafka, rdkafka-ruby, rust
> rdkafka) default to **CRC32**. The **same key** therefore maps to a **different
> partition** across client families. Pin the partitioner explicitly if a topic is
> written by more than one client stack.

> Auth: this example uses the no-auth default posture. Runs with no SASL by default
> on a stock dev broker; for SASL/PLAIN + SCRAM (and mTLS principal derivation) see
> [`../../security/sasl-plain-scram`](../../security/sasl-plain-scram) +
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
