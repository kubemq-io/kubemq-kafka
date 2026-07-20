# java — Kafka: Compression and Keys

Produce the same keyed records under every codec — `none`, `gzip`, `snappy`, `lz4`,
`zstd` — and prove two things at once: each codec round-trips byte-for-byte, and a
given key always lands on the **same** partition (the murmur2 partitioner).

## Prerequisites

- JDK 21+ and Maven 3.9+.
- `org.apache.kafka:kafka-clients 3.9.0` (pinned in `../../pom.xml`). `gzip` is
  JDK-native; `snappy`/`lz4`/`zstd` need `snappy-java`, `lz4-java`, and `zstd-jni`
  on the classpath — all declared in `../../pom.xml`.
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). **Connector DISABLED by default — start with
  `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1); set `CONNECTORS_KAFKA_ADVERTISED_HOST`
  for remote clients (gotcha #2).

## How to Run

From `examples/java/`:

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
mvn -q compile
mvn -q exec:exec -Dexec.mainClass=io.kubemq.examples.kafka.produce.compressionandkeys.Main
```

## Expected Output

```
bootstrap.servers = localhost:9092
CreateTopics 'kafka-ex-produce-compress-java' (3 partitions)
Produce codec=none key=alpha -> partition=2 offset=0
Produce codec=none key=bravo -> partition=0 offset=0
Produce codec=none key=charlie -> partition=1 offset=0
Produce codec=none key=delta -> partition=2 offset=1
Produce codec=gzip key=alpha -> partition=2 offset=2
... (snappy, lz4, zstd — every key stays on its partition) ...
Key -> partition map (murmur2): {alpha=2, bravo=0, charlie=1, delta=2}
Fetch <- matched 20/20 codec round-trips
OK: all codecs round-tripped and keys are murmur2-stable
```

The exact partition per key is whatever **murmur2** assigns over 3 partitions; what
matters is that it is **stable** across every codec, and that it matches the Go and
JS suites (see the gotcha below).

## What's Happening

The program creates a 3-partition topic, then for each of the five compression
codecs produces one record per key (`alpha`, `bravo`, `charlie`, `delta`). Each
value encodes its own codec+key so a byte-for-byte read-back proves the codec
round-tripped without corruption. The partition observed the first time a key is
seen is remembered; every later send for that key must land on the same partition,
asserting partitioner stability. Finally it consumes all `5 × 4 = 20` records and
verifies each landed on its key's murmur2 partition.

The Kafka wire flow is `Metadata → CreateTopics → Produce (compressed RecordBatch
v2, per codec) → Fetch`, mirroring connector behavior in `connectors/kafka/`
(compression + partition-assignment path).

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Produce(0), Fetch(1), CreateTopics(19) | acks=all; read_uncommitted | 3 partitions | fresh (throwaway) group | offset = STAN Sequence per partition | none/gzip/snappy/lz4/zstd | **murmur2** (kafka-clients default) | key→partition is murmur2 — same as franz-go/kafkajs, DIFFERENT from librdkafka CRC32 (gotcha #4); growing N re-shards keys (gotcha #5) |

## Related Examples

- Same variant in the other 6 languages: [`../../../go/produce/compression-and-keys`](../../../go/produce/compression-and-keys),
  [`../../../python/produce/compression_and_keys`](../../../python/produce/compression_and_keys),
  [`../../../javascript/produce/compression-and-keys`](../../../javascript/produce/compression-and-keys),
  [`../../../csharp/produce/compression-and-keys`](../../../csharp/produce/compression-and-keys),
  [`../../../ruby/produce/compression_and_keys`](../../../ruby/produce/compression_and_keys),
  [`../../../rust/produce/compression-and-keys`](../../../rust/produce/compression-and-keys).
- Docs: [`../../../../docs/guides/producing.md`](../../../../docs/guides/producing.md),
  [`../../../../docs/concepts/cross-client-partitioning.md`](../../../../docs/concepts/cross-client-partitioning.md).
- Related: [`../basic-acks`](../basic-acks), [`../idempotent`](../idempotent).

> **Gotcha #4 — cross-client partitioner divergence.** `kafka-clients` hashes keys
> with **murmur2**, the same as franz-go and kafkajs v2+. The four librdkafka-based
> clients (python/csharp/ruby/rust) default to **CRC32**, so the same key can land on
> a **different** partition there. This example asserts the **murmur2** partition; see
> [`cross-client-partitioning.md`](../../../../docs/concepts/cross-client-partitioning.md).
> **Gotcha #5** — increasing partition count re-shards keys, so per-key order holds
> only within a fixed-N epoch.

> **Auth.** This example uses the connector's no-auth default posture
> (SHARED-CONVENTIONS §4.3). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md)
> for SASL/PLAIN + SCRAM and TLS/mTLS.
