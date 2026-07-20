# C# — Kafka: Compression & Keyed Partitioning

Round-trip a record under every compression codec (`None/Gzip/Snappy/Lz4/Zstd`) and
show keyed partitioning: a fixed key always lands on the **same** partition.

## Prerequisites

- .NET SDK **8.0**
- **Confluent.Kafka 2.6.0** (bundles gzip/snappy/lz4/zstd; pinned in
  `examples/csharp/Directory.Packages.props`).
- A running KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` (default
  `localhost:9092`) — **start with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1).

## How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
dotnet run --project produce/compression-and-keys
```

## Expected Output

```
[*] Created topic 'kafka-ex-produce-compression-and-keys' with 3 partitions
[x] codec=None   key='customer-42' → partition 1 @ offset 0
[x] codec=Gzip   key='customer-42' → partition 1 @ offset 1
[x] codec=Snappy key='customer-42' → partition 1 @ offset 2
[x] codec=Lz4    key='customer-42' → partition 1 @ offset 3
[x] codec=Zstd   key='customer-42' → partition 1 @ offset 4
[v] key='alpha' → CRC32 partition 2
...
[*] Cleaned up topic 'kafka-ex-produce-compression-and-keys'
[ok] Compression codecs round-trip + CRC32 keyed partitioning verified (gotcha #4)
```

## What's Happening

Each codec produces one keyed record (`customer-42`); the record's
`DeliveryResult.Partition` is the SAME every time — keyed records are sticky. A set
of different keys spreads across more than one partition. All five codec payloads
are then read back intact.

> **Gotcha #4 — CRC32, not murmur2.** `Confluent.Kafka` is librdkafka-based, so its
> default partitioner is **CRC32** (`consistent_random`) — the same group as
> Python / Ruby / Rust, but the **opposite** of Java / franz-go / **kafkajs**
> (murmur2). The partition `customer-42` lands on here **matches the CRC32 clients
> and differs from the Java/JS examples**. Do not copy a murmur2 expected-partition.
> See [`../../../../docs/concepts/cross-client-partitioning.md`](../../../../docs/concepts/cross-client-partitioning.md).
>
> **Gotcha #5 — growing partitions re-shards keys.** If you later `CreatePartitions`
> to a larger N, the CRC32-of-key modulo N changes, so existing keys can move.

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|----------|----------------|------------------|----------------|------------------|-------------|-------------|------------------|
| Metadata, CreateTopics, Produce, Fetch, DeleteTopics | `acks=All` | `kafka-ex-produce-compression-and-keys` / 3 | ephemeral `cs-compression-<uuid>` | offset = STAN `Sequence`; read from `earliest` | `None, Gzip, Snappy, Lz4, Zstd` | **CRC32** (librdkafka) — differs from Java/JS murmur2 | **gotcha #4** (CRC32 ≠ murmur2), **gotcha #5** (growing N re-shards); every codec round-trips; fixed key → stable partition |

## Related Examples

Same variant in the other languages:

- **Go** — [`../../../go/produce/compression-and-keys`](../../../go/produce/compression-and-keys)
- **Python** — [`../../../python/produce/compression_and_keys`](../../../python/produce/compression_and_keys)
- **Java** — [`../../../java/produce/compression-and-keys`](../../../java/produce/compression-and-keys)
- **JS/TS** — [`../../../javascript/produce/compression-and-keys`](../../../javascript/produce/compression-and-keys)
- **Ruby** — [`../../../ruby/produce/compression_and_keys`](../../../ruby/produce/compression_and_keys)
- **Rust** — [`../../../rust/produce/compression-and-keys`](../../../rust/produce/compression-and-keys)

Docs: [`../../../../docs/concepts/cross-client-partitioning.md`](../../../../docs/concepts/cross-client-partitioning.md),
[`../../../../docs/guides/producing.md`](../../../../docs/guides/producing.md)

---

> **Auth:** the connector default is no authentication. SASL/TLS setup lives in
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
