# Cross-Client Partitioning

This page documents the single most surprising cross-client behavior in the Kafka ecosystem — and
it is **not** specific to KubeMQ. When you produce a **keyed** record, the client library, not the
broker, decides which partition it lands on. Different client libraries default to **different**
hash functions, so the same key can land on a **different** partition depending on which client
wrote it.

## The connector does not choose the partition

For keyed records, the **producing client** computes `partition = hash(key) % N` and sends the
record to that partition. The connector honors the partition the client selected — it does not
re-hash. So partition placement is a property of the **client**, not the broker. (This is standard
Kafka semantics; KubeMQ inherits it faithfully.)

## Two default partitioners: murmur2 vs CRC32

The seven clients this repo covers fall into two camps by default hash:

| Default partitioner | Clients |
|---------------------|---------|
| **murmur2** (Java-compatible) | franz-go (Go), Java `kafka-clients`, **kafkajs (v2+ `DefaultPartitioner`)** |
| **CRC32** | librdkafka family: `confluent-kafka` (Python), `Confluent.Kafka` (C#), `rdkafka` (Ruby), `rdkafka` (Rust) |

> **Gotcha #4 — cross-client partitioner divergence.** A key hashed with murmur2 and the same key
> hashed with CRC32 generally pick **different** partitions. So a record keyed `"user-42"` produced
> by franz-go/Java/kafkajs may land on a different partition than the same key produced by
> librdkafka/`kcat`/confluent-kafka. Consumers that assume "same key → same partition across all
> producers" are wrong when producers use different client families.

### Consequence for the examples

The keyed produce example (`produce/compression-and-keys`, variant 3) and this page **expect the
murmur2 result** for the JS/Java/franz-go clients and the CRC32 result for the four librdkafka
clients — they are not the same partition. The JS keyed example specifically must expect the **same**
partition as Java/franz-go (kafkajs v2+ is murmur2), **not** the CRC32 group.

> **kafkajs version caveat.** A pre-2.0 kafkajs pin uses the legacy partitioner (not murmur2). This
> repo floors kafkajs at **≥ 2.0** so its default `DefaultPartitioner` is murmur2 and matches
> Java/franz-go. If you see kafkajs disagreeing with Java on partition placement, check the pin.

### Practical guidance

- If you need identical placement across mixed client families, **set the partition explicitly**
  (produce to a chosen partition) rather than relying on the default keyed partitioner, or
  standardize on one partitioner across all producers.
- Within a **single** client family, keyed placement is consistent and per-key ordering holds (see
  the epoch caveat below).

## Growing N re-shards keys

Even within one partitioner, the partition a key maps to depends on the partition **count**:
`partition = hash(key) % N`. Increasing `N` via `CreatePartitions` changes the modulus, so existing
keys re-shard across the new partition set.

> **Gotcha #5 — growing `N` re-shards keys; per-key order holds only within a fixed-`N` epoch.**
> After a `CreatePartitions` increase, a key that used to map to partition `2` may now map to
> partition `5`. Records for that key written before and after the growth live on **different**
> partitions (different channels), so their relative order is not preserved across the growth
> boundary. Per-key ordering is guaranteed **only within a fixed-`N` epoch**. Plan partition growth
> at a quiescent point, or accept that keyed ordering resets at the reshard. See
> [topics-partitions-offsets.md](topics-partitions-offsets.md).

## Where this surfaces

| Surface | How it appears |
|---------|----------------|
| `produce/compression-and-keys` (variant 3) | Keyed records land per the client's partitioner; the README calls out gotcha #4 and expects murmur2 for JS/Java/franz-go |
| `consumer-groups/*` | Assignment + per-key ordering assumptions depend on a fixed partitioner and fixed `N` |
| This page | The canonical explanation both examples link to |

## See Also

- [topics-partitions-offsets.md](topics-partitions-offsets.md) — partitions as independent channels; the increase-only model.
- [../guides/producing.md](../guides/producing.md) — keyed partitioning and how to pin placement.
- [consumer-groups.md](consumer-groups.md) — rebalance on partition growth.

## Grounding

The connector honors the client-chosen partition (it does not re-hash); franz-go is the connector's
own conformance client and defaults to murmur2. Proven by `produce_test.go`,
`multipartition_integration_test.go`, and `createpartitions_rebalance_test.go` in
`connectors/kafka/`.
