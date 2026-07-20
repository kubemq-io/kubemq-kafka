# Producing

This guide covers the producer surface end to end: acknowledgement levels, the idempotent producer,
compression, and keyed partitioning. Every produced record appends to a KubeMQ **Events Store**
channel `kafka.<topic>` (partition `p>0` → `kafka.<topic>~<p>`), and its offset is the record's STAN
`Sequence`. See [../reference/channel-mapping.md](../reference/channel-mapping.md).

The full ✅ Full produce surface: `Produce` (API key 0; RecordBatch v2), the idempotent producer
(`InitProducerId`, key 22), and compression `none` / `gzip` / `snappy` / `lz4` / `zstd`. See
[../reference/capabilities.md](../reference/capabilities.md).

## Acknowledgements (acks 0 / 1 / all)

`Produce` honors the three standard ack levels:

| `acks` | Meaning |
|--------|---------|
| `0` | Fire-and-forget — no broker acknowledgement. |
| `1` | Acknowledged when the leader has the record. |
| `all` | Acknowledged when the record is durably committed (Raft-replicated). |

> **Gotcha #3 — use `acks>=1` on a multi-node broker.** On a multi-node deployment, `acks=0` sent to
> a follower is **silently dropped** — the produce is not durably acknowledged and the record can be
> lost. Use `acks=1` (leader) or `acks=all` (durable) whenever durability matters. The examples
> default to `acks=all`. This surfaces in `produce/basic-acks` and
> [../reference/capabilities.md](../reference/capabilities.md).

A record larger than `MaxMessageBytes` (default 1 MiB, `CONNECTORS_KAFKA_MAX_MESSAGE_BYTES`) is
rejected with `MESSAGE_TOO_LARGE`. The `produce/basic-acks` example proves the round-trip at each
ack level and the oversized rejection.

## Idempotent producer

Enabling idempotence (`enable.idempotence=true`) makes the producer allocate a producer id via
`InitProducerId` (key 22) and tag each record with `(PID, sequence)`. The connector deduplicates
per `(PID, partition)`, so a **retry** of an already-appended record does not create a duplicate:

- exactly-once **within a single producer session** on a partition;
- safe automatic retries without duplicates.

The `produce/idempotent` example proves that retried produces do not duplicate. (Cross-session,
cross-producer exactly-once is a **transactions** concern — see
[transactions-eos.md](../concepts/transactions-eos.md).)

## Compression

The connector accepts RecordBatch v2 with any of the five standard codecs — `none`, `gzip`,
`snappy`, `lz4`, `zstd` — and stores/serves the batch faithfully. Pick the codec on the producer;
consumers decompress transparently. The `produce/compression-and-keys` example exercises each codec.

## Keyed partitioning

A record **key** selects a partition: the **client** computes `partition = hash(key) % N` and sends
the record there. This is the one place where client choice matters more than broker behavior,
because different client libraries default to different hash functions.

> **Gotcha #4 — cross-client partitioner divergence.** franz-go, Java `kafka-clients`, and kafkajs
> (v2+) default to **murmur2**; the four librdkafka clients (`confluent-kafka`, `Confluent.Kafka`,
> `rdkafka` Ruby, `rdkafka` Rust) default to **CRC32**. The same key can therefore land on a
> **different** partition depending on which client produced it. The keyed example
> (`produce/compression-and-keys`) expects the murmur2 result for the JS/Java/franz-go clients and
> the CRC32 result for the librdkafka clients. See
> [../concepts/cross-client-partitioning.md](../concepts/cross-client-partitioning.md).

To force identical placement across mixed client families, **set the partition explicitly** rather
than relying on the default keyed partitioner, or standardize on one partitioner across producers.

> **Gotcha #5 — growing `N` re-shards keys.** Because placement is `hash(key) % N`, increasing the
> partition count via `CreatePartitions` changes which partition a key maps to. Per-key ordering
> holds only **within a fixed-`N` epoch**. See
> [../concepts/topics-partitions-offsets.md](../concepts/topics-partitions-offsets.md).

## Per-partition ordering

Within a fixed partition, records are strictly ordered by offset (STAN Sequence). Order is a
**per-partition** guarantee, not a topic-wide one — records for the same key stay ordered only while
they map to the same partition (see gotcha #5). The burn-in `keyed_ordering.go` worker asserts
per-partition offset-monotonic order.

## Examples

| Variant | Family | What it shows |
|---------|--------|---------------|
| `produce/basic-acks` | produce | Produce at `acks` 0 / 1 / all; oversized → `MESSAGE_TOO_LARGE` |
| `produce/idempotent` | produce | `InitProducerId` + `enable.idempotence`; retries do not duplicate (per-`(PID, partition)` dedup) |
| `produce/compression-and-keys` | produce | `none`/`gzip`/`snappy`/`lz4`/`zstd` + keyed partitioning; **calls out gotcha #4** |

## Error quick reference

| Trigger | Kafka error |
|---------|-------------|
| Record batch > `MaxMessageBytes` | `MESSAGE_TOO_LARGE` |
| `acks=0` to a follower (multi-node) | silent drop (no error surfaced) |
| Produce to a topic name containing `~` | `INVALID_TOPIC_EXCEPTION(17)` |

## See Also

- [../concepts/topics-partitions-offsets.md](../concepts/topics-partitions-offsets.md) — offsets, partitions, the increase-only model.
- [../concepts/cross-client-partitioning.md](../concepts/cross-client-partitioning.md) — murmur2 vs CRC32 and the N-reshard caveat.
- [consuming-and-groups.md](consuming-and-groups.md) — the consume side.
- [transactions-eos.md](transactions-eos.md) — transactional (cross-session exactly-once) produce.

## Source Code

`connectors/kafka/` produce path (`produce_test.go`, `produce_integration_test.go`,
`produce_wire_test.go`, `produce_fixtures_test.go`), idempotent producer (`initproducerid_test.go`,
`pidblock_test.go`), multi-partition placement (`multipartition_integration_test.go`).
