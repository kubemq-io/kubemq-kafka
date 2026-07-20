# Topics, Partitions & Offsets

## Concept

A **Kafka topic** is an append-only log of records; a **partition** is one ordered shard of that
log; and an **offset** is a record's position within a partition. In the KubeMQ Kafka connector all
three map directly onto the **Events Store** primitive:

| Kafka concept | KubeMQ mapping |
|---------------|----------------|
| Topic `orders` | Events-Store channel `kafka.orders` (`channelPrefix = "kafka."`) |
| Partition `0` of `orders` | Channel `kafka.orders` |
| Partition `p>0` of `orders` | Internal channel `kafka.orders~<p>` |
| Offset of a record | The record's STAN `Sequence` on its channel |

This is the mapping every other page in these docs relies on. See
[architecture.md](../architecture.md) and [reference/channel-mapping.md](../reference/channel-mapping.md).

## Offset = STAN Sequence

A Kafka offset is **not** a separate index the connector maintains — it **is** the STAN `Sequence`
of the record on its Events-Store channel. That has three consequences worth internalizing:

- **Durable & restart-stable.** Offsets survive a broker restart because the sequence is part of
  the durable store, not in-memory bookkeeping.
- **Raft-replicated & node-identical.** The same record has the same offset on every cluster node —
  there is no per-node offset divergence.
- **Exact, not approximate.** `earliest` tracks the true log-start sequence; `latest` is the true
  high-water mark. `ListOffsets` (API key 2) answers earliest / latest / by-timestamp against these
  exact sequences.

## Partitions: the increase-only model

A topic starts with partition count **1**. Partition `0` lives on `kafka.<topic>`; each additional
partition `p>0` gets its own channel `kafka.<topic>~<p>`, and each partition is an **independent
ordered offset space** (offsets are monotonic per partition, not across the topic).

Partition count is **increase-only**:

- Grow it with `CreatePartitions` (API key 37) — the new count must be **strictly greater** than
  the current count and **≤ 256** (the hard cap).
- A same-count, decrease, or `>256` request is rejected with `INVALID_PARTITIONS`.

See [guides/admin-and-topics.md](../guides/admin-and-topics.md).

> **Gotcha #6 — `~` is reserved in topic names.** Because `~<p>` is the partition-channel
> delimiter, a topic name containing `~` collides with the internal partition namespace and is
> rejected with `INVALID_TOPIC_EXCEPTION(17)`. Avoid `~` (and `/`) in topic names. The example
> topic-naming convention is `kafka-ex-<family>-<short>` (charset-safe). See
> [reference/migration-from-kafka.md](../reference/migration-from-kafka.md).

> **Gotcha #5 — growing `N` re-shards keys.** Keyed records are assigned to a partition by
> `hash(key) % N`. Increasing `N` changes that mapping, so a given key can land on a **different**
> partition after the growth. Per-key ordering is therefore guaranteed only **within a fixed-`N`
> epoch**, not across a `CreatePartitions` boundary. See
> [cross-client-partitioning.md](cross-client-partitioning.md).

## Cross-node partition-count reads

Partition count is Raft-replicated broker state. During a brief window of Raft replication lag, a
`Metadata` request served by a node that has not yet applied a recent `CreatePartitions` can report
a **stale-low** partition count.

> **Gotcha #10 — cross-node stale-low partition count.** Immediately after a partition increase, a
> `Metadata` read from a lagging node may show the **old** (lower) count. This **self-heals** once
> the node applies the Raft entry — a subsequent metadata refresh returns the correct count. It is
> a read-consistency window, not lost data; producers and consumers converge on the new count on
> refresh. See [reference/migration-from-kafka.md](../reference/migration-from-kafka.md).

## Topic identity by UUID

Topics carry a stable UUID (KIP-516). `Metadata` v13 and later identify topics by UUID; `Fetch`
falls back to name-based identity on v12. Topics are **auto-created** on `Metadata` / `Produce`
(auth-gated) — you do not have to `CreateTopics` first, though the admin surface lets you create
them explicitly with a chosen partition count. See [guides/admin-and-topics.md](../guides/admin-and-topics.md).

## Retention

Kafka retention maps onto the Events-Store channel's limits:

| Kafka topic config | KubeMQ channel limit |
|--------------------|----------------------|
| `retention.ms` | `MaxAge` |
| `retention.bytes` | `MaxBytes` |
| (record count budget) | `MaxMsgs` |

Retention trims the low end of the log; `earliest` then tracks the new log-start offset. See
[guides/admin-and-topics.md](../guides/admin-and-topics.md) and the `offsets/list-and-retention`
example.

## Examples

| Variant | Family | What it shows |
|---------|--------|---------------|
| `consume/seek-offsets-timestamps` | consume / offsets | `seek(offset)` and `ListOffsets` by-timestamp landing on the expected records |
| `offsets/list-and-retention` | offsets | `earliest` tracks log-start; retention (`retention.ms` / `.bytes`) honored |
| `admin/partitions-and-configs` | admin | `CreatePartitions` increase succeeds; same-count / decrease / `>256` → `INVALID_PARTITIONS` |

## See Also

- [../architecture.md](../architecture.md) — the wire-shim model and channel mapping.
- [consumer-groups.md](consumer-groups.md) — committed offsets and lag over these partitions.
- [cross-client-partitioning.md](cross-client-partitioning.md) — how keys pick a partition (murmur2 vs CRC32) and the N-reshard caveat.
- [../reference/channel-mapping.md](../reference/channel-mapping.md) — the exact topic / partition / offset grammar.

## Grounding

`listoffsets_test.go`, `topic_test.go`, `multipartition_integration_test.go`,
`createpartitions_rebalance_test.go`, `offset_durability_integration_test.go` in
`connectors/kafka/`.
