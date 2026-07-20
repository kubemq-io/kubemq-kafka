# Channel Mapping

How Kafka's topic / partition / offset / consumer-group model projects onto KubeMQ's **Events
Store**. This is the mental model every example, doc, and burn-in worker relies on. Grounded in
`docs/24-kafka.md` and `docs/migration/kafka.md`.

## Topic Grammar

Every Kafka topic maps to exactly one KubeMQ **Events Store** channel, under the fixed `kafka.`
prefix:

```
kafka.{topic}
└─┬─┘ └──┬──┘
  │      └─ the Kafka topic name (the same name you pass to the Kafka client)
  └─ fixed connector prefix ("kafka.")
```

| Kafka topic | KubeMQ Events-Store channel |
|---|---|
| `orders` | `kafka.orders` |
| `events` | `kafka.events` |
| `kafka-ex-produce-basic` | `kafka.kafka-ex-produce-basic` |

## Partition Grammar

A single-partition topic lives entirely on `kafka.{topic}`. When a topic has **N > 1** partitions
(M8), partition 0 stays on the base channel and each higher partition gets its own internal channel:

```
kafka.{topic}~{partition}
└──────┬─────┘│└────┬────┘
       │      │     └─ the partition index (p > 0)
       │      └─ fixed "~" partition separator
       └─ the base topic channel
```

| Kafka topic | Partition | KubeMQ channel |
|---|---|---|
| `orders` | `0` | `kafka.orders` |
| `orders` | `1` | `kafka.orders~1` |
| `orders` | `5` | `kafka.orders~5` |

- Each partition is an **independent, ordered offset space**.
- Partition count **starts at 1** and is **increase-only** via `CreatePartitions` (key 37), with a
  hard cap of **256**. Decreasing or re-using a count fails with `INVALID_PARTITIONS`. See
  [../concepts/topics-partitions-offsets.md](../concepts/topics-partitions-offsets.md).
- Growing N **re-shards keys** — per-key order holds only within a fixed-N epoch (gotcha #5).

> **`~` is reserved (gotcha #6).** Because `~` is the partition separator, a Kafka topic name that
> itself contains `~` (once M8 multi-partition is in play) is rejected with
> `INVALID_TOPIC_EXCEPTION(17)`. Example topics use the `kafka-ex-<family>-<short>` convention to
> stay charset-safe; burn-in topics use `burnin.<worker>.<idx:04d>`. Never use `~` or `/` in a topic
> name.

## Offset = STAN Sequence

The Kafka **offset** is the KubeMQ Events-Store **STAN `Sequence`**:

| Kafka concept | KubeMQ mapping |
|---|---|
| Offset | STAN `Sequence` — durable, restart-stable, Raft-replicated, identical across nodes. |
| Earliest offset | Log-start sequence (advances as retention truncates). |
| Latest offset (HWM) | Next sequence to be written. |
| Committed offset | Coordinator-tracked per-`(group, topic, partition)`. |

Because the offset **is** the durable broker sequence, it survives restarts and is identical on
every node — there is no separate offset-translation table.

> **Cross-node stale-low read (gotcha #10).** During Raft replication lag, a follower node may
> briefly report a **stale-low** partition count / offset. This self-heals on the next metadata
> refresh. See [../concepts/topics-partitions-offsets.md](../concepts/topics-partitions-offsets.md).

## Consumer Groups

A Kafka consumer group is a coordinator-tracked entity with **durable per-`(group, topic,
partition)` committed offsets**, using the classic (eager) rebalance protocol:

| Kafka concept | KubeMQ mapping |
|---|---|
| Group membership | Coordinator-tracked (`FindCoordinator`/`JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup`). |
| Committed offset | Durable per-`(group, topic, partition)` (`OffsetCommit`/`OffsetFetch`). |
| Lag | `kubemq_kafka_consumer_group_lag{group,topic,partition}` = HWM − committed. |

See [../concepts/consumer-groups.md](../concepts/consumer-groups.md).

## Retention Mapping

Kafka retention config projects onto the Events-Store channel limits:

| Kafka config | KubeMQ channel limit |
|---|---|
| `retention.ms` | `MaxAge` |
| `retention.bytes` | `MaxBytes` |
| _(record count)_ | `MaxMsgs` |

## Reserved `kafka.*` Namespace (no cross-protocol access)

The backing store is an Events-Store channel, but that channel namespace is **reserved for the
connector**. A **native** gRPC / REST KubeMQ client (Events, Events-Store, or Queue) that tries to
subscribe to, read, or write a `kafka.*` (or `_KAFKA_*`) channel is rejected with `Error 443:
channel is reserved for internal connector use`. The guard **fails safe** and runs **before**
authorization, so no grant lets a wire client reach these channels. There is **no** shared-channel
interop and **no** native ↔ Kafka bridge: a Kafka topic is reachable only over the Kafka wire
protocol. See [../concepts/interop-with-native.md](../concepts/interop-with-native.md) and
[error-codes.md](error-codes.md).

## Naming Conventions

| Context | Convention | Reason |
|---|---|---|
| Example topics | `kafka-ex-<family>-<short>` | Kafka-charset-safe; avoids `~` and `/`. |
| Burn-in topics | `burnin.<worker>.<idx:04d>` | Dots are the KubeMQ hierarchy separator; never `~` / `/`. |

## See Also

- [../concepts/topics-partitions-offsets.md](../concepts/topics-partitions-offsets.md) — the concept walk-through.
- [capabilities.md](capabilities.md) — the ✅/🟡/⛔/🔴 surface.
- [configuration.md](configuration.md) — `AdvertisedHost` and the single-endpoint model.
- [migration-from-kafka.md](migration-from-kafka.md) — the `~` breaking change (gotcha #6).

## Source

`connectors/kafka/` (channel prefix, `~<partition>` grammar, offset↔Sequence, group offsets).
Verified against server docs `docs/24-kafka.md` (channel mapping) and `docs/migration/kafka.md`.
Canonical tests include `groupoffsets_test.go`, `listoffsets_test.go`.
