# Admin & Topics

This guide covers the admin surface: topic lifecycle (create / delete / describe), cluster
description, partition growth, config alteration, and record deletion. It carefully separates the
✅ **Full** admin operations from the 🟡 **Partial** ones — never claim a 🟡 op is Full. See
[../reference/capabilities.md](../reference/capabilities.md).

## Topic lifecycle (✅ Full)

The ✅ Full topic-admin surface: `CreateTopics` / `DeleteTopics` / `DescribeConfigs` /
`DescribeCluster` (API keys 19 / 20 / 32 / 60). Topics are also **auto-created** on `Metadata` /
`Produce` (auth-gated), so you do not have to `CreateTopics` first — but the admin path lets you
create a topic explicitly with a chosen partition count.

- **`CreateTopics`** — create a topic with a partition count (starts at 1 or higher, ≤ 256).
- **`DeleteTopics`** — remove a topic and its channels.
- **`DescribeConfigs`** — read topic/broker configs.
- **`DescribeCluster`** — describe the (single-endpoint) cluster. The connector advertises **one**
  broker at `AdvertisedHost:AdvertisedPort` — there is no Kafka multi-listener model.
- Topic identity is by UUID (KIP-516, `Metadata` v13); `Fetch` is name-based on v12.

> **Gotcha #6 — `~` is reserved in topic names.** `~<p>` is the partition-channel delimiter, so a
> topic name containing `~` collides with the internal partition namespace and is rejected with
> `INVALID_TOPIC_EXCEPTION(17)`. Avoid `~` (and `/`) in topic names. See
> [../reference/migration-from-kafka.md](../reference/migration-from-kafka.md).

The `admin/topics-lifecycle` example creates, describes, and deletes a topic, and proves the `~`
rejection.

## Partitions: increase-only (🟡 CreatePartitions)

`CreatePartitions` (API key 37) is **increase-only**:

- the new count must be **strictly greater** than the current count and **≤ 256** (the hard cap);
- a same-count, decrease, or `>256` request → `INVALID_PARTITIONS`.

Partition `0` lives on `kafka.<topic>`; each `p>0` gets `kafka.<topic>~<p>`. Growing the count makes
new partitions assignable and triggers a consumer-group rebalance.

> **Gotcha #5 — growing `N` re-shards keys.** Keyed placement is `hash(key) % N`, so increasing `N`
> moves existing keys to different partitions; per-key ordering holds only **within a fixed-`N`
> epoch**. See [../concepts/cross-client-partitioning.md](../concepts/cross-client-partitioning.md).

> **Gotcha #10 — cross-node stale-low partition count.** Right after an increase, a `Metadata` read
> from a Raft-lagging node may report the **old** count; it self-heals on refresh. See
> [../concepts/topics-partitions-offsets.md](../concepts/topics-partitions-offsets.md).

The `admin/partitions-and-configs` example proves an increase succeeds and that same-count /
decrease / `>256` → `INVALID_PARTITIONS`.

## Config alteration (🟡 IncrementalAlterConfigs)

`IncrementalAlterConfigs` (API key 44) is **partial**: a subset of configs is recognized and
applied; many others are **accepted-but-no-op** (acknowledged so clients proceed, but not acted
on). Do not assume every config you set takes effect — only the recognized subset does. The
retention configs that **do** map are `retention.ms` → channel `MaxAge` and `retention.bytes` →
`MaxBytes` (plus a record-count budget → `MaxMsgs`). See
[../concepts/topics-partitions-offsets.md](../concepts/topics-partitions-offsets.md).

`DescribeTopicPartitions` (API key 75) is 🟡 — it **falls back to `Metadata`**.

## Record deletion (🟡 DeleteRecords)

`DeleteRecords` (API key 21) is **partial**: it performs **low-end log truncation only** — it
advances the log-start offset (trimming the earliest records), it does **not** delete arbitrary
records mid-log. After a `DeleteRecords`, `earliest` tracks the new log-start. This is the same
shape as retention-driven trimming, just explicitly requested.

## ACL management (🟡) & quotas (🟡)

- **ACL management** (API keys 29 / 30 / 31) — enforcement is ✅ **Full** (Kafka ACLs → Casbin), but
  **management** is partial: describe returns an honest empty view or `SECURITY_DISABLED` rather than
  a full ACL store. Manage authorization through the KubeMQ server's Casbin policy, not the Kafka
  ACL-management RPCs. See [security-sasl-tls.md](security-sasl-tls.md).
- **Quotas** (API keys 48 / 49) — a per-principal produce + fetch **token-bucket baseline**
  (`{Produce,Fetch}ByteRate`, `0` = unlimited). See [../configuration.md](../configuration.md).

## What is not here

- **✅ Log compaction** (`cleanup.policy=compact`) is **GA on the `next` engine** — the only engine
  Kafka runs on — so the compaction-dependent ecosystem, **Kafka Streams** and **Kafka Connect**, is
  **supported**. The genuine non-goals are the Schema-Registry **service** and ksqlDB
  (Schema-Registry **wire** interop, the 5-byte magic prefix, still works), MirrorMaker 2, Confluent
  Control Center, and Cruise Control.
- **🔴 Transaction-admin RPCs** (`WriteTxnMarkers`(27) / `DescribeProducers`(61) /
  `DescribeTransactions`(65) / `ListTransactions`(66)) are not-yet — no CLI `--abort` for a wedged
  transaction (bounded instead by the `transaction.timeout.ms` reaper). See
  [transactions-eos.md](transactions-eos.md).

See [../reference/capabilities.md](../reference/capabilities.md) for the full ✅ / 🟡 / ⛔ / 🔴 tables.

## Examples

| Variant | Family | What it shows |
|---------|--------|---------------|
| `admin/topics-lifecycle` | admin | `CreateTopics` / `DeleteTopics` / `DescribeConfigs` / `DescribeCluster`; `~` rejected (gotcha #6) |
| `admin/partitions-and-configs` | admin | `CreatePartitions` increase (≤ 256); `IncrementalAlterConfigs` (🟡); `DeleteRecords` (🟡); bad increase → `INVALID_PARTITIONS` |
| `offsets/list-and-retention` | offsets | `ListOffsets` earliest / latest / by-ts; `retention.ms` / `.bytes` mapping honored |

## Error quick reference

| Trigger | Kafka error |
|---------|-------------|
| Topic name contains `~` | `INVALID_TOPIC_EXCEPTION(17)` |
| `CreatePartitions` same-count / decrease / `>256` | `INVALID_PARTITIONS` |
| Admin op without authorization | `TOPIC_AUTHORIZATION_FAILED` |
| ACL-management describe with security disabled | `SECURITY_DISABLED` (or an empty view) |

## See Also

- [../concepts/topics-partitions-offsets.md](../concepts/topics-partitions-offsets.md) — the increase-only model, retention, stale-low reads.
- [../reference/capabilities.md](../reference/capabilities.md) — the honest ✅ / 🟡 / ⛔ / 🔴 scope.
- [../configuration.md](../configuration.md) — capacity limits and quota knobs.

## Source Code

`connectors/kafka/` topic admin (`topic_test.go`, `topicadmin_test.go`, `topicconfig_test.go`,
`admin_nullable_fields_test.go`), partitions (`createpartitions_rebalance_test.go`,
`createpartitions_rebalance_integration_test.go`), cluster admin (`clusteradmin_test.go`), metadata
(`metadata_test.go`), quotas (`clientquotas_test.go`, `quota_test.go`, `quota_hook_test.go`).
