# Consumer Groups

## Concept

A **consumer group** lets a set of consumers cooperatively read a topic: each partition is assigned
to exactly one member, and the group's committed offsets record how far it has read. The connector
implements the **classic** Kafka group protocol — `FindCoordinator` / `JoinGroup` / `SyncGroup` /
`Heartbeat` / `LeaveGroup` (API keys 10 / 11 / 14 / 12 / 13) — plus `OffsetCommit` / `OffsetFetch`
(keys 8 / 9). Committed offsets are **durable per-`(group, topic, partition)`** coordinator state,
Raft-replicated and restart-stable.

> **KIP-848 next-generation consumer groups are 🔴 not yet supported** (deferred), and so are
> **static membership** and **share groups (KIP-932)**. Use the classic protocol. See
> [reference/capabilities.md](../reference/capabilities.md).

## Join, assignment & rebalance

1. `FindCoordinator` resolves the group coordinator.
2. `JoinGroup` — members join; the coordinator elects a leader and collects each member's
   subscription.
3. `SyncGroup` — the leader computes the partition assignment; the coordinator distributes it.
4. `Heartbeat` — members keep their membership alive; a missed heartbeat (or `LeaveGroup`) triggers
   a **rebalance** that recomputes the assignment across the remaining members.

Because partitions map to independent channels (`kafka.<topic>` and `kafka.<topic>~<p>`), a
rebalance simply re-assigns those channels among members; committed offsets are unaffected by the
reassignment, so a member that picks up a partition resumes from that partition's last committed
offset — no records are lost or double-delivered across a clean rebalance.

## Commit & fetch durability

`OffsetCommit` records the group's position for a `(topic, partition)`; `OffsetFetch` reads it
back. Both are durable coordinator state:

- A committed offset **survives a broker restart** — the offset is the STAN `Sequence`, and the
  commit is durable, Raft-replicated state.
- A consumer that stops and rejoins the same group **resumes from the committed offset**, not from
  `earliest`/`latest`.
- `OffsetFetch RequireStable` is honored under `read_committed` — see
  [transactions-eos.md](transactions-eos.md).

## Consumer-group lag

The connector exposes lag as a Prometheus gauge:

```
kubemq_kafka_consumer_group_lag{group="...", topic="...", partition="..."}
```

Lag is `high-water-mark − committed-offset` for each `(group, topic, partition)`. Because both
values are exact STAN sequences, the reported lag is exact (any residual is only in-flight poll
skew). The burn-in harness's `offset_commit_lag.go` worker asserts reported lag against the
tracker's true `(HWM − committed)` within a 1-message tolerance.

## Rebalance on partition growth

Partition count is **increase-only** (see
[topics-partitions-offsets.md](topics-partitions-offsets.md)). When a topic grows via
`CreatePartitions`, the new partitions become assignable and the group **rebalances** to distribute
them. Two caveats follow:

- **Per-key ordering only within a fixed-`N` epoch (gotcha #5).** Growing `N` re-shards keys, so a
  key can move to a different partition after the growth. A group that relies on per-key ordering
  must account for the reshard boundary. See [cross-client-partitioning.md](cross-client-partitioning.md).
- **Stale-low count during Raft lag (gotcha #10).** A lagging node may briefly report the old
  partition count on `Metadata`; it self-heals on refresh, and the group converges on the new
  assignment. See [topics-partitions-offsets.md](topics-partitions-offsets.md).

## Capacity

A node serves up to `MaxGroups` consumer groups (default **10000**, `CONNECTORS_KAFKA_MAX_GROUPS`).
See [configuration.md](../configuration.md).

## Examples

| Variant | Family | What it shows |
|---------|--------|---------------|
| `consumer-groups/join-rebalance` | consumer-groups | Join / Sync / Heartbeat / Leave; multi-consumer rebalance redistributes partitions with no loss |
| `consumer-groups/commit-and-lag` | consumer-groups | `OffsetCommit` / `OffsetFetch` resume-from-committed; the consumer-group lag metric |

## See Also

- [topics-partitions-offsets.md](topics-partitions-offsets.md) — the partition / offset model groups read over.
- [../guides/consuming-and-groups.md](../guides/consuming-and-groups.md) — the full consume + group guide.
- [../reference/capabilities.md](../reference/capabilities.md) — classic-protocol scope; KIP-848 / static / share groups are 🔴 not-yet.

## Grounding

`findcoordinator_test.go`, `joingroup_test.go`, `syncgroup_test.go`, `heartbeat_test.go`,
`leavegroup_test.go`, `groupoffsets_test.go`, `grouplag_test.go`, `group_integration_test.go` in
`connectors/kafka/`.
