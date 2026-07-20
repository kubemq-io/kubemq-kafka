# Consuming & Consumer Groups

This guide covers the consume surface end to end: fetching from the beginning or latest, the bounded
long-poll, seeking by offset or timestamp, and consumer groups (join, commit, lag). Every consumer
reads a KubeMQ **Events Store** channel `kafka.<topic>` (partition `p>0` → `kafka.<topic>~<p>`), and
offsets are exact STAN sequences. See
[../concepts/topics-partitions-offsets.md](../concepts/topics-partitions-offsets.md).

The ✅ Full consume surface: `Fetch` (API key 1; bounded-read long-poll), `ListOffsets` (key 2;
earliest / latest / by-timestamp), the classic consumer-group protocol (keys 10 / 11 / 14 / 12 / 13),
and `OffsetCommit` / `OffsetFetch` (keys 8 / 9). See
[../reference/capabilities.md](../reference/capabilities.md).

## From beginning vs latest

`auto.offset.reset` selects the start position when a consumer has no committed offset:

| `auto.offset.reset` | Start position |
|---------------------|----------------|
| `earliest` | The log-start offset (`ListOffsets` earliest) — reads all retained records. |
| `latest` | The high-water mark — reads only records produced after the consumer joins. |

Because offsets are exact STAN sequences, `earliest` tracks the true log-start (which advances as
retention trims the low end) and `latest` is the true high-water mark. The
`consume/from-beginning-latest` example proves both start positions.

## Fetch & the bounded long-poll

`Fetch` (key 1) is a **bounded-read long-poll**: it returns available records immediately, or parks
the request until records arrive or the fetch deadline elapses. A node serves up to **1024** parked
fetch long-polls (the capacity ceiling); beyond that, fetches degrade rather than park. See
[../configuration.md](../configuration.md).

## Seek by offset & timestamp

- **`seek(offset)`** — reposition a partition's consume position to an explicit offset (STAN
  Sequence).
- **`ListOffsets` by-timestamp** — resolve the first offset at or after a timestamp, then consume
  from there.

The `consume/seek-offsets-timestamps` example proves that a seek (by offset and by timestamp) lands
on the expected records.

## Consumer groups

The connector implements the **classic** group protocol. A group cooperatively reads a topic: each
partition is assigned to exactly one member, and the group's committed offsets record progress.

1. `FindCoordinator` → resolve the coordinator.
2. `JoinGroup` → members join; the coordinator elects a leader.
3. `SyncGroup` → the leader's assignment is distributed.
4. `Heartbeat` → keep membership alive; a miss (or `LeaveGroup`) triggers a **rebalance**.

Because partitions are independent channels, a rebalance re-assigns channels among members without
touching committed offsets — a member that picks up a partition resumes from that partition's last
committed offset. The `consumer-groups/join-rebalance` example proves partitions redistribute across
a multi-consumer rebalance with no loss. See
[../concepts/consumer-groups.md](../concepts/consumer-groups.md).

> **KIP-848 next-gen groups, static membership, and share groups (KIP-932) are 🔴 not-yet**
> (deferred). Use the classic protocol. See [../reference/capabilities.md](../reference/capabilities.md).

## Commit & resume

`OffsetCommit` (key 8) records the group's position; `OffsetFetch` (key 9) reads it back. Committed
offsets are **durable** coordinator state (Raft-replicated, restart-stable):

- a committed offset survives a broker restart;
- a consumer that stops and rejoins the same group resumes from the committed offset, not from
  `earliest`/`latest`.

The `consumer-groups/commit-and-lag` example proves resume-from-committed after a restart of the
consumer.

Committing consumed offsets **inside a transaction** (`TxnOffsetCommit`) additionally requires WRITE
on the group (**gotcha #8**, stricter than stock Kafka) — see
[transactions-eos.md](transactions-eos.md).

## Consumer-group lag

Lag is exposed as a Prometheus gauge:

```
kubemq_kafka_consumer_group_lag{group="...", topic="...", partition="..."}
```

Lag is `high-water-mark − committed-offset` per `(group, topic, partition)`. Both values are exact
STAN sequences, so the reported lag is exact (any residual is only in-flight poll skew). The
`consumer-groups/commit-and-lag` example reads the lag metric; the burn-in `offset_commit_lag.go`
worker asserts it within a 1-message tolerance.

## Examples

| Variant | Family | What it shows |
|---------|--------|---------------|
| `consume/from-beginning-latest` | consume | `auto.offset.reset` earliest / latest — both start positions correct |
| `consume/seek-offsets-timestamps` | consume / offsets | `seek(offset)` and `ListOffsets` by-timestamp land on the expected records |
| `consumer-groups/join-rebalance` | consumer-groups | Join / Sync / Heartbeat / Leave; multi-consumer rebalance redistributes partitions, no loss |
| `consumer-groups/commit-and-lag` | consumer-groups | `OffsetCommit` / `OffsetFetch` resume-from-committed; the lag metric |

## Error quick reference

| Trigger | Kafka error |
|---------|-------------|
| Fetch/consume without READ on the topic/group | `TOPIC_AUTHORIZATION_FAILED` / `GROUP_AUTHORIZATION_FAILED` |
| `OffsetFetch RequireStable` while a txn is open | `UNSTABLE_OFFSET_COMMIT(88)` |

## See Also

- [../concepts/consumer-groups.md](../concepts/consumer-groups.md) — the group-protocol concept, lag, rebalance on growth.
- [../concepts/topics-partitions-offsets.md](../concepts/topics-partitions-offsets.md) — offsets, partitions, retention.
- [producing.md](producing.md) — the produce side.
- [transactions-eos.md](transactions-eos.md) — `read_committed` consuming and the LSO.

## Source Code

`connectors/kafka/` fetch path (`fetch_test.go`), list-offsets (`listoffsets_test.go`), group
protocol (`findcoordinator_test.go`, `joingroup_test.go`, `syncgroup_test.go`, `heartbeat_test.go`,
`leavegroup_test.go`, `group_integration_test.go`), offsets + lag (`groupoffsets_test.go`,
`grouplag_test.go`).
