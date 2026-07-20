# C# — Kafka: Consumer Group Join & Rebalance

Two consumers in the same group share a multi-partition topic. The coordinator runs
Join/Sync/Heartbeat so partitions redistribute; every record is consumed **exactly
once** across the group even as ownership moves.

## Prerequisites

- .NET SDK **8.0**
- **Confluent.Kafka 2.6.0** (pinned in `examples/csharp/Directory.Packages.props`).
- A running KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` (default
  `localhost:9092`) — **start with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1).

## How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
dotnet run --project consumer-groups/join-rebalance
```

## Expected Output

```
[*] Created topic 'kafka-ex-consumer-groups-join-rebalance' with 4 partitions
[x] produced 40 records across 4 partitions
[*] member-1 assigned: [0, 1, 2, 3]
[v] member-1 got 'msg #0' from partition 0
[*] member-1 revoked: [0, 1, 2, 3]
[*] member-1 assigned: [0, 1]
[*] member-2 assigned: [2, 3]
[v] member-2 got 'msg #5' from partition 2
...
[*] Cleaned up topic 'kafka-ex-consumer-groups-join-rebalance'
[ok] Rebalance across 2 members: all 40 records consumed, no loss
```

## What's Happening

40 keyed records are produced across 4 partitions. `member-1` joins first and owns
all partitions; `member-2` joins ~2 s later, triggering a **rebalance**
(Join → Sync → new assignment) visible through the assigned/revoked handlers.
Both members drain their partitions; a shared collector proves all 40 distinct
records are consumed — **no loss** across the ownership change.

> Partition count must exceed 1 for two members to each own a share; here it is 4.
> Records are keyed so the CRC32 partitioner spreads them across all partitions.

This mirrors the connector's group-coordinator (Join/Sync/Heartbeat/Leave) path in
`connectors/kafka/`.

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|----------|----------------|------------------|----------------|------------------|-------------|-------------|------------------|
| Metadata, Produce, FindCoordinator, JoinGroup, SyncGroup, Heartbeat, Fetch, LeaveGroup | `acks=All` produce | `kafka-ex-consumer-groups-join-rebalance` / 4 | 2 members, same `GroupId` | per-partition ordered; each record consumed exactly once | none | CRC32 (librdkafka) | needs `NumPartitions>=2`; rebalance redistributes partitions; asserts all 40 records, no loss |

## Related Examples

Same variant in the other languages:

- **Go** — [`../../../go/consumer-groups/join-rebalance`](../../../go/consumer-groups/join-rebalance)
- **Python** — [`../../../python/consumer-groups/join_rebalance`](../../../python/consumer-groups/join_rebalance)
- **Java** — [`../../../java/consumer-groups/join-rebalance`](../../../java/consumer-groups/join-rebalance)
- **JS/TS** — [`../../../javascript/consumer-groups/join-rebalance`](../../../javascript/consumer-groups/join-rebalance)
- **Ruby** — [`../../../ruby/consumer-groups/join_rebalance`](../../../ruby/consumer-groups/join_rebalance)
- **Rust** — [`../../../rust/consumer-groups/join-rebalance`](../../../rust/consumer-groups/join-rebalance)

Docs: [`../../../../docs/concepts/consumer-groups.md`](../../../../docs/concepts/consumer-groups.md),
[`../../../../docs/guides/consuming-and-groups.md`](../../../../docs/guides/consuming-and-groups.md)

---

> **Auth:** the connector default is no authentication. SASL/TLS setup lives in
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
