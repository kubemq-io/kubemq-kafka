# javascript — Kafka: Consumer Group Join and Rebalance

Join two consumers to the **same** group on a 2-partition topic, observe the coordinator redistribute
partitions across the members (Join/Sync/Heartbeat/Leave), and prove every record is consumed exactly
once across the rebalance — no loss, no duplication. The Kafka topic `kafka-ex-cg-rebalance` maps onto
the Events-Store channel `kafka.kafka-ex-cg-rebalance`.

## Prerequisites

- Node.js 18+ and `npm install` in `examples/javascript/` (pins `kafkajs` `^2.2.4` — v2+, murmur2
  `DefaultPartitioner`).
- A running KubeMQ server with the Kafka connector **enabled** (`CONNECTORS_KAFKA_ENABLE=true` — the
  connector is **disabled by default**, gotcha #1), reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). For external clients, set `CONNECTORS_KAFKA_ADVERTISED_HOST` (gotcha #2).

## How to Run

```bash
cd examples/javascript
npm install
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
npx tsx consumer-groups/join-rebalance/index.ts
```

## Expected Output

```
Connecting to KubeMQ Kafka connector at localhost:9092 (topic "kafka-ex-cg-rebalance", 2 partitions, group "kafka-ex-cg-rebalance-grp")
m1 GROUP_JOIN -> assigned partitions [0, 1]
m2 GROUP_JOIN -> assigned partitions [1]
m1 GROUP_JOIN -> assigned partitions [0]
Produced 10 records across 2 partitions
m1 consumed 5, m2 consumed 5, total 10

Rebalance proven: 2 members split the partitions and consumed every record exactly once
```

> The exact per-member split (which partition each ends up owning, and the m1/m2 counts) can vary; the
> invariant asserted is that both members get an assignment and all 10 records are consumed exactly once.

## What's Happening

- `m1` joins first and is assigned both partitions (`[0, 1]`).
- When `m2` joins the same group, the coordinator triggers a rebalance (Join key 11 / Sync key 14 /
  Heartbeat key 12 / Leave key 13); each member ends up owning one partition.
- The producer writes 10 keyed records spread across both partitions; the members consume them in
  parallel.
- The program asserts both members received an assignment and the union of consumed records equals the
  full set exactly once (no loss, no dup).
- Mirrors connector behavior in `connectors/kafka/` (group membership + assignment; see
  `connectors/kafka/group_test.go`).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| JoinGroup (11), SyncGroup (14), Heartbeat (12), LeaveGroup (13), Produce (0), Fetch (1) | read_uncommitted | `kafka-ex-cg-rebalance` / 2 partitions | one shared group, two members | per-partition offsets | none | murmur2 (DefaultPartitioner) | partitions redistribute on the second join; no records lost across the rebalance |

## Related Examples

- Same variant, other languages: [`../../../go/consumer-groups/join-rebalance`](../../../go/consumer-groups/join-rebalance),
  [`../../../java/consumer-groups/join-rebalance`](../../../java/consumer-groups/join-rebalance),
  [`../../../csharp/consumer-groups/join-rebalance`](../../../csharp/consumer-groups/join-rebalance),
  [`../../../rust/consumer-groups/join-rebalance`](../../../rust/consumer-groups/join-rebalance),
  [`../../../python/consumer-groups/join_rebalance`](../../../python/consumer-groups/join_rebalance),
  [`../../../ruby/consumer-groups/join_rebalance`](../../../ruby/consumer-groups/join_rebalance).
- Doc: [`../../../../docs/concepts/consumer-groups.md`](../../../../docs/concepts/consumer-groups.md),
  [`../../../../docs/guides/consuming-and-groups.md`](../../../../docs/guides/consuming-and-groups.md).
- Next: [`../commit-and-lag/`](../commit-and-lag/).

> **Gotcha — a rebalance can briefly reassign or replay.** During the join, kafkajs revokes and
> reassigns partitions; a self-asserting test must let the rebalance settle before producing, and
> dedupe by value to prove exactly-once delivery across the group.

> **Auth.** The dev default is no SASL over plain TCP (`:9092`). For a secured connector, configure
> SASL/PLAIN or SASL/SCRAM (and TLS on `:9093`). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
