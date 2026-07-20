# Go — Kafka: Consumer-Groups Join / Rebalance

Two consumers join one group, share the 4 partitions, then one leaves and triggers
a rebalance — the example asserts every record is delivered exactly once across the
group with **no loss** through the rebalance.

## Prerequisites

- Go 1.24+ and `github.com/twmb/franz-go v1.21.4` (pinned in `../../go.mod`).
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). **The connector is DISABLED by default — start the
  broker with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1). For any non-same-host
  client, also set `CONNECTORS_KAFKA_ADVERTISED_HOST` or the client connects then
  hangs (gotcha #2).

## How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
go run ./consumer-groups/join-rebalance
```

## Expected Output

```
[kubemq-kafka] consumer-groups/join-rebalance | bootstrap=localhost:9092 partitioner=murmur2(franz-go)
CreateTopic: kafka-ex-cg-rebal-<8hex> (partitions=4) group=kafka-ex-cg-grp-<8hex>
Produce: 40 records across 4 partitions
Both members active: delivered <n>/40 so far
Member A left the group (LeaveGroup) -> rebalance
Delivered all 40 records across the group (rebalance lost none)
DeleteTopic: ok
PASS: join/rebalance with no record loss verified
```

> The topic and group are suffixed with random hex so concurrent runs across the
> language examples never collide on the same group id.

## What's Happening

The program produces 40 keyed records across a 4-partition topic, then starts two
consumers (`member A`, `member B`) in the **same group**. Each sends `JoinGroup` +
`SyncGroup` and is assigned a subset of the 4 partitions; heartbeats keep the group
alive. After both have made progress, `member A` calls `LeaveGroup`, which triggers
a rebalance that reassigns A's partitions to B. The program tracks the union of
delivered record values and asserts all 40 distinct records arrive across the group
— a record lost or never re-delivered across the rebalance fails the process.

The wire flow is `FindCoordinator → JoinGroup → SyncGroup → Heartbeat → Fetch →
OffsetCommit → LeaveGroup → (rebalance) → JoinGroup/SyncGroup`, mirroring connector
behavior in `connectors/kafka/groupoffsets_test.go`.

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| FindCoordinator(10), JoinGroup(11), SyncGroup(14), Heartbeat(12), LeaveGroup(13), OffsetCommit(8), Fetch(1) | acks=all; read_uncommitted | 4 partitions | one group, 2 members | committed offset per partition; resumes after reassignment | none | murmur2 (franz-go) | **gotcha #4** — keys hash to partitions via murmur2 (CRC32 on librdkafka); rebalance redistributes partitions with no record loss |

## Related Examples

- Same variant in other languages:
  `../../../python/consumer-groups/join_rebalance`,
  `../../../javascript/consumer-groups/join-rebalance`,
  `../../../java/consumer-groups/join-rebalance`,
  `../../../csharp/consumer-groups/join-rebalance`,
  `../../../ruby/consumer-groups/join_rebalance`,
  `../../../rust/consumer-groups/join-rebalance`.
- Docs: `../../../../docs/concepts/consumer-groups.md`.
- Related: [`../commit-and-lag`](../commit-and-lag).

> **Gotcha #4 — partitioner divergence affects group assignment.** Which partition
> a keyed record lands on (and therefore which member handles it) depends on the
> producer's partitioner: franz-go/Java/kafkajs use murmur2, librdkafka clients use
> CRC32. A mixed-client topic can distribute the same key to different members
> depending on who produced it. Pin the partitioner when producers span client
> families.

> Auth: this example uses the no-auth default posture. Runs with no SASL by default
> on a stock dev broker; for SASL/PLAIN + SCRAM (and mTLS principal derivation) see
> [`../../security/sasl-plain-scram`](../../security/sasl-plain-scram) +
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
