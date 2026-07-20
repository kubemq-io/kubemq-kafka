# java — Kafka: Consumer Group Join / Rebalance

Two consumers in the same group subscribe to a 2-partition topic. When both join,
the partitions redistribute across them; the example asserts **no record is lost**
across the rebalance.

## Prerequisites

- JDK 21+ and Maven 3.9+.
- `org.apache.kafka:kafka-clients 3.9.0` (pinned in `../../pom.xml`).
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). **Connector DISABLED by default — start with
  `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1); set `CONNECTORS_KAFKA_ADVERTISED_HOST`
  for remote clients (gotcha #2).

## How to Run

From `examples/java/`:

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
mvn -q compile
mvn -q exec:exec -Dexec.mainClass=io.kubemq.examples.kafka.consumergroups.joinrebalance.Main
```

## Expected Output

```
bootstrap.servers = localhost:9092
CreateTopics 'kafka-ex-cg-rebalance-java' (2 partitions)
Produced 20 records across 2 partitions
[A] assigned [kafka-ex-cg-rebalance-java-0]
[B] assigned [kafka-ex-cg-rebalance-java-1]
Collected 20/20 unique values
A held partitions [0] | B held partitions [1]
OK: group rebalanced across 2 members with no record loss
```

The two members split the 2 partitions one-each; between them they collect all 20
records exactly once.

## What's Happening

The program produces 20 records across a 2-partition topic, then starts two
consumers (threads in one JVM) sharing a `group.id`, each with a
`ConsumerRebalanceListener` that prints assigned/revoked partitions. The group
coordinator runs the classic rebalance protocol (JoinGroup → SyncGroup, with
Heartbeats keeping members alive and LeaveGroup on shutdown), assigning each
consumer one partition. Both poll until the group has collectively seen all 20
unique values, asserting no loss and no partition owned by two members at once.

> **Classic protocol only.** Do **not** set `group.protocol=consumer` — the
> connector serves the classic consumer-group protocol; KIP-848 next-gen groups are
> unsupported.

The Kafka wire flow is `FindCoordinator(10) → JoinGroup(11) → SyncGroup(14) →
Heartbeat(12) → Fetch(1) → LeaveGroup(13)`, mirroring connector behavior in
`connectors/kafka/` (`groupoffsets_test.go`).

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| FindCoordinator(10), JoinGroup(11), SyncGroup(14), Heartbeat(12), LeaveGroup(13), Fetch(1) | acks=all; read_uncommitted | 2 partitions | one shared group, 2 members | assignment redistributes; no records lost | none | murmur2 | classic protocol only (no `group.protocol=consumer`, KIP-848 🔴); members are threads in one JVM |

## Related Examples

- Same variant in the other 6 languages: [`../../../go/consumer-groups/join-rebalance`](../../../go/consumer-groups/join-rebalance),
  [`../../../python/consumer-groups/join_rebalance`](../../../python/consumer-groups/join_rebalance),
  [`../../../javascript/consumer-groups/join-rebalance`](../../../javascript/consumer-groups/join-rebalance),
  [`../../../csharp/consumer-groups/join-rebalance`](../../../csharp/consumer-groups/join-rebalance),
  [`../../../ruby/consumer-groups/join_rebalance`](../../../ruby/consumer-groups/join_rebalance),
  [`../../../rust/consumer-groups/join-rebalance`](../../../rust/consumer-groups/join-rebalance).
- Docs: [`../../../../docs/guides/consuming-and-groups.md`](../../../../docs/guides/consuming-and-groups.md),
  [`../../../../docs/concepts/consumer-groups.md`](../../../../docs/concepts/consumer-groups.md).
- Next: [`../commit-and-lag`](../commit-and-lag).

> **Gotcha — classic groups only.** The connector implements the classic
> consumer-group protocol. Never set `group.protocol=consumer` (KIP-848); on a
> `kafka-clients` 4.x bump, confirm the default stays classic.

> **Auth.** This example uses the connector's no-auth default posture
> (SHARED-CONVENTIONS §4.3). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md)
> for SASL/PLAIN + SCRAM and TLS/mTLS.
