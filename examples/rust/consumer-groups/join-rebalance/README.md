# Rust — Kafka: Consumer group join & rebalance

Two consumers in one group split a 2-partition topic; together they consume every record — no loss
across the rebalance.

## 1. Prerequisites

- Rust 1.75+ + Cargo; `rdkafka` 0.37 (librdkafka) on `tokio`, via the workspace.
- A running KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`).
- **`CONNECTORS_KAFKA_ENABLE=true`** on the broker (gotcha #1).

## 2. How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
cargo run -p join-rebalance
```

## 3. Expected Output

```text
[kubemq-kafka] consumer-groups/join-rebalance bootstrap=localhost:9092 (no-auth; connector must be enabled: CONNECTORS_KAFKA_ENABLE=true)
topic 'kafka-ex-cg-rebalance' ready with 2 partitions
produced 12 records
[A] consumed <n> records, assigned 1 partition(s)
[B] consumed <n> records, assigned 1 partition(s)
join-rebalance OK: 12/12 records covered, partitions split 1+1
```

## 4. What's Happening

A 2-partition topic is created and seeded with 12 records. Two consumers join the same group
concurrently (FindCoordinator → JoinGroup → SyncGroup → Heartbeat); the leader assigns one partition to
each member. The union of records the two members deliver covers all 12 — the rebalance loses nothing.

## 5. Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| JoinGroup(11), SyncGroup(14), Heartbeat(12) | acks=all, read_uncommitted | kafka-ex-cg-rebalance / 2 | shared group, 2 members | earliest, manual commit off | none | librdkafka CRC32 | one partition per member |

## 6. Related Examples

`../../../{go,java,javascript,csharp}/consumer-groups/join-rebalance`,
`../../../{python,ruby}/consumer-groups/join_rebalance`. Guide: `../../../../docs/guides/consuming-and-groups.md`.

## 7. Gotcha callout

Rebalance splits partitions across members — with a **single-partition** topic only one member ever holds
the partition and the other idles. This example creates a 2-partition topic so both members get work.
Because this client is librdkafka (CRC32), keyed records land on different partitions than a
murmur2 client (gotcha #4).

## 8. Auth

Auth is off by default. See `../../../../docs/guides/security-sasl-tls.md`.
