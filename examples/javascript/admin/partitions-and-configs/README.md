# javascript — Kafka: Partitions and Configs (Admin)

Exercise the 🟡 partial-support admin paths: `createPartitions` (increase-only, ≤256),
`alterConfigs` (IncrementalAlterConfigs, recognized subset), and `deleteTopicRecords` (DeleteRecords,
low-end truncation). A decrease request is rejected with `INVALID_PARTITIONS`. The Kafka topic
`kafka-ex-admin-parts` maps onto the Events-Store channel `kafka.kafka-ex-admin-parts`.

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
npx tsx admin/partitions-and-configs/index.ts
```

## Expected Output

```
Connecting to KubeMQ Kafka connector at localhost:9092 (topic "kafka-ex-admin-parts")
CreatePartitions 2 -> 4: now 4 partitions
CreatePartitions 4 -> 2 (decrease): rejected INVALID_PARTITIONS
alterConfigs retention.ms -> 7200000
deleteTopicRecords(offset=3) -> partition 0 log-start (low) offset now 3

Partitions + configs proven: increase-only enforced, subset config applied, low-end truncation done
```

## What's Happening

- `admin.createPartitions` grows the topic from 2 → 4 partitions (increase-only, capped at 256).
- A decrease request (4 → 2) is rejected with `INVALID_PARTITIONS` — the connector never shrinks a topic.
- `admin.alterConfigs` (IncrementalAlterConfigs, 🟡 partial) applies a recognized subset; here
  `retention.ms=7200000` is accepted and read back via `describeConfigs`.
- `admin.deleteTopicRecords` (DeleteRecords, 🟡 partial) truncates the low end: after deleting below
  offset 3, the partition-0 log-start (`low`) offset advances to 3.
- Mirrors connector behavior in `connectors/kafka/` (partitions/configs/deleterecords; see
  `connectors/kafka/admin_test.go`).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| CreatePartitions (37), IncrementalAlterConfigs (44), DeleteRecords (21), DescribeConfigs (32), Produce (0), ListOffsets (2) | acks default | `kafka-ex-admin-parts` / 2 → 4 partitions | none | low (log-start) advances after truncation | none | murmur2 (DefaultPartitioner) | increase-only ≤256; decrease/same/>256 → `INVALID_PARTITIONS`; growing N re-shards keys (gotcha #5) |

## Related Examples

- Same variant, other languages: [`../../../go/admin/partitions-and-configs`](../../../go/admin/partitions-and-configs),
  [`../../../java/admin/partitions-and-configs`](../../../java/admin/partitions-and-configs),
  [`../../../csharp/admin/partitions-and-configs`](../../../csharp/admin/partitions-and-configs),
  [`../../../rust/admin/partitions-and-configs`](../../../rust/admin/partitions-and-configs),
  [`../../../python/admin/partitions_and_configs`](../../../python/admin/partitions_and_configs),
  [`../../../ruby/admin/partitions_and_configs`](../../../ruby/admin/partitions_and_configs).
- Doc: [`../../../../docs/guides/admin-and-topics.md`](../../../../docs/guides/admin-and-topics.md),
  [`../../../../docs/reference/capabilities.md`](../../../../docs/reference/capabilities.md).
- Next: [`../../offsets/list-and-retention/`](../../offsets/list-and-retention/).

> **Gotcha #5 — growing N re-shards keys.** Increasing the partition count changes the murmur2 key →
> partition mapping, so per-key ordering only holds within a fixed-N epoch. Adding partitions can scatter
> a key's future records onto a different partition than its history.

> **Auth.** The dev default is no SASL over plain TCP (`:9092`). For a secured connector, configure
> SASL/PLAIN or SASL/SCRAM (and TLS on `:9093`). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
