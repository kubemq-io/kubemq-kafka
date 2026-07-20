# javascript — Kafka: Topic Lifecycle (Admin)

Drive the admin API — CreateTopics, Metadata, DescribeConfigs, DescribeCluster, DeleteTopics — and
prove a topic name containing the reserved `~` is rejected with `INVALID_TOPIC_EXCEPTION` (gotcha #6).
The Kafka topic `kafka-ex-admin-topics` maps onto the Events-Store channel `kafka.kafka-ex-admin-topics`.

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
npx tsx admin/topics-lifecycle/index.ts
```

## Expected Output

```
Connecting to KubeMQ Kafka connector at localhost:9092 (topic "kafka-ex-admin-topics")
CreateTopics    -> created=true
Metadata        -> topic "kafka-ex-admin-topics" partitions=2
DescribeConfigs -> 1 config entries (e.g. retention.ms=3600000)
DescribeCluster -> clusterId=kubemq brokers=1 controller=0
CreateTopics("kafka-ex-admin~bad") -> rejected: INVALID_TOPIC_EXCEPTION
DeleteTopics    -> deleted "kafka-ex-admin-topics"

Topic lifecycle proven: create/describe-configs/describe-cluster/delete OK; ~ name rejected
```

> The `clusterId`, `controller`, and config-entry count reflect the connector; the exact values can
> differ by deployment. The `~` rejection is the load-bearing assertion.

## What's Happening

- `admin.createTopics` registers a 2-partition topic with a `retention.ms` config entry.
- `admin.fetchTopicMetadata` (Metadata, key 3) confirms the partition count.
- `admin.describeConfigs` (key 32) returns the topic config entries; `admin.describeCluster` (key 60)
  returns the cluster id, broker list, and controller.
- Creating `kafka-ex-admin~bad` fails: `~` is reserved by the channel mapping →
  `INVALID_TOPIC_EXCEPTION` (error code 17).
- `admin.deleteTopics` (key 20) removes the topic.
- Mirrors connector behavior in `connectors/kafka/` (admin metadata/config path; see
  `connectors/kafka/admin_test.go`).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| CreateTopics (19), Metadata (3), DescribeConfigs (32), DescribeCluster (60), DeleteTopics (20) | n/a (admin) | `kafka-ex-admin-topics` / 2 partitions | none | n/a | none | n/a | `~` in a topic name → `INVALID_TOPIC_EXCEPTION(17)` (gotcha #6) |

## Related Examples

- Same variant, other languages: [`../../../go/admin/topics-lifecycle`](../../../go/admin/topics-lifecycle),
  [`../../../java/admin/topics-lifecycle`](../../../java/admin/topics-lifecycle),
  [`../../../csharp/admin/topics-lifecycle`](../../../csharp/admin/topics-lifecycle),
  [`../../../rust/admin/topics-lifecycle`](../../../rust/admin/topics-lifecycle),
  [`../../../python/admin/topics_lifecycle`](../../../python/admin/topics_lifecycle),
  [`../../../ruby/admin/topics_lifecycle`](../../../ruby/admin/topics_lifecycle).
- Doc: [`../../../../docs/guides/admin-and-topics.md`](../../../../docs/guides/admin-and-topics.md),
  [`../../../../docs/reference/migration-from-kafka.md`](../../../../docs/reference/migration-from-kafka.md).
- Next: [`../partitions-and-configs/`](../partitions-and-configs/).

> **Gotcha #6 — `~` is reserved in topic names.** The channel mapping reserves `~`, so any topic name
> containing it is rejected with `INVALID_TOPIC_EXCEPTION(17)`. Avoid `~` (and `/` in `transactional.id`).

> **Auth.** The dev default is no SASL over plain TCP (`:9092`). For a secured connector, configure
> SASL/PLAIN or SASL/SCRAM (and TLS on `:9093`). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
