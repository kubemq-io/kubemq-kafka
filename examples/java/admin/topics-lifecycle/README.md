# java — Kafka: Topics Lifecycle

The full AdminClient topic lifecycle — `createTopics → describeTopics →
describeConfigs → describeCluster → deleteTopics` — plus the connector's reserved-
name guard: a topic containing `~` is rejected.

## Prerequisites

- JDK 21+ and Maven 3.9+.
- `org.apache.kafka:kafka-clients 3.9.0` (pinned in `../../pom.xml`), providing the
  `Admin`/`AdminClient` API.
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). **Connector DISABLED by default — start with
  `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1); set `CONNECTORS_KAFKA_ADVERTISED_HOST`
  for remote clients (gotcha #2).

## How to Run

From `examples/java/`:

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
mvn -q compile
mvn -q exec:exec -Dexec.mainClass=io.kubemq.examples.kafka.admin.topicslifecycle.Main
```

## Expected Output

```
bootstrap.servers = localhost:9092
CreateTopics 'kafka-ex-admin-lifecycle-java' (2 partitions)
DescribeTopics 'kafka-ex-admin-lifecycle-java' -> partitions=2
DescribeConfigs -> 25 entries (e.g. retention.ms)
DescribeCluster -> clusterId=kubemq-kafka nodes=1
DeleteTopics 'kafka-ex-admin-lifecycle-java'
CreateTopics 'kafka-ex-admin~bad-java' rejected -> InvalidTopicException
OK: topic lifecycle + invalid-name guard verified
```

(The config entry count, clusterId, and node count depend on the connector build;
the assertions check that create/describe/delete succeed and the `~` name is
rejected.)

## What's Happening

The program creates a 2-partition topic, then reads it back three ways:
`describeTopics` (asserts partition count), `describeConfigs` on the topic
`ConfigResource` (asserts config entries are returned), and `describeCluster`
(asserts a cluster id and at least one node). It deletes the topic, then attempts to
create a topic whose name contains `~` — a character KubeMQ reserves in channel
names — and asserts the connector rejects it with `InvalidTopicException`
(`INVALID_TOPIC_EXCEPTION`, error code 17).

The Kafka wire flow is `CreateTopics(19) → Metadata/DescribeTopics(3) →
DescribeConfigs(32) → DescribeCluster(60) → DeleteTopics(20)`, mirroring connector
behavior in `connectors/kafka/` (admin path).

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| CreateTopics(19), DeleteTopics(20), DescribeConfigs(32), DescribeCluster(60), Metadata(3) | n/a (admin) | 2 partitions | none | n/a | none | n/a | `~` in a topic name → `INVALID_TOPIC_EXCEPTION(17)` (gotcha #6) |

## Related Examples

- Same variant in the other 6 languages: [`../../../go/admin/topics-lifecycle`](../../../go/admin/topics-lifecycle),
  [`../../../python/admin/topics_lifecycle`](../../../python/admin/topics_lifecycle),
  [`../../../javascript/admin/topics-lifecycle`](../../../javascript/admin/topics-lifecycle),
  [`../../../csharp/admin/topics-lifecycle`](../../../csharp/admin/topics-lifecycle),
  [`../../../ruby/admin/topics_lifecycle`](../../../ruby/admin/topics_lifecycle),
  [`../../../rust/admin/topics-lifecycle`](../../../rust/admin/topics-lifecycle).
- Docs: [`../../../../docs/guides/admin-and-topics.md`](../../../../docs/guides/admin-and-topics.md).
- Next: [`../partitions-and-configs`](../partitions-and-configs).

> **Gotcha #6 — `~` is reserved in topic names.** KubeMQ reserves `~` in channel
> names (M8+), so a topic named with `~` is rejected with
> `INVALID_TOPIC_EXCEPTION(17)`. Use dots/hyphens instead. See
> [`../../../../docs/reference/migration-from-kafka.md`](../../../../docs/reference/migration-from-kafka.md).

> **Auth.** This example uses the connector's no-auth default posture
> (SHARED-CONVENTIONS §4.3). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md)
> for SASL/PLAIN + SCRAM and TLS/mTLS.
