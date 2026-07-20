# Go — Kafka: Admin Topics Lifecycle

The full admin topic lifecycle against the KubeMQ Kafka connector via kadm:
`CreateTopics → ListTopics → DescribeConfigs → DescribeCluster → DeleteTopics`,
plus the reserved-`~` rejection (gotcha #6).

## Prerequisites

- Go 1.24+ and `github.com/twmb/franz-go v1.21.4` (kadm + kgo, pinned in
  `../../go.mod`).
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). **The connector is DISABLED by default — start the
  broker with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1). For any non-same-host
  client, also set `CONNECTORS_KAFKA_ADVERTISED_HOST` or the client connects then
  hangs (gotcha #2).

## How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
go run ./admin/topics-lifecycle
```

## Expected Output

```
[kubemq-kafka] admin/topics-lifecycle | bootstrap=localhost:9092 partitioner=murmur2(franz-go)
CreateTopic: kafka-ex-admin-life-<8hex> (partitions=2)
ListTopics: kafka-ex-admin-life-<8hex> present (2 partitions)
DescribeTopicConfigs: retention.ms=<value>
DescribeCluster: clusterID="<id>" brokers=<n>
DeleteTopics: kafka-ex-admin-life-<8hex> removed
CreateTopic("kafka-ex-admin-bad~name"): rejected with INVALID_TOPIC_EXCEPTION (expected)
PASS: topic lifecycle + reserved-char rejection verified
```

> The topic is suffixed with 8 random hex chars so concurrent runs of the other
> language examples against the same connector do not collide.

## What's Happening

Using a kadm admin client, the program creates a 2-partition topic and asserts it
appears in `ListTopics` with 2 partitions. It reads the topic config via
`DescribeConfigs` (checking `retention.ms` is returned) and the cluster metadata
via `DescribeCluster` (asserting at least one broker). It then deletes the topic
and asserts it is gone. Finally it attempts to create a topic whose name contains
`~` and asserts the broker rejects it with `INVALID_TOPIC_EXCEPTION` — `~` is
reserved as the KubeMQ partition separator (gotcha #6). Any unexpected result
fails the process.

The wire flow is `CreateTopics → Metadata → DescribeConfigs → DescribeCluster →
DeleteTopics → CreateTopics(reject)`, mirroring connector behavior in
`connectors/kafka/topicadmin_test.go`.

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| CreateTopics(19), DeleteTopics(20), Metadata(3), DescribeConfigs(32), DescribeCluster(60) | n/a (admin only) | 2 partitions | none | n/a | none | n/a | **gotcha #6** — `~` reserved in topic names (M8+) → `INVALID_TOPIC_EXCEPTION(17)`; create/describe/delete all asserted |

## Related Examples

- Same variant in other languages: `../../../python/admin/topics_lifecycle`,
  `../../../javascript/admin/topics-lifecycle`,
  `../../../java/admin/topics-lifecycle`,
  `../../../csharp/admin/topics-lifecycle`,
  `../../../ruby/admin/topics_lifecycle`,
  `../../../rust/admin/topics-lifecycle`.
- Docs: `../../../../docs/guides/admin-and-topics.md`.
- Related: [`../partitions-and-configs`](../partitions-and-configs).

> **Gotcha #6 — `~` is reserved in topic names.** The connector maps partition
> `p>0` to the channel `kafka.<topic>~<p>`, so `~` cannot appear in a topic name;
> a create with `~` returns `INVALID_TOPIC_EXCEPTION(17)`. Likewise `/` is rejected
> in `transactional.id` (gotcha #7). Use `-`/`.` as separators in example topics.

> Auth: this example uses the no-auth default posture. Runs with no SASL by default
> on a stock dev broker; for SASL/PLAIN + SCRAM (and mTLS principal derivation) see
> [`../../security/sasl-plain-scram`](../../security/sasl-plain-scram) +
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
