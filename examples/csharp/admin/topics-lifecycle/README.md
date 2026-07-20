# C# — Kafka: Topic Lifecycle

Full `AdminClient` lifecycle: `CreateTopics` → `DescribeConfigs` →
`DescribeCluster` (via `GetMetadata`) → `DeleteTopics`, plus the `~`-in-name
rejection (gotcha #6).

## Prerequisites

- .NET SDK **8.0**
- **Confluent.Kafka 2.6.0** (pinned in `examples/csharp/Directory.Packages.props`).
- A running KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` (default
  `localhost:9092`) — **start with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1).

## How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
dotnet run --project admin/topics-lifecycle
```

## Expected Output

```
[*] Created topic 'kafka-ex-admin-topics-lifecycle' (2 partitions, retention.ms=3600000)
[v] [describe] retention.ms = 3600000
[v] [cluster] 1 broker(s); topic present = True
[*] [gotcha #6] 'bad~topic' rejected: TopicException
[*] Deleted topic 'kafka-ex-admin-topics-lifecycle'
[ok] Topic lifecycle verified: create → describe → cluster → delete (+ '~' rejected, gotcha #6)
```

## What's Happening

`CreateTopics` makes a 2-partition topic with `retention.ms=3600000`.
`DescribeConfigs` reads that config back. `GetMetadata` (the DescribeCluster
surface) lists the broker(s) and confirms the topic is present. Finally
`DeleteTopics` removes it.

> **Gotcha #6 — reserved characters.** A topic name containing `~` maps to an
> illegal Events-Store channel and is rejected with
> `INVALID_TOPIC_EXCEPTION` (`ErrorCode.TopicException`, code 17). The catch
> broadens to related rejection codes so the assertion holds across connector
> versions.

This mirrors the connector's CreateTopics / DescribeConfigs / DeleteTopics path in
`connectors/kafka/`.

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|----------|----------------|------------------|----------------|------------------|-------------|-------------|------------------|
| CreateTopics, DescribeConfigs, Metadata (DescribeCluster), DeleteTopics | n/a (admin only) | `kafka-ex-admin-topics-lifecycle` / 2 | n/a | n/a | n/a | n/a | **gotcha #6** (`~` in name → `INVALID_TOPIC_EXCEPTION` 17); describe echoes `retention.ms`; cluster has ≥1 broker |

## Related Examples

Same variant in the other languages:

- **Go** — [`../../../go/admin/topics-lifecycle`](../../../go/admin/topics-lifecycle)
- **Python** — [`../../../python/admin/topics_lifecycle`](../../../python/admin/topics_lifecycle)
- **Java** — [`../../../java/admin/topics-lifecycle`](../../../java/admin/topics-lifecycle)
- **JS/TS** — [`../../../javascript/admin/topics-lifecycle`](../../../javascript/admin/topics-lifecycle)
- **Ruby** — [`../../../ruby/admin/topics_lifecycle`](../../../ruby/admin/topics_lifecycle)
- **Rust** — [`../../../rust/admin/topics-lifecycle`](../../../rust/admin/topics-lifecycle)

Docs: [`../../../../docs/guides/admin-and-topics.md`](../../../../docs/guides/admin-and-topics.md),
[`../../../../docs/reference/error-codes.md`](../../../../docs/reference/error-codes.md)

---

> **Auth:** the connector default is no authentication. SASL/TLS setup lives in
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
