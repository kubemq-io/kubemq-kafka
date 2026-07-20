# Rust — Kafka: Topic lifecycle (admin)

Create a topic, describe its configs, delete it, and prove a `~`-containing name is rejected.

## 1. Prerequisites

- Rust 1.75+ + Cargo; `rdkafka` 0.37 (librdkafka) on `tokio`, via the workspace.
- A running KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`).
- **`CONNECTORS_KAFKA_ENABLE=true`** on the broker (gotcha #1).

## 2. How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
cargo run -p topics-lifecycle
```

## 3. Expected Output

```text
[kubemq-kafka] admin/topics-lifecycle bootstrap=localhost:9092 (no-auth; connector must be enabled: CONNECTORS_KAFKA_ENABLE=true)
created topic 'kafka-ex-admin-topics'
described topic 'kafka-ex-admin-topics' (<k> config entries)
invalid name 'kafka-ex-admin~bad' correctly rejected: <code> (INVALID_TOPIC_EXCEPTION, gotcha #6)
deleted topic 'kafka-ex-admin-topics'
topics-lifecycle OK: create -> describe -> delete, invalid name rejected
```

## 4. What's Happening

The `AdminClient` drives CreateTopics (19), DescribeConfigs (32), and DeleteTopics (20). A name containing
`~` — the KubeMQ native channel separator — cannot be mapped to a channel, so the connector rejects it
with `INVALID_TOPIC_EXCEPTION` (Kafka error 17).

## 5. Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| CreateTopics(19), DescribeConfigs(32), DeleteTopics(20) | n/a (admin) | kafka-ex-admin-topics / 1 | n/a | n/a | n/a | n/a | `~` name → INVALID_TOPIC_EXCEPTION |

## 6. Related Examples

`../../../{go,java,javascript,csharp}/admin/topics-lifecycle`,
`../../../{python,ruby}/admin/topics_lifecycle`. Guide: `../../../../docs/guides/admin-and-topics.md`.

## 7. Gotcha callout

**Gotcha #6:** any topic name containing `~` is rejected — it collides with the KubeMQ channel separator.
Keep topic names to the usual `[a-zA-Z0-9._-]` set.

## 8. Auth

Auth is off by default. See `../../../../docs/guides/security-sasl-tls.md`.
