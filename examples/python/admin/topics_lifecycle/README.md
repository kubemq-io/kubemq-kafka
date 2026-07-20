# Python — Kafka: Topics Lifecycle

Drive the AdminClient control plane end to end — CreateTopics → DescribeConfigs → DescribeCluster →
DeleteTopics — and prove a reserved-character (`~`) topic name is rejected (gotcha #6) — against the
KubeMQ Kafka connector using native `confluent-kafka`.

## Prerequisites

- Python 3.9+ and [`uv`](https://docs.astral.sh/uv/).
- Kafka client: `confluent-kafka` (installed via `uv sync` from `../../pyproject.toml`).
- A running **KubeMQ Kafka connector** reachable at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`),
  started with **`CONNECTORS_KAFKA_ENABLE=true`** (the connector is disabled by default — gotcha #1).
- `AdvertisedHost` set on the connector for any non-loopback client (gotcha #2).

## How to Run

    cd examples/python
    uv sync
    export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
    uv run python admin/topics_lifecycle/main.py

## Expected Output

> The Python suite namespaces its topic with `KUBEMQ_KAFKA_NAME_PREFIX` (default `py`):
> `kafka-py-admin-lifecycle`. The config-entry count and rejection exception type vary by version.

    === admin/topics-lifecycle — topic 'kafka-py-admin-lifecycle' ===
      bootstrap : localhost:9092
      client    : confluent-kafka (librdkafka; CRC32 default partitioner)
      note      : connector must be started with CONNECTORS_KAFKA_ENABLE=true

    CreateTopics -> 'kafka-py-admin-lifecycle' created
      [OK] created topic is present in cluster metadata
      [OK] DescribeConfigs returned 12 config entries
      [OK] DescribeCluster reports 1 broker(s)
    DeleteTopics -> 'kafka-py-admin-lifecycle' deleted
      [OK] deleted topic is gone from cluster metadata
      [OK] reserved-name topic rejected (gotcha #6): KafkaException: ...

    Round-trip complete.

## What's Happening

- **CreateTopics** registers the topic; it then appears in cluster metadata (`list_topics`).
- **DescribeConfigs** reads the topic's config entries; **DescribeCluster** reports at least one broker
  (falls back to `list_topics()` cluster metadata if `describe_cluster` is unavailable in the pinned
  version).
- **DeleteTopics** removes it; metadata no longer lists it.
- A `~`-bearing name is **rejected** — `~` is reserved in the connector's Events-Store channel mapping
  (partition suffix `kafka.<topic>~<p>`), surfacing as `INVALID_TOPIC_EXCEPTION(17)` — gotcha #6.
- Mirrors connector behavior in `connectors/kafka/` (metadata/topic control plane).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| CreateTopics(19), DeleteTopics(20), DescribeConfigs(32), Metadata(3), DescribeCluster(60) | n/a (control plane) | 1 partition | n/a | n/a | n/a | n/a | `~` in topic name → `INVALID_TOPIC_EXCEPTION(17)` (gotcha #6) |

## Related Examples

- Same variant in the other languages: [`../../../go/admin/topics-lifecycle/`](../../../go/admin/topics-lifecycle/),
  [`../../../java/admin/topics-lifecycle/`](../../../java/admin/topics-lifecycle/),
  [`../../../javascript/admin/topics-lifecycle/`](../../../javascript/admin/topics-lifecycle/),
  [`../../../csharp/admin/topics-lifecycle/`](../../../csharp/admin/topics-lifecycle/),
  [`../../../ruby/admin/topics_lifecycle/`](../../../ruby/admin/topics_lifecycle/),
  [`../../../rust/admin/topics-lifecycle/`](../../../rust/admin/topics-lifecycle/).
- [`../partitions_and_configs/`](../partitions_and_configs/) — partition growth and config alters.
- [`../../../../docs/guides/admin-and-topics.md`](../../../../docs/guides/admin-and-topics.md)

> **Gotcha #6 — `~` is reserved in topic names.** The connector maps partition `p>0` to the channel
> suffix `kafka.<topic>~<p>`, so a literal `~` in a topic name collides with that scheme and is
> rejected with `INVALID_TOPIC_EXCEPTION(17)`. Also avoid `/`, which is rejected in `transactional.id`.

> **Auth.** The stock dev connector runs with SASL off. For a secured connector, configure SASL/PLAIN
> or SCRAM and see [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
