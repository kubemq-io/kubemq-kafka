# Python — Kafka: Partitions and Configs

Grow a topic's partition count (increase-only, ≤256), apply a partial `IncrementalAlterConfigs`, and
truncate the log low-end with `DeleteRecords` — proving invalid partition changes are rejected with
`INVALID_PARTITIONS` — against the KubeMQ Kafka connector using native `confluent-kafka`.

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
    uv run python admin/partitions_and_configs/main.py

## Expected Output

> The Python suite namespaces its topic with `KUBEMQ_KAFKA_NAME_PREFIX` (default `py`):
> `kafka-py-admin-partitions`. `IncrementalAlterConfigs` and `DeleteRecords` are partial-support (🟡);
> exact `confluent-kafka` method shapes are confirmed at implementation.

    === admin/partitions-and-configs — topic 'kafka-py-admin-partitions' ===
      bootstrap : localhost:9092
      client    : confluent-kafka (librdkafka; CRC32 default partitioner)
      note      : connector must be started with CONNECTORS_KAFKA_ENABLE=true

    CreateTopics -> 'kafka-py-admin-partitions' created (2 partitions)
      [OK] CreatePartitions increased 2 -> 4 partitions
      [OK] same-count CreatePartitions rejected (INVALID_PARTITIONS)
      [OK] decrease CreatePartitions rejected (INVALID_PARTITIONS)
      [OK] >256 CreatePartitions rejected (INVALID_PARTITIONS)
      [OK] IncrementalAlterConfigs accepted (retention.ms) [partial — 🟡]
      [OK] DeleteRecords truncated low end to offset 5 [partial — 🟡]

    Round-trip complete.

## What's Happening

- **CreatePartitions** increases the partition count to a strictly-greater value ≤256; each new
  partition is an independent ordered offset space.
- Same-count, decrease, and `>256` requests are all **rejected with `INVALID_PARTITIONS`** — partitions
  are increase-only and hard-capped at 256.
- **IncrementalAlterConfigs** applies a subset of topic configs (e.g. `retention.ms`); unrecognized
  keys are accepted-but-no-op — partial support (🟡).
- **DeleteRecords** truncates the log low-end to a given offset — partial support (🟡).
- Growing N re-shards keys across partitions — per-key order holds only within a fixed-N epoch
  (gotcha #5).
- Mirrors connector behavior in `connectors/kafka/` (CreatePartitions / config alter / DeleteRecords
  paths).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| CreatePartitions(37), IncrementalAlterConfigs(44) 🟡, DeleteRecords(21) 🟡, DescribeConfigs(32) | n/a (control plane) | 2 → 4 partitions (≤256 cap) | n/a | DeleteRecords advances log-start offset | n/a | growing N re-shards keys | invalid increase → `INVALID_PARTITIONS`; gotcha #5 |

## Related Examples

- Same variant in the other languages: [`../../../go/admin/partitions-and-configs/`](../../../go/admin/partitions-and-configs/),
  [`../../../java/admin/partitions-and-configs/`](../../../java/admin/partitions-and-configs/),
  [`../../../javascript/admin/partitions-and-configs/`](../../../javascript/admin/partitions-and-configs/),
  [`../../../csharp/admin/partitions-and-configs/`](../../../csharp/admin/partitions-and-configs/),
  [`../../../ruby/admin/partitions_and_configs/`](../../../ruby/admin/partitions_and_configs/),
  [`../../../rust/admin/partitions-and-configs/`](../../../rust/admin/partitions-and-configs/).
- [`../topics_lifecycle/`](../topics_lifecycle/) — create/describe/delete.
- [`../../../../docs/guides/admin-and-topics.md`](../../../../docs/guides/admin-and-topics.md)

> **Gotcha #5 — growing N re-shards keys.** Adding partitions changes the key→partition mapping, so
> per-key ordering is guaranteed only within a fixed partition-count epoch. Plan capacity before
> committing keyed traffic. Decreasing partitions is never allowed (`INVALID_PARTITIONS`).

> **Auth.** The stock dev connector runs with SASL off. For a secured connector, configure SASL/PLAIN
> or SCRAM and see [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
