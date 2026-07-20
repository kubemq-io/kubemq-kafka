# Python — Kafka: Commit and Lag

Commit an offset (OffsetCommit), resume from it in a second consumer (OffsetFetch), and compute
consumer-group lag as `high-watermark − committed` — against the KubeMQ Kafka connector using native
`confluent-kafka`.

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
    uv run python consumer-groups/commit_and_lag/main.py

## Expected Output

> The Python suite namespaces its topic with `KUBEMQ_KAFKA_NAME_PREFIX` (default `py`):
> `kafka-py-cgroups-commit`.

    === consumer-groups/commit-and-lag — topic 'kafka-py-cgroups-commit' ===
      bootstrap : localhost:9092
      client    : confluent-kafka (librdkafka; CRC32 default partitioner)
      note      : connector must be started with CONNECTORS_KAFKA_ENABLE=true

      [OK] first consumer read 8 records
    OffsetCommit -> group 'commit-lag-grp' committed offset 8
      [OK] OffsetFetch returns the committed offset (8)
      [OK] lag == HWM(20) - committed(8) == 12
      [OK] second consumer RESUMED at offset 8 (read the 12-record tail)

    Round-trip complete.

## What's Happening

- Twenty records are produced. A first consumer reads 8 and commits the **next** offset (9th record's
  position) synchronously via **OffsetCommit**.
- A second, cold consumer in the same group fetches that offset (**OffsetFetch**) and resumes exactly
  where the first stopped — no re-reading, no gap.
- Lag is computed client-side as `high-watermark(20) − committed(8) = 12`, matching the un-consumed
  tail. The server also exposes this as the metric
  `kubemq_kafka_consumer_group_lag{group,topic,partition}` (a server-side read).
- Mirrors connector behavior in `connectors/kafka/` (OffsetCommit/OffsetFetch; see
  `groupoffsets_test.go`).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| OffsetCommit(8), OffsetFetch(9), ListOffsets(2), Fetch(1), FindCoordinator(10) | acks=all producer; read_uncommitted | 1 partition | `commit-lag-grp` (subscribe; manual commit) | committed offset durable; lag = HWM − committed | none | CRC32 (librdkafka default) | resume from committed offset; lag computed client-side |

## Related Examples

- Same variant in the other languages: [`../../../go/consumer-groups/commit-and-lag/`](../../../go/consumer-groups/commit-and-lag/),
  [`../../../java/consumer-groups/commit-and-lag/`](../../../java/consumer-groups/commit-and-lag/),
  [`../../../javascript/consumer-groups/commit-and-lag/`](../../../javascript/consumer-groups/commit-and-lag/),
  [`../../../csharp/consumer-groups/commit-and-lag/`](../../../csharp/consumer-groups/commit-and-lag/),
  [`../../../ruby/consumer-groups/commit_and_lag/`](../../../ruby/consumer-groups/commit_and_lag/),
  [`../../../rust/consumer-groups/commit-and-lag/`](../../../rust/consumer-groups/commit-and-lag/).
- [`../join_rebalance/`](../join_rebalance/) — multi-member partition assignment.
- [`../../../../docs/guides/consuming-and-groups.md`](../../../../docs/guides/consuming-and-groups.md)

> **Gotcha — lag metric is server-side.** The example computes lag from the client for a self-contained
> proof; the authoritative `kubemq_kafka_consumer_group_lag` gauge is exported by the server and read
> from the metrics endpoint, not the Kafka client. Manual commits (`enable.auto.commit=False`,
> `asynchronous=False`) keep the offset deterministic.

> **Auth.** The stock dev connector runs with SASL off. For a secured connector, configure SASL/PLAIN
> or SCRAM and see [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
