# Python — Kafka: From Beginning / Latest

Prove both ends of `auto.offset.reset` for a group with no committed offset: `earliest` sees
pre-existing records, `latest` sees only records produced after it joined — against the KubeMQ Kafka
connector using native `confluent-kafka`.

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
    uv run python consume/from_beginning_latest/main.py

## Expected Output

> The Python suite namespaces its topic with `KUBEMQ_KAFKA_NAME_PREFIX` (default `py`):
> `kafka-py-consume-reset`.

    === consume/from-beginning-latest — topic 'kafka-py-consume-reset' ===
      bootstrap : localhost:9092
      client    : confluent-kafka (librdkafka; CRC32 default partitioner)
      note      : connector must be started with CONNECTORS_KAFKA_ENABLE=true

      [OK] earliest consumer saw all 5 pre-existing records
      [OK] latest consumer received its partition assignment
      [OK] latest consumer saw the record produced after it joined
      [OK] latest consumer did NOT see any pre-existing record

    Round-trip complete.

## What's Happening

- Five records are produced first, then two groups with **no committed offset** join the topic.
- The `earliest` group resolves its start position to the log start and reads all pre-existing records
  via the connector's **Fetch** (long-poll) path.
- The `latest` group resolves its start position to the log end. Because a `subscribe` has no
  partitions until the first rebalance completes, the program waits for the `on_assign` callback, then
  produces a marker — only that marker is delivered to the latest consumer, never the pre-existing five.
- Mirrors connector behavior in `connectors/kafka/` (Fetch long-poll + `auto.offset.reset` resolution;
  see `fetch_test.go`).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Fetch(1), ListOffsets(2), Metadata(3), JoinGroup(11)/SyncGroup(14), CreateTopics(19) | acks=all producer; read_uncommitted | 1 partition | `reset-earliest`, `reset-latest` (subscribe) | `auto.offset.reset` earliest/latest with no committed offset | none | CRC32 (librdkafka default) | latest requires produce-after-assign; assignment race handled via `on_assign` |

## Related Examples

- Same variant in the other languages: [`../../../go/consume/from-beginning-latest/`](../../../go/consume/from-beginning-latest/),
  [`../../../java/consume/from-beginning-latest/`](../../../java/consume/from-beginning-latest/),
  [`../../../javascript/consume/from-beginning-latest/`](../../../javascript/consume/from-beginning-latest/),
  [`../../../csharp/consume/from-beginning-latest/`](../../../csharp/consume/from-beginning-latest/),
  [`../../../ruby/consume/from_beginning_latest/`](../../../ruby/consume/from_beginning_latest/),
  [`../../../rust/consume/from-beginning-latest/`](../../../rust/consume/from-beginning-latest/).
- [`../seek_offsets_timestamps/`](../seek_offsets_timestamps/) — explicit random-access reads.
- [`../../../../docs/guides/consuming-and-groups.md`](../../../../docs/guides/consuming-and-groups.md)

> **Gotcha — `auto.offset.reset` only applies without a committed offset.** Once the group commits, the
> committed offset wins on the next join and `auto.offset.reset` is ignored. See
> [`../../consumer-groups/commit_and_lag/`](../../consumer-groups/commit_and_lag/).

> **Auth.** The stock dev connector runs with SASL off. For a secured connector, configure SASL/PLAIN
> or SCRAM and see [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
