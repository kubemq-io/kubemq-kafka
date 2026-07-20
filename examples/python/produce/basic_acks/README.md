# Python — Kafka: Basic Acks

Produce records at acks 0/1/all, read them back, and prove an oversized record is rejected — against
the KubeMQ Kafka connector using native `confluent-kafka` (librdkafka).

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
    uv run python produce/basic_acks/main.py

## Expected Output

> Topic names are namespaced with `KUBEMQ_KAFKA_NAME_PREFIX` (default `py`) so the Python suite does
> not collide with the other languages on the same connector: `kafka-py-produce-acks`.

    === produce/basic-acks — topic 'kafka-py-produce-acks' ===
      bootstrap : localhost:9092
      client    : confluent-kafka (librdkafka; CRC32 default partitioner)
      note      : connector must be started with CONNECTORS_KAFKA_ENABLE=true

    CreateTopics -> 'kafka-py-produce-acks' created
      [OK] acks=0 record delivered (partition 0, offset -1)
      [OK] acks=1 record delivered (partition 0, offset 1)
      [OK] acks=all record delivered (partition 0, offset 2)
      [OK] read back all acks records: ['acks=0', 'acks=1', 'acks=all']
      [OK] oversized record rejected (MESSAGE_TOO_LARGE): ...

    Note (gotcha #3): on a multi-node connector, acks=0 to a follower can
    silently drop — use acks>=1 for durability.
    Note (gotcha #1): the connector must be enabled with CONNECTORS_KAFKA_ENABLE=true.
    Round-trip complete.

## What's Happening

- `AdminClient.create_topics` registers the topic, which maps to the Events-Store channel
  `kafka.kafka-py-produce-acks` (offset = STAN Sequence).
- Each `Producer.produce(...)` is confirmed by a **delivery report**: `acks=all` waits for the durable
  commit and returns the assigned offset; `acks=0` is fire-and-forget (offset `-1`).
- The consumer `assign`s partition 0 from offset 0 and reads every record back.
- The oversized record uses a raised client `message.max.bytes` so it reaches the broker, which
  rejects it with `MESSAGE_TOO_LARGE`.
- Mirrors connector behavior in `connectors/kafka/` (RecordBatch v2 Produce path).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Produce(0), Metadata(3), CreateTopics(19), Fetch(1), ListOffsets(2) | acks 0/1/all; read_uncommitted | 1 partition | `basic-acks-reader` (assign, no subscribe) | offset = STAN Sequence; acks=0 → no offset | none | CRC32 (librdkafka default) | oversized → `MESSAGE_TOO_LARGE`; gotcha #3 acks on multi-node |

## Related Examples

- Same variant in the other languages: [`../../../go/produce/basic-acks/`](../../../go/produce/basic-acks/),
  [`../../../java/produce/basic-acks/`](../../../java/produce/basic-acks/),
  [`../../../javascript/produce/basic-acks/`](../../../javascript/produce/basic-acks/),
  [`../../../csharp/produce/basic-acks/`](../../../csharp/produce/basic-acks/),
  [`../../../ruby/produce/basic_acks/`](../../../ruby/produce/basic_acks/),
  [`../../../rust/produce/basic-acks/`](../../../rust/produce/basic-acks/).
- [`../idempotent/`](../idempotent/) — exactly-once-within-a-producer.
- [`../../../../docs/guides/producing.md`](../../../../docs/guides/producing.md)

> **Gotcha #3 — acks on multi-node.** `acks=0` to a follower can be silently dropped. Use `acks>=1`
> (ideally `acks=all` with idempotence) for durability.

> **Auth.** The stock dev connector runs with SASL off. For a secured connector, configure SASL/PLAIN
> or SCRAM and see [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
