# Python — Kafka: Idempotent Producer

Enable the idempotent producer, publish a fixed batch, read it all back, and prove there are **no
duplicates** even under librdkafka's internal retries — against the KubeMQ Kafka connector using native
`confluent-kafka` (librdkafka).

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
    uv run python produce/idempotent/main.py

## Expected Output

> The Python suite namespaces its topic with `KUBEMQ_KAFKA_NAME_PREFIX` (default `py`):
> `kafka-py-produce-idem`.

    === produce/idempotent — topic 'kafka-py-produce-idem' ===
      bootstrap : localhost:9092
      client    : confluent-kafka (librdkafka; CRC32 default partitioner)
      note      : connector must be started with CONNECTORS_KAFKA_ENABLE=true

    CreateTopics -> 'kafka-py-produce-idem' created
      [OK] all 50 records delivered (acks=all, idempotent)
      [OK] read-back count == produced count (50 == 50)
      [OK] NO duplicates (all 50 read-back records distinct)
      [OK] read-back set exactly matches the produced set

    Note: per-(PID,partition) sequence dedup guarantees at-most-once storage
    under librdkafka's internal retries — a retried batch is dropped by the broker.
    Round-trip complete.

## What's Happening

- Setting `enable.idempotence=True` makes librdkafka call **`InitProducerId`** to obtain a producer id
  (PID) and starting epoch, and auto-forces `acks=all`, `retries>0`, and `max.in.flight<=5`.
- Every RecordBatch is tagged with `(PID, epoch, base-sequence)`; the connector de-duplicates per
  `(PID, partition)` on the sequence number, so an internally-retried batch can **never** create a
  duplicate on the log.
- The program produces 50 uniquely-numbered records, flushes, reads them all back from partition 0, and
  asserts the read-back set equals the produced set with no duplicates.
- Mirrors the connector idempotent Produce path (`InitProducerId` + per-`(PID,partition)` sequence
  dedup) in `connectors/kafka/`.

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| InitProducerId(22), Produce(0), Metadata(3), CreateTopics(19), Fetch(1) | acks=all (forced by idempotence); read_uncommitted | 1 partition | `idempotent-reader` (assign) | offset = STAN Sequence | none | CRC32 (librdkafka default) | per-`(PID,partition)` sequence dedup → no dup on retry |

## Related Examples

- Same variant in the other languages: [`../../../go/produce/idempotent/`](../../../go/produce/idempotent/),
  [`../../../java/produce/idempotent/`](../../../java/produce/idempotent/),
  [`../../../javascript/produce/idempotent/`](../../../javascript/produce/idempotent/),
  [`../../../csharp/produce/idempotent/`](../../../csharp/produce/idempotent/),
  [`../../../ruby/produce/idempotent/`](../../../ruby/produce/idempotent/),
  [`../../../rust/produce/idempotent/`](../../../rust/produce/idempotent/).
- [`../basic_acks/`](../basic_acks/) — the acks levels idempotence builds on.
- [`../../../../docs/guides/producing.md`](../../../../docs/guides/producing.md)

> **Gotcha #3 — acks under idempotence.** Idempotence requires `acks=all`; librdkafka forces it for you.
> On a multi-node connector never drop below `acks>=1` — an `acks=0` write to a follower can be silently
> lost, which idempotence cannot recover.

> **Auth.** The stock dev connector runs with SASL off. For a secured connector, configure SASL/PLAIN
> or SCRAM and see [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
