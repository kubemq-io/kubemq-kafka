# Python — Kafka: Seek by Offset and Timestamp

Two random-access reads over a single partition: `seek(offset)` lands on an exact record, and
`offsets_for_times()` (Kafka ListOffsets by-timestamp) resolves the first offset at-or-after a
wall-clock time — against the KubeMQ Kafka connector using native `confluent-kafka`.

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
    uv run python consume/seek_offsets_timestamps/main.py

## Expected Output

> The Python suite namespaces its topic with `KUBEMQ_KAFKA_NAME_PREFIX` (default `py`):
> `kafka-py-consume-seek`. The resolved timestamp value varies per run; the landed offset does not.

    === consume/seek-offsets-timestamps — topic 'kafka-py-consume-seek' ===
      bootstrap : localhost:9092
      client    : confluent-kafka (librdkafka; CRC32 default partitioner)
      note      : connector must be started with CONNECTORS_KAFKA_ENABLE=true

      [OK] seek(offset=4) landed on 'rec-4'
      [OK] offsets_for_times resolved ts 1752... -> offset 6
      [OK] timestamp lookup read the first record >= ts: 'rec-6'

    Round-trip complete.

## What's Happening

- Ten records `rec-0..rec-9` are produced with a small time gap; the wall-clock timestamp of `rec-6`
  is captured.
- **seek(offset):** the consumer assigns partition 0 at offset 0, seeks to offset 4, and the first
  record read back is exactly `rec-4`.
- **seek(timestamp):** `offsets_for_times` issues a Kafka **ListOffsets by-timestamp** and returns the
  first offset with `timestamp >= ts`; seeking there reads `rec-6`, the first record produced at-or-after
  the captured time.
- Mirrors connector behavior in `connectors/kafka/` (ListOffsets by-timestamp; see `listoffsets_test.go`).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Fetch(1), ListOffsets(2), Metadata(3), CreateTopics(19) | acks=all producer; read_uncommitted | 1 partition | `seek-offset-reader`, `seek-ts-reader` (assign) | seek by explicit offset; ListOffsets resolves offset ≥ timestamp | none | CRC32 (librdkafka default) | `seek()` may transiently raise before assignment is live — retried |

## Related Examples

- Same variant in the other languages: [`../../../go/consume/seek-offsets-timestamps/`](../../../go/consume/seek-offsets-timestamps/),
  [`../../../java/consume/seek-offsets-timestamps/`](../../../java/consume/seek-offsets-timestamps/),
  [`../../../javascript/consume/seek-offsets-timestamps/`](../../../javascript/consume/seek-offsets-timestamps/),
  [`../../../csharp/consume/seek-offsets-timestamps/`](../../../csharp/consume/seek-offsets-timestamps/),
  [`../../../ruby/consume/seek_offsets_timestamps/`](../../../ruby/consume/seek_offsets_timestamps/),
  [`../../../rust/consume/seek-offsets-timestamps/`](../../../rust/consume/seek-offsets-timestamps/).
- [`../../offsets/list_and_retention/`](../../offsets/list_and_retention/) — ListOffsets earliest/latest/by-ts.
- [`../../../../docs/concepts/topics-partitions-offsets.md`](../../../../docs/concepts/topics-partitions-offsets.md)

> **Gotcha — offset is the STAN Sequence.** The connector's offset is a durable, restart-stable STAN
> `Sequence`, not an in-memory counter, so seek targets survive a broker restart. Timestamp resolution
> returns the first offset `>= ts`, never an exact match.

> **Auth.** The stock dev connector runs with SASL off. For a secured connector, configure SASL/PLAIN
> or SCRAM and see [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
