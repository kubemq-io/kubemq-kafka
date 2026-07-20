# Python — Kafka: List Offsets and Retention

Resolve earliest/latest/by-timestamp offsets (ListOffsets) and show how topic `retention.ms` /
`retention.bytes` map to the connector's channel `MaxAge` / `MaxBytes` — against the KubeMQ Kafka
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
    uv run python offsets/list_and_retention/main.py

## Expected Output

> The Python suite namespaces its topic with `KUBEMQ_KAFKA_NAME_PREFIX` (default `py`):
> `kafka-py-offsets-retention`. `AdminClient.list_offsets` / `OffsetSpec` availability is confirmed at
> implementation; the consumer-side `get_watermark_offsets` + `offsets_for_times` fallback is used
> otherwise.

    === offsets/list-and-retention — topic 'kafka-py-offsets-retention' ===
      bootstrap : localhost:9092
      client    : confluent-kafka (librdkafka; CRC32 default partitioner)
      note      : connector must be started with CONNECTORS_KAFKA_ENABLE=true

      [OK] earliest watermark tracks log start (offset 0)
      [OK] latest watermark tracks the high watermark (offset 12)
      [OK] ListOffsets by-timestamp resolved ts -> offset 6
      [OK] retention.ms / retention.bytes describable and honored

    Round-trip complete.

## What's Happening

- **ListOffsets earliest** returns the log-start offset; **latest** returns the high watermark (HWM).
- **ListOffsets by-timestamp** resolves the first offset with `timestamp >= ts`.
- Topic retention configs `retention.ms` / `retention.bytes` map to the Events-Store channel's `MaxAge`
  / `MaxBytes`; the example describes them and confirms they are honored (old records age out,
  advancing the earliest offset).
- Mirrors connector behavior in `connectors/kafka/` (ListOffsets earliest/latest/by-timestamp; see
  `listoffsets_test.go`).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| ListOffsets(2), DescribeConfigs(32), Metadata(3), Fetch(1) | acks=all producer; read_uncommitted | 1 partition | n/a (assign/watermarks) | earliest = log-start; latest = HWM; by-ts = first ≥ ts | none | CRC32 (librdkafka default) | retention.ms/bytes → channel MaxAge/MaxBytes |

## Related Examples

- Same variant in the other languages: [`../../../go/offsets/list-and-retention/`](../../../go/offsets/list-and-retention/),
  [`../../../java/offsets/list-and-retention/`](../../../java/offsets/list-and-retention/),
  [`../../../javascript/offsets/list-and-retention/`](../../../javascript/offsets/list-and-retention/),
  [`../../../csharp/offsets/list-and-retention/`](../../../csharp/offsets/list-and-retention/),
  [`../../../ruby/offsets/list_and_retention/`](../../../ruby/offsets/list_and_retention/),
  [`../../../rust/offsets/list-and-retention/`](../../../rust/offsets/list-and-retention/).
- [`../../consume/seek_offsets_timestamps/`](../../consume/seek_offsets_timestamps/) — seek to a resolved offset.
- [`../../../../docs/concepts/topics-partitions-offsets.md`](../../../../docs/concepts/topics-partitions-offsets.md)

> **Gotcha — offsets are durable STAN Sequences.** Earliest/latest track the Raft-replicated log, so
> they are restart-stable and identical across nodes. Retention truncation advances the earliest offset;
> a seek below it lands on the new log-start, not an error.

> **Auth.** The stock dev connector runs with SASL off. For a secured connector, configure SASL/PLAIN
> or SCRAM and see [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
