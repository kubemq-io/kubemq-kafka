# Python — Kafka: Compression and Keys

Round-trip every Kafka compression codec (none/gzip/snappy/lz4/zstd) and demonstrate keyed
partitioning — including the **cross-client partitioner divergence** (gotcha #4) between librdkafka's
default CRC32 and murmur2 — against the KubeMQ Kafka connector using native `confluent-kafka`.

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
    uv run python produce/compression_and_keys/main.py

## Expected Output

> The Python suite namespaces its topic with `KUBEMQ_KAFKA_NAME_PREFIX` (default `py`):
> `kafka-py-produce-codec-keys`. The exact CRC32 vs murmur2 partition numbers depend on the key
> `order-42` and the 3-partition count.

    === produce/compression-and-keys — topic 'kafka-py-produce-codec-keys' ===
      bootstrap : localhost:9092
      client    : confluent-kafka (librdkafka; CRC32 default partitioner)
      note      : connector must be started with CONNECTORS_KAFKA_ENABLE=true

    CreateTopics -> 'kafka-py-produce-codec-keys' created (3 partitions)
      [OK] all 5 codec records delivered
      [OK] codec 'none' round-tripped intact
      [OK] codec 'gzip' round-tripped intact
      [OK] codec 'snappy' round-tripped intact
      [OK] codec 'lz4' round-tripped intact
      [OK] codec 'zstd' round-tripped intact
      [OK] librdkafka default (CRC32) partitioner is STABLE for the key: partition [1, 1]
      [OK] murmur2_random lands on the computed murmur2 partition (2)

      gotcha #4: CRC32(default)->p1  murmur2->p2
      -> the SAME key lands on a DIFFERENT partition than Java/franz-go/kafkajs.

    Round-trip complete.

## What's Happening

- Each of the five codecs is produced under `compression.type=<codec>` and read back intact — the
  connector stores the decompressed record, so compression is purely a wire-format concern.
- Keyed placement is proven twice: the **default** librdkafka partitioner (CRC32,
  `consistent_random`) places the fixed key on a stable partition; a producer with
  `partitioner='murmur2_random'` places the same key on the partition computed independently by the
  built-in `confluent_kafka.murmur2(key, n)`.
- Because librdkafka defaults to CRC32 while Java `kafka-clients`, franz-go, and kafkajs (v2+) default
  to murmur2, the **same key lands on a different partition** across client families — gotcha #4.
- Mirrors connector behavior in `connectors/kafka/` (Produce path stores decompressed records; the
  broker honors the client-selected partition).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Produce(0), Metadata(3), CreateTopics(19), Fetch(1) | acks default; read_uncommitted | 3 partitions | `codec-keys-reader` (assign all partitions) | offset = STAN Sequence per partition | none/gzip/snappy/lz4/zstd | CRC32 default; `murmur2_random` for parity | gotcha #4 — CRC32 ≠ murmur2 partition for the same key |

## Related Examples

- Same variant in the other languages: [`../../../go/produce/compression-and-keys/`](../../../go/produce/compression-and-keys/),
  [`../../../java/produce/compression-and-keys/`](../../../java/produce/compression-and-keys/),
  [`../../../javascript/produce/compression-and-keys/`](../../../javascript/produce/compression-and-keys/),
  [`../../../csharp/produce/compression-and-keys/`](../../../csharp/produce/compression-and-keys/),
  [`../../../ruby/produce/compression_and_keys/`](../../../ruby/produce/compression_and_keys/),
  [`../../../rust/produce/compression-and-keys/`](../../../rust/produce/compression-and-keys/).
- [`../../../../docs/concepts/cross-client-partitioning.md`](../../../../docs/concepts/cross-client-partitioning.md)

> **Gotcha #4 — cross-client partitioner divergence.** librdkafka clients (confluent-kafka,
> Confluent.Kafka, rdkafka-ruby, rust rdkafka) default to **CRC32**; Java/franz-go/kafkajs default to
> **murmur2**. A fixed key therefore maps to a different partition depending on which client wrote it.
> Pin the partitioner explicitly (`partitioner='murmur2_random'`) when keyed ordering must match across
> client families.

> **Auth.** The stock dev connector runs with SASL off. For a secured connector, configure SASL/PLAIN
> or SCRAM and see [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
