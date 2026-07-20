# Ruby — Kafka: Compression & Keyed Partitioning

Produce the same keyed record under each codec (none/gzip/snappy/lz4/zstd); prove every codec
round-trips and that a fixed key always lands on the SAME partition (CRC32 partitioner).

## Prerequisites
- Ruby 3.3.x (rbenv); `rdkafka` builds librdkafka natively, so a **C toolchain** is required.
- `rdkafka >= 0.19` via `bundle install` (`../../Gemfile`).
- KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` with `CONNECTORS_KAFKA_ENABLE=true`
  (gotcha #1) and `CONNECTORS_KAFKA_ADVERTISED_HOST` set for non-loopback (gotcha #2).

## How to Run
```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
bundle exec ruby produce/compression_and_keys/main.rb
```

## Expected Output
Banner, `CreateTopic -> ... partitions=3`, five `Produce(<codec>) -> partition=P` lines (same P),
`Assert -> key => stable partition P (CRC32)`, five `Fetch(<codec>) -> round-tripped OK`, `PASS`.

## What's Happening
Each codec compresses the RecordBatch differently but the value round-trips byte-for-byte. Keyed
routing is deterministic: partition = CRC32(key) % partitions.

## Kafka specifics
| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Produce(0), Fetch(1), CreateTopics(19), DeleteTopics(20) | acks default | 1 topic / 3 partitions | ephemeral, earliest | offset = STAN Sequence | none/gzip/snappy/lz4/zstd | **CRC32** (librdkafka default) | keyed routing |

## Gotcha
**#4 — partitioner family.** rdkafka/librdkafka defaults to **CRC32** (`consistent_random`), the
same group as Python confluent-kafka, C# Confluent.Kafka, and Rust rdkafka. A given key lands on a
**different** partition than Java kafka-clients / franz-go / kafkajs (all **murmur2**). To match the
murmur2 group set `"partitioner" => "murmur2_random"`. See
`../../../../docs/concepts/cross-client-partitioning.md`.

## Related Examples
- `../../../{go,java,javascript,csharp,rust}/produce/compression-and-keys`, `../../../python/produce/compression_and_keys`.
- Concept: `../../../../docs/concepts/cross-client-partitioning.md`.

## Auth
No auth by default. For SASL/TLS see `../../../../docs/guides/security-sasl-tls.md`.
