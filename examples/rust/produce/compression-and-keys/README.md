# Rust — Kafka: Produce compression and keys

Round-trip a keyed record under every codec (`none`/`gzip`/`snappy`/`lz4`/`zstd`) and prove keyed
partitioning is deterministic (same key → same partition).

## 1. Prerequisites

- Rust 1.75+ + Cargo; `rdkafka` 0.37 (librdkafka, **`zstd` feature**) on `tokio`, via the workspace.
- A running KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`).
- **`CONNECTORS_KAFKA_ENABLE=true`** on the broker (gotcha #1).

## 2. How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
cargo run -p compression-and-keys
```

## 3. Expected Output

```text
[kubemq-kafka] produce/compression-and-keys bootstrap=localhost:9092 (no-auth; connector must be enabled: CONNECTORS_KAFKA_ENABLE=true)
topic 'kafka-ex-produce-compkeys' ready with 4 partitions
codec=none key=alpha -> partition=<p> offset=<n>
...
keyed partitioning stable (CRC32, librdkafka default): {"alpha": <p>, "beta": <p>, "gamma": <p>}
fetched partition=<p> offset=<n> body='zstd:gamma'
compression-and-keys OK: all 15 records round-tripped across 5 codecs
```

## 4. What's Happening

The topic is created with 4 partitions (auto-create would give 1). For each codec, a keyed record is
produced and Fetched back; all codecs decode transparently on the consumer. The same key always maps to
the same partition — librdkafka's default `consistent_random` partitioner = **CRC32(key) % partitions**.

## 5. Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| CreateTopics(19), Produce(0), Fetch(1) | acks=all, read_uncommitted | kafka-ex-produce-compkeys / 4 | one-shot earliest group | offset=STAN Sequence | none/gzip/snappy/lz4/zstd | **librdkafka CRC32** | codec decode on fetch |

## 6. Related Examples

`../../../{go,java,javascript,csharp}/produce/compression-and-keys`,
`../../../{python,ruby}/produce/compression_and_keys`. Concept: `../../../../docs/concepts/cross-client-partitioning.md`.

## 7. Gotcha callout

**Gotcha #4 — cross-client partitioning divergence.** This client (librdkafka/CRC32) maps a given key to
a **different partition** than Java `kafka-clients`, franz-go, or kafkajs v2+ (all **murmur2**). Do not
"fix" this by forcing `partitioner=murmur2_random`; this example demonstrates native Rust behavior. If
you need cross-client key parity, all producers must agree on one partitioner — see the concepts page.

## 8. Auth

Auth is off by default. See `../../../../docs/guides/security-sasl-tls.md`.
