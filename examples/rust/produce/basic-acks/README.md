# Rust — Kafka: Produce basic acks

Produce one record under `acks=0`, `acks=1`, and `acks=all`, Fetch all three back, then prove an
oversized record is rejected with `MESSAGE_TOO_LARGE`.

## 1. Prerequisites

- Rust 1.75+ (edition 2021) + Cargo.
- `rdkafka` 0.37 (librdkafka, `cmake-build`) on `tokio`, pinned via the `examples/rust` workspace.
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`).
- **`CONNECTORS_KAFKA_ENABLE=true`** on the broker (gotcha #1 — the connector is off by default).

## 2. How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
cargo run -p basic-acks
```

## 3. Expected Output

```text
[kubemq-kafka] produce/basic-acks bootstrap=localhost:9092 (no-auth; connector must be enabled: CONNECTORS_KAFKA_ENABLE=true)
produced acks=0 -> partition=0 offset=<n>
produced acks=1 -> partition=0 offset=<n>
produced acks=all -> partition=0 offset=<n>
fetched offset=<n> body='order-42 acks=0'
fetched offset=<n> body='order-42 acks=1'
fetched offset=<n> body='order-42 acks=all'
round-trip OK: produced+fetched 3 records under acks 0/1/all
oversized 2 MiB record correctly rejected: MESSAGE_TOO_LARGE
basic-acks complete
```

(`<n>` offsets vary per run; offsets are STAN Sequence values, durable.)

## 4. What's Happening

Metadata auto-creates topic `kafka-ex-produce-acks` (native channel `kafka.kafka-ex-produce-acks`);
Produce sends a RecordBatch v2 under each acks level; Fetch long-polls the three back; the 2 MiB record
exceeds the connector `MaxMessageBytes` (1 MiB) → `MESSAGE_TOO_LARGE`. Mirrors connector behavior in
`connectors/kafka/` (Produce key 0 / Fetch key 1; §2.3).

## 5. Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Produce(0), Fetch(1) | acks 0/1/all, read_uncommitted | kafka-ex-produce-acks / 1 | one-shot earliest group | offset=STAN Sequence | none | librdkafka CRC32 (single partition, N/A here) | oversized→MESSAGE_TOO_LARGE; gotcha #3 acks≥1 on multi-node |

## 6. Related Examples

Same variant in the other 6 languages: `../../../{go,java,javascript,csharp}/produce/basic-acks`,
`../../../{python,ruby}/produce/basic_acks`. Guide: `../../../../docs/guides/producing.md`.

## 7. Gotcha callout

**Gotcha #3:** `acks=0` on a follower in a multi-node cluster can silently drop the record; use `acks≥1`
(this example uses `acks=all` for the durable path).

## 8. Auth

The stock dev broker runs with auth off. To require SASL/TLS, see
`../../../../docs/guides/security-sasl-tls.md` and the `security/sasl-plain-scram` variant.
