# Rust — Kafka: Produce idempotent

Enable idempotence, produce N distinct records, Fetch them back, and assert **exactly N** arrive — no
duplicates from producer-internal retries.

## 1. Prerequisites

- Rust 1.75+ + Cargo; `rdkafka` 0.37 (librdkafka) on `tokio`, via the `examples/rust` workspace.
- A running KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`).
- **`CONNECTORS_KAFKA_ENABLE=true`** on the broker (gotcha #1).

## 2. How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
cargo run -p idempotent
```

## 3. Expected Output

```text
[kubemq-kafka] produce/idempotent bootstrap=localhost:9092 (no-auth; connector must be enabled: CONNECTORS_KAFKA_ENABLE=true)
idempotent producer created (enable.idempotence=true; PID assigned via InitProducerId)
produced 'evt-0' -> partition=0 offset=<n>
...
fetched offset=<n> body='evt-4'
idempotent OK: exactly 5 distinct records, no duplicates
```

## 4. What's Happening

`enable.idempotence=true` makes librdkafka call **InitProducerId** (key 22) to obtain a Producer ID, and
forces `acks=all` with bounded in-flight. Every Produce batch carries the PID + a monotonic base
sequence, so a re-delivered batch (from a retry) is dropped by the broker as a duplicate. Because user
code cannot deterministically force an internal retry, this example verifies the observable guarantee —
**exactly-N delivery, no duplicates**.

## 5. Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| InitProducerId(22), Produce(0), Fetch(1) | acks=all (forced), read_uncommitted | kafka-ex-produce-idem / 1 | one-shot earliest group | offset=STAN Sequence | none | librdkafka CRC32 | per-(PID,partition,seq) dedup |

## 6. Related Examples

`../../../{go,java,javascript,csharp}/produce/idempotent`, `../../../{python,ruby}/produce/idempotent`.
Guide: `../../../../docs/guides/producing.md`.

## 7. Gotcha callout

Idempotence **forces `acks=all`** — a non-`all` acks value with idempotence enabled is rejected by the
client (`DisableIdempotentWrite`). Leave acks at its idempotent default here.

## 8. Auth

Auth is off by default. See `../../../../docs/guides/security-sasl-tls.md`.
