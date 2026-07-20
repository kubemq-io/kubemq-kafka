# Rust — Kafka: Transactions (commit & abort)

Run one committed and one aborted producer transaction; a `read_committed` consumer sees the committed
record and never the aborted one.

## 1. Prerequisites

- Rust 1.75+ + Cargo; `rdkafka` 0.37 (librdkafka) on `tokio`, via the workspace.
- A running KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`).
- **`CONNECTORS_KAFKA_ENABLE=true`** on the broker (gotcha #1).

## 2. How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
cargo run -p eos-commit-abort
```

## 3. Expected Output

```text
[kubemq-kafka] transactions/eos-commit-abort bootstrap=localhost:9092 (no-auth; connector must be enabled: CONNECTORS_KAFKA_ENABLE=true)
transactional producer initialized (transactional.id=kafka-ex-eos-rust)
committed transaction with 'committed-record'
aborted transaction with 'aborted-record'
read_committed fetched offset=<n> body='committed-record'
eos-commit-abort OK: committed visible, aborted absent under read_committed
```

## 4. What's Happening

The producer initializes a transaction (InitProducerId key 22, with a `transactional.id`), then runs two
transactions: one committed (EndTxn commit, key 26) and one aborted (EndTxn abort). librdkafka issues
AddPartitionsToTxn (key 24) internally. A `read_committed` consumer filters out the aborted record.

## 5. Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| InitProducerId(22), AddPartitionsToTxn(24), EndTxn(26) | acks=all, **read_committed** | kafka-ex-eos / 1 | one-shot earliest group | committed visible / aborted absent | none | librdkafka CRC32 | transactional.id → channel |

## 6. Related Examples

`../../../{go,java,javascript,csharp}/transactions/eos-commit-abort`,
`../../../{python,ruby}/transactions/eos_commit_abort`. Guide: `../../../../docs/guides/transactions-eos.md`.

## 7. Gotcha callout

- **Gotcha #7:** a `/` in `transactional.id` is rejected with `INVALID_REQUEST` (error 42) — the id maps
  to a channel where `/` is illegal. This example uses `kafka-ex-eos-rust`.
- **Gotcha #9 — KIP-890 V1 ceiling (honest scope):** the connector implements the KIP-890 **V1**
  transaction protocol. A same-epoch "zombie" producer can, in a narrow window, still append after a
  fence — a residual **shared with upstream Kafka at the V1 level, not a connector defect**. Full
  hardening requires the V2 protocol (KIP-890 part 2). No guarantee is claimed beyond spec §2.

## 8. Auth

Auth is off by default. See `../../../../docs/guides/security-sasl-tls.md`.
