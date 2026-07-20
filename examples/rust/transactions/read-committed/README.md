# Rust — Kafka: read_committed isolation

A `read_committed` consumer never delivers aborted records, and the delivered set is smaller than the
high-water mark (aborted record + transaction markers occupy skipped offsets).

## 1. Prerequisites

- Rust 1.75+ + Cargo; `rdkafka` 0.37 (librdkafka) on `tokio`, via the workspace.
- A running KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`).
- **`CONNECTORS_KAFKA_ENABLE=true`** on the broker (gotcha #1).

## 2. How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
cargo run -p read-committed
```

## 3. Expected Output

```text
[kubemq-kafka] transactions/read-committed bootstrap=localhost:9092 (no-auth; connector must be enabled: CONNECTORS_KAFKA_ENABLE=true)
committed 2 records (commit-0, commit-1)
aborted 1 record (abort-0)
read_committed delivered offset=<n> body='commit-0'
read_committed delivered offset=<n> body='commit-1'
delivered=2 HWM=<h> (aborted record + txn markers occupy the gap)
read-committed OK: aborted never delivered; delivered 2 < HWM <h>
```

## 4. What's Happening

One committed transaction (2 records) and one aborted transaction (1 record) are produced. A
`read_committed` consumer delivers only the 2 committed records. The high-water mark is larger than 2
because the aborted record and the transaction control markers still occupy log offsets — the
read_committed reader skips them.

## 5. Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Fetch(1), ListOffsets(2) | acks=all, **read_committed** | kafka-ex-readcommitted / 1 | one-shot earliest group | LSO ≤ HWM; aborted skipped | none | librdkafka CRC32 | client-side abort filtering |

## 6. Related Examples

`../../../{go,java,javascript,csharp}/transactions/read-committed`,
`../../../{python,ruby}/transactions/read_committed`. Guide: `../../../../docs/guides/transactions-eos.md`.

## 7. Gotcha callout

- **Gotcha #12:** `read_committed` filtering is **client-side** — Fetch returns records plus an
  `AbortedTransactions` list and librdkafka drops the aborted ones. There is no server-side record
  filter. While a transaction is open, `ListOffsets(latest, read_committed)` returns the **LSO**, which
  lags the HWM until the txn resolves.
- **Gotcha #9 — KIP-890 V1 ceiling (honest scope):** the connector implements the KIP-890 **V1**
  transaction protocol; the same-epoch zombie residual is shared with upstream Kafka, not a defect. No
  guarantee is claimed beyond spec §2.

## 8. Auth

Auth is off by default. See `../../../../docs/guides/security-sasl-tls.md`.
