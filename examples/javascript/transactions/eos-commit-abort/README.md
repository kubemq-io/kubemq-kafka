# javascript — Kafka: Transactions — EOS Commit and Abort

Run a transactional producer through `InitProducerId → AddPartitionsToTxn → txn Produce →
EndTxn(commit|abort)`: commit one transaction, abort another, and prove a `read_committed` consumer
sees the committed records and never the aborted ones. The Kafka topic `kafka-ex-txn-eos` maps onto
the Events-Store channel `kafka.kafka-ex-txn-eos`.

## Prerequisites

- Node.js 18+ and `npm install` in `examples/javascript/` (pins `kafkajs` `^2.2.4` — v2+, murmur2
  `DefaultPartitioner`).
- A running KubeMQ server with the Kafka connector **enabled** (`CONNECTORS_KAFKA_ENABLE=true` — the
  connector is **disabled by default**, gotcha #1), reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). For external clients, set `CONNECTORS_KAFKA_ADVERTISED_HOST` (gotcha #2).

## How to Run

```bash
cd examples/javascript
npm install
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
npx tsx transactions/eos-commit-abort/index.ts
```

## Expected Output

```
Connecting to KubeMQ Kafka connector at localhost:9092 (topic "kafka-ex-txn-eos", transactional.id "kafka-ex-txn-eos-tid")
Txn 1 -> committed 2 records (committed-1, committed-2)
Txn 2 -> aborted 2 records (aborted-1, aborted-2)
read_committed consumer saw: [committed-1, committed-2]

EOS proven: committed records visible, aborted records absent under read_committed (EOS V1; KIP-890 out of scope)
```

## What's Happening

- The transactional producer (`{ transactionalId, idempotent: true, maxInFlightRequests: 1 }`) performs
  an `InitProducerId` (key 22) on connect.
- `producer.transaction()` starts a transaction; `txn.send(...)` issues `AddPartitionsToTxn` (key 24)
  then transactional Produce (key 0); `txn.commit()` / `txn.abort()` issues `EndTxn` (key 26).
- Txn 1 commits two records; Txn 2 aborts two records.
- A `read_committed` consumer (`readUncommitted: false`) delivers only `committed-1`/`committed-2` —
  the aborted records never surface (asserted; an aborted record appearing would fail the process).
- Mirrors connector behavior in `connectors/kafka/` (transaction RPCs; see
  `connectors/kafka/txn_rpcs_test.go`).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| InitProducerId (22), AddPartitionsToTxn (24), Produce (0), EndTxn (26), Fetch (1) | acks=all; consumer read_committed | `kafka-ex-txn-eos` / 1 partition | ephemeral verify group | committed records durable; aborted absent | none | murmur2 (DefaultPartitioner) | 🟡 EOS V1 only — **KIP-890 / TransactionV2 out of scope** (gotcha #9); `/` in `transactional.id` → `INVALID_TRANSACTIONAL_ID` (gotcha #7) |

## Related Examples

- Same variant, other languages: [`../../../go/transactions/eos-commit-abort`](../../../go/transactions/eos-commit-abort),
  [`../../../java/transactions/eos-commit-abort`](../../../java/transactions/eos-commit-abort),
  [`../../../csharp/transactions/eos-commit-abort`](../../../csharp/transactions/eos-commit-abort),
  [`../../../rust/transactions/eos-commit-abort`](../../../rust/transactions/eos-commit-abort),
  [`../../../python/transactions/eos_commit_abort`](../../../python/transactions/eos_commit_abort),
  [`../../../ruby/transactions/eos_commit_abort`](../../../ruby/transactions/eos_commit_abort).
- Doc: [`../../../../docs/guides/transactions-eos.md`](../../../../docs/guides/transactions-eos.md),
  [`../../../../docs/concepts/transactions-eos.md`](../../../../docs/concepts/transactions-eos.md).
- Next: [`../read-committed/`](../read-committed/).

> **Gotcha #9 — KIP-890 V1 EOS ceiling.** This connector implements the EOS **V1** transaction protocol.
> The KIP-890 server-side transaction improvements (TransactionV2 / the newer AddPartitionsToTxn
> verification flow) are **not** in scope — do not rely on behavior beyond EOS V1.

> **Gotcha #7 — no `/` in `transactional.id`.** A `transactional.id` containing `/` is rejected with
> `INVALID_TRANSACTIONAL_ID` → `INVALID_REQUEST(42)`. This example uses `kafka-ex-txn-eos-tid`.

> **Auth.** The dev default is no SASL over plain TCP (`:9092`). For a secured connector, configure
> SASL/PLAIN or SASL/SCRAM (and TLS on `:9093`). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
