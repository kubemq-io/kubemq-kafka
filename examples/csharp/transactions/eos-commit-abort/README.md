# C# — Kafka: EOS Commit vs Abort

Transactional producer (`InitTransactions → BeginTransaction → Produce → Commit |
Abort`). A `read_committed` consumer sees the **committed** records and **never**
the aborted ones.

## Prerequisites

- .NET SDK **8.0**
- **Confluent.Kafka 2.6.0** (pinned in `examples/csharp/Directory.Packages.props`).
- A running KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` (default
  `localhost:9092`) — **start with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1), with
  the transaction coordinator enabled.

## How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
dotnet run --project transactions/eos-commit-abort
```

## Expected Output

```
[*] Created topic 'kafka-ex-transactions-eos-commit-abort'
[*] InitTransactions ok (transactional.id='cs-eos-<uuid>')
[x] committed txn with 2 records (committed-A, committed-B)
[x] aborted txn with 2 records (aborted-X, aborted-Y)
[v] [read_committed] saw 'committed-A'
[v] [read_committed] saw 'committed-B'
[*] Cleaned up topic 'kafka-ex-transactions-eos-commit-abort'
[ok] EOS verified: committed records visible, aborted records absent under read_committed (KIP-890 V1)
```

## What's Happening

The producer enables idempotence and sets a `transactional.id`, then
`InitTransactions` runs the `InitProducerId` handshake with the transaction
coordinator. One transaction commits two records; a second produces two records and
**aborts**. A `read_committed` consumer reads the log and sees only `committed-A` /
`committed-B` — the aborted records are filtered out.

> **KIP-890 V1 ceiling (gotcha #9).** The connector implements the transaction
> coordinator at the KIP-890 **V1** level. In narrow races a same-epoch "zombie"
> producer can still be admitted — this is an **upstream-shared protocol ceiling,
> not a connector defect** and not a failure of this example. `(PID, epoch)` fencing
> still surfaces `INVALID_PRODUCER_EPOCH(47)` / `PRODUCER_FENCED(90)` for a fenced
> zombie. This example does not claim guarantees beyond spec §2.
>
> **Gotcha #7 — no `/` in `transactional.id`** (→ `INVALID_REQUEST(42)`); a
> `cs-eos-<uuid>` shape is used. **Gotcha #8 — transactional offset-commit needs
> Group WRITE** when combining consume+produce.

This mirrors the connector's txn path (AddPartitionsToTxn / EndTxn) in `connectors/kafka/`.

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|----------|----------------|------------------|----------------|------------------|-------------|-------------|------------------|
| InitProducerId, AddPartitionsToTxn, Produce, EndTxn (commit\|abort), Fetch | `read_committed` consume; idempotent `acks=All` produce | `kafka-ex-transactions-eos-commit-abort` / 1 | `cs-eos-read-<uuid>`, `read_committed` | committed records visible; aborted filtered | none | CRC32 (librdkafka) | **gotcha #9** (KIP-890 V1 ceiling), **#7** (no `/` in `transactional.id`), **#8** (txn offset-commit needs Group WRITE); fencing → `INVALID_PRODUCER_EPOCH(47)`/`PRODUCER_FENCED(90)` |

## Related Examples

Same variant in the other languages:

- **Go** — [`../../../go/transactions/eos-commit-abort`](../../../go/transactions/eos-commit-abort)
- **Python** — [`../../../python/transactions/eos_commit_abort`](../../../python/transactions/eos_commit_abort)
- **Java** — [`../../../java/transactions/eos-commit-abort`](../../../java/transactions/eos-commit-abort)
- **JS/TS** — [`../../../javascript/transactions/eos-commit-abort`](../../../javascript/transactions/eos-commit-abort)
- **Ruby** — [`../../../ruby/transactions/eos_commit_abort`](../../../ruby/transactions/eos_commit_abort)
- **Rust** — [`../../../rust/transactions/eos-commit-abort`](../../../rust/transactions/eos-commit-abort)

Docs: [`../../../../docs/concepts/transactions-eos.md`](../../../../docs/concepts/transactions-eos.md),
[`../../../../docs/guides/transactions-eos.md`](../../../../docs/guides/transactions-eos.md)

---

> **Auth:** the connector default is no authentication. SASL/TLS setup lives in
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
