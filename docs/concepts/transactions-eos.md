# Transactions & Exactly-Once Semantics (EOS)

> **Scope: Transactions V1 (🟡 Partial).** The connector implements Kafka transactions at **V1**
> (WP-9.1) with in-log COMMIT/ABORT markers (WP-9.2) and `read_committed` serving (WP-9.3). It is a
> **real, working transactional path** — but it sits below TV2, and that has a precisely-known
> soundness ceiling (see [the KIP-890 ceiling](#the-kip-890-v1-ceiling-never-overstate) below). This
> page and every EOS artifact in this repo cite that ceiling.

## Concept

Kafka **exactly-once semantics** let a producer write a batch of records atomically: consumers
reading with `read_committed` see **all** of a committed transaction's records or **none** of an
aborted one. The connector supports the full transactional flow:

```
InitProducerId  →  AddPartitionsToTxn  →  (transactional Produce …)  →  EndTxn(commit | abort)
   (key 22/24)        (key 24)                 (key 0, txnal)              (key 26)
```

- **`InitProducerId`** allocates a producer id (PID) and epoch, fencing older instances of the same
  `transactional.id`.
- **`AddPartitionsToTxn`** enrolls each partition the transaction will write.
- **Transactional `Produce`** appends records tagged with the `(PID, epoch)`.
- **`EndTxn(commit)`** writes an in-log **COMMIT** marker; **`EndTxn(abort)`** writes an **ABORT**
  marker. `read_committed` consumers then include or exclude those records accordingly.

Input offsets are staged with `AddOffsetsToTxn` + `TxnOffsetCommit` (keys 25 / 28) and materialized
on commit / discarded on abort — the read-process-write consume-transform-produce loop.

## `(PID, epoch)` fencing

Every transactional record and control request carries `(PID, epoch)`. A stale producer instance is
fenced:

- A request from an out-of-date epoch → `INVALID_PRODUCER_EPOCH(47)`.
- A producer that has been superseded → `PRODUCER_FENCED(90)`.

This is the mechanism that stops a zombie producer from corrupting a live transaction — subject to
the V1 ceiling below.

## `read_committed` is client-side filtering

The connector serves `read_committed` by returning the records **plus** the transaction's
`AbortedTransactions` list, and the **client library filters aborted records out**. There is no
server-side record-level filter — the connector does not strip aborted records from the fetch
response; it tells the client which producer-id/offset ranges to drop.

> **Gotcha #12 — `read_committed` filtering is client-side.** Aborted records are excluded by the
> **client**, using the `AbortedTransactions` metadata the connector returns, not removed server-
> side. A conformant Kafka client does this automatically; the point is that the filtering
> boundary is the client, and the Last Stable Offset (LSO) governs how far a `read_committed`
> consumer may advance. `ListOffsets(latest, read_committed)` returns the **LSO**, which sits below
> the high-water mark while a transaction is open.

## The KIP-890 V1 ceiling (never overstate)

> **Known limitation — the V1 (no TV2) transactional soundness ceiling (KIP-890).** KubeMQ's V1
> transaction implementation does **not** bump the producer epoch on every `EndTxn` (it pins below
> the TV2 protocol versions). The consequence: a zombie produce from the **same** producer epoch,
> delayed past its own `EndTxn`, can still be admitted into that producer's **next** transaction.
>
> This is the **upstream-shared** KIP-890 ceiling — **every pre-TV2 Kafka deployment has it** — and
> it is **NOT a KubeMQ defect**. It is explicitly **not** counted as a soak or conformance failure:
> the burn-in EOS worker excludes the same-epoch residual from its `max_eos_violations: 0` gate. The
> exhaustive multi-client EOS conformance matrix and real-cluster failover LSO-continuity soak are
> the exit gate for closing this (deferred).

Practically: fencing across **different** epochs works (the normal zombie-fencing case); the
residual is only the **same-epoch, post-`EndTxn`, delayed** produce. Do not design a system that
depends on same-epoch post-commit fencing, and do not claim strictly-once beyond what TV2 would
provide.

## Not-yet: transaction-admin RPCs

The transaction-**admin** RPCs are 🔴 not-yet (deferred): `WriteTxnMarkers(27)`,
`DescribeProducers(61)`, `DescribeTransactions(65)`, `ListTransactions(66)`. There is **no CLI
`--abort`** for a wedged transaction; a stuck transaction is instead bounded by the
`transaction.timeout.ms` reaper. See [reference/capabilities.md](../reference/capabilities.md).

## Two operational gotchas

- **Gotcha #7 — `/` is rejected in `transactional.id`.** A `transactional.id` containing `/` maps
  to `INVALID_TRANSACTIONAL_ID`, surfaced as `INVALID_REQUEST(42)`. Keep transactional ids in the
  safe charset. See [reference/error-codes.md](../reference/error-codes.md).
- **Gotcha #8 — txn offset-commit requires Group WRITE.** Committing consumed offsets inside a
  transaction (`TxnOffsetCommit`) requires **WRITE** on the group — stricter than stock Kafka
  (decision D141). See [../guides/transactions-eos.md](../guides/transactions-eos.md).

## Examples

| Variant | Family | What it shows |
|---------|--------|---------------|
| `transactions/eos-commit-abort` | transactions | Full flow `InitProducerId`→`AddPartitionsToTxn`→txn `Produce`→`EndTxn(commit\|abort)`; committed visible / aborted absent under `read_committed`; **cites the KIP-890 note** |
| `transactions/read-committed` | transactions | `read_committed` never delivers aborted records; `ListOffsets(latest, read_committed)` = LSO < HWM while a txn is open |

## See Also

- [../guides/transactions-eos.md](../guides/transactions-eos.md) — the transactional producer + `read_committed` consumer guide, Group-WRITE requirement, and the ceiling restated.
- [../reference/capabilities.md](../reference/capabilities.md) — Transactions V1 stated as 🟡; the KIP-890 ceiling and the 🔴 txn-admin RPCs.
- [../reference/error-codes.md](../reference/error-codes.md) — `INVALID_PRODUCER_EPOCH(47)`, `PRODUCER_FENCED(90)`, `INVALID_TRANSACTIONAL_ID`, `UNSTABLE_OFFSET_COMMIT(88)`.

## Grounding

`txn_rpcs_test.go`, `txnlog_test.go`, `txnreaper_test.go`, `coordinatorproxy_txn_test.go`,
`findcoordinator_txn_test.go`, `authz_txn_test.go`, `initproducerid_test.go` in
`connectors/kafka/`.
