# Transactions & EOS (Guide)

This guide is the hands-on companion to the [transactions & EOS concept
page](../concepts/transactions-eos.md). It walks the transactional producer and the `read_committed`
consumer, the Group-WRITE requirement, and restates the **KIP-890 V1 ceiling** — which every EOS
artifact in this repo cites.

> **Scope: Transactions V1 (🟡 Partial).** A real, working transactional path — but below TV2, with
> the precisely-known soundness ceiling documented at the end of this page. Do not claim
> strictly-once beyond what V1 provides. See [../reference/capabilities.md](../reference/capabilities.md).

## The transactional flow

```
InitProducerId  →  AddPartitionsToTxn  →  (transactional Produce …)  →  EndTxn(commit | abort)
```

1. **`InitProducerId`** (key 22/24) — set `transactional.id` on the producer; the connector
   allocates a `(PID, epoch)` and fences older instances of the same `transactional.id`.
2. **`AddPartitionsToTxn`** (key 24) — enroll each partition the transaction will write.
3. **Transactional `Produce`** (key 0) — append records tagged with `(PID, epoch)`.
4. **`EndTxn(commit)`** (key 26) — write an in-log **COMMIT** marker (records become visible to
   `read_committed`); **`EndTxn(abort)`** writes an **ABORT** marker (records are hidden).

For the read-process-write loop, stage input offsets with `AddOffsetsToTxn` + `TxnOffsetCommit`
(keys 25 / 28) — they are materialized on commit and discarded on abort.

The `transactions/eos-commit-abort` example runs the full flow and proves that committed records are
visible and aborted records are absent under `read_committed`.

## The `read_committed` consumer

A consumer set to `isolation.level=read_committed`:

- never sees records from an **aborted** transaction;
- never sees records from an **open** (not-yet-committed) transaction — it can advance only up to
  the **Last Stable Offset (LSO)**, which sits below the high-water mark while a transaction is open;
- `ListOffsets(latest, read_committed)` returns the **LSO**, not the HWM.

> **Gotcha #12 — `read_committed` filtering is client-side.** The connector returns records plus the
> `AbortedTransactions` metadata, and the **client library** drops aborted records — there is no
> server-side record filter. A conformant Kafka client does this automatically. See
> [../concepts/transactions-eos.md](../concepts/transactions-eos.md).

The `transactions/read-committed` example proves the consumer never sees aborted records and that
the LSO stays below the HWM while a transaction is open.

## `(PID, epoch)` fencing

Every transactional request carries `(PID, epoch)`. A stale producer instance is fenced:

- out-of-date epoch → `INVALID_PRODUCER_EPOCH(47)`;
- superseded producer → `PRODUCER_FENCED(90)`.

Fencing across **different** epochs works — the normal zombie-fencing case. The residual is only the
same-epoch case described in the ceiling below.

## Two operational requirements

> **Gotcha #8 — txn offset-commit requires Group WRITE.** `TxnOffsetCommit` requires **WRITE** on
> the group — stricter than stock Kafka (which needs only READ; decision D141). Grant the
> transactional consumer WRITE on its group, or the commit is denied. See
> [security-sasl-tls.md](security-sasl-tls.md).

> **Gotcha #7 — `/` is rejected in `transactional.id`.** A `transactional.id` containing `/` maps to
> `INVALID_TRANSACTIONAL_ID`, surfaced as `INVALID_REQUEST(42)`. Keep transactional ids in the safe
> charset. See [../reference/error-codes.md](../reference/error-codes.md).

## Not-yet: transaction-admin RPCs

The transaction-**admin** RPCs are 🔴 not-yet (deferred): `WriteTxnMarkers(27)`,
`DescribeProducers(61)`, `DescribeTransactions(65)`, `ListTransactions(66)`. There is **no CLI
`--abort`** for a wedged transaction — a stuck transaction is bounded by the
`transaction.timeout.ms` reaper instead. Plan your `transaction.timeout.ms` accordingly.

## The KIP-890 V1 ceiling (never overstate)

> **Known limitation — the V1 (no TV2) transactional soundness ceiling (KIP-890).** KubeMQ's V1
> transaction implementation does **not** bump the producer epoch on every `EndTxn` (it pins below
> the TV2 protocol versions). Consequence: a zombie produce from the **same** producer epoch,
> delayed past its own `EndTxn`, can still be admitted into that producer's **next** transaction.
>
> This is the **upstream-shared** KIP-890 ceiling — **every pre-TV2 Kafka deployment has it** — and
> it is **NOT a KubeMQ defect**. It is explicitly **not** counted as a soak or conformance failure:
> the burn-in EOS worker (`transactions_eos.go`) excludes the same-epoch residual from its
> `max_eos_violations: 0` gate. The exhaustive multi-client EOS conformance matrix and real-cluster
> failover LSO-continuity soak are the deferred exit gate.

**Practical guidance:** rely on cross-epoch fencing (which works); do **not** design a system that
depends on same-epoch, post-`EndTxn` fencing; and do not advertise strictly-once beyond what TV2
would deliver. Every EOS example and the burn-in worker restate this ceiling.

## Examples

| Variant | Family | What it shows |
|---------|--------|---------------|
| `transactions/eos-commit-abort` | transactions | Full flow → `EndTxn(commit\|abort)`; committed visible / aborted absent under `read_committed`; **cites the KIP-890 note** |
| `transactions/read-committed` | transactions | `read_committed` never delivers aborted; `ListOffsets(latest, read_committed)` = LSO < HWM while open |

## Error quick reference

| Trigger | Kafka error |
|---------|-------------|
| Out-of-date producer epoch | `INVALID_PRODUCER_EPOCH(47)` |
| Superseded producer | `PRODUCER_FENCED(90)` |
| `/` in `transactional.id` | `INVALID_TRANSACTIONAL_ID` → `INVALID_REQUEST(42)` |
| `OffsetFetch RequireStable` while a txn is open | `UNSTABLE_OFFSET_COMMIT(88)` |
| `TxnOffsetCommit` without Group WRITE | `GROUP_AUTHORIZATION_FAILED` |

## See Also

- [../concepts/transactions-eos.md](../concepts/transactions-eos.md) — the EOS concept, in-log markers, and the ceiling in depth.
- [security-sasl-tls.md](security-sasl-tls.md) — the Group-WRITE requirement.
- [../reference/capabilities.md](../reference/capabilities.md) — Transactions V1 stated as 🟡; the 🔴 txn-admin RPCs.
- [../reference/error-codes.md](../reference/error-codes.md) — the transaction error codes.

## Source Code

`connectors/kafka/` transaction RPCs (`txn_rpcs_test.go`, `txnlog_test.go`, `txnreaper_test.go`,
`coordinatorproxy_txn_test.go`, `findcoordinator_txn_test.go`, `authz_txn_test.go`), producer id /
epoch (`initproducerid_test.go`, `pidblock_test.go`, `epoch_fake_test.go`).
