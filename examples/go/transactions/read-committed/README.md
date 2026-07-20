# Go — Kafka: Transactions Read-Committed

The `read_committed` isolation contract against the KubeMQ Kafka connector: while a
transaction is open the Last-Stable-Offset (LSO) sits below the high-water mark, a
`read_committed` consumer never sees the aborted record, and `ListOffsets(latest,
read_committed)` returns the LSO.

## Prerequisites

- Go 1.24+ and `github.com/twmb/franz-go v1.21.4` (pinned in `../../go.mod`).
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). **The connector is DISABLED by default — start the
  broker with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1). For any non-same-host
  client, also set `CONNECTORS_KAFKA_ADVERTISED_HOST` or the client connects then
  hangs (gotcha #2).

## How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
go run ./transactions/read-committed
```

## Expected Output

```
[kubemq-kafka] transactions/read-committed | bootstrap=localhost:9092 partitioner=murmur2(franz-go)
CreateTopic: kafka-ex-txn-rc-<8hex> (partitions=1) txn.id=kafka-ex-txn-rcid-<8hex>
Txn #1: produced "committed-rc-1", committed
Txn #2: produced "aborted-rc-2", LEFT OPEN
While txn open: LSO(read_committed)=1 HWM(read_uncommitted)=2
Txn #2: aborted
After abort: LSO=2 HWM=2
read_committed: delivered only "committed-rc-1" (aborted never seen)
DeleteTopic: ok
PASS: LSO < HWM while open, aborted never delivered under read_committed
```

> The topic and `transactional.id` are suffixed with random hex so concurrent runs
> across the language examples never collide. Exact LSO/HWM values depend on the
> connector's control-record accounting; the invariants (`LSO < HWM` while open,
> `LSO == HWM` after abort) are what the example asserts.

## What's Happening

Transaction #1 produces `committed-rc-1` and commits. Transaction #2 produces
`aborted-rc-2` and is **left open**. While it is open, the program queries
`ListOffsets` at both isolation levels and asserts `LSO(read_committed) <
HWM(read_uncommitted)` — the open transaction blocks the stable offset from
advancing past its records. It then aborts #2 and asserts `LSO == HWM` (the stable
offset catches up). Finally a `read_committed` consumer reads the topic and is
asserted to deliver **only** the committed record — the aborted one is filtered
client-side via the batch's `AbortedTransactions` list and is never surfaced. Any
LSO/HWM inversion or an aborted record delivered fails the process.

The wire flow is `InitProducerId → Produce(txn#1) → EndTxn(commit) → Produce(txn#2,
open) → ListOffsets(read_committed vs read_uncommitted) → EndTxn(abort) →
Fetch(read_committed)`, mirroring connector behavior in
`connectors/kafka/txn_rpcs_test.go`.

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| InitProducerId(22), Produce(0), EndTxn(26), ListOffsets(2), Fetch(1) | acks=all; **read_committed** consumer | 1 partition | none | LSO = last stable offset ≤ HWM; `ListOffsets(latest, read_committed)` returns LSO | none | murmur2 (franz-go) | **gotcha #12** — `read_committed` filtering is **client-side** (`AbortedTransactions`), no server-side record filter; **gotcha #9** — KIP-890 V1 ceiling |

## Related Examples

- Same variant in other languages:
  `../../../python/transactions/read_committed`,
  `../../../javascript/transactions/read-committed`,
  `../../../java/transactions/read-committed`,
  `../../../csharp/transactions/read-committed`,
  `../../../ruby/transactions/read_committed`,
  `../../../rust/transactions/read-committed`.
- Docs: `../../../../docs/concepts/transactions-eos.md`.
- Related: [`../eos-commit-abort`](../eos-commit-abort).

> **Gotcha #12 — `read_committed` filtering is client-side.** The broker ships each
> fetched batch together with its `AbortedTransactions` metadata; the **client**
> drops aborted records — there is no server-side per-record filter. A client that
> ignores that metadata would surface aborted records, so use a real Kafka client
> and never hand-roll the fetch path. **Gotcha #9:** V1 EOS (no TV2) still applies.

> Auth: this example uses the no-auth default posture. Runs with no SASL by default
> on a stock dev broker; for SASL/PLAIN + SCRAM (and mTLS principal derivation) see
> [`../../security/sasl-plain-scram`](../../security/sasl-plain-scram) +
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
