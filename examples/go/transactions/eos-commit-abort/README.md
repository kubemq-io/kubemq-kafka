# Go — Kafka: Transactions EOS Commit / Abort

Exactly-once-semantics transactions against the KubeMQ Kafka connector:
`InitProducerId → AddPartitionsToTxn → txn Produce → EndTxn(commit|abort)`, then a
`read_committed` consumer asserts the committed record is visible and the aborted
one is absent.

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
go run ./transactions/eos-commit-abort
```

## Expected Output

```
[kubemq-kafka] transactions/eos-commit-abort | bootstrap=localhost:9092 partitioner=murmur2(franz-go)
CreateTopic: kafka-ex-txn-eos-<8hex> (partitions=1) txn.id=kafka-ex-txn-id-<8hex>
InitProducerId: pid=<pid> epoch=0
Txn #1: produced "committed-order-1", EndTransaction(commit)
Txn #2: produced "aborted-order-2", EndTransaction(abort)
read_committed: saw only "committed-order-1" (aborted "aborted-order-2" absent)
DeleteTopic: ok
PASS: committed visible + aborted absent under read_committed (KIP-890 V1)
```

> The topic and `transactional.id` are suffixed with random hex so concurrent runs
> across the language examples never collide. The `transactional.id` uses `-`, never
> `/` (gotcha #7).

## What's Happening

The program opens a transactional producer (`TransactionalID` set), which triggers
`InitProducerId` to fence a `(PID, epoch)`. Transaction #1 begins, produces
`committed-order-1`, and `EndTransaction(commit)`. Transaction #2 begins, produces
`aborted-order-2`, and `EndTransaction(abort)`. A consumer opened with
`FetchIsolationLevel(read_committed)` then reads the topic and asserts it sees
**exactly** the committed record and never the aborted one. An aborted record
visible under `read_committed` fails the process.

The wire flow is `InitProducerId → AddPartitionsToTxn → Produce → EndTxn(commit) →
AddPartitionsToTxn → Produce → EndTxn(abort) → Fetch(read_committed)`, mirroring
connector behavior in `connectors/kafka/txn_rpcs_test.go`.

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| InitProducerId(22), AddPartitionsToTxn(24), Produce(0), EndTxn(26), Fetch(1) | acks=all; **read_committed** consumer | 1 partition | none | committed records visible; aborted skipped under read_committed | none | murmur2 (franz-go) | **gotcha #7** (`/` in `transactional.id` → `INVALID_REQUEST`), **gotcha #8** (txn offset-commit needs Group WRITE), **gotcha #9** (KIP-890 V1 EOS ceiling — no TV2) |

## Related Examples

- Same variant in other languages:
  `../../../python/transactions/eos_commit_abort`,
  `../../../javascript/transactions/eos-commit-abort`,
  `../../../java/transactions/eos-commit-abort`,
  `../../../csharp/transactions/eos-commit-abort`,
  `../../../ruby/transactions/eos_commit_abort`,
  `../../../rust/transactions/eos-commit-abort`.
- Docs: `../../../../docs/guides/transactions-eos.md`,
  `../../../../docs/concepts/transactions-eos.md`.
- Related: [`../read-committed`](../read-committed).

> **Gotcha #9 — KIP-890 V1 EOS ceiling.** The connector implements the V1
> transaction protocol (no Transaction Verification V2 / TV2). This is an
> upstream-shared limitation, not a defect — EOS works, but the newer
> hanging-transaction fencing of TV2 is not available. **Gotcha #7:** a `/` in the
> `transactional.id` is rejected (`INVALID_REQUEST(42)`); this example uses `-`.

> Auth: this example uses the no-auth default posture. Runs with no SASL by default
> on a stock dev broker; for SASL/PLAIN + SCRAM (and mTLS principal derivation) see
> [`../../security/sasl-plain-scram`](../../security/sasl-plain-scram) +
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
