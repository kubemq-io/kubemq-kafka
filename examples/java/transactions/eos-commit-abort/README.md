# java — Kafka: Transactions — EOS Commit / Abort

A transactional producer commits one batch and aborts another; a `read_committed`
consumer proves the committed records are **visible** and the aborted records are
**absent** — exactly-once semantics end to end.

## Prerequisites

- JDK 21+ and Maven 3.9+.
- `org.apache.kafka:kafka-clients 3.9.0` (pinned in `../../pom.xml`).
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). **Connector DISABLED by default — start with
  `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1); set `CONNECTORS_KAFKA_ADVERTISED_HOST`
  for remote clients (gotcha #2).

## How to Run

From `examples/java/`:

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
mvn -q compile
mvn -q exec:exec -Dexec.mainClass=io.kubemq.examples.kafka.transactions.eoscommitabort.Main
```

## Expected Output

```
bootstrap.servers = localhost:9092
CreateTopics 'kafka-ex-txn-eos-java' (1 partition)
initTransactions() -> transactional.id=kafka-ex.txn-eos.java
commitTransaction() -> 4 records
abortTransaction() -> 3 records discarded
read_committed saw: [committed-0, committed-1, committed-2, committed-3]
OK: committed txn visible, aborted txn absent (read_committed)
```

The `read_committed` consumer sees exactly the 4 committed records and none of the 3
aborted ones.

## What's Happening

The transactional producer sets a `transactional.id`, calls `initTransactions()`
(triggering `InitProducerId`), then runs two transactions: the first produces 4
records and `commitTransaction()`s; the second produces 3 records and
`abortTransaction()`s. A consumer with `isolation.level=read_committed` reads the
topic and asserts it delivers only the 4 committed records. Under the hood a
transaction is `InitProducerId → AddPartitionsToTxn → (txn Produce) → EndTxn`, and
commit/abort are control markers on the log.

> **`transactional.id` must not contain `/`.** The connector rejects a `/` in the
> id with `INVALID_REQUEST(42)` — this example uses dots and hyphens
> (`kafka-ex.txn-eos.java`). Transactional offset-commit additionally requires
> Group WRITE permission (stricter than upstream Kafka).

The Kafka wire flow is `InitProducerId(22) → AddPartitionsToTxn(24) → Produce(0) →
EndTxn(26, commit|abort) → Fetch(1, read_committed)`, mirroring connector behavior in
`connectors/kafka/` (`txn_rpcs_test.go`).

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| InitProducerId(22), AddPartitionsToTxn(24), Produce(0), EndTxn(26), Fetch(1) | acks=all; **read_committed** consume | 1 partition | fresh (throwaway) group | committed visible / aborted absent | none | murmur2 | **KIP-890 V1 EOS ceiling** (gotcha #9); `/` in `transactional.id` → `INVALID_REQUEST(42)` (gotcha #7); txn offset-commit needs Group WRITE (gotcha #8) |

## Related Examples

- Same variant in the other 6 languages: [`../../../go/transactions/eos-commit-abort`](../../../go/transactions/eos-commit-abort),
  [`../../../python/transactions/eos_commit_abort`](../../../python/transactions/eos_commit_abort),
  [`../../../javascript/transactions/eos-commit-abort`](../../../javascript/transactions/eos-commit-abort),
  [`../../../csharp/transactions/eos-commit-abort`](../../../csharp/transactions/eos-commit-abort),
  [`../../../ruby/transactions/eos_commit_abort`](../../../ruby/transactions/eos_commit_abort),
  [`../../../rust/transactions/eos-commit-abort`](../../../rust/transactions/eos-commit-abort).
- Docs: [`../../../../docs/guides/transactions-eos.md`](../../../../docs/guides/transactions-eos.md),
  [`../../../../docs/concepts/transactions-eos.md`](../../../../docs/concepts/transactions-eos.md).
- Next: [`../read-committed`](../read-committed).

> **Gotcha #9 — KIP-890 V1 EOS ceiling.** The connector implements the KIP-890 V1
> transaction protocol (no TV2). This is an upstream-shared ceiling, not a defect: a
> same-epoch zombie residual is expected behavior, not a failure. **Gotcha #7** — no
> `/` in `transactional.id`. **Gotcha #8** — transactional offset-commit requires
> Group WRITE.

> **Auth.** This example uses the connector's no-auth default posture
> (SHARED-CONVENTIONS §4.3). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md)
> for SASL/PLAIN + SCRAM and TLS/mTLS.
