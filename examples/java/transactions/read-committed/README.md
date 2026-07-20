# java — Kafka: Transactions — Read Committed

While a transaction is open, a `read_committed` consumer sees only the last stable
offset (LSO) — it never delivers the in-flight (later aborted) records, and the LSO
stays below the high-water-mark until the txn resolves.

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
mvn -q exec:exec -Dexec.mainClass=io.kubemq.examples.kafka.transactions.readcommitted.Main
```

## Expected Output

```
bootstrap.servers = localhost:9092
CreateTopics 'kafka-ex-txn-readcommitted-java' (1 partition)
Committed baseline of 3 records
Opened a txn with 4 uncommitted (flushed) records
HWM(read_uncommitted)=7 LSO(read_committed)=3
read_committed saw: [base-0, base-1, base-2]
abortTransaction() -> the 4 records are discarded
OK: read_committed never saw the open/aborted txn; LSO < HWM while open
```

The high-water-mark advances to 7 as the 4 uncommitted records are flushed to the
log, but the last stable offset (what `read_committed` can see) stays at 3 while the
txn is open. The consumer delivers only the 3 committed baseline records, never the
in-flight ones — which are then aborted.

## What's Happening

The program commits a 3-record baseline, then opens a transaction and produces
(flushes) 4 records **without committing**. Using `Admin.listOffsets`, it reads the
`read_uncommitted` HWM (=7, includes the flushed records) and the `read_committed`
LSO (=3, stops before the open txn). A `read_committed` consumer reads the topic and
asserts it sees only the 3 baseline records — the open transaction is invisible.
Finally the txn is aborted, discarding the 4 records permanently.

> **`read_committed` filtering is client-side.** The consumer fetches the
> `AbortedTransactions` list with the records and filters aborted batches locally —
> there is no server-side record filter. A `read_committed` `listOffsets(latest)`
> returns the LSO, not the HWM. `UNSTABLE_OFFSET_COMMIT(88)` may surface transiently
> while a txn is open.

The Kafka wire flow is `InitProducerId(22) → AddPartitionsToTxn(24) → Produce(0,
flush) → ListOffsets(2, read_committed=LSO) → Fetch(1, read_committed) →
EndTxn(26, abort)`, mirroring connector behavior in `connectors/kafka/`
(`txn_rpcs_test.go`).

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| InitProducerId(22), AddPartitionsToTxn(24), Produce(0), ListOffsets(2), Fetch(1), EndTxn(26) | **read_committed** consume | 1 partition | fresh (throwaway) group | LSO < HWM while a txn is open; aborted never delivered | none | murmur2 | client-side `AbortedTransactions` filtering (gotcha #12); KIP-890 V1 ceiling (gotcha #9); `UNSTABLE_OFFSET_COMMIT(88)` may surface |

## Related Examples

- Same variant in the other 6 languages: [`../../../go/transactions/read-committed`](../../../go/transactions/read-committed),
  [`../../../python/transactions/read_committed`](../../../python/transactions/read_committed),
  [`../../../javascript/transactions/read-committed`](../../../javascript/transactions/read-committed),
  [`../../../csharp/transactions/read-committed`](../../../csharp/transactions/read-committed),
  [`../../../ruby/transactions/read_committed`](../../../ruby/transactions/read_committed),
  [`../../../rust/transactions/read-committed`](../../../rust/transactions/read-committed).
- Docs: [`../../../../docs/guides/transactions-eos.md`](../../../../docs/guides/transactions-eos.md),
  [`../../../../docs/concepts/transactions-eos.md`](../../../../docs/concepts/transactions-eos.md).
- Related: [`../eos-commit-abort`](../eos-commit-abort).

> **Gotcha #12 — `read_committed` filtering is client-side.** The connector returns
> the `AbortedTransactions` list and the client filters; there is no server-side
> record filter. **Gotcha #9** — the KIP-890 V1 EOS ceiling applies here too.

> **Auth.** This example uses the connector's no-auth default posture
> (SHARED-CONVENTIONS §4.3). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md)
> for SASL/PLAIN + SCRAM and TLS/mTLS.
