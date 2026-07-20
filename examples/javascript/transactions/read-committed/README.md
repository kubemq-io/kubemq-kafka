# javascript — Kafka: Read-Committed Isolation

Observe `read_committed` isolation and the Last Stable Offset (LSO): while a transaction is open the
HWM advances past uncommitted records but the LSO does not, and once the open transaction is aborted a
`read_committed` consumer delivers only the committed records — never the aborted ones. The Kafka topic
`kafka-ex-txn-rc` maps onto the Events-Store channel `kafka.kafka-ex-txn-rc`.

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
npx tsx transactions/read-committed/index.ts
```

## Expected Output

```
Connecting to KubeMQ Kafka connector at localhost:9092 (topic "kafka-ex-txn-rc")
Committed txn -> rc-committed-1, rc-committed-2
Opened txn (uncommitted) -> rc-open-1, rc-open-2 (HWM advances, LSO does not)
HWM (with open txn) = 4 (> committed count 2 because uncommitted records advanced the HWM)
Aborted the open txn -> rc-open-* must never be delivered under read_committed
read_committed consumer saw: [rc-committed-1, rc-committed-2]

read_committed proven: HWM > LSO while a txn is open; aborted records never delivered (EOS V1; KIP-890 out of scope)
```

## What's Happening

- A committed transaction writes `rc-committed-1`/`rc-committed-2`.
- A second transaction is left **open**: its records advance the HWM (to 4) but not the LSO — a
  `read_committed` consumer cannot advance past an in-flight transaction.
- The open transaction is aborted, then a `read_committed` consumer (`readUncommitted: false`) reads
  the topic: it delivers only the two committed records. `read_committed` filtering is **client-side**
  via the `AbortedTransactions` list (gotcha #12), so the aborted records are dropped by the client.
- Mirrors connector behavior in `connectors/kafka/` (LSO / AbortedTransactions; see
  `connectors/kafka/txn_rpcs_test.go`).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| InitProducerId (22), AddPartitionsToTxn (24), Produce (0), EndTxn (26), Fetch (1), ListOffsets (2) | consumer read_committed | `kafka-ex-txn-rc` / 1 partition | ephemeral verify group | LSO < HWM while a txn is open; `read_committed` end = LSO | none | murmur2 (DefaultPartitioner) | 🟡 EOS V1 — **KIP-890 out of scope** (gotcha #9); `read_committed` filtering is client-side (gotcha #12) |

## Related Examples

- Same variant, other languages: [`../../../go/transactions/read-committed`](../../../go/transactions/read-committed),
  [`../../../java/transactions/read-committed`](../../../java/transactions/read-committed),
  [`../../../csharp/transactions/read-committed`](../../../csharp/transactions/read-committed),
  [`../../../rust/transactions/read-committed`](../../../rust/transactions/read-committed),
  [`../../../python/transactions/read_committed`](../../../python/transactions/read_committed),
  [`../../../ruby/transactions/read_committed`](../../../ruby/transactions/read_committed).
- Doc: [`../../../../docs/guides/transactions-eos.md`](../../../../docs/guides/transactions-eos.md),
  [`../../../../docs/concepts/transactions-eos.md`](../../../../docs/concepts/transactions-eos.md).
- Next: [`../../security/sasl-plain-scram/`](../../security/sasl-plain-scram/).

> **Gotcha #12 — `read_committed` filtering is client-side.** The broker returns records plus an
> `AbortedTransactions` list; the *client* filters aborted records out. There is no server-side record
> filter, so the isolation guarantee depends on the consumer honoring `read_committed`.

> **Gotcha #9 — KIP-890 V1 EOS ceiling.** EOS **V1** semantics only; KIP-890 / TransactionV2 improvements
> are out of scope.

> **Auth.** The dev default is no SASL over plain TCP (`:9092`). For a secured connector, configure
> SASL/PLAIN or SASL/SCRAM (and TLS on `:9093`). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
