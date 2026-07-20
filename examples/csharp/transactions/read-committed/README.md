# C# — Kafka: Read-Committed & the LSO

Focus on the **consumer** side of EOS: `read_committed` never sees aborted records,
and while a transaction is open the read_committed high offset (the **Last Stable
Offset**, LSO) is pinned below the high-watermark.

## Prerequisites

- .NET SDK **8.0**
- **Confluent.Kafka 2.6.0** (pinned in `examples/csharp/Directory.Packages.props`).
- A running KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` (default
  `localhost:9092`) — **start with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1), with
  the transaction coordinator enabled.

## How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
dotnet run --project transactions/read-committed
```

## Expected Output

```
[*] Created topic 'kafka-ex-transactions-read-committed'
[x] committed txn #1 (2 records)
[*] opened txn #2 and produced 'rc-open-3' (still uncommitted)
[v] [read_committed] LSO (high) while txn open = 2
[*] aborted txn #2
[v] [read_committed] saw 'rc-committed-1'
[v] [read_committed] saw 'rc-committed-2'
[v] [read_uncommitted] saw 3 record(s) incl. aborted = True
[*] Cleaned up topic 'kafka-ex-transactions-read-committed'
[ok] read_committed never sees aborted records; LSO pinned below HWM while a txn is open (KIP-890 V1)
```

## What's Happening

Transaction #1 commits two records. Transaction #2 opens and produces `rc-open-3`
but does not commit — so the **read_committed LSO stays at 2** (the committed
boundary), even though the physical high-watermark is 3. After #2 is **aborted**, a
read_committed consumer reads only the two committed records. A read_uncommitted
consumer, by contrast, ships all three including the aborted one — proving that
`read_committed` filtering is **client-side**.

> **Gotcha #12 — client-side filtering.** The broker sends aborted records plus an
> `AbortedTransactions` list; the **consumer** drops them. `read_committed` is a
> client behavior, not a server-side redaction.
>
> **KIP-890 V1 ceiling (gotcha #9).** At V1 a same-epoch zombie may be admitted in
> narrow races — upstream-shared, not a defect. This example asserts only the V1
> read_committed guarantee. See `docs/concepts/transactions-eos.md`.

This mirrors the connector's LSO / AbortedTransactions handling in `connectors/kafka/`.

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|----------|----------------|------------------|----------------|------------------|-------------|-------------|------------------|
| InitProducerId, AddPartitionsToTxn, Produce, EndTxn, ListOffsets (read_committed=LSO), Fetch | `read_committed` vs `read_uncommitted` consume | `kafka-ex-transactions-read-committed` / 1 | `cs-rc-read-<uuid>` (RC), `cs-ru-read-<uuid>` (RU) | `read_committed` high = LSO ≤ HWM while txn open | none | CRC32 (librdkafka) | **gotcha #12** (client-side `AbortedTransactions` filter), **#9** (KIP-890 V1); RC never sees aborted, RU sees all |

## Related Examples

Same variant in the other languages:

- **Go** — [`../../../go/transactions/read-committed`](../../../go/transactions/read-committed)
- **Python** — [`../../../python/transactions/read_committed`](../../../python/transactions/read_committed)
- **Java** — [`../../../java/transactions/read-committed`](../../../java/transactions/read-committed)
- **JS/TS** — [`../../../javascript/transactions/read-committed`](../../../javascript/transactions/read-committed)
- **Ruby** — [`../../../ruby/transactions/read_committed`](../../../ruby/transactions/read_committed)
- **Rust** — [`../../../rust/transactions/read-committed`](../../../rust/transactions/read-committed)

Docs: [`../../../../docs/concepts/transactions-eos.md`](../../../../docs/concepts/transactions-eos.md),
[`../../../../docs/guides/transactions-eos.md`](../../../../docs/guides/transactions-eos.md)

---

> **Auth:** the connector default is no authentication. SASL/TLS setup lives in
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
