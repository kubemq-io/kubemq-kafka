# Python — Kafka: Read Committed

Consume with `isolation.level=read_committed` and prove the consumer never sees aborted records, and
that `ListOffsets(latest, read_committed)` returns the Last Stable Offset (LSO) — which stays below the
high watermark while a transaction is open — against the KubeMQ Kafka connector using native
`confluent-kafka`.

## Prerequisites

- Python 3.9+ and [`uv`](https://docs.astral.sh/uv/).
- Kafka client: `confluent-kafka` (installed via `uv sync` from `../../pyproject.toml`).
- A running **KubeMQ Kafka connector** reachable at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`),
  started with **`CONNECTORS_KAFKA_ENABLE=true`** (the connector is disabled by default — gotcha #1).
- `AdvertisedHost` set on the connector for any non-loopback client (gotcha #2).

## How to Run

    cd examples/python
    uv sync
    export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
    uv run python transactions/read_committed/main.py

## Expected Output

> The Python suite namespaces its topic with `KUBEMQ_KAFKA_NAME_PREFIX` (default `py`):
> `kafka-py-txn-readcommitted`.

    === transactions/read-committed — topic 'kafka-py-txn-readcommitted' ===
      bootstrap : localhost:9092
      client    : confluent-kafka (librdkafka; CRC32 default partitioner)
      note      : connector must be started with CONNECTORS_KAFKA_ENABLE=true

      [OK] read_committed consumer never delivered aborted records
      [OK] read_committed consumer delivered the committed records
      [OK] LSO < HWM while a transaction is open
      [OK] LSO advanced to HWM after the transaction committed

    Round-trip complete.

## What's Happening

- One committed and one aborted transaction are produced to the same partition.
- A `read_committed` consumer filters out the aborted records using the `AbortedTransactions` list the
  broker returns in each Fetch response — the filter is **client-side** (gotcha #12), there is no
  server-side record filter.
- While a transaction is open, `ListOffsets(latest, read_committed)` returns the **Last Stable Offset
  (LSO)**, which is strictly below the high watermark (HWM); after commit, the LSO advances to the HWM.
- Mirrors connector behavior in `connectors/kafka/` (LSO/aborted-txn handling in the Fetch path; see
  `txn_rpcs_test.go`).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Fetch(1), ListOffsets(2), InitProducerId(22), AddPartitionsToTxn(24), EndTxn(26) | read_committed reader | 1 partition | `read_committed` reader | LSO < HWM while txn open; LSO = HWM after commit | none | CRC32 (librdkafka default) | gotcha #12 (client-side `AbortedTransactions` filter), gotcha #9 (KIP-890 V1) |

## Related Examples

- Same variant in the other languages: [`../../../go/transactions/read-committed/`](../../../go/transactions/read-committed/),
  [`../../../java/transactions/read-committed/`](../../../java/transactions/read-committed/),
  [`../../../javascript/transactions/read-committed/`](../../../javascript/transactions/read-committed/),
  [`../../../csharp/transactions/read-committed/`](../../../csharp/transactions/read-committed/),
  [`../../../ruby/transactions/read_committed/`](../../../ruby/transactions/read_committed/),
  [`../../../rust/transactions/read-committed/`](../../../rust/transactions/read-committed/).
- [`../eos_commit_abort/`](../eos_commit_abort/) — the producer-side commit/abort handshake.
- [`../../../../docs/concepts/transactions-eos.md`](../../../../docs/concepts/transactions-eos.md)

> **Gotcha #12 / #9 — read_committed is client-side.** The connector returns the `AbortedTransactions`
> list per Fetch and the client filters aborted records locally; there is no server-side record filter.
> EOS runs the **KIP-890 V1** protocol (no TV2) — an upstream-shared ceiling, not a defect.

> **Auth.** The stock dev connector runs with SASL off. For a secured connector, configure SASL/PLAIN
> or SCRAM and see [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
