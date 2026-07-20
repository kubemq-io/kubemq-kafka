# Python — Kafka: EOS Commit / Abort

Run a transactional producer through the full EOS handshake
(`InitProducerId` → `AddPartitionsToTxn` → txn Produce → `EndTxn(commit|abort)`) and prove committed
records are visible while aborted records are absent under `read_committed` — against the KubeMQ Kafka
connector using native `confluent-kafka`.

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
    uv run python transactions/eos_commit_abort/main.py

## Expected Output

> The Python suite namespaces its topic with `KUBEMQ_KAFKA_NAME_PREFIX` (default `py`):
> `kafka-py-txn-eos`. Use a `transactional.id` **without `/`** (gotcha #7).

    === transactions/eos-commit-abort — topic 'kafka-py-txn-eos' ===
      bootstrap : localhost:9092
      client    : confluent-kafka (librdkafka; CRC32 default partitioner)
      note      : connector must be started with CONNECTORS_KAFKA_ENABLE=true

      [OK] init_transactions() obtained a producer id
      [OK] committed transaction: records visible under read_committed
      [OK] aborted transaction: records ABSENT under read_committed

    Round-trip complete.

## What's Happening

- `init_transactions()` calls **`InitProducerId`** to fence a PID/epoch for the `transactional.id`.
- `begin_transaction()` + the first `produce()` to a partition triggers **`AddPartitionsToTxn`**;
  records are written as part of the open transaction.
- `commit_transaction()` issues **`EndTxn(commit)`** — the records become visible under
  `read_committed`. A second transaction is aborted with **`EndTxn(abort)`** — its records are never
  delivered to a `read_committed` consumer.
- Mirrors connector behavior in `connectors/kafka/` (transaction coordinator RPCs; see
  `txn_rpcs_test.go`).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| InitProducerId(22), AddPartitionsToTxn(24), EndTxn(26), Produce(0), Fetch(1) | acks=all (idempotent); read_committed reader | 1 partition | `read_committed` reader | committed visible; aborted absent | none | CRC32 (librdkafka default) | gotcha #7 (`/` in txn.id), gotcha #8 (Group WRITE), gotcha #9 (KIP-890 V1) |

## Related Examples

- Same variant in the other languages: [`../../../go/transactions/eos-commit-abort/`](../../../go/transactions/eos-commit-abort/),
  [`../../../java/transactions/eos-commit-abort/`](../../../java/transactions/eos-commit-abort/),
  [`../../../javascript/transactions/eos-commit-abort/`](../../../javascript/transactions/eos-commit-abort/),
  [`../../../csharp/transactions/eos-commit-abort/`](../../../csharp/transactions/eos-commit-abort/),
  [`../../../ruby/transactions/eos_commit_abort/`](../../../ruby/transactions/eos_commit_abort/),
  [`../../../rust/transactions/eos-commit-abort/`](../../../rust/transactions/eos-commit-abort/).
- [`../read_committed/`](../read_committed/) — consumer-side isolation and LSO.
- [`../../../../docs/guides/transactions-eos.md`](../../../../docs/guides/transactions-eos.md)

> **Gotcha #7 / #9 — EOS honesty.** A `/` in `transactional.id` is rejected
> (`INVALID_TRANSACTIONAL_ID` → `INVALID_REQUEST(42)`) — never namespace the id with slashes. The
> connector implements the **KIP-890 V1** transaction protocol (no TV2); this ceiling is upstream-shared,
> not a defect — a same-epoch zombie residual is expected, not a failure. Txn offset-commit also
> requires Group WRITE (gotcha #8), stricter than stock Kafka.

> **Auth.** The stock dev connector runs with SASL off. For a secured connector, configure SASL/PLAIN
> or SCRAM and see [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
