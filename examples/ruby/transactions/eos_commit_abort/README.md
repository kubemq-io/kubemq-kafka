# Ruby — Kafka: EOS Commit vs Abort

A transactional producer (InitProducerId → AddPartitionsToTxn → txn Produce → EndTxn) commits one
batch and aborts another; a `read_committed` consumer sees the committed batch and never the aborted
one.

## Prerequisites
- Ruby 3.3.x (rbenv); `rdkafka` builds librdkafka natively, so a **C toolchain** is required.
- `rdkafka >= 0.19` via `bundle install` (`../../Gemfile`). Note: some `rdkafka` builds (e.g. 0.29.0)
  do not bind the librdkafka transactional-producer API (`init_transactions`); on those the example
  prints a justified N/A and exits 0 — use a `franz-go` / Java / `confluent-kafka` client for EOS.
- KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` with `CONNECTORS_KAFKA_ENABLE=true`
  (gotcha #1) and `CONNECTORS_KAFKA_ADVERTISED_HOST` set for non-loopback (gotcha #2).

## How to Run
```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
bundle exec ruby transactions/eos_commit_abort/main.rb
```

## Expected Output
Banner, `Txn(commit) -> ... committed`, `Txn(abort) -> ... aborted`,
`read_committed -> saw ["committed-0","committed-1"]`, `DeleteTopic -> ok`, `PASS`.

## What's Happening
`init_transactions`/`begin_transaction`/`commit_transaction`/`abort_transaction` on the rdkafka
producer; committed records are visible under `read_committed`, aborted records are filtered.

> **KIP-890 ceiling (spec §2.5, gotcha #9):** the connector implements the KIP-890 V1 transaction
> protocol. It closes the classic hanging-transaction window but leaves a residual **same-epoch
> zombie** edge — a documented protocol-level limit, NOT an assertion this example makes. We never
> claim EOS beyond the V1 guarantee.

## Kafka specifics
| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| InitProducerId(22), AddPartitionsToTxn(24), EndTxn(26), Produce(0), Fetch(1) | acks=all, read_committed | 1 topic / 1 partition | ephemeral, earliest | offset = STAN Sequence | none | CRC32 | commit visible / abort absent |

## Gotcha
**#7 — transactional.id charset.** A `/` in `transactional.id` → INVALID_TRANSACTIONAL_ID →
INVALID_REQUEST(42). Use a `.`-safe id (this example does).

## Related Examples
- `../../../{go,java,javascript,csharp,rust}/transactions/eos-commit-abort`, `../../../python/transactions/eos_commit_abort`.
- Guide: `../../../../docs/guides/transactions-eos.md`.

## Auth
No auth by default. For SASL/TLS see `../../../../docs/guides/security-sasl-tls.md`.
