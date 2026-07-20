# Ruby — Kafka: read_committed Isolation

A `read_committed` consumer never delivers aborted-transaction records; its readable end is the Last
Stable Offset (LSO), below the high-watermark while a txn is open. Aborted filtering is CLIENT-side
(AbortedTransactions list), not a server-side record filter (gotcha #12).

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
bundle exec ruby transactions/read_committed/main.rb
```

## Expected Output
Banner, `Txn(commit) -> keep-0..keep-2 committed`, `Txn(abort) -> drop-0..drop-2 aborted`,
`Watermarks -> HWM(...)=..`, `read_committed -> delivered ["keep-0","keep-1","keep-2"]`, `PASS`.

## What's Happening
The aborted batch still occupies offsets (so HWM advanced), but librdkafka drops those records for a
`read_committed` consumer. The delivered count is strictly less than HWM — proof of client-side filtering.

> **KIP-890 ceiling (spec §2.5, gotcha #9):** the connector implements the KIP-890 V1 transaction
> protocol. It closes the classic hanging-transaction window but leaves a residual **same-epoch
> zombie** edge — a documented protocol-level limit, NOT an assertion this example makes. We never
> claim EOS beyond the V1 guarantee.

## Kafka specifics
| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Fetch(1), ListOffsets(2), InitProducerId(22), EndTxn(26) | read_committed | 1 topic / 1 partition | ephemeral, earliest | offset = STAN Sequence (LSO ≤ HWM) | none | CRC32 | aborted filtered client-side |

## Gotcha
**#12 — client-side filtering.** `read_committed` filtering happens in the client via the
AbortedTransactions list; the broker does not remove aborted records from the log.

## Related Examples
- `../../../{go,java,javascript,csharp,rust}/transactions/read-committed`, `../../../python/transactions/read_committed`.
- Guide: `../../../../docs/guides/transactions-eos.md`.

## Auth
No auth by default. For SASL/TLS see `../../../../docs/guides/security-sasl-tls.md`.
