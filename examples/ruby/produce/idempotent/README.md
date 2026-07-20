# Ruby — Kafka: Idempotent Producer

Turn on `enable.idempotence` (InitProducerId / per-(PID,partition) sequence numbers) and
prove N produces yield N unique, gap-free offsets with no duplicates on read-back.

## Prerequisites
- Ruby 3.3.x (rbenv); `rdkafka` builds librdkafka natively, so a **C toolchain** is required.
- `rdkafka >= 0.19` via `bundle install` (`../../Gemfile`).
- KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` with `CONNECTORS_KAFKA_ENABLE=true`
  (gotcha #1) and `CONNECTORS_KAFKA_ADVERTISED_HOST` set for non-loopback (gotcha #2).

## How to Run
```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
bundle exec ruby produce/idempotent/main.rb
```

## Expected Output
Banner, five `Produce(idem) -> seq=i ... offset=N` lines, `Assert -> 5 unique, gap-free offsets`,
`Fetch -> read back 5 unique records`, `DeleteTopic -> ok`, `PASS: ...`.

## What's Happening
`enable.idempotence` forces `acks=all` and assigns a Producer ID; every record carries a
monotonic sequence so the broker de-duplicates retried batches. The observable invariant we
assert is no-duplicate / no-gap offsets. Mirrors `connectors/kafka/` InitProducerId (key 22).

## Kafka specifics
| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| InitProducerId(22), Produce(0), Fetch(1) | acks=all (forced) | 1 topic / 1 partition | ephemeral, earliest | offset = STAN Sequence | none | CRC32 | per-(PID,partition) dedup |

## Related Examples
- `../../../{go,java,javascript,csharp,rust}/produce/idempotent`, `../../../python/produce/idempotent`.
- Guide: `../../../../docs/guides/producing.md`.

## Gotcha
Idempotence requires `acks=all`; a non-`all` acks disables idempotent writes (DisableIdempotentWrite).

## Auth
No auth by default. For SASL/TLS see `../../../../docs/guides/security-sasl-tls.md`.
