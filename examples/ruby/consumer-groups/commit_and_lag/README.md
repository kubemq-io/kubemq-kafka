# Ruby — Kafka: Commit & Lag

Manual OffsetCommit (key 8): a first consumer processes part of a topic and commits; a second
consumer in the same group resumes exactly from the committed offset (OffsetFetch, key 9). Lag =
high-watermark − committed offset.

## Prerequisites
- Ruby 3.3.x (rbenv); `rdkafka` builds librdkafka natively, so a **C toolchain** is required.
- `rdkafka >= 0.19` via `bundle install` (`../../Gemfile`).
- KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` with `CONNECTORS_KAFKA_ENABLE=true`
  (gotcha #1) and `CONNECTORS_KAFKA_ADVERTISED_HOST` set for non-loopback (gotcha #2).

## How to Run
```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
bundle exec ruby consumer-groups/commit_and_lag/main.rb
```

## Expected Output
Banner, `Produce -> 10 records`, `Consumer#1 -> processed [...], committed`,
`Consumer#2 -> resumed [...]`, `Lag -> high=10 committed=4 lag=6`, `PASS`.

## What's Happening
`enable.auto.commit=false` + synchronous `commit`; the group's committed offset is durable so a
second member resumes after it. The server also exposes `kubemq_kafka_consumer_group_lag`.

## Kafka specifics
| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Produce(0), Fetch(1), OffsetCommit(8), OffsetFetch(9), ListOffsets(2) | read_uncommitted | 1 topic / 1 partition | one group, two members | offset = STAN Sequence | none | CRC32 | manual commit + lag |

## Related Examples
- `../../../{go,java,javascript,csharp,rust}/consumer-groups/commit-and-lag`, `../../../python/consumer-groups/commit_and_lag`.
- Guide: `../../../../docs/guides/consuming-and-groups.md`.

## Gotcha
**Commit synchronously before you close.** With `enable.auto.commit=false` an un-committed offset is
lost on shutdown and the next member reprocesses. Call `commit(nil, async: false)` before `close`.
The committed offset is the *next* offset to read (last processed + 1), so lag = HWM − committed.

## Auth
No auth by default. For SASL/TLS see `../../../../docs/guides/security-sasl-tls.md`.
