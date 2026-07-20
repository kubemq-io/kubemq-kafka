# Ruby — Kafka: From Beginning vs Latest

`auto.offset.reset` on a fresh group: `earliest` replays the whole log; `latest` reads only
records produced after the subscription is live.

## Prerequisites
- Ruby 3.3.x (rbenv); `rdkafka` builds librdkafka natively, so a **C toolchain** is required.
- `rdkafka >= 0.19` via `bundle install` (`../../Gemfile`).
- KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` with `CONNECTORS_KAFKA_ENABLE=true`
  (gotcha #1) and `CONNECTORS_KAFKA_ADVERTISED_HOST` set for non-loopback (gotcha #2).

## How to Run
```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
bundle exec ruby consume/from_beginning_latest/main.rb
```

## Expected Output
Banner, `Produce -> pre-1, pre-2`, `earliest -> saw ["pre-1","pre-2"]`, `Produce -> post-1`,
`latest -> saw ["post-1"]`, `DeleteTopic -> ok`, `PASS`.

## What's Happening
Fetch long-poll (key 1) with the two reset policies. `earliest` starts at the log-start offset;
`latest` starts at the log-end offset so pre-existing records are skipped.

## Kafka specifics
| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Produce(0), Fetch(1), ListOffsets(2) | read_uncommitted | 1 topic / 1 partition | two fresh groups | offset = STAN Sequence | none | CRC32 | auto.offset.reset earliest/latest |

## Related Examples
- `../../../{go,java,javascript,csharp,rust}/consume/from-beginning-latest`, `../../../python/consume/from_beginning_latest`.
- Guide: `../../../../docs/guides/consuming-and-groups.md`.

## Gotcha
**`latest` on a fresh group can miss records.** `auto.offset.reset` only fires when the group has no
committed offset; `latest` then starts at the log-end, so anything produced before the
subscription/rebalance completes is skipped. Use `earliest` (or commit offsets) when you must not
miss the start of the log.

## Auth
No auth by default. For SASL/TLS see `../../../../docs/guides/security-sasl-tls.md`.
