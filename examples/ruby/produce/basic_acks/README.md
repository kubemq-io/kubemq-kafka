# Ruby — Kafka: Basic Acks

Produce one record at each durability level (`acks=all` / `1` / `0`), read all three
back, then prove an oversized record is rejected with `MESSAGE_TOO_LARGE`.

## Prerequisites
- Ruby (built/verified on 3.3.x via rbenv). `rdkafka` compiles librdkafka as a native
  extension at install, so a **C toolchain** is required (`build-essential` on Linux,
  Xcode Command Line Tools on macOS).
- `rdkafka >= 0.19` — `bundle install` (see `../../Gemfile`).
- A running KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` started with
  `CONNECTORS_KAFKA_ENABLE=true` (gotcha #1) and `CONNECTORS_KAFKA_ADVERTISED_HOST`
  set for any non-loopback client (gotcha #2).

## How to Run
```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
bundle install
bundle exec ruby produce/basic_acks/main.rb
```

## Expected Output
```
[kafka] bootstrap=localhost:9092 client=rdkafka (librdkafka, CRC32 partitioner) topic=kafka-ex-produce-acks-...
Produce(acks=all) -> partition=0 offset=0
Produce(acks=  1) -> partition=0 offset=1
Produce(acks=  0) -> partition=0 offset=2
Fetch          -> partition=0 offset=0 payload="order #1138 — 2 widgets [acks=all]"
Fetch          -> partition=0 offset=1 payload="order #1138 — 2 widgets [acks=1]"
Fetch          -> partition=0 offset=2 payload="order #1138 — 2 widgets [acks=0]"
Produce(oversized) -> rejected: msg_size_too_large (MESSAGE_TOO_LARGE) as expected
DeleteTopic    -> ok
PASS: acks round-trip + oversized rejection succeeded
```
Offsets vary per run; the topic name carries a `-<pid>-<rand>` suffix.

## What's Happening
Metadata (auto-create on first Produce) → Produce (RecordBatch v2 at each acks level) →
Fetch long-poll read-back → an oversized payload guarded by the connector's
`MaxMessageBytes` (1 MiB). Mirrors connector behavior in
`kubemq-server/connectors/kafka/` (produce path; `MaxMessageBytes` oversize guard).

## Kafka specifics
| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Produce(0), Fetch(1), Metadata(3), DeleteTopics(20) | acks 0/1/all, read_uncommitted | 1 topic / 1 partition | ephemeral group, earliest | offset = STAN Sequence | none | librdkafka default (CRC32) | oversized → MESSAGE_TOO_LARGE (1 MiB) |

## Related Examples
- `../../../go/produce/basic-acks`, `../../../java/produce/basic-acks`,
  `../../../javascript/produce/basic-acks`, `../../../csharp/produce/basic-acks`,
  `../../../rust/produce/basic-acks`, `../../../python/produce/basic_acks`.
- Guide: `../../../../docs/guides/producing.md`.

## Gotcha
**#3 — `acks>=1` on multi-node.** On a clustered broker `acks=0` can land on a follower
and be silently dropped; always use `acks>=1` (ideally `all`) in production.

## Auth
No auth by default. For SASL/TLS see `../../../../docs/guides/security-sasl-tls.md`.
