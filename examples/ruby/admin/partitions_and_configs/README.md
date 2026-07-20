# Ruby — Kafka: Partitions & Configs (partial support)

CreatePartitions (key 37) is **increase-only** (new_total in (current, 256]); a same-count or
decreasing request → `INVALID_PARTITIONS`. IncrementalAlterConfigs (44) 🟡 and DeleteRecords (45) 🟡
are partial and version-dependent on rdkafka-ruby.

## Prerequisites
- Ruby 3.3.x (rbenv); `rdkafka` builds librdkafka natively, so a **C toolchain** is required.
- `rdkafka >= 0.19` via `bundle install` (`../../Gemfile`).
- KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` with `CONNECTORS_KAFKA_ENABLE=true`
  (gotcha #1) and `CONNECTORS_KAFKA_ADVERTISED_HOST` set for non-loopback (gotcha #2).

## How to Run
```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
bundle exec ruby admin/partitions_and_configs/main.rb
```

## Expected Output
Banner, `CreateTopic -> ... partitions=2`, `CreatePartitions(4) -> now partitions=4`,
`CreatePartitions(4->4) -> rejected: <code> (INVALID_PARTITIONS)`, `CreatePartitions(4->2) -> rejected ...`,
IncrementalAlterConfigs/DeleteRecords availability lines, `DeleteTopic -> ok`, `PASS`.

## What's Happening
Partition count can only grow, capped at 256. The 🟡 config/records operations are exercised where
the pinned gem exposes them and reported as a **justified N/A** (spec §6.3) otherwise.

> **N/A note:** if IncrementalAlterConfigs / DeleteRecords are absent on the pinned rdkafka-ruby,
> this folder + README document the limit and point to `../../../go/admin/partitions-and-configs`.

## Kafka specifics
| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| CreatePartitions(37), IncrementalAlterConfigs(44)🟡, DeleteRecords(45)🟡 | n/a | 2→4 partitions | none | offset = STAN Sequence | none | CRC32 | increase-only ≤256; bad increase → INVALID_PARTITIONS |

## Related Examples
- `../../../{go,java,javascript,csharp,rust}/admin/partitions-and-configs`, `../../../python/admin/partitions_and_configs`.
- Guide: `../../../../docs/guides/admin-and-topics.md`.

## Gotcha
**#5 — growing N re-shards keys.** Increasing the partition count changes the `key → partition`
mapping, so per-key ordering holds only within a fixed-partition-count epoch. Size partitions up
front when key ordering matters. (Increase-only, capped at 256; a same-count or decreasing request →
`INVALID_PARTITIONS`.)

## Auth
No auth by default. For SASL/TLS see `../../../../docs/guides/security-sasl-tls.md`.
