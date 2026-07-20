# Ruby — Kafka: Seek by Offset & Timestamp

Random-access reads: position by OFFSET (assign-at-offset TopicPartitionList) and by TIMESTAMP
(`offsets_for_times`), using ListOffsets watermarks for the log bounds.

## Prerequisites
- Ruby 3.3.x (rbenv); `rdkafka` builds librdkafka natively, so a **C toolchain** is required.
- `rdkafka >= 0.19` via `bundle install` (`../../Gemfile`).
- KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` with `CONNECTORS_KAFKA_ENABLE=true`
  (gotcha #1) and `CONNECTORS_KAFKA_ADVERTISED_HOST` set for non-loopback (gotcha #2).

## How to Run
```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
bundle exec ruby consume/seek_offsets_timestamps/main.rb
```

## Expected Output
Banner, `Produce -> 6 records`, `Watermarks -> low=.. high=..`, `Seek(offset=..) -> ... payload="rec-2"`,
`offsets_for_times(...) -> offset=..`, `Seek(by-ts) -> ...`, `DeleteTopic -> ok`, `PASS`.

## What's Happening
`query_watermark_offsets` returns [low, high]. Seek-by-offset assigns the partition at a chosen
offset. Seek-by-timestamp maps a wall-clock time to the first offset with ts >= target.

> **N/A note:** `offsets_for_times` is present on rdkafka >= 0.13. If a pinned gem lacks it, the
> by-timestamp half is N/A — keep the seek-by-offset demo and point to `../../../go/consume/seek-offsets-timestamps`.

## Kafka specifics
| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Produce(0), Fetch(1), ListOffsets(2) | read_uncommitted | 1 topic / 1 partition | assign-based | offset = STAN Sequence | none | CRC32 | seek by offset + timestamp |

## Related Examples
- `../../../{go,java,javascript,csharp,rust}/consume/seek-offsets-timestamps`, `../../../python/consume/seek_offsets_timestamps`.
- Concept: `../../../../docs/concepts/topics-partitions-offsets.md`.

## Gotcha
**Seeks apply on the next poll.** librdkafka is asynchronous — `seek`/`assign` reposition the
consumer but only take effect on the following `poll`. And `offsets_for_times` returns `-1` when no
record has a timestamp ≥ the target; guard for that before seeking.

## Auth
No auth by default. For SASL/TLS see `../../../../docs/guides/security-sasl-tls.md`.
