# Ruby — Kafka: List Offsets & Retention

ListOffsets (key 2) earliest/latest via `query_watermark_offsets`; producing advances `latest` by
the record count, while `earliest` (log-start) moves only when retention truncates the log.

## Prerequisites
- Ruby 3.3.x (rbenv); `rdkafka` builds librdkafka natively, so a **C toolchain** is required.
- `rdkafka >= 0.19` via `bundle install` (`../../Gemfile`).
- KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` with `CONNECTORS_KAFKA_ENABLE=true`
  (gotcha #1) and `CONNECTORS_KAFKA_ADVERTISED_HOST` set for non-loopback (gotcha #2).

## How to Run
```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
bundle exec ruby offsets/list_and_retention/main.rb
```

## Expected Output
Banner, `CreateTopic -> ... retention.ms=600000 retention.bytes=10485760`,
`ListOffsets -> earliest=0 latest=0 (empty)`, `ListOffsets -> earliest=0 latest=7 (after 7 produced)`,
`DeleteTopic -> ok`, `PASS`.

## What's Happening
The program asserts the offset math (latest tracks produced count; earliest holds at log-start). The
retention EFFECT is time/size-bounded and not asserted here.

## Retention mapping (spec §2.2)
| Kafka topic config | KubeMQ channel setting |
|---|---|
| `retention.ms` | `MaxAge` |
| `retention.bytes` | `MaxBytes` |
| (message count cap) | `MaxMsgs` |

## Kafka specifics
| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| ListOffsets(2), Produce(0), CreateTopics(19) | read_uncommitted | 1 topic / 1 partition | assign-based | offset = STAN Sequence | none | CRC32 | earliest/latest watermarks + retention |

## Related Examples
- `../../../{go,java,javascript,csharp,rust}/offsets/list-and-retention`, `../../../python/offsets/list_and_retention`.
- Concept: `../../../../docs/concepts/topics-partitions-offsets.md`.

## Gotcha
**Retention advances `earliest`.** When `retention.ms`/`retention.bytes` (→ channel
`MaxAge`/`MaxBytes`) evict the log head, the earliest watermark moves forward and a previously-valid
offset becomes `OFFSET_OUT_OF_RANGE`. Across nodes the low watermark can also read momentarily stale
during Raft lag (gotcha #10) — it self-heals on the next metadata refresh.

## Auth
No auth by default. For SASL/TLS see `../../../../docs/guides/security-sasl-tls.md`.
