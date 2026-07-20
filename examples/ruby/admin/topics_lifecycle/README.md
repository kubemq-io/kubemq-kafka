# Ruby — Kafka: Topics Lifecycle

CreateTopics (key 19) → confirm via metadata/watermarks → DeleteTopics (key 20). Then prove a
topic name containing `~` is rejected (gotcha #6 — `~` is the connector's reserved partition
separator).

## Prerequisites
- Ruby 3.3.x (rbenv); `rdkafka` builds librdkafka natively, so a **C toolchain** is required.
- `rdkafka >= 0.19` via `bundle install` (`../../Gemfile`).
- KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` with `CONNECTORS_KAFKA_ENABLE=true`
  (gotcha #1) and `CONNECTORS_KAFKA_ADVERTISED_HOST` set for non-loopback (gotcha #2).

## How to Run
```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
bundle exec ruby admin/topics_lifecycle/main.rb
```

## Expected Output
Banner, `CreateTopic -> ... partitions=3 rf=1`, three `Describe(pN) -> low=.. high=..`,
`DeleteTopic -> ok`, `CreateTopic(~) -> rejected: <code> (INVALID_TOPIC_EXCEPTION)`, `PASS`.

## What's Happening
Create/delete via the Admin client; existence is confirmed with `query_watermark_offsets` per
partition (portable across gem lines).

> **N/A note:** rdkafka-ruby's DescribeConfigs surface varies by version. Where it is absent, the
> metadata/watermark probe is the portable describe; see `../../../go/admin/topics-lifecycle` for
> full DescribeConfigs/DescribeCluster.

## Kafka specifics
| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| CreateTopics(19), DeleteTopics(20), Metadata(3), ListOffsets(2) | n/a | up to 3 partitions | none | offset = STAN Sequence | none | CRC32 | `~` name → INVALID_TOPIC_EXCEPTION(17) |

## Gotcha
**#6 — reserved `~`.** The connector maps partition p>0 onto `kafka.<topic>~<p>`, so a user topic
containing `~` is rejected.

## Related Examples
- `../../../{go,java,javascript,csharp,rust}/admin/topics-lifecycle`, `../../../python/admin/topics_lifecycle`.
- Guide: `../../../../docs/guides/admin-and-topics.md`.

## Auth
No auth by default. For SASL/TLS see `../../../../docs/guides/security-sasl-tls.md`.
