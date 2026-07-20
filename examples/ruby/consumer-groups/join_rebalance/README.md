# Ruby — Kafka: Group Join & Rebalance

Two consumers in one `group.id` subscribe to a 4-partition topic; the coordinator
(JoinGroup/SyncGroup/Heartbeat) splits partitions across members and every record is delivered
exactly once (no loss across the rebalance).

## Prerequisites
- Ruby 3.3.x (rbenv); `rdkafka` builds librdkafka natively, so a **C toolchain** is required.
- `rdkafka >= 0.19` via `bundle install` (`../../Gemfile`).
- KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` with `CONNECTORS_KAFKA_ENABLE=true`
  (gotcha #1) and `CONNECTORS_KAFKA_ADVERTISED_HOST` set for non-loopback (gotcha #2).

## How to Run
```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
bundle exec ruby consumer-groups/join_rebalance/main.rb
```

## Expected Output
Banner, `CreateTopic -> ... partitions=4`, `Produce -> 40 records`, `Assignment -> member1 ... member2 ...`,
`Consumed -> member1=.. member2=..`, `Assert -> all 40 records delivered exactly once`, `PASS`.

## What's Happening
Both members join the group; partitions are distributed between them. The union of consumed
records equals every produced record, each exactly once.

## Kafka specifics
| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Produce(0), Fetch(1), FindCoordinator/JoinGroup/SyncGroup/Heartbeat | read_uncommitted | 1 topic / 4 partitions | 2 members, one group | offset = STAN Sequence | none | CRC32 | rebalance, no loss |

## Gotcha
**#4** applies to mixed-client groups: a murmur2 producer and a CRC32 consumer group still work,
but keys map to partitions differently across client families.

## Related Examples
- `../../../{go,java,javascript,csharp,rust}/consumer-groups/join-rebalance`, `../../../python/consumer-groups/join_rebalance`.
- Guide: `../../../../docs/guides/consuming-and-groups.md`.

## Auth
No auth by default. For SASL/TLS see `../../../../docs/guides/security-sasl-tls.md`.
