# javascript — Kafka: Commit Offsets and Lag

Manually `commitOffsets()` (OffsetCommit, key 8) after reading half the records, then start a second
consumer in the **same** group that resumes from the committed offset (OffsetFetch, key 9) with no
re-read, and compute lag as `HWM − committed`. The Kafka topic `kafka-ex-cg-commit` maps onto the
Events-Store channel `kafka.kafka-ex-cg-commit`.

## Prerequisites

- Node.js 18+ and `npm install` in `examples/javascript/` (pins `kafkajs` `^2.2.4` — v2+, murmur2
  `DefaultPartitioner`).
- A running KubeMQ server with the Kafka connector **enabled** (`CONNECTORS_KAFKA_ENABLE=true` — the
  connector is **disabled by default**, gotcha #1), reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). For external clients, set `CONNECTORS_KAFKA_ADVERTISED_HOST` (gotcha #2).

## How to Run

```bash
cd examples/javascript
npm install
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
npx tsx consumer-groups/commit-and-lag/index.ts
```

## Expected Output

```
Connecting to KubeMQ Kafka connector at localhost:9092 (topic "kafka-ex-cg-commit", group "kafka-ex-cg-commit-grp")
Produced 10 records
First consumer read 5 and committed: [e-0, e-1, e-2, e-3, e-4]
HWM=10 committed=5 lag=5
Second consumer resumed and read 5: [e-5, e-6, e-7, e-8, e-9]

Commit + lag proven: resumed from committed offset with no re-read; lag = HWM - committed
```

## What's Happening

- The first consumer (`autoCommit: false`) reads five records and explicitly
  `commitOffsets([{ offset: lastProcessed + 1 }])` — the Kafka committed offset is *next-to-read*.
- Lag is computed as `HWM − committed`: `admin.fetchTopicOffsets` gives the high water mark (10) and
  `admin.fetchOffsets({ groupId })` gives the committed offset (5), so `lag = 5`.
- The second consumer joins the same group and resumes from the committed offset via OffsetFetch —
  it reads only `e-5..e-9`, never re-reading what the first consumer committed.
- The connector also exposes `kubemq_kafka_consumer_group_lag{group,topic,partition}` as a server-side
  cross-check of the same number.
- Mirrors connector behavior in `connectors/kafka/` (OffsetCommit/OffsetFetch; see
  `connectors/kafka/groupoffsets_test.go`).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| OffsetCommit (8), OffsetFetch (9), Produce (0), Fetch (1), ListOffsets (2) | read_uncommitted | `kafka-ex-cg-commit` / 1 partition | one shared group, two sessions | committed offset = last-processed + 1; lag = HWM − committed | none | murmur2 (DefaultPartitioner) | resume from committed offset (no re-read); `kubemq_kafka_consumer_group_lag` cross-check |

## Related Examples

- Same variant, other languages: [`../../../go/consumer-groups/commit-and-lag`](../../../go/consumer-groups/commit-and-lag),
  [`../../../java/consumer-groups/commit-and-lag`](../../../java/consumer-groups/commit-and-lag),
  [`../../../csharp/consumer-groups/commit-and-lag`](../../../csharp/consumer-groups/commit-and-lag),
  [`../../../rust/consumer-groups/commit-and-lag`](../../../rust/consumer-groups/commit-and-lag),
  [`../../../python/consumer-groups/commit_and_lag`](../../../python/consumer-groups/commit_and_lag),
  [`../../../ruby/consumer-groups/commit_and_lag`](../../../ruby/consumer-groups/commit_and_lag).
- Doc: [`../../../../docs/guides/consuming-and-groups.md`](../../../../docs/guides/consuming-and-groups.md),
  [`../../../../docs/concepts/consumer-groups.md`](../../../../docs/concepts/consumer-groups.md).
- Next: [`../../admin/topics-lifecycle/`](../../admin/topics-lifecycle/).

> **Gotcha — committed offset is next-to-read.** Commit `lastProcessedOffset + 1`, not the processed
> offset itself; committing the processed offset would replay that record on resume.

> **Auth.** The dev default is no SASL over plain TCP (`:9092`). For a secured connector, configure
> SASL/PLAIN or SASL/SCRAM (and TLS on `:9093`). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
