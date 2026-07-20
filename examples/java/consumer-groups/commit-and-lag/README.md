# java — Kafka: Commit and Lag

Commit a partial offset, then bring up a second consumer in the same group and prove
it **resumes from the committed offset** — plus compute consumer-group lag as
`HWM − committed`.

## Prerequisites

- JDK 21+ and Maven 3.9+.
- `org.apache.kafka:kafka-clients 3.9.0` (pinned in `../../pom.xml`), including the
  `Admin` API for `listConsumerGroupOffsets` / `listOffsets`.
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). **Connector DISABLED by default — start with
  `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1); set `CONNECTORS_KAFKA_ADVERTISED_HOST`
  for remote clients (gotcha #2).

## How to Run

From `examples/java/`:

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
mvn -q compile
mvn -q exec:exec -Dexec.mainClass=io.kubemq.examples.kafka.consumergroups.commitandlag.Main
```

## Expected Output

```
bootstrap.servers = localhost:9092
CreateTopics 'kafka-ex-cg-commit-java' (1 partition)
Produced 10 records from base offset 0
[c1] committed offset 4
Admin: committed=4 HWM=10 lag=6
[c2] resumed at offset 4 (expected 4)
OK: commit/resume works and lag matches HWM - committed
```

The first consumer commits after reading 4 records; the Admin API reports
`committed=4`, high-water-mark `10`, so lag is `6`. A brand-new consumer in the same
group then resumes at offset 4 — it does not re-read the first 4.

## What's Happening

The program produces 10 records, then consumer `c1` reads a few and `commitSync`s
the offset after the 4th record (`OffsetCommit`, key 8). Using `Admin` it fetches
the committed offset (`listConsumerGroupOffsets` → `OffsetFetch`, key 9) and the
partition high-water-mark (`listOffsets`), computing lag = HWM − committed. A second
consumer `c2` joins the **same group**, and because the committed offset is durable
(offset = STAN Sequence), its first delivered record is at the committed offset —
proving resume-from-committed rather than a reset.

The Kafka wire flow is `Fetch(1) → OffsetCommit(8) → OffsetFetch(9) →
ListOffsets(2)`, mirroring connector behavior in `connectors/kafka/`
(`groupoffsets_test.go`).

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Fetch(1), OffsetCommit(8), OffsetFetch(9), ListOffsets(2) | acks=all; read_uncommitted | 1 partition | one shared group, sequential members | resume from committed; lag = HWM − committed | none | murmur2 | lag computed via Admin offsets diff (the `kubemq_kafka_consumer_group_lag` metric scrape is doc-only) |

## Related Examples

- Same variant in the other 6 languages: [`../../../go/consumer-groups/commit-and-lag`](../../../go/consumer-groups/commit-and-lag),
  [`../../../python/consumer-groups/commit_and_lag`](../../../python/consumer-groups/commit_and_lag),
  [`../../../javascript/consumer-groups/commit-and-lag`](../../../javascript/consumer-groups/commit-and-lag),
  [`../../../csharp/consumer-groups/commit-and-lag`](../../../csharp/consumer-groups/commit-and-lag),
  [`../../../ruby/consumer-groups/commit_and_lag`](../../../ruby/consumer-groups/commit_and_lag),
  [`../../../rust/consumer-groups/commit-and-lag`](../../../rust/consumer-groups/commit-and-lag).
- Docs: [`../../../../docs/guides/consuming-and-groups.md`](../../../../docs/guides/consuming-and-groups.md),
  [`../../../../docs/concepts/consumer-groups.md`](../../../../docs/concepts/consumer-groups.md).
- Related: [`../join-rebalance`](../join-rebalance).

> **Note — committed offsets are durable.** Reusing a group id means "resume". This
> example intentionally reuses the group so the second consumer resumes from the
> committed offset instead of re-reading from the start.

> **Auth.** This example uses the connector's no-auth default posture
> (SHARED-CONVENTIONS §4.3). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md)
> for SASL/PLAIN + SCRAM and TLS/mTLS.
