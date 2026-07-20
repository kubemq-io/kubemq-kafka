# java — Kafka: Partitions and Configs

Three partial-support admin operations, each with its exact scope: increase-only
`createPartitions`, subset-apply `incrementalAlterConfigs`, and low-end-only
`deleteRecords` — plus the `INVALID_PARTITIONS` guard on a non-increasing change.

## Prerequisites

- JDK 21+ and Maven 3.9+.
- `org.apache.kafka:kafka-clients 3.9.0` (pinned in `../../pom.xml`), providing the
  `Admin` API.
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). **Connector DISABLED by default — start with
  `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1); set `CONNECTORS_KAFKA_ADVERTISED_HOST`
  for remote clients (gotcha #2).

## How to Run

From `examples/java/`:

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
mvn -q compile
mvn -q exec:exec -Dexec.mainClass=io.kubemq.examples.kafka.admin.partitionsandconfigs.Main
```

## Expected Output

```
bootstrap.servers = localhost:9092
CreateTopics 'kafka-ex-admin-partitions-java' (2 partitions)
CreatePartitions increaseTo(3) -> now 3
IncrementalAlterConfigs retention.ms -> 3600000
DeleteRecords beforeOffset(2) -> earliest offset now 2
createPartitions increaseTo(3) again rejected -> InvalidPartitionsException
OK: partitions increase + config change + truncation + bad-increase guard
```

`createPartitions` grows 2 → 3; `incrementalAlterConfigs` sets `retention.ms`;
`deleteRecords` truncates the low end so the earliest offset advances to 2; and a
second `increaseTo(3)` (same count) is rejected with `InvalidPartitionsException`.

## What's Happening

The program creates a 2-partition topic and exercises three operations that are each
**partially** supported (🟡), stating the scope of each:

- **`createPartitions` — increase-only, ≤ 256.** Growing to 3 succeeds; a
  same-count / decrease / >256 request is rejected with `INVALID_PARTITIONS`.
- **`incrementalAlterConfigs` — subset only.** Setting `retention.ms` applies; many
  Kafka configs are silently no-ops on the connector.
- **`deleteRecords` — low-end truncation only.** Deleting before an offset advances
  the log-start (earliest) offset; it cannot delete from the middle or high end.

It then re-issues `increaseTo(3)` and asserts the `InvalidPartitionsException`
(non-increasing change), unwrapping the `ExecutionException` cause first.

The Kafka wire flow is `CreatePartitions(37) → IncrementalAlterConfigs(44) →
Produce(0) → DeleteRecords(21) → ListOffsets(2)`, mirroring connector behavior in
`connectors/kafka/` (admin path).

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| CreatePartitions(37), IncrementalAlterConfigs(44), DeleteRecords(21), ListOffsets(2) | acks=all; read_uncommitted | 2 → 3 partitions | none | `deleteRecords` advances earliest offset | none | murmur2 | 🟡 increase-only ≤256; 🟡 config subset; 🟡 low-end truncation; bad increase → `INVALID_PARTITIONS`; growing N re-shards keys (gotcha #5) |

## Related Examples

- Same variant in the other 6 languages: [`../../../go/admin/partitions-and-configs`](../../../go/admin/partitions-and-configs),
  [`../../../python/admin/partitions_and_configs`](../../../python/admin/partitions_and_configs),
  [`../../../javascript/admin/partitions-and-configs`](../../../javascript/admin/partitions-and-configs),
  [`../../../csharp/admin/partitions-and-configs`](../../../csharp/admin/partitions-and-configs),
  [`../../../ruby/admin/partitions_and_configs`](../../../ruby/admin/partitions_and_configs),
  [`../../../rust/admin/partitions-and-configs`](../../../rust/admin/partitions-and-configs).
- Docs: [`../../../../docs/guides/admin-and-topics.md`](../../../../docs/guides/admin-and-topics.md).
- Related: [`../topics-lifecycle`](../topics-lifecycle), [`../../offsets/list-and-retention`](../../offsets/list-and-retention).

> **Gotcha #5 — growing N re-shards keys.** Increasing partitions changes the
> murmur2 key→partition mapping, so per-key ordering holds only within a fixed-N
> epoch. Partition changes are increase-only and capped at 256.

> **Auth.** This example uses the connector's no-auth default posture
> (SHARED-CONVENTIONS §4.3). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md)
> for SASL/PLAIN + SCRAM and TLS/mTLS.
