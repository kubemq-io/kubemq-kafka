# java — Kafka: List Offsets and Retention

Query the log boundaries three ways — `earliest`, `latest`, and by-timestamp — and
set `retention.ms` / `retention.bytes`, which the connector maps onto channel
MaxAge / MaxBytes.

## Prerequisites

- JDK 21+ and Maven 3.9+.
- `org.apache.kafka:kafka-clients 3.9.0` (pinned in `../../pom.xml`), providing the
  `Admin` API (`listOffsets`, `incrementalAlterConfigs`).
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). **Connector DISABLED by default — start with
  `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1); set `CONNECTORS_KAFKA_ADVERTISED_HOST`
  for remote clients (gotcha #2).

## How to Run

From `examples/java/`:

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
mvn -q compile
mvn -q exec:exec -Dexec.mainClass=io.kubemq.examples.kafka.offsets.listandretention.Main
```

## Expected Output

```
bootstrap.servers = localhost:9092
CreateTopics 'kafka-ex-offsets-retention-java' with retention.ms/retention.bytes
Produced 6 records from base offset 0
ListOffsets earliest=0 latest=6
ListOffsets forTimestamp(1752422403000) -> offset=3
Retention config accepted: retention.ms=3600000 retention.bytes=1048576
OK: ListOffsets earliest/latest/by-timestamp + retention config verified
```

`earliest` tracks the log-start (0), `latest` tracks the high-water-mark (6), and
the by-timestamp query resolves to the first offset at/after the timestamp. The
retention config is accepted (time-based expiry is not observable in a fast run, so
the assertion is on config-accepted + offset semantics).

## What's Happening

The program creates a topic that carries `retention.ms` and `retention.bytes` in its
config, produces 6 records with increasing timestamps, then calls `listOffsets` with
`OffsetSpec.earliest()`, `OffsetSpec.latest()`, and `OffsetSpec.forTimestamp(ts)`.
It asserts earliest = log-start, latest = HWM, and that the by-timestamp lookup
lands on the expected offset. Finally it confirms the retention config round-trips
via `describeConfigs`. On the connector, `retention.ms`/`retention.bytes` map to the
Events-Store channel's `MaxAge` / `MaxBytes` (and `MaxMsgs`); wall-clock expiry is
too slow to assert in a short example, so the example asserts the mapping is
accepted rather than live deletion.

The Kafka wire flow is `CreateTopics(19) → Produce(0) → ListOffsets(2,
earliest/latest/by-ts) → DescribeConfigs(32)`, mirroring connector behavior in
`connectors/kafka/` (`listoffsets_test.go`).

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| ListOffsets(2), CreateTopics(19), Produce(0), DescribeConfigs(32) | acks=all; read_uncommitted | 1 partition | none | earliest=log-start, latest=HWM, by-ts=first ≥ ts | none | murmur2 | `retention.ms`/`retention.bytes` → channel MaxAge/MaxBytes/MaxMsgs; expiry not asserted (config-accepted only) |

## Related Examples

- Same variant in the other 6 languages: [`../../../go/offsets/list-and-retention`](../../../go/offsets/list-and-retention),
  [`../../../python/offsets/list_and_retention`](../../../python/offsets/list_and_retention),
  [`../../../javascript/offsets/list-and-retention`](../../../javascript/offsets/list-and-retention),
  [`../../../csharp/offsets/list-and-retention`](../../../csharp/offsets/list-and-retention),
  [`../../../ruby/offsets/list_and_retention`](../../../ruby/offsets/list_and_retention),
  [`../../../rust/offsets/list-and-retention`](../../../rust/offsets/list-and-retention).
- Docs: [`../../../../docs/guides/admin-and-topics.md`](../../../../docs/guides/admin-and-topics.md),
  [`../../../../docs/concepts/topics-partitions-offsets.md`](../../../../docs/concepts/topics-partitions-offsets.md).
- Related: [`../../consume/seek-offsets-timestamps`](../../consume/seek-offsets-timestamps).

> **Note — retention is mapped, not asserted live.** `retention.ms`/`retention.bytes`
> translate to the channel's `MaxAge`/`MaxBytes`/`MaxMsgs`. Wall-clock expiry is slow
> to observe, so this example asserts the config is accepted and the offset semantics
> are correct rather than waiting for deletion.

> **Auth.** This example uses the connector's no-auth default posture
> (SHARED-CONVENTIONS §4.3). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md)
> for SASL/PLAIN + SCRAM and TLS/mTLS.
