# java — Kafka: Seek by Offset and Timestamp

Assign a partition explicitly, then jump around the log two ways: `seek(offset)` to
a known offset, and `offsetsForTimes(...)` → `seek` to the first record at or after
a timestamp.

## Prerequisites

- JDK 21+ and Maven 3.9+.
- `org.apache.kafka:kafka-clients 3.9.0` (pinned in `../../pom.xml`).
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). **Connector DISABLED by default — start with
  `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1); set `CONNECTORS_KAFKA_ADVERTISED_HOST`
  for remote clients (gotcha #2).

## How to Run

From `examples/java/`:

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
mvn -q compile
mvn -q exec:exec -Dexec.mainClass=io.kubemq.examples.kafka.consume.seekoffsetstimestamps.Main
```

## Expected Output

```
bootstrap.servers = localhost:9092
CreateTopics 'kafka-ex-consume-seek-java' (1 partition)
Produce rec-0 -> offset=0 ts=1752422400000
Produce rec-1 -> offset=1 ts=1752422401000
Produce rec-2 -> offset=2 ts=1752422402000
Produce rec-3 -> offset=3 ts=1752422403000
Produce rec-4 -> offset=4 ts=1752422404000
Produce rec-5 -> offset=5 ts=1752422405000
seek(offset=3) -> value=rec-3
offsetsForTimes(ts=1752422404000) -> offset=4 value=rec-4
seek(by-ts) -> value=rec-4 ts=1752422404000
OK: seek by offset and by timestamp both landed correctly
```

(Timestamps are illustrative epoch-millis; the offsets and values are what the
assertions check.) `seek(3)` lands the reader on `rec-3`; `offsetsForTimes` resolves
the query timestamp to offset `4` and the subsequent read returns `rec-4`.

## What's Happening

The program produces 6 records with monotonically increasing timestamps (1s apart),
then uses `assign(partition)` — not `subscribe` — so seeks are deterministic (no
group coordination). It calls `seek(tp, 3)` and asserts the next `poll` returns the
record at offset 3. Then it calls `offsetsForTimes({tp → ts})`, which performs a
`ListOffsets` by-timestamp lookup returning the first offset whose record timestamp
is at or after the query, seeks there, and asserts the returned record.

The Kafka wire flow is `Metadata → Produce → ListOffsets (by-timestamp, key 2) →
Fetch (at the sought offset)`, mirroring connector behavior in `connectors/kafka/`
(`listoffsets_test.go`).

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Produce(0), Fetch(1), ListOffsets(2, by-timestamp) | acks=all; read_uncommitted | 1 partition | none (`assign`, not `subscribe`) | explicit `seek(offset)`; by-timestamp resolves to first offset ≥ ts | none | murmur2 | `assign` (not `subscribe`) so seeks are deterministic |

## Related Examples

- Same variant in the other 6 languages: [`../../../go/consume/seek-offsets-timestamps`](../../../go/consume/seek-offsets-timestamps),
  [`../../../python/consume/seek_offsets_timestamps`](../../../python/consume/seek_offsets_timestamps),
  [`../../../javascript/consume/seek-offsets-timestamps`](../../../javascript/consume/seek-offsets-timestamps),
  [`../../../csharp/consume/seek-offsets-timestamps`](../../../csharp/consume/seek-offsets-timestamps),
  [`../../../ruby/consume/seek_offsets_timestamps`](../../../ruby/consume/seek_offsets_timestamps),
  [`../../../rust/consume/seek-offsets-timestamps`](../../../rust/consume/seek-offsets-timestamps).
- Docs: [`../../../../docs/guides/consuming-and-groups.md`](../../../../docs/guides/consuming-and-groups.md),
  [`../../../../docs/concepts/topics-partitions-offsets.md`](../../../../docs/concepts/topics-partitions-offsets.md).
- Related: [`../from-beginning-latest`](../from-beginning-latest), [`../../offsets/list-and-retention`](../../offsets/list-and-retention).

> **Note — use `assign`, not `subscribe`, when seeking.** Group rebalance can move a
> partition mid-seek; `assign` pins the partition so `seek` is deterministic.

> **Auth.** This example uses the connector's no-auth default posture
> (SHARED-CONVENTIONS §4.3). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md)
> for SASL/PLAIN + SCRAM and TLS/mTLS.
