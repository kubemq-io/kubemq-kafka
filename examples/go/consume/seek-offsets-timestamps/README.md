# Go — Kafka: Consume Seek by Offset / Timestamp

Two seek modes against the KubeMQ Kafka connector: jump to an explicit offset, and
resolve an offset from a wall-clock timestamp via `ListOffsets` (by-timestamp),
then consume from the resolved position.

## Prerequisites

- Go 1.24+ and `github.com/twmb/franz-go v1.21.4` (pinned in `../../go.mod`).
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). **The connector is DISABLED by default — start the
  broker with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1). For any non-same-host
  client, also set `CONNECTORS_KAFKA_ADVERTISED_HOST` or the client connects then
  hangs (gotcha #2).

## How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
go run ./consume/seek-offsets-timestamps
```

## Expected Output

```
[kubemq-kafka] consume/seek-offsets-timestamps | bootstrap=localhost:9092 partitioner=murmur2(franz-go)
CreateTopic: kafka-ex-consume-seek-<8hex> (partitions=1)
Produce: 6 records, timestamp boundary between rec-2 and rec-3
SetOffsets(4): first delivered record offset=4 value="rec-4"
ListOffsets(by-ts=<ms>): resolved offset=3
by-timestamp seek: first delivered record offset=3 value="rec-3"
DeleteTopic: ok
PASS: seek by offset + seek by timestamp verified
```

> The topic is suffixed with 8 random hex chars so concurrent runs of the other
> language examples against the same connector do not collide.

## What's Happening

The program writes 6 records (`rec-0`…`rec-5`) with a recorded timestamp boundary
between `rec-2` and `rec-3`. It first calls `SetOffsets(4)` and asserts the next
delivered record is exactly offset 4 (`rec-4`) — a direct offset seek. It then
calls `ListOffsets` with the boundary millisecond, which resolves to the offset of
the **first record at-or-after** that timestamp (offset 3 = `rec-3`), seeks there,
and asserts the first delivered record is `rec-3`. A wrong resolved offset or a
wrong first-delivered record fails the process.

The wire flow is `Metadata → Produce → ListOffsets (by-timestamp) → Fetch`,
mirroring connector behavior in `connectors/kafka/listoffsets_test.go`.

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| ListOffsets(2), Fetch(1), Produce(0), CreateTopics(19), DeleteTopics(20) | acks=all; read_uncommitted | 1 partition | none (explicit seek) | offset seek is exact; by-timestamp resolves to first record ≥ ts | none | murmur2 (franz-go) | by-timestamp `ListOffsets` returns the first offset at-or-after the timestamp; both seeks asserted to land on the expected record |

## Related Examples

- Same variant in other languages:
  `../../../python/consume/seek_offsets_timestamps`,
  `../../../javascript/consume/seek-offsets-timestamps`,
  `../../../java/consume/seek-offsets-timestamps`,
  `../../../csharp/consume/seek-offsets-timestamps`,
  `../../../ruby/consume/seek_offsets_timestamps`,
  `../../../rust/consume/seek-offsets-timestamps`.
- Docs: `../../../../docs/guides/consuming-and-groups.md`,
  `../../../../docs/concepts/topics-partitions-offsets.md`.
- Related: [`../from-beginning-latest`](../from-beginning-latest),
  [`../../offsets/list-and-retention`](../../offsets/list-and-retention).

> **By-timestamp resolves to ≥ the timestamp.** `ListOffsets(timestamp)` returns
> the offset of the first record whose broker timestamp is at-or-after the query,
> or the log-end offset if none qualify. Because the connector's offsets are STAN
> Sequences, the resolution is stable across restarts and identical on every node.

> Auth: this example uses the no-auth default posture. Runs with no SASL by default
> on a stock dev broker; for SASL/PLAIN + SCRAM (and mTLS principal derivation) see
> [`../../security/sasl-plain-scram`](../../security/sasl-plain-scram) +
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
