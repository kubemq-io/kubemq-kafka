# Go — Kafka: Offsets List and Retention

The three `ListOffsets` queries (earliest / latest / by-timestamp) and topic
retention config (`retention.ms` / `retention.bytes`) against the KubeMQ Kafka
connector.

## Prerequisites

- Go 1.24+ and `github.com/twmb/franz-go v1.21.4` (kadm + kgo, pinned in
  `../../go.mod`).
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). **The connector is DISABLED by default — start the
  broker with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1). For any non-same-host
  client, also set `CONNECTORS_KAFKA_ADVERTISED_HOST` or the client connects then
  hangs (gotcha #2).

## How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
go run ./offsets/list-and-retention
```

## Expected Output

```
[kubemq-kafka] offsets/list-and-retention | bootstrap=localhost:9092 partitioner=murmur2(franz-go)
CreateTopic: kafka-ex-offsets-ret-<8hex> (retention.ms=600000 retention.bytes=104857600)
DescribeTopicConfigs: retention.ms=600000 retention.bytes=104857600 (accepted)
Produce: 5 records
ListStartOffsets (earliest): 0
ListEndOffsets (latest): 5
ListOffsetsAfterMilli (by-ts=<ms>): 2
DeleteTopic: ok
PASS: earliest/latest/by-timestamp offsets + retention config verified
```

> The topic is suffixed with 8 random hex chars so concurrent runs of the other
> language examples against the same connector do not collide.

## What's Happening

The program creates a topic with `retention.ms=600000` and
`retention.bytes=104857600`, reads them back via `DescribeConfigs` and asserts both
were accepted. It produces 5 records, then runs the three offset queries:
`ListStartOffsets` returns the log-start (0), `ListEndOffsets` returns the
high-water mark (5 = one past the last record), and `ListOffsetsAfterMilli` with a
boundary timestamp resolves to the first offset at-or-after it (2). A wrong offset
on any query, or a rejected retention config, fails the process.

The wire flow is `CreateTopics → DescribeConfigs → Produce → ListOffsets
(earliest/latest/by-timestamp)`, mirroring connector behavior in
`connectors/kafka/listoffsets_test.go`.

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| ListOffsets(2), DescribeConfigs(32), Produce(0), CreateTopics(19), DeleteTopics(20) | acks=all; read_uncommitted | 1 partition | none | earliest = log-start; latest = HWM; by-ts = first record ≥ ts | none | murmur2 (franz-go) | `retention.ms` / `retention.bytes` accepted and echoed by `DescribeConfigs`; earliest tracks log-start after truncation |

## Related Examples

- Same variant in other languages: `../../../python/offsets/list_and_retention`,
  `../../../javascript/offsets/list-and-retention`,
  `../../../java/offsets/list-and-retention`,
  `../../../csharp/offsets/list-and-retention`,
  `../../../ruby/offsets/list_and_retention`,
  `../../../rust/offsets/list-and-retention`.
- Docs: `../../../../docs/concepts/topics-partitions-offsets.md`.
- Related: [`../../consume/seek-offsets-timestamps`](../../consume/seek-offsets-timestamps),
  [`../../admin/partitions-and-configs`](../../admin/partitions-and-configs).

> **Latest is one-past-the-last, earliest tracks truncation.** `ListEndOffsets`
> returns the high-water mark (5 for 5 records at offsets 0–4), and `ListStartOffsets`
> tracks the log-start, which advances when records are deleted or aged out by
> retention. Because offsets are STAN Sequences, they never renumber — a deleted
> low-end just moves the start forward.

> Auth: this example uses the no-auth default posture. Runs with no SASL by default
> on a stock dev broker; for SASL/PLAIN + SCRAM (and mTLS principal derivation) see
> [`../../security/sasl-plain-scram`](../../security/sasl-plain-scram) +
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
