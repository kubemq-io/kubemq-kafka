# Go — Kafka: Consumer-Groups Commit and Lag

Offset commit, resume-from-committed, and consumer-group lag against the KubeMQ
Kafka connector: consumer #1 reads and commits half the log, a lag query reports
the remainder, and consumer #2 resumes exactly where #1 left off.

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
go run ./consumer-groups/commit-and-lag
```

## Expected Output

```
[kubemq-kafka] consumer-groups/commit-and-lag | bootstrap=localhost:9092 partitioner=murmur2(franz-go)
CreateTopic: kafka-ex-cg-commit-<8hex> (partitions=1) group=kafka-ex-cg-cgrp-<8hex>
Produce: 10 records
Consumer #1: read + committed first 5 records (through offset 4)
Lag: end=10 committed=5 lag=5
Consumer #2: resumed at offset 5, read the remaining 5 records (no re-read)
DeleteTopic: ok
PASS: commit + resume-from-committed + lag verified
```

> The topic and group are suffixed with random hex so concurrent runs across the
> language examples never collide on the same group id.

## What's Happening

The program produces 10 records, then consumer #1 (in a named group) reads the
first 5 and `CommitRecords` through offset 4, so the committed **next** offset is 5.
It queries the group's committed offset and the topic's end offset and computes lag
= `end − committed` = `10 − 5` = 5, asserting it matches the un-read remainder.
A second consumer in the same group then starts and, finding a committed offset,
resumes at offset 5 — it reads exactly the remaining 5 records and re-reads none.
Any wrong lag, wrong resume offset, or re-read fails the process.

The wire flow is `FindCoordinator → OffsetCommit → OffsetFetch → ListOffsets
(latest) → Fetch`, mirroring connector behavior in
`connectors/kafka/groupoffsets_test.go`.

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| FindCoordinator(10), OffsetCommit(8), OffsetFetch(9), ListOffsets(2), Fetch(1) | acks=all; read_uncommitted | 1 partition | one group, sequential members | committed = next-to-read; lag = end − committed | none | murmur2 (franz-go) | committed offset is durable (STAN Sequence); consumer #2 resumes from it with no re-read; lag asserted = 5 |

## Related Examples

- Same variant in other languages:
  `../../../python/consumer-groups/commit_and_lag`,
  `../../../javascript/consumer-groups/commit-and-lag`,
  `../../../java/consumer-groups/commit-and-lag`,
  `../../../csharp/consumer-groups/commit-and-lag`,
  `../../../ruby/consumer-groups/commit_and_lag`,
  `../../../rust/consumer-groups/commit-and-lag`.
- Docs: `../../../../docs/concepts/consumer-groups.md`,
  `../../../../docs/guides/consuming-and-groups.md`.
- Related: [`../join-rebalance`](../join-rebalance).

> **Committed offset is the *next* offset, not the last-read.** A commit "through
> offset 4" stores committed = 5, so a resuming member starts at 5. Off-by-one
> handling here is the difference between re-reading the last record and skipping
> the first un-read one — the example asserts the exact resume offset.

> Auth: this example uses the no-auth default posture. Runs with no SASL by default
> on a stock dev broker; for SASL/PLAIN + SCRAM (and mTLS principal derivation) see
> [`../../security/sasl-plain-scram`](../../security/sasl-plain-scram) +
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
