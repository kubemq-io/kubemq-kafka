# Go — Kafka: Consume From-Beginning / Latest

The two `auto.offset.reset` start positions against the KubeMQ Kafka connector:
an `earliest` consumer reads all pre-existing records, a `latest` consumer reads
only records produced **after** it subscribed.

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
go run ./consume/from-beginning-latest
```

## Expected Output

```
[kubemq-kafka] consume/from-beginning-latest | bootstrap=localhost:9092 partitioner=murmur2(franz-go)
CreateTopic: kafka-ex-consume-reset-<8hex> (partitions=1)
Seed: produced 3 pre-existing records
earliest (AtStart): read 3 pre-existing records
Produce: 1 record AFTER the latest consumer subscribed
latest (AtEnd): read only the post-subscribe record "post-latest"
DeleteTopic: ok
PASS: auto.offset.reset earliest/latest verified
```

> The topic is suffixed with 8 random hex chars so concurrent runs of the other
> language examples against the same connector do not collide.

## What's Happening

The program seeds 3 records, then opens a consumer at `ConsumeStartOffset(AtStart)`
(the `auto.offset.reset=earliest` analog) and asserts it drains exactly those 3.
It then opens a second consumer at `AtEnd` (`auto.offset.reset=latest`), which
positions at the log's high-water mark and sees nothing yet; a single record is
produced afterward, and the program asserts the `latest` consumer reads **only**
that one post-subscribe record — never the 3 pre-existing ones. A mismatch on
either count fails the process.

The wire flow is `Metadata → ListOffsets (earliest/latest) → Fetch (long-poll)`,
mirroring connector behavior in `connectors/kafka/fetch_test.go`.

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| ListOffsets(2), Fetch(1), Produce(0), CreateTopics(19), DeleteTopics(20) | acks=all; read_uncommitted | 1 partition | none (explicit start offset) | earliest = log-start (0); latest = high-water mark | none | murmur2 (franz-go) | `earliest` reads the full backlog; `latest` sees only post-subscribe records; both counts asserted |

## Related Examples

- Same variant in other languages: `../../../python/consume/from_beginning_latest`,
  `../../../javascript/consume/from-beginning-latest`,
  `../../../java/consume/from-beginning-latest`,
  `../../../csharp/consume/from-beginning-latest`,
  `../../../ruby/consume/from_beginning_latest`,
  `../../../rust/consume/from-beginning-latest`.
- Docs: `../../../../docs/guides/consuming-and-groups.md`.
- Related: [`../seek-offsets-timestamps`](../seek-offsets-timestamps).

> **`latest` can miss in-flight records.** A `latest` consumer only sees records
> whose offset is ≥ the high-water mark at subscribe time; anything produced in the
> gap between "opened" and "positioned" is skipped. Use `earliest` (or an explicit
> committed offset) when you must not lose the backlog.

> Auth: this example uses the no-auth default posture. Runs with no SASL by default
> on a stock dev broker; for SASL/PLAIN + SCRAM (and mTLS principal derivation) see
> [`../../security/sasl-plain-scram`](../../security/sasl-plain-scram) +
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
