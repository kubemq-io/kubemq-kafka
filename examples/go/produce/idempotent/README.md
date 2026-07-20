# Go — Kafka: Produce Idempotent

The idempotent-producer round-trip against the KubeMQ Kafka connector:
`InitProducerId → Produce(acks=all, enable.idempotence) → Fetch`, asserting a
producer ID is assigned and no offset is duplicated on retry.

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
go run ./produce/idempotent
```

## Expected Output

```
[kubemq-kafka] produce/idempotent | bootstrap=localhost:9092 partitioner=murmur2(franz-go)
CreateTopic: kafka-ex-produce-idem-<8hex> (partitions=1)
ProducerID: id=<pid> epoch=0 (idempotence ON)
Produce: 5 idempotent records (acks=all)
Fetch: 5 distinct records, 5 distinct offsets (no duplicates)
DeleteTopic: ok
PASS: idempotent producer — PID assigned, no duplicate offsets
```

> The topic is suffixed with 8 random hex chars so concurrent runs of the other
> language examples against the same connector do not collide.

## What's Happening

franz-go turns on the idempotent producer by default, so on the first Produce the
client issues `InitProducerId` and the broker assigns a `(Producer ID, epoch)`
pair. The program asserts the returned PID is non-negative, then writes 5 records
with `acks=all` (idempotence requires `acks=all`; franz-go keeps it on only when
acks are all-ISR). It reads every record back and asserts 5 **distinct** offsets —
the broker's per-`(PID, partition)` sequence-number dedup means a retried batch is
collapsed to one write, never a duplicate offset. Any duplicate fails the process.

The wire flow is `Metadata → InitProducerId → Produce (idempotent RecordBatch v2)
→ Fetch`, mirroring connector behavior in `connectors/kafka/produce_test.go`.

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| InitProducerId(22), Produce(0), Fetch(1), CreateTopics(19), DeleteTopics(20) | acks=all (required for idempotence); read_uncommitted | 1 partition | none | offset = STAN Sequence, starts at 0; one offset per record, no gaps | none | murmur2 (franz-go) | idempotence is default-ON and needs `acks=all`; use `DisableIdempotentWrite` only if you intentionally drop to non-all acks; asserts no duplicate offset on retry |

## Related Examples

- Same variant in other languages: `../../../python/produce/idempotent`,
  `../../../javascript/produce/idempotent`, `../../../java/produce/idempotent`,
  `../../../csharp/produce/idempotent`, `../../../ruby/produce/idempotent`,
  `../../../rust/produce/idempotent`.
- Docs: `../../../../docs/guides/producing.md`.
- Related: [`../basic-acks`](../basic-acks), [`../compression-and-keys`](../compression-and-keys).

> **Idempotence ⇒ `acks=all`.** The default-ON idempotent producer only stays on
> with `acks=all`. Dropping to `acks=1`/`acks=0` disables the per-`(PID,partition)`
> dedup — franz-go requires an explicit `DisableIdempotentWrite` to do so, which
> trades exactly-once-per-partition semantics for throughput.

> Auth: this example uses the no-auth default posture. Runs with no SASL by default
> on a stock dev broker; for SASL/PLAIN + SCRAM (and mTLS principal derivation) see
> [`../../security/sasl-plain-scram`](../../security/sasl-plain-scram) +
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
