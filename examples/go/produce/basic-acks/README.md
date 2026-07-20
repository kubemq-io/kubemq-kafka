# Go — Kafka: Produce Basic Acks

The Produce round-trip against the KubeMQ Kafka connector across all three ack
levels, plus the oversized → `MESSAGE_TOO_LARGE` rejection:
`CreateTopic → Produce(acks=all) → Fetch → Produce(acks=1|0) → Produce(2 MiB)→reject`.

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
go run ./produce/basic-acks
```

## Expected Output

```
[kubemq-kafka] produce/basic-acks | bootstrap=localhost:9092 partitioner=murmur2(franz-go)
CreateTopic: kafka-ex-produce-acks-<8hex> (partitions=1)
Produce(acks=all): partition=0 offset=0
Fetch: offset=0 value="order #4242 — 3x widget, ship express"
Produce(acks=1): ok
Produce(acks=0): ok
Produce(2 MiB): rejected with MESSAGE_TOO_LARGE (expected)
DeleteTopic: ok
PASS: produce acks 0/1/all round-trip + MESSAGE_TOO_LARGE verified
```

> The topic is suffixed with 8 random hex chars so concurrent runs of the other
> language examples against the same connector do not collide.

## What's Happening

The program creates a single-partition topic, produces one record with `acks=all`
(the durable default), reads it back from the start of the log and asserts the body
round-trips byte-for-byte. It then confirms `acks=1` and `acks=0` also accept a
record — on a multi-node deployment `acks=0` against a follower can silently drop
(gotcha #3), so production code uses `acks>=1`. Finally it produces a 2 MiB record,
which exceeds the connector's `CONNECTORS_KAFKA_MAX_MESSAGE_BYTES` (1 MiB default),
and asserts the broker returns `MESSAGE_TOO_LARGE`. Any mismatch exits non-zero.

The wire flow is `Metadata → CreateTopics → Produce (RecordBatch v2) → Fetch`,
mirroring connector behavior in `connectors/kafka/produce_test.go`.

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Produce(0), Fetch(1), CreateTopics(19), DeleteTopics(20), Metadata(3) | acks 0/1/all; read_uncommitted | 1 partition | none | offset = STAN Sequence, starts at 0 | none | **murmur2** (franz-go) | 2 MiB → `MESSAGE_TOO_LARGE`; raises `ProducerBatchMaxBytes` so the oversized record reaches the broker |

## Related Examples

- Same variant in other languages: `../../../python/produce/basic_acks`,
  `../../../javascript/produce/basic-acks`, `../../../java/produce/basic-acks`,
  `../../../csharp/produce/basic-acks`, `../../../ruby/produce/basic_acks`,
  `../../../rust/produce/basic-acks`.
- Docs: `../../../../docs/guides/producing.md`.
- Next: [`../idempotent`](../idempotent), [`../compression-and-keys`](../compression-and-keys).

> **Gotcha #3 — `acks>=1` on multi-node.** `acks=0` gives no delivery guarantee; on
> a multi-node cluster a record sent to a follower can be silently dropped. Use
> `acks=all` (or at least `acks=1`) for durability.

> Auth: this example uses the no-auth default posture. See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md)
> for SASL/PLAIN + SCRAM.
