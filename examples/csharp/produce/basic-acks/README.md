# C# — Kafka: Basic Acks

Produce with **acks=All** (durable) and **acks=1** (leader-ack), read both records
back, and prove the connector rejects an oversized payload with
`MESSAGE_TOO_LARGE`. A runnable proof, not a demo.

## Prerequisites

- .NET SDK **8.0**
- **Confluent.Kafka 2.6.0** (pinned in `examples/csharp/Directory.Packages.props`).
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`) — **disabled by default; start the server with
  `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1).

## How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
dotnet run --project produce/basic-acks
```

## Expected Output

```
[*] Created topic 'kafka-ex-produce-basic-acks' → channel 'kafka.kafka-ex-produce-basic-acks'
[x] acks=All produced to kafka-ex-produce-basic-acks [[0]] @0 (status=Persisted)
[x] acks=1 produced to kafka-ex-produce-basic-acks [[0]] @1
[v] Consumed 'order #1001' at kafka-ex-produce-basic-acks [[0]] @0
[v] Consumed 'order #1002' at kafka-ex-produce-basic-acks [[0]] @1
[*] Oversized payload rejected: MsgSizeTooLarge (MESSAGE_TOO_LARGE)
[*] Cleaned up topic 'kafka-ex-produce-basic-acks'
[ok] Basic-acks produce round-trip complete (acks=All, acks=1, oversized rejected)
```

## What's Happening

`Metadata → CreateTopics → Produce (acks=All, then acks=1) → Fetch → oversized
Produce → DeleteTopics`. The `acks=All` produce returns a `DeliveryResult` whose
`Status == Persisted` and whose `Offset` is the Events-Store STAN Sequence. The
`acks=1` produce disables idempotence (default-ON requires `acks=all`). Both
records are read back through a fresh consumer group from `earliest`. A 2 MiB
payload exceeds the connector's 1 MiB `MaxMessageBytes`, so the broker replies
`MESSAGE_TOO_LARGE` (`ErrorCode.MsgSizeTooLarge`).

> **Gotcha #3 — `acks>=1` on a multi-node cluster.** With `acks=0` a produce to a
> follower can be silently dropped; every example defaults to `acks>=1` so a
> `DeliveryResult` offset is always confirmed. Never assert on an un-awaited
> `ProduceAsync`.

This mirrors connector behavior in `connectors/kafka/` (produce path).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|----------|----------------|------------------|----------------|------------------|-------------|-------------|------------------|
| Metadata, CreateTopics, Produce, Fetch, DeleteTopics | `acks=All`, `acks=1` | `kafka-ex-produce-basic-acks` / 1 | ephemeral `cs-basic-acks-<uuid>` | offset = STAN `Sequence`; read from `earliest` | none | CRC32 (librdkafka) | gotcha #3 (`acks>=1` multi-node); oversized → `MESSAGE_TOO_LARGE`; asserts `Persisted` + round-trip |

## Related Examples

Same variant in the other languages:

- **Go** — [`../../../go/produce/basic-acks`](../../../go/produce/basic-acks)
- **Python** — [`../../../python/produce/basic_acks`](../../../python/produce/basic_acks)
- **Java** — [`../../../java/produce/basic-acks`](../../../java/produce/basic-acks)
- **JS/TS** — [`../../../javascript/produce/basic-acks`](../../../javascript/produce/basic-acks)
- **Ruby** — [`../../../ruby/produce/basic_acks`](../../../ruby/produce/basic_acks)
- **Rust** — [`../../../rust/produce/basic-acks`](../../../rust/produce/basic-acks)

Docs: [`../../../../docs/guides/producing.md`](../../../../docs/guides/producing.md),
[`../../../../docs/reference/error-codes.md`](../../../../docs/reference/error-codes.md)

---

> **Auth:** the connector default is no authentication (plaintext, accept-any).
> SASL/TLS setup lives in
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
