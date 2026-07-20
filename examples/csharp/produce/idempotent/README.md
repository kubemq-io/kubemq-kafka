# C# — Kafka: Idempotent Producer

Enable the idempotent producer (`EnableIdempotence=true`) so a producer-side retry
never appends a duplicate. Produce N records, read them all back, assert **no
duplicates**.

## Prerequisites

- .NET SDK **8.0**
- **Confluent.Kafka 2.6.0** (pinned in `examples/csharp/Directory.Packages.props`).
- A running KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` (default
  `localhost:9092`) — **start the server with `CONNECTORS_KAFKA_ENABLE=true`**
  (gotcha #1).

## How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
dotnet run --project produce/idempotent
```

## Expected Output

```
[*] Created topic 'kafka-ex-produce-idempotent'
[x] produced 'idempotent #1' at kafka-ex-produce-idempotent [[0]] @0
...
[v] consumed 'idempotent #5' at kafka-ex-produce-idempotent [[0]] @4
[*] Cleaned up topic 'kafka-ex-produce-idempotent'
[ok] Idempotent produce complete: 5 records, no duplicates (PID dedup via InitProducerId)
```

## What's Happening

`EnableIdempotence=true` makes librdkafka issue **`InitProducerId`** (ApiKey 22) to
obtain a Producer Id (PID). Every record carries `(PID, epoch, sequence)`; the
broker dedups per `(PID, partition)`, so any transient retry within the send is
collapsed. Idempotence also forces `acks=all` (asserted via
`dr.Status == Persisted`). Reading the log back shows exactly `count` unique
records.

> Idempotence is the **default** for `acks=All` in Confluent.Kafka 2.x. This
> example sets it explicitly to make the guarantee visible; the transactions
> examples build on the same PID handshake.

This mirrors the connector's idempotent-produce path in `connectors/kafka/`.

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|----------|----------------|------------------|----------------|------------------|-------------|-------------|------------------|
| Metadata, CreateTopics, InitProducerId, Produce, Fetch, DeleteTopics | `acks=All` (forced by idempotence) | `kafka-ex-produce-idempotent` / 1 | ephemeral `cs-idempotent-<uuid>` | offset = STAN `Sequence`; read from `earliest` | none | CRC32 (librdkafka) | `EnableIdempotence` → `InitProducerId` (key 22), per-`(PID,partition)` dedup; asserts consumed count == produced, all distinct |

## Related Examples

Same variant in the other languages:

- **Go** — [`../../../go/produce/idempotent`](../../../go/produce/idempotent)
- **Python** — [`../../../python/produce/idempotent`](../../../python/produce/idempotent)
- **Java** — [`../../../java/produce/idempotent`](../../../java/produce/idempotent)
- **JS/TS** — [`../../../javascript/produce/idempotent`](../../../javascript/produce/idempotent)
- **Ruby** — [`../../../ruby/produce/idempotent`](../../../ruby/produce/idempotent)
- **Rust** — [`../../../rust/produce/idempotent`](../../../rust/produce/idempotent)

Docs: [`../../../../docs/guides/producing.md`](../../../../docs/guides/producing.md),
[`../../../../docs/concepts/transactions-eos.md`](../../../../docs/concepts/transactions-eos.md)

---

> **Auth:** the connector default is no authentication. SASL/TLS setup lives in
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
