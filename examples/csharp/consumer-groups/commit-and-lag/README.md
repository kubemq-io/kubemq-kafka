# C# — Kafka: Commit & Lag

Manual offset commit (`EnableAutoCommit=false` + `Commit`), resume-from-committed in
a new consumer of the same group, and observe **lag** (high-watermark − committed).

## Prerequisites

- .NET SDK **8.0**
- **Confluent.Kafka 2.6.0** (pinned in `examples/csharp/Directory.Packages.props`).
- A running KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` (default
  `localhost:9092`) — **start with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1).

## How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
dotnet run --project consumer-groups/commit-and-lag
```

## Expected Output

```
[*] Created topic 'kafka-ex-consumer-groups-commit-and-lag'
[x] produced 10 records
[v] [A] consumed 'job #0' at offset 0
...
[*] [A] committed offset 5
[v] [lag] highWatermark=10 committed=5 → lag=5
[v] [B] resumed 'job #5' at offset 5
...
[*] Cleaned up topic 'kafka-ex-consumer-groups-commit-and-lag'
[ok] Manual commit + resume-from-committed + lag all verified
```

## What's Happening

Consumer A reads the first 5 records and `Commit`s (committed offset = last + 1 = 5),
then stops. `ListConsumerGroupOffsets` confirms the committed offset; comparing it to
the high-watermark (`QueryWatermarkOffsets`) gives **lag = 5**. Consumer B, in the
same group, resumes exactly at offset 5 (OffsetFetch) and drains the remaining 5 —
never re-reading the committed records.

> **Lag metric source.** The connector also exposes lag server-side as
> `kubemq_kafka_consumer_group_lag{group,topic,partition}`; this example computes
> it client-side from committed vs high-watermark so the assertion is self-contained.

This mirrors the connector's OffsetCommit / OffsetFetch path in `connectors/kafka/`.

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|----------|----------------|------------------|----------------|------------------|-------------|-------------|------------------|
| Metadata, Produce, OffsetCommit, OffsetFetch, ListOffsets (watermark) | `acks=All` produce | `kafka-ex-consumer-groups-commit-and-lag` / 1 | A then B, same `GroupId`, `EnableAutoCommit=false` | committed = last-read + 1; resume from committed | none | CRC32 (librdkafka) | lag = HWM − committed; connector also exposes `kubemq_kafka_consumer_group_lag{group,topic,partition}`; asserts resume + lag==5 |

## Related Examples

Same variant in the other languages:

- **Go** — [`../../../go/consumer-groups/commit-and-lag`](../../../go/consumer-groups/commit-and-lag)
- **Python** — [`../../../python/consumer-groups/commit_and_lag`](../../../python/consumer-groups/commit_and_lag)
- **Java** — [`../../../java/consumer-groups/commit-and-lag`](../../../java/consumer-groups/commit-and-lag)
- **JS/TS** — [`../../../javascript/consumer-groups/commit-and-lag`](../../../javascript/consumer-groups/commit-and-lag)
- **Ruby** — [`../../../ruby/consumer-groups/commit_and_lag`](../../../ruby/consumer-groups/commit_and_lag)
- **Rust** — [`../../../rust/consumer-groups/commit-and-lag`](../../../rust/consumer-groups/commit-and-lag)

Docs: [`../../../../docs/concepts/consumer-groups.md`](../../../../docs/concepts/consumer-groups.md),
[`../../../../docs/reference/capabilities.md`](../../../../docs/reference/capabilities.md)

---

> **Auth:** the connector default is no authentication. SASL/TLS setup lives in
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
