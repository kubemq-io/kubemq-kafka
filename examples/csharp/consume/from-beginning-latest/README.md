# C# — Kafka: Consume From Beginning vs Latest

Show how `auto.offset.reset` decides where a fresh consumer group starts:
**earliest** replays the whole log; **latest** sees only records produced after the
subscription is live.

## Prerequisites

- .NET SDK **8.0**
- **Confluent.Kafka 2.6.0** (pinned in `examples/csharp/Directory.Packages.props`).
- A running KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` (default
  `localhost:9092`) — **start with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1).

## How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
dotnet run --project consume/from-beginning-latest
```

## Expected Output

```
[*] Created topic 'kafka-ex-consume-from-beginning-latest'
[x] seeded 3 pre-existing records
[v] [earliest] saw 3 record(s): pre #1, pre #2, pre #3
[*] [latest] partitions assigned; producing marker now
[v] [latest] saw 1 record(s): MARKER-after-subscribe
[*] Cleaned up topic 'kafka-ex-consume-from-beginning-latest'
[ok] auto.offset.reset verified: earliest replays history, latest sees only new records
```

## What's Happening

Three records are seeded first. A fresh group with `AutoOffsetReset.Earliest`
replays all three. A second fresh group with `AutoOffsetReset.Latest` subscribes,
waits for its **partitions-assigned** callback (so the start position is pinned to
the log end), then a marker is produced — the latest consumer sees ONLY the marker,
never the pre-existing records. Fetch is a long-poll (`Consume(timeout)`).

> The partitions-assigned handler is the deterministic way to know the `latest`
> consumer's position is set before producing the marker — avoids a startup race.

This mirrors the connector's Fetch + auto.offset.reset handling in `connectors/kafka/`.

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|----------|----------------|------------------|----------------|------------------|-------------|-------------|------------------|
| Metadata, Produce, Fetch (long-poll), FindCoordinator, ListOffsets | `acks=All` produce | `kafka-ex-consume-from-beginning-latest` / 1 | fresh `cs-earliest-<uuid>` / `cs-latest-<uuid>` | `auto.offset.reset` = `earliest` (log start) vs `latest` (log end) | none | CRC32 (librdkafka) | partitions-assigned handler pins `latest` position before marker; earliest sees 3, latest sees only marker |

## Related Examples

Same variant in the other languages:

- **Go** — [`../../../go/consume/from-beginning-latest`](../../../go/consume/from-beginning-latest)
- **Python** — [`../../../python/consume/from_beginning_latest`](../../../python/consume/from_beginning_latest)
- **Java** — [`../../../java/consume/from-beginning-latest`](../../../java/consume/from-beginning-latest)
- **JS/TS** — [`../../../javascript/consume/from-beginning-latest`](../../../javascript/consume/from-beginning-latest)
- **Ruby** — [`../../../ruby/consume/from_beginning_latest`](../../../ruby/consume/from_beginning_latest)
- **Rust** — [`../../../rust/consume/from-beginning-latest`](../../../rust/consume/from-beginning-latest)

Docs: [`../../../../docs/guides/consuming-and-groups.md`](../../../../docs/guides/consuming-and-groups.md),
[`../../../../docs/concepts/topics-partitions-offsets.md`](../../../../docs/concepts/topics-partitions-offsets.md)

---

> **Auth:** the connector default is no authentication. SASL/TLS setup lives in
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
