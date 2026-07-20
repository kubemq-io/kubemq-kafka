# C# — Kafka: Seek by Offset & Timestamp

Re-position a consumer by exact **offset** (`Seek`) and by **wall-clock time**
(`OffsetsForTimes` → ListOffsets by-timestamp), using manual `Assign` so `Seek` is
valid.

## Prerequisites

- .NET SDK **8.0**
- **Confluent.Kafka 2.6.0** (pinned in `examples/csharp/Directory.Packages.props`).
- A running KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` (default
  `localhost:9092`) — **start with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1).

## How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
dotnet run --project consume/seek-offsets-timestamps
```

## Expected Output

```
[*] Created topic 'kafka-ex-consume-seek-offsets-timestamps'
[x] produced 6 records; captured mid-time before record #3
[v] [seek] offset 4 → 'record #4' at offset 4
[v] [timestamp] midTime → offset 3
[v] [timestamp] first record at/after midTime → 'record #3'
[*] Cleaned up topic 'kafka-ex-consume-seek-offsets-timestamps'
[ok] Seek-by-offset and OffsetsForTimes (by-timestamp) both verified
```

## What's Happening

Six records are produced with a small gap; the wall-clock time just before record
`#3` is captured. The consumer uses manual `Assign` (not `Subscribe`) so `Seek`
targets a fixed partition. `Seek(offset 4)` reads exactly `record #4`.
`OffsetsForTimes(midTime)` returns the first offset whose record timestamp is
`>= midTime` — offset 3 — and seeking there reads `record #3`.

> Manual `Assign` is required for `Seek`/`OffsetsForTimes` to operate on a known
> partition; `Subscribe` hands partition choice to the group coordinator.

This mirrors the connector's ListOffsets (by-timestamp) + Fetch(seek) path in
`connectors/kafka/`.

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|----------|----------------|------------------|----------------|------------------|-------------|-------------|------------------|
| Metadata, Produce, ListOffsets (by-timestamp), Fetch | `acks=All` produce | `kafka-ex-consume-seek-offsets-timestamps` / 1 | manual `Assign` (no group balancing) | `Seek(offset)` = absolute; `OffsetsForTimes` = first offset with ts ≥ target | none | CRC32 (librdkafka) | manual `Assign` (not `Subscribe`) required for `Seek`; seek→offset 4, timestamp→record #3 |

## Related Examples

Same variant in the other languages:

- **Go** — [`../../../go/consume/seek-offsets-timestamps`](../../../go/consume/seek-offsets-timestamps)
- **Python** — [`../../../python/consume/seek_offsets_timestamps`](../../../python/consume/seek_offsets_timestamps)
- **Java** — [`../../../java/consume/seek-offsets-timestamps`](../../../java/consume/seek-offsets-timestamps)
- **JS/TS** — [`../../../javascript/consume/seek-offsets-timestamps`](../../../javascript/consume/seek-offsets-timestamps)
- **Ruby** — [`../../../ruby/consume/seek_offsets_timestamps`](../../../ruby/consume/seek_offsets_timestamps)
- **Rust** — [`../../../rust/consume/seek-offsets-timestamps`](../../../rust/consume/seek-offsets-timestamps)

Docs: [`../../../../docs/guides/consuming-and-groups.md`](../../../../docs/guides/consuming-and-groups.md),
[`../../../../docs/reference/channel-mapping.md`](../../../../docs/reference/channel-mapping.md)

---

> **Auth:** the connector default is no authentication. SASL/TLS setup lives in
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
