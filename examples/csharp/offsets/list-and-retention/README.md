# C# — Kafka: List Offsets & Retention

Query the log's **earliest** and **latest** offsets and a **by-timestamp** offset
(`ListOffsets`), and set topic **retention** (`retention.ms` / `retention.bytes`)
that the connector maps to channel `MaxAge` / `MaxBytes`.

## Prerequisites

- .NET SDK **8.0**
- **Confluent.Kafka 2.6.0** (pinned in `examples/csharp/Directory.Packages.props`).
- A running KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` (default
  `localhost:9092`) — **start with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1).

## How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
dotnet run --project offsets/list-and-retention
```

## Expected Output

```
[*] Created topic 'kafka-ex-offsets-list-and-retention' (retention.ms=600000, retention.bytes=1048576)
[x] produced 8 records
[v] [watermarks] earliest(low)=0 latest(high)=8
[v] [by-timestamp] midTime → offset 4
[v] [retention] retention.ms = 600000 (→ channel MaxAge)
[*] Cleaned up topic 'kafka-ex-offsets-list-and-retention'
[ok] ListOffsets earliest/latest/by-timestamp + retention config round-trip verified
```

## What's Happening

Eight records are produced. `QueryWatermarkOffsets` returns the earliest offset
(log-start = 0) and the latest (high-watermark = 8). `OffsetsForTimes` maps a
mid-run wall-clock time to an offset inside the log. The topic's `retention.ms` /
`retention.bytes` are set at create-time and read back with `DescribeConfigs`
(they map to channel `MaxAge` / `MaxBytes`).

> Retention is **time/size-based** — the example asserts the config **round-trips**,
> not that eviction happens within the run (that would require waiting out `MaxAge`).

This mirrors the connector's ListOffsets + retention-config mapping in `connectors/kafka/`.

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|----------|----------------|------------------|----------------|------------------|-------------|-------------|------------------|
| Metadata, CreateTopics, Produce, ListOffsets (earliest/latest/by-ts), DescribeConfigs | `acks=All` produce | `kafka-ex-offsets-list-and-retention` / 1 | ephemeral watermark queries | earliest = log-start (0), latest = HWM (count); by-ts → in-range offset | none | CRC32 (librdkafka) | `retention.ms`/`retention.bytes` → channel `MaxAge`/`MaxBytes`; asserts config round-trips (not eviction) |

## Related Examples

Same variant in the other languages:

- **Go** — [`../../../go/offsets/list-and-retention`](../../../go/offsets/list-and-retention)
- **Python** — [`../../../python/offsets/list_and_retention`](../../../python/offsets/list_and_retention)
- **Java** — [`../../../java/offsets/list-and-retention`](../../../java/offsets/list-and-retention)
- **JS/TS** — [`../../../javascript/offsets/list-and-retention`](../../../javascript/offsets/list-and-retention)
- **Ruby** — [`../../../ruby/offsets/list_and_retention`](../../../ruby/offsets/list_and_retention)
- **Rust** — [`../../../rust/offsets/list-and-retention`](../../../rust/offsets/list-and-retention)

Docs: [`../../../../docs/concepts/topics-partitions-offsets.md`](../../../../docs/concepts/topics-partitions-offsets.md),
[`../../../../docs/reference/configuration.md`](../../../../docs/reference/configuration.md)

---

> **Auth:** the connector default is no authentication. SASL/TLS setup lives in
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
