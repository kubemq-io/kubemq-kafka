# C# — Kafka: Partitions & Configs

Increase partitions (increase-only, ≤256), reject a bad decrease with
`INVALID_PARTITIONS`, and exercise the two **partial (🟡)** admin ops —
`IncrementalAlterConfigs` and `DeleteRecords`.

## Prerequisites

- .NET SDK **8.0**
- **Confluent.Kafka 2.6.0** (pinned in `examples/csharp/Directory.Packages.props`).
- A running KubeMQ Kafka connector at `KUBEMQ_KAFKA_BOOTSTRAP` (default
  `localhost:9092`) — **start with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1).

## How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
dotnet run --project admin/partitions-and-configs
```

## Expected Output

```
[*] Created topic 'kafka-ex-admin-partitions-and-configs' with 2 partitions
[v] [partitions] increased to 4
[*] [bad increase] IncreaseTo=2 (< 4) rejected: InvalidPartitions
[v] [incremental-configs 🟡] retention.ms now '7200000' (subset-recognized)
[v] [delete-records 🟡] log-start of partition 0 advanced to 3
[*] Cleaned up topic 'kafka-ex-admin-partitions-and-configs'
[ok] Partitions increase-only + INVALID_PARTITIONS + incremental-configs/delete-records (🟡) verified
```

## What's Happening

`CreatePartitions` grows the topic from 2 → 4 partitions (increase-only). Asking to
go **back** to 2 is rejected with `INVALID_PARTITIONS`. `IncrementalAlterConfigs`
sets `retention.ms`, read back via `DescribeConfigs`. `DeleteRecords` advances the
log-start of partition 0 to offset 3 (low-end truncation).

> **🟡 Partial semantics.** `IncrementalAlterConfigs` is **subset-recognized**: the
> connector maps a known set of topic configs (e.g. `retention.ms` → channel
> `MaxAge`) and ignores the rest. `DeleteRecords` supports **low-end truncation
> only** (advancing log-start), not arbitrary mid-log deletion. If the pinned
> `Confluent.Kafka` AdminClient or the connector does not expose one of these for a
> given key, the program prints the code and continues — the supported alternative
> is `kcat` / franz-go admin. No silent drop.
>
> **Gotcha #5 — growing N re-shards keys.** After increasing partition count, a
> CRC32-keyed record can land on a different partition than before; keyed consumers
> must tolerate this.

This mirrors the connector's CreatePartitions / IncrementalAlterConfigs /
DeleteRecords path in `connectors/kafka/`.

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|----------|----------------|------------------|----------------|------------------|-------------|-------------|------------------|
| CreatePartitions, IncrementalAlterConfigs (🟡), DeleteRecords (🟡), DescribeConfigs | n/a (admin only) | `kafka-ex-admin-partitions-and-configs` / 2→4 | n/a | `DeleteRecords` = low-end truncation only | n/a | n/a | **gotcha #5** (growing N re-shards keys); increase-only ≤256; decrease/>256 → `INVALID_PARTITIONS`; 🟡 IncrementalAlterConfigs subset-recognized |

## Related Examples

Same variant in the other languages:

- **Go** — [`../../../go/admin/partitions-and-configs`](../../../go/admin/partitions-and-configs)
- **Python** — [`../../../python/admin/partitions_and_configs`](../../../python/admin/partitions_and_configs)
- **Java** — [`../../../java/admin/partitions-and-configs`](../../../java/admin/partitions-and-configs)
- **JS/TS** — [`../../../javascript/admin/partitions-and-configs`](../../../javascript/admin/partitions-and-configs)
- **Ruby** — [`../../../ruby/admin/partitions_and_configs`](../../../ruby/admin/partitions_and_configs)
- **Rust** — [`../../../rust/admin/partitions-and-configs`](../../../rust/admin/partitions-and-configs)

Docs: [`../../../../docs/guides/admin-and-topics.md`](../../../../docs/guides/admin-and-topics.md),
[`../../../../docs/reference/capabilities.md`](../../../../docs/reference/capabilities.md)

---

> **Auth:** the connector default is no authentication. SASL/TLS setup lives in
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
