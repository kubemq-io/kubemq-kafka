# kubemq-kafka

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Languages](https://img.shields.io/badge/languages-7%20(Go%2C%20Python%2C%20Java%2C%20JS%2FTS%2C%20C%23%2C%20Ruby%2C%20Rust)-informational.svg)](examples/)
[![Examples](https://img.shields.io/badge/examples-~91-success.svg)](examples/)
[![Protocol](https://img.shields.io/badge/protocol-Apache%20Kafka%20wire-orange.svg)](docs/)
[![Direct Connect](https://img.shields.io/badge/KubeMQ-direct--connect-9cf.svg)](https://kubemq.io/)

**Point an Apache Kafka app at KubeMQ by changing only `bootstrap.servers`.**

## Contents

- [Connection](#connection)
- [Repository map](#repository-map)
- [Languages & Kafka clients](#languages--kafka-clients)
- [Quickstart](#quickstart)
- [Protocol scope](#protocol-scope)
- [License](#license)

KubeMQ embeds an **Apache Kafka wire-protocol connector** inside `kubemq-server` — a dedicated
listener (plain TCP **9092**, TLS **9093**) speaking the real Kafka wire protocol. Any standard,
unmodified Kafka client connects by repointing only `bootstrap.servers` — no code change, no
library swap, no KubeMQ SDK.

Unlike the AMQP/MQTT connectors (enabled by default), the Kafka connector is **DISABLED by
default** — set **`CONNECTORS_KAFKA_ENABLE=true`** to open the `:9092`/`:9093` listeners. It is
compiled into the default server build since v3.1 (no `kafka` build tag).

This repository is **documentation + runnable examples + a Go burn-in soak harness**. It ships
**no installable package**, **no proto / gRPC bindings**, and **no published client library**. It
teaches driving the connector from 7 native Kafka clients (Go `franz-go`, Python
`confluent-kafka`, Java `kafka-clients`, JS/TS `kafkajs`, C# `Confluent.Kafka`, Ruby `rdkafka`,
Rust `rdkafka`).

> **Mental model.** Kafka maps onto a **single** KubeMQ pattern (**Events Store**), so this repo
> is organized by the Kafka **concept vocabulary** (`produce/`, `consume/`, `consumer-groups/`,
> `admin/`, `offsets/`, `transactions/`, `security/`), not by KubeMQ's
> events/queues/commands patterns.
>
> - **Kafka topic `orders` ↔ internal KubeMQ Events-Store channel `kafka.orders`** (channel prefix =
>   `kafka.`). The `kafka.*` namespace is **reserved for the connector** — native KubeMQ gRPC/REST
>   clients cannot read or write it (`Error 443`); topics are reachable only over the Kafka wire
>   protocol (no cross-protocol interop).
> - **Offset = STAN `Sequence`** — durable, restart-stable, Raft-replicated, identical across
>   nodes.
> - Multi-partition (N>1): partition p>0 → internal channel `kafka.orders~<p>`; p0 stays
>   `kafka.orders`. Partition count is **increase-only** (`CreatePartitions`), hard cap **256**.

## Connection

A KubeMQ server with the Kafka connector **enabled** (`CONNECTORS_KAFKA_ENABLE=true`, listening on
port **9092**) is assumed to be running and reachable. The examples expose a single convenience
variable used as `bootstrap.servers`:

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"   # default; used as bootstrap.servers
```

Each language sets its client's `bootstrap.servers` from `KUBEMQ_KAFKA_BOOTSTRAP` (franz-go
`kgo.SeedBrokers`, confluent-kafka `{'bootstrap.servers': ...}`, Java `bootstrap.servers`, kafkajs
`{ brokers: [...] }`, Confluent.Kafka `BootstrapServers`, rdkafka-ruby `"bootstrap.servers"`, rust
rdkafka `.set("bootstrap.servers", ...)`).

> **AdvertisedHost banner.** For **external** clients you MUST set
> `CONNECTORS_KAFKA_ADVERTISED_HOST` on the server — an empty value advertises the pod hostname, so
> clients connect then hang (the "M-23" footgun). TLS SAN must cover `AdvertisedHost`.
> Single-endpoint model — no Kafka multi-listener `advertised.listeners`. See
> [`docs/getting-started.md`](docs/getting-started.md).

## Repository map

| Path | What it is |
|------|------------|
| [`docs/`](docs/README.md) | Architecture, getting-started, configuration, concepts, guides, reference (incl. `migration-from-kafka.md`) |
| [`examples/`](examples/README.md) | 13 example variants × 7 languages (Go, Python, Java, JS/TS, C#, Ruby, Rust) = ~84-91 programs |
| [`burnin/`](burnin/) | Standalone Go burn-in soak harness (franz-go transport; one worker per Kafka operation family incl. EOS) |
| [`SHARED-CONVENTIONS.md`](SHARED-CONVENTIONS.md) | Single source of truth: directory naming, README templates, the `KUBEMQ_KAFKA_BOOTSTRAP` convention, the 13-variant table, the gotchas, exit-code rules |
| [`examples/SHARED-CONVENTIONS.md`](examples/SHARED-CONVENTIONS.md) | A verbatim copy of the root conventions, alongside the examples |
| `LICENSE` | Apache-2.0 |

## Languages & Kafka clients

| Language | Kafka client (floor; bump+lock at impl) | Partitioner default | bootstrap.servers set via |
|----------|------------------------------------------|---------------------|---------------------------|
| Go (examples + burn-in) | `github.com/twmb/franz-go` **(server's own conformance client)** | murmur2 | `kgo.SeedBrokers(...)` |
| Python | `confluent-kafka` (librdkafka; via **uv**) | CRC32 | `{'bootstrap.servers': ...}` |
| Java | `org.apache.kafka:kafka-clients` (+ AdminClient) | murmur2 | `bootstrap.servers` prop |
| JS/TS | `kafkajs` (≥2.0, murmur2 default) | murmur2 | `{ brokers: [...] }` |
| C# / .NET | `Confluent.Kafka` (librdkafka) | CRC32 | `BootstrapServers` |
| Ruby | `rdkafka` (rdkafka-ruby; librdkafka) | CRC32 | `"bootstrap.servers"` config |
| Rust | `rdkafka` (librdkafka) + `tokio` | CRC32 | `.set("bootstrap.servers", ...)` |

> Only **`franz-go`** is exercised by `kubemq-server`'s own Kafka conformance tests. The other six
> clients are wire-compatible and THIS repo's examples + burn-in are their proof — stated honestly
> throughout. **Partitioner divergence (gotcha #4):** franz-go / Java / kafkajs default to
> **murmur2**; the four librdkafka clients (confluent-kafka, Confluent.Kafka, rdkafka-ruby, rust
> rdkafka) default to **CRC32** — the same key can land on different partitions across clients.

## Quickstart

1. Ensure a KubeMQ server with the Kafka connector is reachable on port 9092
   (`CONNECTORS_KAFKA_ENABLE=true`, `CONNECTORS_KAFKA_ADVERTISED_HOST` set for remote clients); set
   `KUBEMQ_KAFKA_BOOTSTRAP`.
2. Pick a language under [`examples/`](examples/README.md) and start with `produce/basic-acks`.
3. Read [`docs/getting-started.md`](docs/getting-started.md) for the first-message walkthrough
   (enable → advertise → `kcat`/franz-go smoke).

## Protocol scope

The connector speaks the real Apache Kafka wire protocol. Scope is stated honestly below — every
🟡 states its exact scope, and every EOS claim cites the KIP-890 V1 ceiling.

**✅ Full Kafka APIs:**

> Produce (acks 0/1/all, RecordBatch v2) · Idempotent producer (`InitProducerId`, per-(PID,partition)
> dedup) · Fetch (bounded long-poll) · ListOffsets (earliest/latest/by-timestamp) · Compression
> none/gzip/snappy/lz4/zstd · Classic consumer groups (Find/Join/Sync/Heartbeat/Leave) ·
> OffsetCommit/OffsetFetch · Consumer-group lag (`kubemq_kafka_consumer_group_lag`) · Topic identity
> by UUID (KIP-516) · CreateTopics/DeleteTopics/DescribeConfigs/DescribeCluster (auto-create) ·
> SASL/PLAIN · SASL/SCRAM-SHA-256/512 · mTLS principal (verified-chain CN) · Kafka ACL enforcement →
> Casbin · Retention · `read_committed` + `AbortedTransactions` + `OffsetFetch RequireStable`
> (Transactions V1) · Multi-partition topics N>1 (increase-only).

**🟡 Partial — exact scope, never "Full":**

| API (key) | Exact partial scope |
|-----------|---------------------|
| `DeleteRecords` (21) | low-end log truncation only |
| `CreatePartitions` (37) | increase-only, strictly-greater, ≤256, else `INVALID_PARTITIONS` |
| `IncrementalAlterConfigs` (44) | subset recognized; many accepted-but-no-op |
| `DescribeTopicPartitions` (75) | falls back to `Metadata` |
| ACL management (29/30/31) | enforcement full; management = honest empty view / `SECURITY_DISABLED` |
| Quota (48/49) | per-principal produce+fetch token-bucket baseline |
| **Transactions / EOS** (24/25/26/28 + txn Produce) | **V1** + in-log COMMIT/ABORT markers + `read_committed` serving; full `InitProducerId`→`AddPartitionsToTxn`→txn Produce→`EndTxn` with `(PID,epoch)` fencing (`INVALID_PRODUCER_EPOCH(47)`/`PRODUCER_FENCED(90)`). **Subject to the KIP-890 V1 ceiling below.** |

**🟡 KIP-890 V1 EOS ceiling — verbatim honesty, cited in every EOS artifact:**

> **Known limitation — the V1 (no TV2) transactional-soundness ceiling (KIP-890).** KubeMQ's V1
> transaction implementation does not bump the producer epoch on every `EndTxn`. A same-epoch zombie
> produce delayed past its own `EndTxn` can still be admitted into that producer's next transaction.
> This is the **upstream-shared** KIP-890 ceiling — every pre-TV2 Kafka deployment has it — **not a
> KubeMQ defect, and explicitly not counted as a soak/conformance failure.**

**✅ Supported on `next` / ⛔ Non-goals / 🔴 Not-yet:**

> ✅ Supported on `next` (the compaction-dependent ecosystem): log compaction
> (`cleanup.policy=compact`, GA on `next`); Kafka Streams / Connect (rely on compacted internal
> topics, which `next` provides); Schema-Registry **wire** interop (5-byte magic prefix); OAUTHBEARER
> (SASL_SSL / OIDC only). ⛔ Genuine non-goals: Schema-Registry **service** / ksqlDB; MirrorMaker 2;
> Confluent Control Center / Cruise Control; GSSAPI/Kerberos. 🔴 KIP-848 next-gen groups; static
> membership; delegation tokens; share groups; txn-admin RPCs (27/61/65/66).

There is **no docker-compose / boot-the-server quickstart** — a running connector is provided by
the environment and reached via `KUBEMQ_KAFKA_BOOTSTRAP`.

## License

Apache-2.0 — see [`LICENSE`](LICENSE).