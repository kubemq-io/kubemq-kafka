# KubeMQ Kafka examples

Runnable, copy-paste examples that drive the KubeMQ embedded Apache Kafka wire-protocol connector
using **standard, unmodified Kafka clients** (no KubeMQ SDK).
Every example is documentation you can execute: it prints human-readable progress, **asserts** the
expected behavior, exits **0 on success** and **non-zero on any failed assertion** — proofs, not
demos.

> **The connector is disabled by default.** Start the KubeMQ server with
> `CONNECTORS_KAFKA_ENABLE=true` before running anything here (gotcha #1). The authoritative
> conventions — the `KUBEMQ_KAFKA_BOOTSTRAP` convention, the 13-variant master table, the
> per-example README template, the directory-naming rules, and the 12 Kafka gotchas — live in
> [`SHARED-CONVENTIONS.md`](SHARED-CONVENTIONS.md).

## Connection

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"   # default; Kafka bootstrap.servers
# server side: CONNECTORS_KAFKA_ENABLE=true  (disabled by default!)  + AdvertisedHost for non-localhost
```

Each example reads `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`) and uses it as the client's
`bootstrap.servers` — nothing else changes versus talking to a real Kafka broker. For clients
connecting from another host, set `CONNECTORS_KAFKA_ADVERTISED_HOST` on the server so the broker
advertises a reachable address (gotcha #2).

## Languages & pinned Kafka clients

| Language | Native Kafka client (floor) | Manifest | Run prereq |
|----------|-----------------------------|----------|------------|
| Go | `github.com/twmb/franz-go` (+ `pkg/kadm`) | `go/go.mod` | Go 1.24+ |
| Python | `confluent-kafka>=2.6,<3` (librdkafka) | `python/pyproject.toml` | Python 3.9+, `uv` |
| Java | `org.apache.kafka:kafka-clients:3.9.0` | `java/pom.xml` | JDK 21+, Maven 3.9+ |
| JS/TS | `kafkajs@^2.2.4` (murmur2 default) | `javascript/package.json` | Node 18+ |
| C# | `Confluent.Kafka 2.6.1` | `csharp/Directory.Packages.props` | .NET SDK 8.0 |
| Ruby | `rdkafka ~> 0.19` (librdkafka) | `ruby/Gemfile` | Ruby 3.3.x + C toolchain |
| Rust | `rdkafka 0.37` (librdkafka) + `tokio 1` | `rust/Cargo.toml` | Rust 1.75+ |

> Versions are MINIMUM floors as of 2026-07; bump to the latest stable and lock via `/check-deps`
> at implementation. franz-go is the connector's own conformance client and the de-facto reference
> for the other six languages.
>
> **Partitioner split (gotcha #4).** franz-go, Java `kafka-clients`, and kafkajs (≥2.0
> `DefaultPartitioner`) default to **murmur2**; the four librdkafka clients (`confluent-kafka`,
> `Confluent.Kafka`, `rdkafka-ruby`, rust `rdkafka`) default to **CRC32**. The same key lands on a
> **different** partition across the two groups — see
> [`../docs/concepts/cross-client-partitioning.md`](../docs/concepts/cross-client-partitioning.md).

## The 13 variants (concept matrix)

Grouped by Kafka concept (`produce/`, `consume/`, `consumer-groups/`, `admin/`, `offsets/`,
`transactions/`, `security/`), NOT KubeMQ patterns. Concept-group dirs use the same
kebab tokens in every language; variant-leaf dirs are kebab-case for go/javascript/java/csharp/rust
and snake_case for python/ruby.

| # | Group | Variant | Concept |
|---|-------|---------|---------|
| 1 | `produce/` | `basic-acks` | Produce with `acks=0/1/all`; delivery report / offset assigned |
| 2 | `produce/` | `idempotent` | Idempotent producer (PID + sequence); no duplicates under retry |
| 3 | `produce/` | `compression-and-keys` | gzip/snappy/lz4/zstd + keyed records; partitioner placement (gotcha #4) |
| 4 | `consume/` | `from-beginning-latest` | `auto.offset.reset` earliest vs latest start positions |
| 5 | `consume/` | `seek-offsets-timestamps` | `seek` by offset + `offsetsForTimes` by timestamp |
| 6 | `consumer-groups/` | `join-rebalance` | Two members share a `group.id`; partitions rebalance, no loss |
| 7 | `consumer-groups/` | `commit-and-lag` | OffsetCommit/Fetch; resume from committed; client-side lag |
| 8 | `admin/` | `topics-lifecycle` | CreateTopics → DescribeConfigs → DeleteTopics; `~` name rejected (gotcha #6) |
| 9 | `admin/` | `partitions-and-configs` | CreatePartitions (grow ≤256) + config alter; `INVALID_PARTITIONS` |
| 10 | `offsets/` | `list-and-retention` | ListOffsets earliest/latest/by-time; retention → channel `MaxAge`/`MaxBytes` |
| 11 | `transactions/` | `eos-commit-abort` | Transactional produce commit vs abort; KIP-890 V1 ceiling (gotcha #9) |
| 12 | `transactions/` | `read-committed` | `read_committed` consumer never sees aborted records (gotcha #12) |
| 13 | `security/` | `sasl-plain-scram` | SASL/PLAIN + SCRAM-256/512 (runnable); TLS/mTLS (doc-only) |

**13 variants × 7 languages = ~84–91 programs.** Per-language parity is the goal; a small number
of cells are **justified N/A** (the leaf dir + README always ship and explain the limitation — no
silent drops). See [`SHARED-CONVENTIONS.md`](SHARED-CONVENTIONS.md) §4 for the master table with
canonical citations.

## Coverage matrix

| # | Variant | Go | Python | Java | JS/TS | C# | Ruby | Rust |
|---|---------|----|--------|------|-------|----|------|------|
| 1 | `produce/basic-acks` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 2 | `produce/idempotent` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 3 | `produce/compression-and-keys` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 4 | `consume/from-beginning-latest` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 5 | `consume/seek-offsets-timestamps` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 6 | `consumer-groups/join-rebalance` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 7 | `consumer-groups/commit-and-lag` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 8 | `admin/topics-lifecycle` | ✅ | ✅ | ✅ | ✅ | ✅ | 🟡¹ | ✅ |
| 9 | `admin/partitions-and-configs` | ✅ | ✅ | ✅ | 🟡² | ✅ | 🟡¹ | ✅ |
| 10 | `offsets/list-and-retention` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 11 | `transactions/eos-commit-abort` | ✅ | ✅ | ✅ | ✅ | ✅ | 🟡³ | ✅ |
| 12 | `transactions/read-committed` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 13 | `security/sasl-plain-scram` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

✅ full · 🟡 justified N/A (folder + README ship and point to the supported alternative)

¹ **Ruby `rdkafka` admin surface** — create/delete topics is covered, but
`CreatePartitions`/`DescribeConfigs`/`IncrementalAlterConfigs` coverage is thin; where an op is
missing the README documents it and points at `admin/topics-lifecycle` in Go/Java.
² **kafkajs `DeleteRecords`** — `admin.createPartitions` is supported, but `DeleteRecords` is not in
kafkajs's admin API; that sub-assertion is marked N/A in the JS variant-9 README, pointing to
Go/Java.
³ **Ruby transactional producer** — the rdkafka-ruby transaction API exists in ≥0.15; if the pinned
floor lacks it, variant 11's Ruby README documents the limitation and defers to the Go/Java EOS
example. Every EOS artifact cites the **KIP-890 V1 ceiling** (gotcha #9).

## Not in the 13 (future / listed-not-built)

- **TLS / mTLS on `:9093`** — documented, not a runnable variant. The stock dev broker ships no
  certs, so `security/sasl-plain-scram` runs SASL/PLAIN + SCRAM only and documents the TLS/mTLS
  path; see [`../docs/guides/security-sasl-tls.md`](../docs/guides/security-sasl-tls.md).
- **Supported on `next` (✅):** log compaction (`cleanup.policy=compact`, GA on `next`), Kafka
  Streams, Kafka Connect (they rely on the compacted internal topics `next` provides), Schema-Registry
  **wire** interop, and SASL/OAUTHBEARER (SASL_SSL / OIDC only) — none ship a runnable variant here;
  see [`../docs/reference/capabilities.md`](../docs/reference/capabilities.md).
- **Non-goals (⛔):** a Schema Registry service, ksqlDB, MirrorMaker 2 — see
  [`../docs/reference/capabilities.md`](../docs/reference/capabilities.md).
- **Not-yet (🔴):** KIP-848 next-gen consumer groups, static membership, delegation tokens, share
  groups (KIP-932), and the transactional-admin RPCs — tracked as future work, not built here.

---

> **Auth:** the connector default is **no authentication** — a plaintext listener that accepts any
> client. `security/sasl-plain-scram` is the only variant that requires a server configured with a
> Kafka credential store; see [`../docs/guides/security-sasl-tls.md`](../docs/guides/security-sasl-tls.md).
