# Migrating from Apache Kafka

## The Pitch

**Point your existing Kafka application at KubeMQ by changing only `bootstrap.servers`.** Same Kafka
client library, same code, same Kafka wire protocol. There is no SDK to adopt, no proto, no client
library swap — Kafka topics live in normal KubeMQ **Events Store** channels (`kafka.<topic>`).
Rollback is **config-only** (`CONNECTORS_KAFKA_ENABLE=false`).

But this is a **start-fresh repoint**, and several behaviors deviate from a stock Kafka broker. Read
the sections below before you migrate.

## Endpoint Swap

Only the bootstrap endpoint changes. The connector is **disabled by default** — enable it first.

| Aspect | Apache Kafka | KubeMQ Kafka connector |
|---|---|---|
| Bootstrap | `bootstrap.servers=broker:9092` | `bootstrap.servers=kubemq:9092` — **only this changes** |
| Enablement | always on | **disabled by default** — set `CONNECTORS_KAFKA_ENABLE=true` (gotcha #1) |
| Advertised host | `advertised.listeners` (multi-listener) | single `CONNECTORS_KAFKA_ADVERTISED_HOST` — **must be set** for external clients (gotcha #2) |
| TLS | `:9093`, per-listener certs | `:9093` (`CONNECTORS_KAFKA_TLS_PORT`); SAN must cover `AdvertisedHost` |
| Channel model | Kafka log segments | KubeMQ Events-Store channel `kafka.<topic>`; offset = STAN `Sequence` |

The examples wrap the bootstrap endpoint in a single `KUBEMQ_KAFKA_BOOTSTRAP` var (default
`localhost:9092`). See [../getting-started.md](../getting-started.md) and
[configuration.md](configuration.md).

## Start-Fresh Repoint (gotcha #11)

> **Historical data and committed offsets are NOT imported.** Repointing `bootstrap.servers` gives
> you a **fresh** broker: existing topics, their records, and existing consumer-group offsets on
> your old Kafka cluster do **not** migrate. Consumers start from `auto.offset.reset` (earliest /
> latest) on the KubeMQ side. If you need the old data, dual-write or replay it yourself; there is no
> built-in importer. Plan the cutover accordingly.

## The Deviations

| # | Area | Apache Kafka | KubeMQ Kafka connector | Gotcha |
|---|---|---|---|---|
| 1 | **Enablement** | always on | **disabled by default** (`CONNECTORS_KAFKA_ENABLE=true`) | #1 |
| 2 | **Advertised host** | multi-listener `advertised.listeners` | single `AdvertisedHost`; empty → connect-then-hang | #2 |
| 3 | **`acks=0` on multi-node** | as configured | an `acks=0` write to a follower can be **silently dropped** — use `acks>=1` | #3 |
| 4 | **Partitioner** | client-defined | murmur2 (franz-go/Java/kafkajs) vs CRC32 (librdkafka) → same key, different partition | #4 |
| 5 | **Growing N** | re-shards keys | re-shards keys — per-key order holds only within a fixed-N epoch | #5 |
| 6 | **`~` in topic names** | allowed | **reserved** (partition separator, M8) → `INVALID_TOPIC_EXCEPTION(17)` | #6 |
| 7 | **`/` in `transactional.id`** | allowed | **rejected** → `INVALID_TRANSACTIONAL_ID` → `INVALID_REQUEST(42)` | #7 |
| 8 | **Txn offset-commit** | Group READ suffices | requires Group **WRITE** (stricter, D141) | #8 |
| 9 | **EOS / transactions** | full (TV2 available) | **V1 only** — the KIP-890 same-epoch zombie ceiling applies (upstream-shared, not a defect) | #9 |
| 10 | **Cross-node partition count** | consistent | may read **stale-low** during Raft lag; self-heals on refresh | #10 |
| 11 | **Historical data / offsets** | present | **not imported** — start-fresh repoint | #11 |
| 12 | **`read_committed` filtering** | server-side | **client-side** (via `AbortedTransactions`) — no server-side record filter | #12 |

## Compatibility Matrix

What repoints cleanly, what changes, and what does not exist. Full detail in
[capabilities.md](capabilities.md).

| Kafka feature | Status | Note |
|---|---|---|
| Produce / Fetch / ListOffsets | ✅ Full | acks 0/1/all; compression none/gzip/snappy/lz4/zstd. |
| Idempotent producer | ✅ Full | `enable.idempotence`; per-`(PID,partition)` dedup. |
| Classic consumer groups + commit/lag | ✅ Full | Classic (eager) rebalance protocol. |
| Admin (Create/Delete/DescribeConfigs/DescribeCluster) | ✅ Full | Auto-create on `Metadata`/`Produce`. |
| SASL/PLAIN + SCRAM-256/512, mTLS, ACL enforcement | ✅ Full | See [../guides/security-sasl-tls.md](../guides/security-sasl-tls.md). |
| Multi-partition topics (N>1) | ✅ Full | Increase-only, ≤ 256. |
| `CreatePartitions` / `IncrementalAlterConfigs` / `DeleteRecords` | 🟡 Partial | Increase-only / subset / low-end truncation. |
| ACL management / Quota | 🟡 Partial | Enforcement Full; management empty-view. |
| Transactions / EOS | 🟡 Partial (V1) | KIP-890 ceiling — never overstate. |
| Log compaction (`cleanup.policy=compact`), Kafka Streams / Connect, Schema-Registry **wire** interop, OAUTHBEARER (SASL_SSL / OIDC only) | ✅ Supported on `next` | Compaction is GA on `next`; Streams/Connect rely on the compacted internal topics `next` provides. |
| KIP-848 groups, static membership, delegation tokens, share groups, txn-admin RPCs | 🔴 Not-yet | Deferred. |
| Schema-Registry service / ksqlDB, MirrorMaker 2, GSSAPI/Kerberos | ⛔ Never | Architectural non-goals (Schema-Registry **wire** interop still works). |

## Before You Migrate

1. **Enable the connector.** Set `CONNECTORS_KAFKA_ENABLE=true` and `CONNECTORS_KAFKA_ADVERTISED_HOST`
   to a reachable host (gotchas #1, #2). Verify with `kcat -L`.
2. **Rename topics that contain `~` or use `/` in a `transactional.id`** (gotchas #6, #7).
3. **Set `acks>=1`** on any multi-node deployment (gotcha #3).
4. **Pin one partitioner family** if the same keys are produced by multiple client libraries, or
   accept the murmur2/CRC32 divergence (gotcha #4). See
   [../concepts/cross-client-partitioning.md](../concepts/cross-client-partitioning.md).
5. **Grant transactional principals Group WRITE** (gotcha #8) and cite the **KIP-890 V1 ceiling**
   (gotcha #9) in your EOS runbook.
6. **Plan the cutover** — historical data and offsets do not migrate (gotcha #11).

## Rollback

Rollback is **config-only**:

```bash
export CONNECTORS_KAFKA_ENABLE=false   # closes the Kafka listeners (:9092 / :9093)
```

There is **no data migration to undo** — Kafka topics are ordinary KubeMQ Events-Store channels.
Point your app back at your Kafka cluster and you are done (subject to the start-fresh caveat — data
written to KubeMQ while cut over does not flow back).

## See Also

- [capabilities.md](capabilities.md) — the full ✅/🟡/⛔/🔴 surface.
- [channel-mapping.md](channel-mapping.md) — the `kafka.<topic>` / `~<partition>` grammar.
- [error-codes.md](error-codes.md) — the errors behind the deviations.
- [configuration.md](configuration.md) — enable + `AdvertisedHost` + TLS.

## Source

Server docs `docs/migration/kafka.md` (compatibility matrix, deviations) and `docs/24-kafka.md`.
Connector: `connectors/kafka/`.
