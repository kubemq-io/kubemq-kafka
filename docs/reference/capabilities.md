# Capabilities

What the Kafka connector supports at **Full** fidelity, what it implements **Partially** (with an
exact, stated scope), what it will **never** do, and what is **not-yet** built. Every claim here is
grounded in the connector source (`connectors/kafka/`) and the authoritative server docs
(`docs/24-kafka.md`, `docs/migration/kafka.md`). Docs, example READMEs, and burn-in workers **must
not** claim beyond this page. On any conflict, the merged connector code wins.

The four legend symbols used throughout this repo:

| Symbol | Meaning |
|---|---|
| ✅ **Full** | Implemented at Kafka fidelity; safe to rely on. |
| 🟡 **Partial** | Implemented with a **stated** scope limit — never described as "Full". |
| ⛔ **Never** | An architectural non-goal; will not be built. |
| 🔴 **Not-yet** | Deferred; not implemented today, may arrive later. |

## ✅ Full Kafka APIs — the runnable surface

These are exercised by the runnable examples (variants 1–10, 13) and the burn-in workers. Each maps
to a `connectors/kafka/*_test.go` canonical test.

| Area | API / key | Notes |
|---|---|---|
| Produce | `Produce` (key 0) | RecordBatch v2; `acks` 0/1/all; per-partition order; oversized → `MESSAGE_TOO_LARGE`. |
| Idempotent producer | `InitProducerId` (key 22) | `enable.idempotence`; per-`(PID,partition)` dedup. |
| Fetch | `Fetch` (key 1) | Bounded-read long-poll. |
| List offsets | `ListOffsets` (key 2) | earliest / latest / by-timestamp. |
| Compression | — | `none` / `gzip` / `snappy` / `lz4` / `zstd`. |
| Classic consumer groups | `FindCoordinator`/`JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup` (keys 10/11/14/12/13) | Classic (eager) rebalance protocol. |
| Offset commit/fetch | `OffsetCommit`/`OffsetFetch` (keys 8/9) | Durable per-`(group,topic,partition)`. |
| Consumer-group lag | metric | `kubemq_kafka_consumer_group_lag{group,topic,partition}`. |
| Topic identity by UUID | KIP-516, `Metadata` v13 | `Fetch` is name-based at v12. |
| Admin — topics | `CreateTopics`/`DeleteTopics`/`DescribeConfigs`/`DescribeCluster` (keys 19/20/32/60) | Auto-create on `Metadata`/`Produce`. |
| Security — SASL | `SASL/PLAIN`, `SASL/SCRAM-SHA-256`, `SASL/SCRAM-SHA-512` | See [../guides/security-sasl-tls.md](../guides/security-sasl-tls.md). |
| Security — OAUTHBEARER | OIDC-federated `SASL/OAUTHBEARER` (M4) | **SASL_SSL / OIDC only** — advertised/accepted on the TLS listener only; refused on plaintext with `UNSUPPORTED_SASL_MECHANISM(33)`. See [../guides/security-sasl-tls.md](../guides/security-sasl-tls.md). |
| Security — mTLS | principal = CN of the verified chain | |
| Authorization | Kafka ACL enforcement → Casbin | Enforcement is full (management is Partial — below). |
| Retention | `retention.ms` / `retention.bytes` | → channel `MaxAge` / `MaxBytes` / `MaxMsgs`. |
| Transactions — read side | `read_committed` isolation + `AbortedTransactions` + `OffsetFetch RequireStable` | Transactions V1. |
| Multi-partition topics | N > 1 (M8) | Increase-only; see [channel-mapping.md](channel-mapping.md). |

## 🟡 Partial — implemented, with an exact stated scope

Never describe any of these as "Full". Each row states the precise boundary.

| Area | API / key | Exact scope |
|---|---|---|
| Delete records | `DeleteRecords` (key 21) | **Low-end log truncation only.** |
| Create partitions | `CreatePartitions` (key 37) | **Increase-only**, strictly-greater, ≤ 256; else `INVALID_PARTITIONS`. |
| Alter configs | `IncrementalAlterConfigs` (key 44) | Subset recognized; many accepted-but-no-op. |
| Describe partitions | `DescribeTopicPartitions` (key 75) | Falls back to `Metadata`. |
| ACL management | keys 29/30/31 | **Enforcement is Full**; management = honest empty view / `SECURITY_DISABLED`. |
| Quota | keys 48/49 | Per-principal produce+fetch token-bucket baseline. |
| Transactions / EOS | keys 24/25/26/28 + transactional `Produce` | V1 — see below. |

### Transactions / EOS (Partial) — the exact flow

Transactions are **V1 (WP-9.1) + in-log COMMIT/ABORT markers (WP-9.2) + `read_committed` serving
(WP-9.3)**. The full flow works:

```
InitProducerId → AddPartitionsToTxn → transactional Produce → EndTxn(commit|abort)
```

- `(PID, epoch)` fencing: a fenced producer sees `INVALID_PRODUCER_EPOCH(47)` /
  `PRODUCER_FENCED(90)`.
- `AddOffsetsToTxn` + `TxnOffsetCommit` stage input offsets — **materialized on commit / discarded
  on abort**.
- `read_committed` filtering is **client-side** (via `AbortedTransactions`); there is no server-side
  record filter (gotcha #12).
- Txn offset-commit requires **Group WRITE** — stricter than real Kafka (gotcha #8).

> ### Known limitation — the KIP-890 V1 EOS ceiling (never overstate this)
>
> KubeMQ's V1 transaction implementation does **not** bump the producer epoch on every `EndTxn` (it
> pins below TV2 versions). Consequence: a zombie produce from the **same** producer epoch, delayed
> past its own `EndTxn`, can still be admitted into that producer's **next** transaction.
>
> This is the **upstream-shared** KIP-890 ceiling — every pre-TV2 Kafka deployment has it. It is
> **NOT a KubeMQ defect**, and it is **explicitly NOT counted** as a soak or conformance failure. The
> exhaustive multi-client EOS conformance matrix + real-cluster failover LSO-continuity soak are the
> WP-9.4 exit gate (deferred). **Every EOS doc, example, and burn-in worker must cite this ceiling.**
> See [../concepts/transactions-eos.md](../concepts/transactions-eos.md).

## ✅ Supported on `next` (the compaction-dependent ecosystem)

The Kafka connector runs **only on the `next` engine**, and `next` ships log compaction
(`cleanup.policy=compact`, tombstones, latest-value-per-key) on Kafka topic channels as a GA
capability. Because the compacted internal topics these depend on are available, the following are
**supported**, not non-goals:

| Supported on `next` | Note |
|---|---|
| **Log compaction** (`cleanup.policy=compact`) | **GA on `next`** — latest-value-per-key log rewrite with tombstones on Kafka topic channels, round-tripped via `CreateTopics`/`IncrementalAlterConfigs`. |
| **Kafka Connect** | Relies on compacted internal topics, which `next` provides. |
| **Kafka Streams** | Relies on compacted internal topics, which `next` provides. |
| **Schema-Registry _wire_ interop** | The 5-byte magic-byte value prefix passes through; the payload is opaque to the broker. |

## ⛔ Never — architectural non-goals

These are out of the connector's charter regardless of engine — they are **not** gated by
compaction (which `next` provides). They are never used as working examples.

| Non-goal | Note |
|---|---|
| **Schema-Registry _service_** | The hosted REST API + `_schemas`-topic-backed server is a non-goal — but Schema-Registry **wire** interop (the 5-byte magic prefix) works. |
| **ksqlDB** | A separate stream-processing runtime KubeMQ does not host as an embedded engine. |
| **MirrorMaker 2** | Cross-cluster mirroring is not offered as a hosted tool; migration is start-fresh by design. |
| **Confluent Control Center / Cruise Control** | Depend on proprietary broker-side metrics-reporter plugins. |
| **GSSAPI / Kerberos SASL** | No KDC integration; SASL/PLAIN and SASL/SCRAM are the supported mechanisms. |

## 🔴 Not-yet — deferred

| Deferred | Note |
|---|---|
| **KIP-848 next-gen consumer groups** | Classic protocol only today. |
| **Static membership** | — |
| **Delegation tokens** | — |
| **Share groups (KIP-932)** | — |
| **Txn-admin RPCs** | `WriteTxnMarkers`(27) / `DescribeProducers`(61) / `DescribeTransactions`(65) / `ListTransactions`(66) — **no CLI `--abort`**. A wedged transaction is bounded by the `transaction.timeout.ms` reaper. |

## Capacity limits

Grounded in `docs/24-kafka.md`; see [configuration.md](configuration.md) for the env-var knobs.

| Limit | Value |
|---|---|
| Topics / node | Unbounded (auth-gated). |
| Partitions / topic | **256** hard cap. |
| Connections / node | 1000 (`CONNECTORS_KAFKA_MAX_CONNECTIONS`; 0 = unlimited). |
| Consumer groups | 10000 (`CONNECTORS_KAFKA_MAX_GROUPS`). |
| Max message bytes | 1 MiB (`CONNECTORS_KAFKA_MAX_MESSAGE_BYTES`, `1048576`). |
| Parked fetch long-polls | 1024. |
| Produce / Fetch quota | `CONNECTORS_KAFKA_{PRODUCE,FETCH}_BYTE_RATE` (0 = unlimited). |

## Proven real clients

The connector is verified against real Kafka client libraries — no shim, no fork:

- **franz-go** — the server's own conformance client (murmur2 partitioner).
- **Java** `kafka-clients` / `AdminClient` / Spring.
- **librdkafka** / `kcat` / `confluent-kafka` (CRC32 partitioner).
- **sarama**.
- **segmentio** (`kafka-go`).

> **Partitioner divergence (gotcha #4).** franz-go, Java `kafka-clients`, and `kafkajs` (v2+
> `DefaultPartitioner`) default to **murmur2**; the librdkafka-based clients default to **CRC32**.
> The same key can land on a different partition across client families. See
> [../concepts/cross-client-partitioning.md](../concepts/cross-client-partitioning.md).

## See Also

- [channel-mapping.md](channel-mapping.md) — topic/partition/offset/group → channel grammar.
- [error-codes.md](error-codes.md) — the Kafka error codes this connector surfaces.
- [configuration.md](configuration.md) — every `CONNECTORS_KAFKA_*` field.
- [migration-from-kafka.md](migration-from-kafka.md) — the start-fresh repoint + what does/doesn't migrate.

## Source

`connectors/kafka/` (merged connector code — the source of truth). Server docs `docs/24-kafka.md`
(API surface, capacity table, EOS ceiling) and `docs/migration/kafka.md`. Canonical tests include
`fetch_test.go`, `listoffsets_test.go`, `groupoffsets_test.go`, `txn_rpcs_test.go`.
