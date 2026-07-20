# Architecture

## Overview

The KubeMQ **Apache Kafka connector** is an embedded, wire-protocol bridge inside `kubemq-server`
that speaks the genuine Kafka wire protocol on two dedicated listeners (plain TCP **9092**, TLS
**9093**). Any standard Kafka client connects to it with **only a `bootstrap.servers` change** —
no code changes, no library swap. The connector is compiled into the default server build (since
v3.1, when the `kafka` Go build tag was dropped) but is **disabled by default**: it opens no
listeners until `CONNECTORS_KAFKA_ENABLE=true`.

> **Engine requirement (DE-57).** The Kafka connector runs **only on the `next` storage engine**
> (`store.engine: next`). Enabling Kafka on a legacy-engine cluster is refused at startup with a
> configuration error. On a fresh store the engine auto-selects `next`; pin explicitly with
> `store.engine: next` / `STORE_ENGINE=next`.

Unlike the CloudEvents / MQTT / AMQP connectors (which bridge across several KubeMQ patterns), the
Kafka connector maps onto **exactly one** KubeMQ primitive: the **Events Store**. A Kafka topic
`orders` is the KubeMQ Events-Store channel `kafka.orders`, and a Kafka offset is the STAN
`Sequence` of a record on that channel. This single fact drives the mental model and the way this
repo is organized (`produce/`, `consume/`, `consumer-groups/`, `admin/`, `offsets/`,
`transactions/`, `security/` — the Kafka concept vocabulary, not the KubeMQ pattern
vocabulary).

## Stack diagram

```
Kafka client (franz-go / kafka-clients / librdkafka|kcat|confluent-kafka / kafkajs / Confluent.Kafka / rdkafka-ruby / rust rdkafka)
         │
         │  bootstrap.servers = host:9092   (plain TCP)   |   host:9093 (TLS, security.protocol=SSL)
         ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│  Kafka connector  (in kubemq-server; disabled by default — CONNECTORS_KAFKA_ENABLE)│
│                                                                                    │
│   Metadata → the connector advertises a SINGLE endpoint (AdvertisedHost:Port)      │
│            │  no Kafka multi-listener advertised.listeners; one broker id          │
│            ▼                                                                        │
│   Kafka RPC dispatch  (Produce 0 / Fetch 1 / ListOffsets 2 / group + admin keys)   │
│            │   SASL (PLAIN / SCRAM-256/512) + mTLS-CN principal                     │
│            │   per-topic / per-group Casbin write/read  (Kafka ACLs → Casbin)       │
│            ▼                                                                        │
│   ┌──────────────────────────────┐   ┌───────────────────────────────────────────┐│
│   │ Records                       │  │ Coordinator                                ││
│   │  Produce → append to          │  │  FindCoordinator / Join / Sync / Heartbeat ││
│   │    Events-Store kafka.<topic> │  │  OffsetCommit / OffsetFetch (durable)      ││
│   │  Fetch  → bounded long-poll   │  │  transactional PID/epoch + in-log markers  ││
│   └───────────────┬──────────────┘   └────────────────────┬──────────────────────┘│
└───────────────────┼───────────────────────────────────────┼───────────────────────┘
                    ▼                                        ▼
       Events-Store channel:  kafka.<topic>        committed offsets (per group/topic/partition)
       partition p>0 channel: kafka.<topic>~<p>    (durable, Raft-replicated, restart-stable)
                    │
                    ▼
               STAN / NATS store  (durable; offset = Sequence)
```

Kafka records land on normal KubeMQ Events-Store channels. Offsets, committed group positions, and
transactional state are all **durable, Raft-replicated broker state** — identical across cluster
nodes.

## Service model & channel mapping

This is the single most important mental model.

| Concept | Behavior |
|---------|----------|
| **Listeners** | Two dedicated TCP listeners: `Connectors.Kafka.Port` (default **9092**, plain) and `Connectors.Kafka.TlsPort` (default **9093**, TLS). Disabled until `CONNECTORS_KAFKA_ENABLE=true`. |
| **Single advertised endpoint** | The connector advertises **one** broker at `AdvertisedHost:AdvertisedPort`. There is no Kafka multi-listener `advertised.listeners` model. `AdvertisedHost` MUST be set for any non-loopback client. |
| **Topic → channel** | Kafka topic `orders` ↔ internal Events-Store channel `kafka.orders` (`channelPrefix = "kafka."`). The `kafka.*` namespace is **reserved** — a native gRPC/REST KubeMQ client cannot read or write it (`Error 443`); only the connector accesses these records. |
| **Partition → channel** | Partition `0` stays on `kafka.<topic>`; partition `p>0` maps to the internal channel `kafka.<topic>~<p>`. Each partition is an independent ordered offset space. |
| **Offset = STAN Sequence** | A Kafka offset is the record's STAN `Sequence` on its channel — **durable, restart-stable, Raft-replicated, and identical across nodes**. There is no separate offset index to lose. |
| **Consumer group** | Coordinator-tracked, durable per-`(group, topic, partition)` committed offsets (the classic group protocol). Kafka `retention.ms` / `retention.bytes` map to the channel's `MaxAge` / `MaxBytes` / `MaxMsgs`. |
| **Auto-create** | Topics are auto-created on `Metadata` / `Produce` (auth-gated). Topic identity is by UUID (KIP-516, `Metadata` v13); Fetch is name-based on v12. |

See [concepts/topics-partitions-offsets.md](concepts/topics-partitions-offsets.md) and
[reference/channel-mapping.md](reference/channel-mapping.md).

> **Why this dictates repo organization:** Kafka maps onto a single KubeMQ pattern (Events Store),
> so — exactly like `kubemq-aws` organizes by `sqs/sns/interop` rather than by KubeMQ patterns —
> the examples and docs here are organized by the **Kafka concept vocabulary**
> (`produce/ consume/ consumer-groups/ admin/ offsets/ transactions/ security/`).

## Partitions & the increase-only model

A topic starts with partition count **1** and grows **increase-only** via `CreatePartitions`
(API key 37) up to a hard cap of **256**. Partition `p>0` lives on `kafka.<topic>~<p>`; each
partition is an independent ordered offset space. Because `~` is the partition-channel delimiter,
it is **reserved in topic names** (a topic containing `~` is rejected with
`INVALID_TOPIC_EXCEPTION(17)` — **gotcha #6**). Growing `N` re-shards keys across the new
partition set, so per-key ordering holds only **within a fixed-`N` epoch** (**gotcha #5**). See
[concepts/cross-client-partitioning.md](concepts/cross-client-partitioning.md).

## Transactions & EOS (V1)

The connector implements Kafka transactions at **V1**: `InitProducerId` →
`AddPartitionsToTxn` → transactional `Produce` → `EndTxn(commit|abort)`, with in-log COMMIT/ABORT
markers and `read_committed` isolation served client-side via `AbortedTransactions`. `(PID, epoch)`
fencing rejects zombies (`INVALID_PRODUCER_EPOCH(47)` / `PRODUCER_FENCED(90)`).

> **The V1 (no TV2) soundness ceiling (KIP-890).** KubeMQ's V1 transaction implementation does not
> bump the producer epoch on every `EndTxn`. A zombie produce from the **same** producer epoch,
> delayed past its own `EndTxn`, can still be admitted into that producer's next transaction. This
> is the **upstream-shared** KIP-890 ceiling — every pre-TV2 Kafka deployment has it — **not a
> KubeMQ defect.** See [concepts/transactions-eos.md](concepts/transactions-eos.md) and
> [guides/transactions-eos.md](guides/transactions-eos.md).

## No cross-protocol access

Although a Kafka topic is backed by an Events-Store channel (`kafka.<topic>`), that channel
namespace is **reserved for the connector**. A native gRPC/REST KubeMQ client that tries to
subscribe to, read, or write a `kafka.*` channel is rejected with `Error 443: channel is reserved
for internal connector use` — the guard fails safe and runs before authorization. So there is **no**
native ↔ Kafka bridge and no shared-channel interop: Kafka topics are reachable only over the Kafka
wire protocol. See [concepts/interop-with-native.md](concepts/interop-with-native.md).

## Honest scope

The connector's ✅ Full / 🟡 Partial / ⛔ Non-goal / 🔴 Not-yet surface is stated exactly as the
server docs state it — see [reference/capabilities.md](reference/capabilities.md). Because the Kafka
connector runs **only on the `next` engine**, the compaction-dependent ecosystem is **supported**:
log compaction (`cleanup.policy=compact`, GA on `next`), Kafka Streams, and Kafka Connect (they rely
on compacted internal topics, which `next` provides), plus Schema-Registry **wire** interop (the
5-byte magic-byte prefix). The genuine architectural non-goals (⛔) are the Schema-Registry
**service** / ksqlDB, MirrorMaker 2, and GSSAPI/Kerberos SASL.

## Source code

`connectors/kafka/` in the KubeMQ server repository (`connector.go`, `dispatch.go`, produce /
fetch / listoffsets / metadata handlers, the group coordinator, and the transaction RPCs) and the
`KafkaConfig` struct in the server configuration. Server docs of record: `docs/24-kafka.md` and
`docs/migration/kafka.md`. The mapping and behaviors above are proven by the connector's own test
suite (e.g. `produce_test.go`, `fetch_test.go`, `listoffsets_test.go`, `groupoffsets_test.go`,
`multipartition_integration_test.go`, `txn_rpcs_test.go`).
