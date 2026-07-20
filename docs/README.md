# KubeMQ Kafka — Documentation

Documentation for the KubeMQ embedded **Apache Kafka wire-protocol connector**.

The connector is a dedicated Kafka listener (plain TCP **9092**, TLS **9093**) inside
`kubemq-server` that speaks the genuine Apache Kafka wire protocol and maps every Kafka topic
onto KubeMQ's **Events Store** primitive (channel `kafka.<topic>`, offset = STAN `Sequence`).
Any standard Kafka client (franz-go, `kafka-clients`, librdkafka / `kcat` / confluent-kafka,
kafkajs, Confluent.Kafka, rdkafka-ruby, rust `rdkafka`) connects with **only a
`bootstrap.servers` change** — no code changes, no library swap.

> This repo ships **documentation + runnable examples + a Go burn-in soak harness**. It ships
> **no installable package, no proto / gRPC bindings, and no published client library.** The
> Kafka client libraries below are the real, unmodified upstream libraries.

## Contents

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | The wire-shim model: Kafka client → `connectors/kafka/` → Events-Store `kafka.<topic>`; single-endpoint advertised-listener model |
| [getting-started.md](getting-started.md) | Enable the connector (`CONNECTORS_KAFKA_ENABLE=true`), set `AdvertisedHost`, smoke-test with `kcat` / franz-go |
| [configuration.md](configuration.md) | All `CONNECTORS_KAFKA_*` env vars + defaults, capacity limits, TLS/mTLS setup |
| **Concepts** | |
| [concepts/topics-partitions-offsets.md](concepts/topics-partitions-offsets.md) | Topic ↔ `kafka.<topic>`, partition ↔ `~<p>` channel, offset ↔ STAN Sequence; increase-only partitions; stale-low cross-node read |
| [concepts/consumer-groups.md](concepts/consumer-groups.md) | Classic group protocol, commit/fetch durability, lag metric, rebalance on partition growth |
| [concepts/transactions-eos.md](concepts/transactions-eos.md) | EOS V1 flow, in-log COMMIT/ABORT markers, `read_committed` client-side filtering, **the KIP-890 V1 ceiling** |
| [concepts/cross-client-partitioning.md](concepts/cross-client-partitioning.md) | murmur2 vs CRC32 partitioner divergence + N-reshard — the dedicated caveat page |
| [concepts/interop-with-native.md](concepts/interop-with-native.md) | Why native KubeMQ clients cannot access `kafka.*` channels — the reserved namespace (`Error 443`), no cross-protocol interop |
| **Guides** | |
| [guides/producing.md](guides/producing.md) | acks 0/1/all, idempotence, compression, keyed partitioning |
| [guides/consuming-and-groups.md](guides/consuming-and-groups.md) | from-beginning / latest, long-poll, seek, group join / commit / lag |
| [guides/admin-and-topics.md](guides/admin-and-topics.md) | Create/Delete topics, DescribeConfigs/Cluster, CreatePartitions (increase-only), IncrementalAlterConfigs / DeleteRecords (partial) |
| [guides/security-sasl-tls.md](guides/security-sasl-tls.md) | SASL/PLAIN + SCRAM (runnable), TLS/mTLS (doc-only), Casbin ACL |
| [guides/transactions-eos.md](guides/transactions-eos.md) | Transactional producer + `read_committed` consumer, the Group-WRITE requirement, KIP-890 ceiling |
| **Reference** | |
| [reference/capabilities.md](reference/capabilities.md) | The full ✅ / 🟡 / ⛔ / 🔴 scope tables (the honest-scope reference) |
| [reference/channel-mapping.md](reference/channel-mapping.md) | Exact topic / partition / offset / group → channel mapping table |
| [reference/error-codes.md](reference/error-codes.md) | Kafka error codes surfaced by the connector |
| [reference/configuration.md](reference/configuration.md) | Field-by-field `CONNECTORS_KAFKA_*` reference (cross-links the server config reference) |
| [reference/migration-from-kafka.md](reference/migration-from-kafka.md) | Start-fresh repoint, compatibility matrix, `~` breaking change, what does/doesn't migrate |

## Reading order

1. [architecture.md](architecture.md) — the wire-shim mental model (Kafka topic = Events-Store channel).
2. [getting-started.md](getting-started.md) — enable the connector and get a message flowing.
3. [concepts/topics-partitions-offsets.md](concepts/topics-partitions-offsets.md) — the core mapping every other page builds on.
4. The concept / guide pair for your task (producing, consuming, admin, transactions, security).
5. The `reference/` pages when you need the exact scope, error code, or config field.

## Honest scope

Every ✅ Full / 🟡 Partial claim in these docs traces to the connector's operational surface
(see [reference/capabilities.md](reference/capabilities.md)) — copied faithfully from the server
docs `24-kafka.md` and `migration/kafka.md`. Docs never claim beyond that surface. In
particular, **every transactions / EOS page cites the KIP-890 V1 soundness ceiling** (see
[concepts/transactions-eos.md](concepts/transactions-eos.md)) — the connector implements
Transactions **V1**, and the same-epoch zombie residual is the upstream-shared pre-TV2
limitation, not a KubeMQ defect.

## Examples

Working code in 7 languages (13 variants each) is in [../examples/](../examples/README.md).

## Prerequisites

All examples and documentation assume:

- A KubeMQ server with the **Kafka connector enabled** — it is **disabled by default**; set
  `CONNECTORS_KAFKA_ENABLE=true` to open the listeners (**gotcha #1** — unlike the AMQP/MQTT
  connectors, which are on by default).
- The connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`), used as the
  Kafka `bootstrap.servers` value.
- **`AdvertisedHost` set** for any non-loopback client — an empty `AdvertisedHost` advertises the
  pod hostname, and external clients connect and then hang (**gotcha #2**). See
  [getting-started.md](getting-started.md).

There is **no docker-compose and no boot-the-server step** — point your Kafka client at an
existing connector. See [getting-started.md](getting-started.md).
