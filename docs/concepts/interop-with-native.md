# Native Access to Kafka Channels Is Reserved

## Concept

Internally, every Kafka topic is backed by a KubeMQ **Events-Store channel** named `kafka.<topic>`
(and `kafka.<topic>~<p>` for partitions `p > 0`). That internal mapping is real — it is how the
connector stores records durably. **But that channel namespace is not open to native KubeMQ
clients.** The broker **reserves** the `kafka.*` (and `_KAFKA_`) channel namespace for the
connector's own internal use, so there is **no** cross-protocol bridge: you cannot read a Kafka
topic with a native KubeMQ client, and you cannot inject records into a Kafka topic by writing its
channel natively.

## The `kafka.*` namespace is reserved

Any native KubeMQ client — Events, Events-Store, or Queue, over gRPC **or** REST — that tries to
**subscribe to, read, or write** a `kafka.*` channel is rejected with:

```
Error 443: channel is reserved for internal connector use
```

This is enforced structurally in the broker, in `services/array/reserved_channel.go`
(`checkReservedChannelRead` / `checkReservedChannelWrite`):

- It **fails safe.** A caller that does not carry the connector's internal marker is **always**
  rejected — the default is denial, not access.
- It runs **before authorization.** The reservation is checked ahead of any ACL / Casbin decision,
  so no amount of granted permission lets a wire client reach a `kafka.*` channel.
- Only **first-party, server-side connector code** may touch these channels, via an **unforgeable
  internal-writer context marker** (`WithInternalWriter`) that is set inside the broker process. No
  gRPC / REST wire client can set that marker, so no wire client can impersonate the connector.

The same reservation covers the connector's `_KAFKA_*` internal-state channels (committed offsets,
consumer-group state, the idempotent-producer dedup index) — they are equally invisible to native
clients.

## Why the namespace is reserved

The reservation exists **on purpose**: it isolates the connector's internal Kafka state. Kafka
records, partition channels (`kafka.<topic>~<p>`), and the offset / sequence bookkeeping the
connector maintains are only sound if the connector is the **sole** writer and reader of those
channels. If a native client could append to, subscribe to, or truncate `kafka.orders`, it could
**tamper with, spoof, or corrupt** the connector's view of the log — breaking offsets,
consumer-group positions, and transactional state. Reserving the namespace makes that class of
interference structurally impossible.

## There is no cross-protocol interop

Because the namespace is reserved, a shared-channel bridge between Kafka and native KubeMQ clients
**does not exist in this build**:

1. **Kafka → native is blocked.** A Kafka `Produce` to topic `orders` appends to `kafka.orders`,
   but a native Events-Store subscriber on `kafka.orders` is rejected with Error 443 — it never
   receives the record.
2. **Native → Kafka is blocked.** A native Events-Store `Send` (or Queue write) to `kafka.orders`
   is rejected with Error 443 — it can never be produced, so a Kafka consumer never sees it.

The only supported way to produce to or consume from a Kafka topic is the **Kafka wire protocol**
(the connector's `:9092` / `:9093` listeners). Use a Kafka client, not a KubeMQ SDK. There is no
`kubemq-go` (or any other KubeMQ SDK) path into these channels, and no interop example ships in this
repo.

## Offsets and partitions are connector-internal

The internal mapping is still true — a Kafka offset **is** the STAN `Sequence` of the record on its
channel, and partition `p > 0` lives on `kafka.<topic>~<p>` — but these are **connector-internal**
facts. No native client can observe the sequence or the partition channels, because it cannot read
the channel at all. Offsets and partition placement are visible **only** through the Kafka wire
protocol. See [topics-partitions-offsets.md](topics-partitions-offsets.md) and
[cross-client-partitioning.md](cross-client-partitioning.md).

## What this means in practice

- **Migrating one side at a time via a shared channel is not possible.** Repointing is a start-fresh
  Kafka repoint (see [../reference/migration-from-kafka.md](../reference/migration-from-kafka.md)),
  not a native/Kafka co-consumption of the same channel.
- **Kafka transactional markers, headers, and `read_committed` semantics** are Kafka-consumer
  concerns served over the Kafka wire protocol; there is no native subscriber that could observe (or
  mis-observe) them, because native subscription to `kafka.*` is refused outright.

## See Also

- [../architecture.md](../architecture.md) — the reserved-namespace note in the architecture overview.
- [../reference/channel-mapping.md](../reference/channel-mapping.md) — the topic / partition → channel grammar (all connector-internal).
- [../reference/error-codes.md](../reference/error-codes.md) — Error 443 (reserved channel).
- [topics-partitions-offsets.md](topics-partitions-offsets.md) — offset = STAN Sequence (a connector-internal fact).

## Grounding

The reservation is enforced by `services/array/reserved_channel.go` in `kubemq-server`
(`isReservedChannel` + `checkReservedChannelRead` / `checkReservedChannelWrite`), which fails safe
and runs before `Authorize`; only the connector's unforgeable internal-writer context marker
(`WithInternalWriter`) is accepted, and a wire client cannot set it. A rejected native access
returns `entities.ErrReservedChannel` — `Error 443: channel is reserved for internal connector use`.
The internal topic → `kafka.<topic>` mapping (offset = STAN Sequence) lives in `connectors/kafka/`,
but that store is reachable **only** through the connector, never through a native KubeMQ client.
