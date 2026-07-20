# C# / .NET — KubeMQ Kafka examples

Native **`Confluent.Kafka`** example apps that talk to the KubeMQ Kafka connector by pointing
`BootstrapServers` at it — NOT a KubeMQ SDK. The same code runs against a real Kafka broker.

> Conventions (the 13-variant master table, the `KUBEMQ_KAFKA_BOOTSTRAP` var, the README template,
> and the 12 Kafka gotchas) live in [`../SHARED-CONVENTIONS.md`](../SHARED-CONVENTIONS.md).

## Prerequisites

- **.NET SDK 8.0** (target `net8.0`).
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`)
  — **disabled by default; start the server with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1).

### Pinned packages (central — see `Directory.Packages.props`)

| Package | Role |
|---------|------|
| `Confluent.Kafka` (`2.6.0`) | Kafka producer/consumer/admin (librdkafka) — CRC32 partitioner |

## Setup

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"   # default; the connector's Kafka listener

cd examples/csharp
dotnet build KubeMQ.Kafka.Examples.sln
```

## Run a variant

```bash
dotnet run --project produce/basic-acks
dotnet run --project consumer-groups/join-rebalance
```

Each example prints progress, asserts the expected behavior, exits **0 on success** and
**non-zero on any failed assertion** — runnable proofs, not demos.

## Idiom notes (Confluent.Kafka)

- **Partitioner = CRC32** (gotcha #4). `Confluent.Kafka` is librdkafka-based, so a keyed record
  lands on a **different** partition than franz-go / Java / kafkajs (all murmur2). The keyed example
  (variant 3) expects the CRC32 partition.
- **`await` every `ProduceAsync` / `Flush`** before asserting delivery.
- **`using`-dispose** all producers, consumers, and admin clients to release native handles.
- **Transactions** need a `TransactionalId` + `InitTransactions()`; never put `~` or `/` in a name
  (gotchas #6/#7).
- The shared helper lives in [`shared/`](shared/) (`Shared.csproj`), referenced by every variant.

## Layout

`produce/` · `consume/` · `consumer-groups/` · `admin/` · `offsets/` · `transactions/` ·
`security/`

## Variant index

| # | Group | Variant | Run |
|---|-------|---------|-----|
| 1 | `produce/` | [`basic-acks`](produce/basic-acks/) | `dotnet run --project produce/basic-acks` |
| 2 | `produce/` | [`idempotent`](produce/idempotent/) | `dotnet run --project produce/idempotent` |
| 3 | `produce/` | [`compression-and-keys`](produce/compression-and-keys/) | `dotnet run --project produce/compression-and-keys` |
| 4 | `consume/` | [`from-beginning-latest`](consume/from-beginning-latest/) | `dotnet run --project consume/from-beginning-latest` |
| 5 | `consume/` | [`seek-offsets-timestamps`](consume/seek-offsets-timestamps/) | `dotnet run --project consume/seek-offsets-timestamps` |
| 6 | `consumer-groups/` | [`join-rebalance`](consumer-groups/join-rebalance/) | `dotnet run --project consumer-groups/join-rebalance` |
| 7 | `consumer-groups/` | [`commit-and-lag`](consumer-groups/commit-and-lag/) | `dotnet run --project consumer-groups/commit-and-lag` |
| 8 | `admin/` | [`topics-lifecycle`](admin/topics-lifecycle/) | `dotnet run --project admin/topics-lifecycle` |
| 9 | `admin/` | [`partitions-and-configs`](admin/partitions-and-configs/) | `dotnet run --project admin/partitions-and-configs` |
| 10 | `offsets/` | [`list-and-retention`](offsets/list-and-retention/) | `dotnet run --project offsets/list-and-retention` |
| 11 | `transactions/` | [`eos-commit-abort`](transactions/eos-commit-abort/) | `dotnet run --project transactions/eos-commit-abort` |
| 12 | `transactions/` | [`read-committed`](transactions/read-committed/) | `dotnet run --project transactions/read-committed` |
| 13 | `security/` | [`sasl-plain-scram`](security/sasl-plain-scram/) | `dotnet run --project security/sasl-plain-scram` |

13 / 13 variants; the same 13 ship in every language under [`../`](..) — see the
[coverage matrix](../README.md#coverage-matrix).

---

> **Auth:** the connector default is no authentication (plaintext, accept-any). Only
> `security/sasl-plain-scram` needs a server with a Kafka credential store; see
> [`../../docs/guides/security-sasl-tls.md`](../../docs/guides/security-sasl-tls.md).
