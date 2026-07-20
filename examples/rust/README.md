# Rust — KubeMQ Kafka examples

Runnable Kafka examples driving the KubeMQ embedded Kafka wire-protocol connector using the
standard **`rdkafka`** (librdkafka) client on `tokio` — no KubeMQ SDK on the Kafka side. Each
example connects by setting `bootstrap.servers`; nothing else changes versus talking to a real
Kafka broker. A Cargo workspace: one binary crate per variant (kebab-case dirs) plus a shared
`kafka-common` helper crate.

> Authoritative conventions (the 13-variant master table, the `KUBEMQ_KAFKA_BOOTSTRAP` var, the
> README template, and the 12 Kafka gotchas) live in
> [`../SHARED-CONVENTIONS.md`](../SHARED-CONVENTIONS.md).

## Prerequisites

- **Rust 1.75+** (edition 2021) and Cargo.
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`)
  — **disabled by default; start the server with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1).

### Pinned Kafka client (see `Cargo.toml`)

| Crate | Floor | Role |
|-------|-------|------|
| `rdkafka` | 0.37 | librdkafka producer/consumer/admin (+ txn) on tokio — CRC32 partitioner |
| `tokio` | 1 | async runtime |

## Setup

```bash
cd examples/rust
cargo build --workspace
```

`Cargo.lock` is gitignored.

## Run a variant

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"

cargo run -p basic-acks
cargo run -p join-rebalance
```

Each example prints progress, asserts the expected behavior, exits **0 on success** and
**non-zero on any failed assertion** — runnable proofs, not demos.

## Idiom notes (rdkafka)

- **Partitioner = CRC32** (gotcha #4). `rdkafka` is librdkafka-based, so a keyed record lands on a
  **different** partition than franz-go / Java / kafkajs (all murmur2). Set the librdkafka
  `partitioner` to `murmur2_random` to force parity.
- **Everything is async** — needs a `tokio` runtime (`#[tokio::main]`); `.await` on every call.
- **Delivery futures** — `FutureProducer::send(...)` returns a future you must await before
  asserting delivery.
- **Errors** surface as `KafkaError` with an `RDKafkaErrorCode`; assert on the code, not the
  message. Never put `~` or `/` in a topic or `transactional.id` (gotchas #6/#7).

## Layout

`produce/` · `consume/` · `consumer-groups/` · `admin/` · `offsets/` · `transactions/` ·
`security/`

## Variant index

| # | Group | Variant | Run |
|---|-------|---------|-----|
| 1 | `produce/` | [`basic-acks`](produce/basic-acks/) | `cargo run -p basic-acks` |
| 2 | `produce/` | [`idempotent`](produce/idempotent/) | `cargo run -p idempotent` |
| 3 | `produce/` | [`compression-and-keys`](produce/compression-and-keys/) | `cargo run -p compression-and-keys` |
| 4 | `consume/` | [`from-beginning-latest`](consume/from-beginning-latest/) | `cargo run -p from-beginning-latest` |
| 5 | `consume/` | [`seek-offsets-timestamps`](consume/seek-offsets-timestamps/) | `cargo run -p seek-offsets-timestamps` |
| 6 | `consumer-groups/` | [`join-rebalance`](consumer-groups/join-rebalance/) | `cargo run -p join-rebalance` |
| 7 | `consumer-groups/` | [`commit-and-lag`](consumer-groups/commit-and-lag/) | `cargo run -p commit-and-lag` |
| 8 | `admin/` | [`topics-lifecycle`](admin/topics-lifecycle/) | `cargo run -p topics-lifecycle` |
| 9 | `admin/` | [`partitions-and-configs`](admin/partitions-and-configs/) | `cargo run -p partitions-and-configs` |
| 10 | `offsets/` | [`list-and-retention`](offsets/list-and-retention/) | `cargo run -p list-and-retention` |
| 11 | `transactions/` | [`eos-commit-abort`](transactions/eos-commit-abort/) | `cargo run -p eos-commit-abort` |
| 12 | `transactions/` | [`read-committed`](transactions/read-committed/) | `cargo run -p read-committed` |
| 13 | `security/` | [`sasl-plain-scram`](security/sasl-plain-scram/) | `cargo run -p sasl-plain-scram` |

13 / 13 variants; the same 13 ship in every language under [`../`](..) — see the
[coverage matrix](../README.md#coverage-matrix).

---

> **Auth:** the connector default is no authentication (plaintext, accept-any). Only
> `security/sasl-plain-scram` needs a server with a Kafka credential store; see
> [`../../docs/guides/security-sasl-tls.md`](../../docs/guides/security-sasl-tls.md).
