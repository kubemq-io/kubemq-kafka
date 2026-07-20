# Go — KubeMQ Kafka examples

Native **`github.com/twmb/franz-go`** example apps that repoint `bootstrap.servers` at the KubeMQ
Kafka connector — no library swap. franz-go is the connector's own conformance client and the
de-facto reference for the other six languages.

> Conventions (the 13-variant master table, the `KUBEMQ_KAFKA_BOOTSTRAP` var, the README template,
> and the 12 Kafka gotchas) live in [`../SHARED-CONVENTIONS.md`](../SHARED-CONVENTIONS.md).

## Prerequisites

- **Go 1.24+**.
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`)
  — **disabled by default; start the server with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1).

### Pinned Kafka client (see `go.mod`)

| Package | Role |
|---------|------|
| `github.com/twmb/franz-go` | Kafka producer/consumer (`kgo`) — murmur2 partitioner |
| `github.com/twmb/franz-go/pkg/kadm` | admin (topics/partitions/configs/offsets) |
| `github.com/twmb/franz-go/pkg/kmsg` | low-level Kafka request/response messages |
| `github.com/google/uuid` | run-unique topic / id generation |

## Setup

```bash
cd examples/go
go build ./...   # resolves go.sum and compiles all 13 variants + the shared helper
```

Both `go.mod` and `go.sum` are committed.

## Run a variant

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"

go run ./produce/basic-acks
go run ./consumer-groups/join-rebalance
```

Each example prints progress, asserts the expected behavior, exits **0 on success** and
**non-zero on any failed assertion** — runnable proofs, not demos.

## Idiom notes (Go / franz-go)

- **Partitioner = murmur2** (gotcha #4). franz-go, Java `kafka-clients`, and kafkajs v2+ default to
  murmur2; the four librdkafka clients (`confluent-kafka` / `Confluent.Kafka` / `rdkafka-ruby` /
  rust `rdkafka`) default to CRC32. The keyed example (variant 3) expects the murmur2 partition.
- **Idempotence is ON by default** — do not pass `kgo.DisableIdempotentWrite`; `InitProducerId` is
  issued implicitly on first produce.
- **`acks` is a client-level option** (`kgo.RequiredAcks`), not per-record — variant 1 uses one
  short-lived client per ack level.
- **admin via `kadm`** — it wraps a `*kgo.Client` you must close yourself; the shared helper returns
  both.
- **Error assertions** use `errors.Is(err, kerr.<Name>)` (`MessageTooLarge`, `InvalidPartitions`,
  `InvalidTopicException`, `TopicAuthorizationFailed`, …); never put `~` or `/` in a topic or
  `transactional.id` (gotchas #6/#7).

## Layout

`produce/` · `consume/` · `consumer-groups/` · `admin/` · `offsets/` · `transactions/` ·
`security/`

## Variant index

| # | Group | Variant | Run |
|---|-------|---------|-----|
| 1 | `produce/` | [`basic-acks`](produce/basic-acks/) | `go run ./produce/basic-acks` |
| 2 | `produce/` | [`idempotent`](produce/idempotent/) | `go run ./produce/idempotent` |
| 3 | `produce/` | [`compression-and-keys`](produce/compression-and-keys/) | `go run ./produce/compression-and-keys` |
| 4 | `consume/` | [`from-beginning-latest`](consume/from-beginning-latest/) | `go run ./consume/from-beginning-latest` |
| 5 | `consume/` | [`seek-offsets-timestamps`](consume/seek-offsets-timestamps/) | `go run ./consume/seek-offsets-timestamps` |
| 6 | `consumer-groups/` | [`join-rebalance`](consumer-groups/join-rebalance/) | `go run ./consumer-groups/join-rebalance` |
| 7 | `consumer-groups/` | [`commit-and-lag`](consumer-groups/commit-and-lag/) | `go run ./consumer-groups/commit-and-lag` |
| 8 | `admin/` | [`topics-lifecycle`](admin/topics-lifecycle/) | `go run ./admin/topics-lifecycle` |
| 9 | `admin/` | [`partitions-and-configs`](admin/partitions-and-configs/) | `go run ./admin/partitions-and-configs` |
| 10 | `offsets/` | [`list-and-retention`](offsets/list-and-retention/) | `go run ./offsets/list-and-retention` |
| 11 | `transactions/` | [`eos-commit-abort`](transactions/eos-commit-abort/) | `go run ./transactions/eos-commit-abort` |
| 12 | `transactions/` | [`read-committed`](transactions/read-committed/) | `go run ./transactions/read-committed` |
| 13 | `security/` | [`sasl-plain-scram`](security/sasl-plain-scram/) | `go run ./security/sasl-plain-scram` |

13 / 13 variants; the same 13 ship in every language under [`../`](..) — see the
[coverage matrix](../README.md#coverage-matrix).

---

> **Auth:** the connector default is no authentication (plaintext, accept-any). Only
> `security/sasl-plain-scram` needs a server with a Kafka credential store; see
> [`../../docs/guides/security-sasl-tls.md`](../../docs/guides/security-sasl-tls.md).
