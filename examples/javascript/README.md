# JavaScript/TypeScript — KubeMQ Kafka examples

Native **`kafkajs`** (murmur2 `DefaultPartitioner`) example apps that repoint `bootstrap.servers`
at the KubeMQ Kafka connector — no KubeMQ SDK. The same code runs against a real Kafka broker.

> Conventions (the 13-variant master table, the `KUBEMQ_KAFKA_BOOTSTRAP` var, the README template,
> and the 12 Kafka gotchas) live in [`../SHARED-CONVENTIONS.md`](../SHARED-CONVENTIONS.md).

## Prerequisites

- **Node 18+** and npm.
- A running KubeMQ server with **`CONNECTORS_KAFKA_ENABLE=true`** (disabled by default — gotcha #1),
  reachable at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`). Set
  `CONNECTORS_KAFKA_ADVERTISED_HOST` for external clients (gotcha #2).

### Pinned Kafka client (see `package.json`)

| Package | Role |
|---------|------|
| `kafkajs` (`^2.2.4`) | Kafka producer/consumer/admin — murmur2 `DefaultPartitioner` (≥2.0 floor) |
| `typescript` / `tsx` / `@types/node` | dev: type-check + run TS directly |

## Setup

```bash
cd examples/javascript
npm install
```

`package-lock.json` is gitignored (see the repo `.gitignore`).

## Run a variant

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"

npx tsx produce/basic-acks/index.ts
npx tsc --noEmit                 # type-check the whole tree
```

Each example prints progress, asserts the expected behavior, and sets `process.exitCode = 1` on
any failed assertion — **0 on success, non-zero on failure** (runnable proofs, not demos).

## Idiom notes (kafkajs)

- **NodeNext modules** — `"type": "module"` + `moduleResolution: NodeNext`, so intra-repo imports
  carry the `.js` extension (`import { newKafka } from '../../shared/client.js'`) even though the
  source is `.ts`. `tsc --noEmit` catches a missing extension.
- **Partitioner = murmur2** (gotcha #4). `Partitioners.DefaultPartitioner` (kafkajs ≥2.0) hashes
  keys with murmur2 — same partition as Java/franz-go, NOT the librdkafka CRC32 clients.
- **`acks` encoding** — `-1` = all, `1` = leader, `0` = none.
- **`consumer.run` is non-blocking** — it starts a background poll loop; assert by polling a
  collected array to a deadline, then `consumer.stop()` / `disconnect()`.
- **Transactions** use `producer.transaction()` + `txn.commit()` / `txn.abort()`; a
  `transactionalId` must not contain `/` (gotcha #7).
- **Compression** — gzip is built in; snappy/lz4/zstd need codecs registered on `CompressionCodecs`.

## Layout

`produce/` · `consume/` · `consumer-groups/` · `admin/` · `offsets/` · `transactions/` ·
`security/`

## Variant index

| # | Group | Variant | Run |
|---|-------|---------|-----|
| 1 | `produce/` | [`basic-acks`](produce/basic-acks/) | `npx tsx produce/basic-acks/index.ts` |
| 2 | `produce/` | [`idempotent`](produce/idempotent/) | `npx tsx produce/idempotent/index.ts` |
| 3 | `produce/` | [`compression-and-keys`](produce/compression-and-keys/) | `npx tsx produce/compression-and-keys/index.ts` |
| 4 | `consume/` | [`from-beginning-latest`](consume/from-beginning-latest/) | `npx tsx consume/from-beginning-latest/index.ts` |
| 5 | `consume/` | [`seek-offsets-timestamps`](consume/seek-offsets-timestamps/) | `npx tsx consume/seek-offsets-timestamps/index.ts` |
| 6 | `consumer-groups/` | [`join-rebalance`](consumer-groups/join-rebalance/) | `npx tsx consumer-groups/join-rebalance/index.ts` |
| 7 | `consumer-groups/` | [`commit-and-lag`](consumer-groups/commit-and-lag/) | `npx tsx consumer-groups/commit-and-lag/index.ts` |
| 8 | `admin/` | [`topics-lifecycle`](admin/topics-lifecycle/) | `npx tsx admin/topics-lifecycle/index.ts` |
| 9 | `admin/` | [`partitions-and-configs`](admin/partitions-and-configs/) | `npx tsx admin/partitions-and-configs/index.ts` (🟡 `DeleteRecords` N/A in kafkajs) |
| 10 | `offsets/` | [`list-and-retention`](offsets/list-and-retention/) | `npx tsx offsets/list-and-retention/index.ts` |
| 11 | `transactions/` | [`eos-commit-abort`](transactions/eos-commit-abort/) | `npx tsx transactions/eos-commit-abort/index.ts` |
| 12 | `transactions/` | [`read-committed`](transactions/read-committed/) | `npx tsx transactions/read-committed/index.ts` |
| 13 | `security/` | [`sasl-plain-scram`](security/sasl-plain-scram/) | `npx tsx security/sasl-plain-scram/index.ts` |

13 / 13 variants; the same 13 ship in every language under [`../`](..) — see the
[coverage matrix](../README.md#coverage-matrix).

---

> **Auth:** the connector default is no authentication (plaintext, accept-any). Only
> `security/sasl-plain-scram` needs a server with a Kafka credential store; see
> [`../../docs/guides/security-sasl-tls.md`](../../docs/guides/security-sasl-tls.md).
