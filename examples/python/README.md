# Python — KubeMQ Kafka examples

Native **`confluent-kafka`** (librdkafka) example apps that repoint `bootstrap.servers` at the
KubeMQ Kafka connector — no library swap. The same code runs against a real Kafka broker.

> Conventions (the 13-variant master table, the `KUBEMQ_KAFKA_BOOTSTRAP` var, the README template,
> and the 12 Kafka gotchas) live in [`../SHARED-CONVENTIONS.md`](../SHARED-CONVENTIONS.md).

## Prerequisites

- **Python 3.9+** and [`uv`](https://docs.astral.sh/uv/).
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`)
  — **disabled by default; start the server with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1).

### Pinned dependencies (see `pyproject.toml`)

| Package | Role |
|---------|------|
| `confluent-kafka` (>=2.6,<3) | Kafka producer/consumer/admin (librdkafka) — CRC32 partitioner |

## Setup

```bash
cd examples/python
uv sync   # resolves and commits uv.lock
```

## Connection

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
```

## Run a variant

```bash
uv run python produce/basic_acks/main.py
uv run python consumer-groups/join_rebalance/main.py
```

Each example prints progress, asserts the expected behavior, exits **0 on success** and
**non-zero on any failed assertion** — runnable proofs, not demos.

## Idiom notes (confluent-kafka)

- **Partitioner = CRC32** (gotcha #4). `confluent-kafka` is librdkafka-based, so its default
  partitioner is `consistent_random` (CRC32) — NOT murmur2. A keyed record lands on a **different**
  partition than franz-go / Java / kafkajs (all murmur2). Set `partitioner='murmur2_random'` to
  force parity; `confluent_kafka.murmur2(key, n)` computes the murmur2 partition.
- **Delivery reports** are asynchronous — drive them with `poll()` / `flush()` before asserting.
- **Manual commits** — set `enable.auto.commit=False` and `commit(..., asynchronous=False)`.
- Always close producers/consumers/admin clients to release the native handles.
- **Lint / format:** `uv run ruff format . && uv run ruff check --fix .`.

## Layout

`produce/` · `consume/` · `consumer-groups/` · `admin/` · `offsets/` · `transactions/` ·
`security/`

## Variant index

| # | Group | Variant | Run |
|---|-------|---------|-----|
| 1 | `produce/` | [`basic_acks`](produce/basic_acks/) | `uv run python produce/basic_acks/main.py` |
| 2 | `produce/` | [`idempotent`](produce/idempotent/) | `uv run python produce/idempotent/main.py` |
| 3 | `produce/` | [`compression_and_keys`](produce/compression_and_keys/) | `uv run python produce/compression_and_keys/main.py` |
| 4 | `consume/` | [`from_beginning_latest`](consume/from_beginning_latest/) | `uv run python consume/from_beginning_latest/main.py` |
| 5 | `consume/` | [`seek_offsets_timestamps`](consume/seek_offsets_timestamps/) | `uv run python consume/seek_offsets_timestamps/main.py` |
| 6 | `consumer-groups/` | [`join_rebalance`](consumer-groups/join_rebalance/) | `uv run python consumer-groups/join_rebalance/main.py` |
| 7 | `consumer-groups/` | [`commit_and_lag`](consumer-groups/commit_and_lag/) | `uv run python consumer-groups/commit_and_lag/main.py` |
| 8 | `admin/` | [`topics_lifecycle`](admin/topics_lifecycle/) | `uv run python admin/topics_lifecycle/main.py` |
| 9 | `admin/` | [`partitions_and_configs`](admin/partitions_and_configs/) | `uv run python admin/partitions_and_configs/main.py` |
| 10 | `offsets/` | [`list_and_retention`](offsets/list_and_retention/) | `uv run python offsets/list_and_retention/main.py` |
| 11 | `transactions/` | [`eos_commit_abort`](transactions/eos_commit_abort/) | `uv run python transactions/eos_commit_abort/main.py` |
| 12 | `transactions/` | [`read_committed`](transactions/read_committed/) | `uv run python transactions/read_committed/main.py` |
| 13 | `security/` | [`sasl_plain_scram`](security/sasl_plain_scram/) | `uv run python security/sasl_plain_scram/main.py` |

13 / 13 variants; the same 13 ship in every language under [`../`](..) — see the
[coverage matrix](../README.md#coverage-matrix).

---

> **Auth:** the connector default is no authentication (plaintext, accept-any). Only
> `security/sasl_plain_scram` needs a server with a Kafka credential store; see
> [`../../docs/guides/security-sasl-tls.md`](../../docs/guides/security-sasl-tls.md).
