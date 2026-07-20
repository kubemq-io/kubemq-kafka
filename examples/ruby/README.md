# Ruby — KubeMQ Kafka examples

Point any native **`rdkafka`** (rdkafka-ruby, librdkafka) app at `KUBEMQ_KAFKA_BOOTSTRAP` and Kafka
just works — no library swap. The same code runs against a real Kafka broker.

> Conventions (the 13-variant master table, the `KUBEMQ_KAFKA_BOOTSTRAP` var, the README template,
> and the 12 Kafka gotchas) live in [`../SHARED-CONVENTIONS.md`](../SHARED-CONVENTIONS.md).

## Prerequisites

- **Ruby 3.3.x** and a **C toolchain** (`rdkafka` compiles/vendors librdkafka at install).
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`)
  — **disabled by default; start the server with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1).

### Pinned Kafka client (see `Gemfile`)

| Gem | Role |
|-----|------|
| `rdkafka` (`>= 0.19`) | Kafka producer/consumer/admin (librdkafka) — CRC32 partitioner |

## Setup

```bash
cd examples/ruby
bundle install
# optional: bundle config --local path vendor/bundle
```

`Gemfile.lock` and `vendor/bundle/` are gitignored.

## Run a variant

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"

bundle exec ruby produce/basic_acks/main.rb
bundle exec ruby consumer-groups/join_rebalance/main.rb
```

Each example prints progress, asserts the expected behavior, exits **0 on success** and
**non-zero on any failed assertion** — runnable proofs, not demos.

## Idiom notes (rdkafka)

- **Partitioner = CRC32** (gotcha #4). `rdkafka`/librdkafka defaults to `consistent_random` (CRC32),
  so a keyed record lands on a **different** partition than franz-go / Java / kafkajs (all murmur2).
  Set `"partitioner" => "murmur2_random"` to force parity.
- **`group.id` is always required** — librdkafka rejects a consumer without one even for
  assign-based reads.
- **Delivery is asynchronous** — `producer.produce(...)` returns a `DeliveryHandle`; call
  `handle.wait(...)` (or `producer.flush`) before asserting; always `close` producers/consumers/admin
  to release native handles.
- **Errors** raise `Rdkafka::RdkafkaError` with a `.code` **symbol** (e.g. `:msg_size_too_large`,
  `:invalid_partitions`, `:topic_authorization_failed`) — assert on `.code`, not the message string.
- Never put `~` or `/` in a topic or `transactional.id` (gotchas #6/#7).

### Toolchain note

Use [`rbenv`](https://github.com/rbenv/rbenv) (or your version manager of choice) to select Ruby
3.3.x. Because `rdkafka` builds a native extension, a working C compiler and `make` must be on
`PATH` before `bundle install`.

## Lint / Format

```bash
bundle exec rubocop -a          # config in .rubocop.yml
# toolchain-free fallback:
find . -name "*.rb" -exec ruby -c {} +
```

## Layout

`produce/` · `consume/` · `consumer-groups/` · `admin/` · `offsets/` · `transactions/` ·
`security/`

## The 13 variants

| # | Group | Variant | Run |
|---|-------|---------|-----|
| 1 | `produce/` | [`basic_acks`](produce/basic_acks/) | `bundle exec ruby produce/basic_acks/main.rb` |
| 2 | `produce/` | [`idempotent`](produce/idempotent/) | `bundle exec ruby produce/idempotent/main.rb` |
| 3 | `produce/` | [`compression_and_keys`](produce/compression_and_keys/) | `bundle exec ruby produce/compression_and_keys/main.rb` |
| 4 | `consume/` | [`from_beginning_latest`](consume/from_beginning_latest/) | `bundle exec ruby consume/from_beginning_latest/main.rb` |
| 5 | `consume/` | [`seek_offsets_timestamps`](consume/seek_offsets_timestamps/) | `bundle exec ruby consume/seek_offsets_timestamps/main.rb` |
| 6 | `consumer-groups/` | [`join_rebalance`](consumer-groups/join_rebalance/) | `bundle exec ruby consumer-groups/join_rebalance/main.rb` |
| 7 | `consumer-groups/` | [`commit_and_lag`](consumer-groups/commit_and_lag/) | `bundle exec ruby consumer-groups/commit_and_lag/main.rb` |
| 8 | `admin/` | [`topics_lifecycle`](admin/topics_lifecycle/) | `bundle exec ruby admin/topics_lifecycle/main.rb` (🟡 thin admin surface — see README) |
| 9 | `admin/` | [`partitions_and_configs`](admin/partitions_and_configs/) | `bundle exec ruby admin/partitions_and_configs/main.rb` (🟡 thin admin surface — see README) |
| 10 | `offsets/` | [`list_and_retention`](offsets/list_and_retention/) | `bundle exec ruby offsets/list_and_retention/main.rb` |
| 11 | `transactions/` | [`eos_commit_abort`](transactions/eos_commit_abort/) | `bundle exec ruby transactions/eos_commit_abort/main.rb` (🟡 txn API floor — see README) |
| 12 | `transactions/` | [`read_committed`](transactions/read_committed/) | `bundle exec ruby transactions/read_committed/main.rb` |
| 13 | `security/` | [`sasl_plain_scram`](security/sasl_plain_scram/) | `bundle exec ruby security/sasl_plain_scram/main.rb` |

13 / 13 variants; the same 13 ship in every language under [`../`](..) — see the
[coverage matrix](../README.md#coverage-matrix). Where the pinned `rdkafka` gem cannot express a
variant, the folder + README still ship, explain the limitation, and point at the Go/Java example
that covers it — never a silent drop.

---

> **Auth:** the connector default is no authentication (plaintext, accept-any). Only
> `security/sasl_plain_scram` needs a server with a Kafka credential store; see
> [`../../docs/guides/security-sasl-tls.md`](../../docs/guides/security-sasl-tls.md).
