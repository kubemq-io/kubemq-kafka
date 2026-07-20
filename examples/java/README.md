# Java — KubeMQ Kafka examples

Native **`org.apache.kafka:kafka-clients`** example apps that repoint `bootstrap.servers` at the
KubeMQ Kafka connector — no library swap. The same code runs against a real Kafka broker.

> Conventions (the 13-variant master table, the `KUBEMQ_KAFKA_BOOTSTRAP` var, the README template,
> and the 12 Kafka gotchas) live in [`../SHARED-CONVENTIONS.md`](../SHARED-CONVENTIONS.md).

## Prerequisites

- **JDK 21+** and **Maven 3.9+**.
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`)
  — **disabled by default; start the server with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1).

### Pinned Kafka client (see `pom.xml`)

| Dependency | Role |
|------------|------|
| `org.apache.kafka:kafka-clients:3.9.0` | Kafka producer/consumer + `Admin` — murmur2 partitioner |
| `org.slf4j:slf4j-simple` | runtime logging binding (silences the SLF4J no-binding warning) |

## Setup

```bash
cd examples/java
mvn -q compile
```

## Run a variant

Each example runs in a **forked JVM** (`exec:exec`) so a failed assertion returns a real non-zero
OS exit code:

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"

mvn -q exec:exec -Dexec.mainClass=io.kubemq.examples.kafka.produce.basicacks.Main
mvn -q exec:exec -Dexec.mainClass=io.kubemq.examples.kafka.consumergroups.joinrebalance.Main
```

Each example prints progress, asserts the expected behavior, exits **0 on success** and
**non-zero on any failed assertion** — runnable proofs, not demos.

## Idiom notes (kafka-clients)

- **Partitioner = murmur2** (gotcha #4). The built-in partitioner hashes keys with murmur2 — same as
  franz-go and kafkajs v2+, and DIFFERENT from the four librdkafka clients (python/csharp/ruby/rust =
  CRC32). The keyed example (variant 3) expects the murmur2 partition.
- **`send().get()` wraps broker rejections** (`MESSAGE_TOO_LARGE`, `INVALID_PARTITIONS`, auth
  failures) in `ExecutionException` — unwrap `e.getCause()` before `instanceof` checks.
- **Resource naming** — topics/`transactional.id`s carry a `-java` suffix so the seven language
  suites never collide; never use `~` or `/` in a name (gotchas #6/#7).
- **`exec:exec` (forked JVM), not `exec:java`** — the forked form returns the example's real exit
  code so a failed assertion fails the process. Always close producers/consumers/admin clients.

## Layout

`produce/` · `consume/` · `consumer-groups/` · `admin/` · `offsets/` · `transactions/` ·
`security/`

## Variant index

Run with `mvn -q exec:exec -Dexec.mainClass=<FQN>` (all under `io.kubemq.examples.kafka.`).

| # | Group | Variant | Main class FQN |
|---|-------|---------|----------------|
| 1 | `produce/` | [`basic-acks`](produce/basic-acks/) | `io.kubemq.examples.kafka.produce.basicacks.Main` |
| 2 | `produce/` | [`idempotent`](produce/idempotent/) | `io.kubemq.examples.kafka.produce.idempotent.Main` |
| 3 | `produce/` | [`compression-and-keys`](produce/compression-and-keys/) | `io.kubemq.examples.kafka.produce.compressionandkeys.Main` |
| 4 | `consume/` | [`from-beginning-latest`](consume/from-beginning-latest/) | `io.kubemq.examples.kafka.consume.frombeginninglatest.Main` |
| 5 | `consume/` | [`seek-offsets-timestamps`](consume/seek-offsets-timestamps/) | `io.kubemq.examples.kafka.consume.seekoffsetstimestamps.Main` |
| 6 | `consumer-groups/` | [`join-rebalance`](consumer-groups/join-rebalance/) | `io.kubemq.examples.kafka.consumergroups.joinrebalance.Main` |
| 7 | `consumer-groups/` | [`commit-and-lag`](consumer-groups/commit-and-lag/) | `io.kubemq.examples.kafka.consumergroups.commitandlag.Main` |
| 8 | `admin/` | [`topics-lifecycle`](admin/topics-lifecycle/) | `io.kubemq.examples.kafka.admin.topicslifecycle.Main` |
| 9 | `admin/` | [`partitions-and-configs`](admin/partitions-and-configs/) | `io.kubemq.examples.kafka.admin.partitionsandconfigs.Main` |
| 10 | `offsets/` | [`list-and-retention`](offsets/list-and-retention/) | `io.kubemq.examples.kafka.offsets.listandretention.Main` |
| 11 | `transactions/` | [`eos-commit-abort`](transactions/eos-commit-abort/) | `io.kubemq.examples.kafka.transactions.eoscommitabort.Main` |
| 12 | `transactions/` | [`read-committed`](transactions/read-committed/) | `io.kubemq.examples.kafka.transactions.readcommitted.Main` |
| 13 | `security/` | [`sasl-plain-scram`](security/sasl-plain-scram/) | `io.kubemq.examples.kafka.security.saslplainscram.Main` |

13 / 13 variants; the same 13 ship in every language under [`../`](..) — see the
[coverage matrix](../README.md#coverage-matrix).

---

> **Auth:** the connector default is no authentication (plaintext, accept-any). Only
> `security/sasl-plain-scram` needs a server with a Kafka credential store; see
> [`../../docs/guides/security-sasl-tls.md`](../../docs/guides/security-sasl-tls.md).
