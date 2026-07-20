# SHARED CONVENTIONS — kubemq-kafka

> The conventions every example, every doc, and the burn-in harness in this repo follow. Single
> source of truth for the connection variable, the channel mapping, the 13-variant table, the
> gotchas, and the per-example README shape. On any conflict, **the merged connector code wins**
> (`connectors/kafka/` in `kubemq-server`). This file is duplicated verbatim at
> [`examples/SHARED-CONVENTIONS.md`](examples/SHARED-CONVENTIONS.md); keep the two copies in sync.
> Where the spec is more detailed, defer to `.work/tasks/kafka-connector/spec.md` (at the **clients
> root** `.work/`, NOT inside this repo).

## 1. Connection

Every example reads a **single** var:

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"   # default; used as bootstrap.servers
```

Named `_BOOTSTRAP` (not `_URL`) because Kafka takes a `host:port` bootstrap list, not a URL scheme
— the honest analog of `bootstrap.servers`.

Port table:

| Port | Transport | Env / notes |
|------|-----------|-------------|
| `9092` | Kafka wire, plain TCP | default; `KUBEMQ_KAFKA_BOOTSTRAP=localhost:9092` |
| `9093` | Kafka wire, TLS | `security.protocol=SSL`; documentation-only unless the broker has certs (§7) |

- **Connector is DISABLED by default** — every "How to Run" notes `CONNECTORS_KAFKA_ENABLE=true`
  (gotcha #1; contrast AMQP/MQTT which are enabled by default).
- **`AdvertisedHost` must be set for external clients** (`CONNECTORS_KAFKA_ADVERTISED_HOST`); empty
  → pod hostname → connect-then-hang (gotcha #2).
- **Burn-in uses a DIFFERENT var:** `KUBEMQ_BROKER_ADDRESS` (default `localhost:9092`), NEVER
  `KUBEMQ_KAFKA_BOOTSTRAP`.

## 2. Channel mapping

- Topic `orders` ↔ Events-Store channel `kafka.orders` (prefix `kafka.`); partition p>0 ↔
  `kafka.orders~<p>` (p0 stays `kafka.orders`).
- Offset = STAN `Sequence` (durable, restart-stable, Raft-replicated, identical across nodes).
- Increase-only partitions (`CreatePartitions`), hard cap **256**; each partition = independent
  ordered offset space.
- **Naming conventions (charset-safe — never `~` or `/`):**
  - Example topics: `kafka-ex-<family>-<short>` (e.g. `kafka-ex-produce-acks`).
  - Burn-in topics: `burnin.<worker>.<idx:04d>` (dots = KubeMQ hierarchy separator).
  - `~` is reserved in topic names (M8+) → `INVALID_TOPIC_EXCEPTION(17)` (gotcha #6); `/` is
    rejected in `transactional.id` → `INVALID_REQUEST(42)` (gotcha #7).

## 3. Auth — none by default

- Runnable default: **no SASL** (stock dev broker, auth off). Only the `security/sasl-plain-scram`
  variant needs credentials.
- Documented production contract: **SASL/PLAIN + SASL/SCRAM-SHA-256/512**; mTLS principal =
  verified-cert CN.
- Every per-example README ends with the standard **auth banner** linking
  `docs/guides/security-sasl-tls.md`:
  > Runs with no SASL by default on a stock dev broker; for SASL/PLAIN + SCRAM (and mTLS principal
  > derivation) see `security/sasl-plain-scram` + `docs/guides/security-sasl-tls.md`.

## 4. The 13-variant master table

Examples MIRROR the connector's supported surface (§2.3/§2.4 of the spec) and, where possible, a
`connectors/kafka/*_test.go` canonical test; only franz-go is server-proven; each per-example
README's "Kafka specifics" table is populated from the matching row.

| # | Variant (dir) | Family | Kafka APIs exercised | Asserts |
|---|---------------|--------|----------------------|---------|
| 1 | `produce/basic-acks` | produce | Produce acks 0/1/all; oversized → `MESSAGE_TOO_LARGE` | round-trip; oversized rejected |
| 2 | `produce/idempotent` | produce | `InitProducerId`; enable.idempotence; per-(PID,partition) dedup | no duplicates on retry |
| 3 | `produce/compression-and-keys` | produce | none/gzip/snappy/lz4/zstd + keyed partitioning | keyed records land per partitioner; **partitioner gotcha #4** |
| 4 | `consume/from-beginning-latest` | consume | Fetch long-poll; auto.offset.reset earliest/latest | both start positions correct |
| 5 | `consume/seek-offsets-timestamps` | consume/offsets | seek(offset); ListOffsets by-timestamp | seeks land on expected records |
| 6 | `consumer-groups/join-rebalance` | consumer-groups | Join/Sync/Heartbeat/Leave; multi-consumer rebalance | partitions redistribute; no loss across rebalance |
| 7 | `consumer-groups/commit-and-lag` | consumer-groups | OffsetCommit/OffsetFetch; consumer-group lag metric | resumes from committed offset; lag reported |
| 8 | `admin/topics-lifecycle` | admin | CreateTopics/DeleteTopics/DescribeConfigs/DescribeCluster | topic created/described/deleted; `~` rejected (gotcha #6) |
| 9 | `admin/partitions-and-configs` | admin | CreatePartitions (increase-only ≤256); IncrementalAlterConfigs (🟡); DeleteRecords (🟡) | increase succeeds; same-count/decrease/>256 → `INVALID_PARTITIONS` |
| 10 | `offsets/list-and-retention` | offsets | ListOffsets earliest/latest/by-ts; retention.ms/bytes mapping | earliest tracks log-start; retention honored |
| 11 | `transactions/eos-commit-abort` | transactions | `InitProducerId`→`AddPartitionsToTxn`→txn Produce→`EndTxn(commit\|abort)` | committed visible, aborted absent under read_committed; **KIP-890 note** |
| 12 | `transactions/read-committed` | transactions | `read_committed` consume; `AbortedTransactions`; ListOffsets(latest,read_committed)=LSO | never sees aborted; LSO<HWM while open |
| 13 | `security/sasl-plain-scram` | security | SASL/PLAIN + SCRAM-256/512 (runnable); TLS/mTLS (doc-only) | authenticated produce/consume; denied → `*_AUTHORIZATION_FAILED` |

> **Per-language parity with justified N/A cells permitted** (aws/amqp-1-0/stomp model): where a
> client library cannot express a variant (missing AdminClient partition ops, no transaction API,
> etc.), the folder + README still exist and the README explains the limitation and points to the
> supported alternative — no silent drops.

## 5. Directory naming

- **kebab-case** variant dirs for `go`, `javascript`, `java`, `csharp`, `rust` (e.g.
  `produce/basic-acks`, `consumer-groups/join-rebalance`).
- **snake_case** variant dirs for `python`, `ruby` (e.g. `produce/basic_acks`,
  `consumer-groups/join_rebalance`) — the concept-group token stays
  kebab in every language; only the leaf variant dir switches case.
- The 7 concept-group dirs (`produce/ consume/ consumer-groups/ admin/ offsets/ transactions/
  security/`) are the same tokens in every language; only the leaf variant dir switches
  case.

## 6. The Kafka gotchas

The "gotcha device" is mandatory: each gotcha appears in the relevant doc **and** the relevant
example READMEs. Gotchas #9 (KIP-890) and #12 (`read_committed` client-side) are first-class for
EOS.

| # | Gotcha | Where surfaced |
|---|--------|----------------|
| 1 | **Connector disabled by default** — set `CONNECTORS_KAFKA_ENABLE=true` (unlike AMQP/MQTT) | every README How-to-Run, `getting-started`, `configuration` |
| 2 | **`AdvertisedHost` must be set** for external clients (empty → connect-then-hang, M-23) | `getting-started`, `reference/configuration`, `guides/security-sasl-tls` |
| 3 | **`acks>=1` on multi-node** — `acks=0` on a follower silently drops | `produce/basic-acks`, `guides/producing`, `reference/capabilities` |
| 4 | **Cross-client partitioner divergence** (Java/franz-go/kafkajs murmur2 vs librdkafka CRC32) — same key → different partition | `produce/compression-and-keys`, `consumer-groups/*`, `concepts/cross-client-partitioning` |
| 5 | **Growing N re-shards keys** — per-key order only within a fixed-N epoch | `admin/partitions-and-configs`, `concepts/cross-client-partitioning` |
| 6 | **`~` reserved in topic names (M8+)** → `INVALID_TOPIC_EXCEPTION(17)` | `admin/topics-lifecycle`, `reference/migration-from-kafka` |
| 7 | **`/` rejected in `transactional.id`** → `INVALID_TRANSACTIONAL_ID` → `INVALID_REQUEST(42)` | `transactions/*`, `reference/error-codes` |
| 8 | **Txn offset-commit requires Group WRITE** (stricter than real Kafka, D141) | `transactions/eos-commit-abort`, `guides/transactions-eos` |
| 9 | **KIP-890 V1 EOS ceiling** (no TV2) — upstream-shared, not a defect | every `transactions/*`, `concepts/transactions-eos`, `guides/transactions-eos` |
| 10 | **Cross-node stale-low partition count** during Raft lag — self-heals on refresh | `concepts/topics-partitions-offsets`, `reference/migration-from-kafka` |
| 11 | **Start-fresh repoint** — historical data/offsets NOT imported | `reference/migration-from-kafka` |
| 12 | **`read_committed` filtering is client-side** (`AbortedTransactions`), no server-side record filter | `transactions/read-committed`, `concepts/transactions-eos` |

## 7. Security depth

- **Runnable:** SASL/PLAIN and SASL/SCRAM-SHA-256/512 (the `security/sasl-plain-scram` variant),
  against a broker configured with a Kafka credential store.
- **Doc-only** (README + `guides/security-sasl-tls.md`, no separate program): TLS on `:9093`
  (`security.protocol=SSL`) and mTLS principal derivation — TLS requires broker certs not present
  on a stock dev broker (same doc-only stance as `kubemq-amqp-1-0`).

## 8. Per-example README template (8 sections)

Every variant ships a `README.md`, H1 = `{Language} — Kafka: {Variant}`, with these 8 sections in
order:

1. **Prerequisites** — language runtime + pinned Kafka client version + a running connector
   reachable at `KUBEMQ_KAFKA_BOOTSTRAP` + the `CONNECTORS_KAFKA_ENABLE=true` note.
2. **How to Run** — the exact per-language command (§9) with `KUBEMQ_KAFKA_BOOTSTRAP` (and, for the
   security variant, SASL) env exports shown.
3. **Expected Output** — the **literal stdout** the program prints on a successful run (copy the
   real lines once built — not a paraphrase). Negative-path examples print the negative result
   explicitly (e.g. `oversized → MESSAGE_TOO_LARGE`, `decrease partitions → INVALID_PARTITIONS`,
   `aborted txn → 0 records under read_committed`).
4. **What's Happening** — prose walkthrough of the Kafka wire flow (Metadata → Produce/Fetch →
   OffsetCommit …), ending "mirrors connector behavior in `connectors/kafka/`" (cite a `*_test.go`
   where one exists — e.g. `fetch_test.go`, `listoffsets_test.go`, `groupoffsets_test.go`,
   `txn_rpcs_test.go`).
5. **Kafka specifics** — this exact table:
   ```
   | API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
   ```
   `special handling` = the applicable gotcha (§6) + the "Asserts" summary from the §4 row.
6. **Related Examples** — same variant in the other 6 languages + the matching doc under
   `../../../../docs/`.
7. **Gotcha callout** — the §6 gotcha(s) that apply, as an inline callout box.
8. **Auth banner** — the standard no-SASL-default banner (§3).

**Exit-code convention:** every example exits **0 on success** and **non-zero on any failed
assertion or unexpected error** — runnable proofs, not demos (a wrong offset, a lost fan-out, an
out-of-order per-partition read, or an aborted-txn record visible under `read_committed` must fail
the process).

## 9. Per-language run / lint

| Language | Kafka client (floor; bump+lock at impl via `/check-deps`) | Build | Run | Lint |
|----------|------------------------------------------------------------|-------|-----|------|
| Go | `github.com/twmb/franz-go` | `go build ./...` | `go run ./<family>/<variant>` | `gofumpt -w . && golangci-lint run ./... --fix` |
| Python | `confluent-kafka` (librdkafka; uv) | `uv sync` | `uv run python <family>/<variant>/main.py` | `uv run ruff format . && uv run ruff check --fix .` |
| Java | `org.apache.kafka:kafka-clients` (+ AdminClient) | `mvn -q compile` | `mvn -q exec:java -Dexec.mainClass=...` | `mvn compile` |
| JS/TS | `kafkajs` (≥2.0) | `npm install` | `npx tsx <family>/<variant>/index.ts` | `npx tsc --noEmit` |
| C# | `Confluent.Kafka` | `dotnet build` | `dotnet run --project <family>/<variant>` | `dotnet format` |
| Ruby | `rdkafka` (rdkafka-ruby; librdkafka) | `bundle install` | `ruby <family>/<variant>/main.rb` | `bundle exec rubocop -a` (or `ruby -c`) |
| Rust | `rdkafka` (librdkafka) + `tokio` | `cargo build --workspace` | `cargo run -p <variant>` | `cargo fmt && cargo clippy --workspace -- -D warnings` |

> **Partitioner note for the matrix:** franz-go, Java `kafka-clients`, **and `kafkajs` (v2+
> `DefaultPartitioner`, Java-compatible)** default to **murmur2**; the **four** librdkafka-based
> clients (confluent-kafka, Confluent.Kafka, rdkafka-ruby, rust rdkafka) default to **CRC32**. So
> the JS keyed example (variant 3) and `concepts/cross-client-partitioning.md` MUST expect the SAME
> partition as Java/franz-go, NOT the CRC32 group. Keyed examples MUST call out gotcha #4. Floor
> `kafkajs` at **≥2.0** so the default is murmur2 (a pre-2.0 pin uses the `LegacyPartitioner`).
>
> Exact client versions are MINIMUM floors as of 2026-07; at implementation, bump each to latest
> stable via `/check-deps` and lock. Commit `go.sum` + `uv.lock`; gitignore `package-lock.json`,
> `Gemfile.lock`, `Cargo.lock`.
