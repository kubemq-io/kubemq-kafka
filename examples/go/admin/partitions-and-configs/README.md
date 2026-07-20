# Go — Kafka: Admin Partitions and Configs

Increase-only partition growth (≤256), partial config alteration, and low-end log
truncation against the KubeMQ Kafka connector:
`CreatePartitions → IncrementalAlterConfigs → DeleteRecords`, with the invalid
decrease/`>256` rejections (`INVALID_PARTITIONS`).

## Prerequisites

- Go 1.24+ and `github.com/twmb/franz-go v1.21.4` (kadm + kgo, pinned in
  `../../go.mod`).
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). **The connector is DISABLED by default — start the
  broker with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1). For any non-same-host
  client, also set `CONNECTORS_KAFKA_ADVERTISED_HOST` or the client connects then
  hangs (gotcha #2).

## How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
go run ./admin/partitions-and-configs
```

## Expected Output

```
[kubemq-kafka] admin/partitions-and-configs | bootstrap=localhost:9092 partitioner=murmur2(franz-go)
CreateTopic: kafka-ex-admin-part-<8hex> (partitions=1)
UpdatePartitions(set=3): 1 -> 3 (final count 3)
UpdatePartitions(set=2): rejected with INVALID_PARTITIONS (increase-only)
UpdatePartitions(set=300): rejected with INVALID_PARTITIONS (>256 cap)
IncrementalAlterConfigs: retention.ms=7200000 (partial, subset)
DeleteRecords(<3): partition 0 log-start now 3 (low-end truncation)
DeleteTopic: ok
PASS: increase-only partitions + partial config + DeleteRecords verified
```

> The topic is suffixed with 8 random hex chars so concurrent runs of the other
> language examples against the same connector do not collide.

## What's Happening

The program creates a 1-partition topic and grows it to 3 via `CreatePartitions`,
asserting the final count is 3. It then proves partition growth is **increase-only
and capped at 256**: a decrease to 2 and a jump to 300 are both rejected with
`INVALID_PARTITIONS`. Next it applies a **partial** `IncrementalAlterConfigs`
(setting only `retention.ms=7200000`, leaving other keys untouched) and reads it
back. Finally it produces records and calls `DeleteRecords` up to offset 3,
asserting the partition's log-start advances to 3 (low-end truncation). Any
unexpected acceptance/rejection fails the process.

The wire flow is `CreateTopics → CreatePartitions → IncrementalAlterConfigs →
Produce → DeleteRecords → ListOffsets (log-start)`, mirroring connector behavior in
`connectors/kafka/createpartitions_rebalance_test.go` and
`connectors/kafka/topicconfig_test.go`.

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| CreatePartitions(37), IncrementalAlterConfigs(44), DeleteRecords(21), DescribeConfigs(32), ListOffsets(2) | acks=all; read_uncommitted | 1 → 3 partitions (increase-only, cap 256) | none | DeleteRecords advances log-start; offsets never renumber | none | murmur2 (franz-go) | decrease / same-count / `>256` → `INVALID_PARTITIONS`; **gotcha #5** — growing N re-shards keys, per-key order holds only within a fixed-N epoch; `IncrementalAlterConfigs`/`DeleteRecords` are partial (🟡) |

## Related Examples

- Same variant in other languages:
  `../../../python/admin/partitions_and_configs`,
  `../../../javascript/admin/partitions-and-configs`,
  `../../../java/admin/partitions-and-configs`,
  `../../../csharp/admin/partitions-and-configs`,
  `../../../ruby/admin/partitions_and_configs`,
  `../../../rust/admin/partitions-and-configs`.
- Docs: `../../../../docs/guides/admin-and-topics.md`,
  `../../../../docs/concepts/cross-client-partitioning.md`.
- Related: [`../topics-lifecycle`](../topics-lifecycle).

> **Gotcha #5 — growing N re-shards keys.** Partition count can only grow, and each
> increase changes `hash(key) % N`, so a key that mapped to partition 1 under N=1
> may map to partition 2 under N=3. Per-key ordering is guaranteed only **within a
> fixed-N epoch** — plan the partition count before you rely on key ordering.

> Auth: this example uses the no-auth default posture. Runs with no SASL by default
> on a stock dev broker; for SASL/PLAIN + SCRAM (and mTLS principal derivation) see
> [`../../security/sasl-plain-scram`](../../security/sasl-plain-scram) +
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
