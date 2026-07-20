# Python — Kafka: Join and Rebalance

Two consumers in the same group split a 4-partition topic via Join/Sync/Heartbeat, and the program
proves the assignments are disjoint-and-covering with **no message loss or double-delivery** across the
rebalance — against the KubeMQ Kafka connector using native `confluent-kafka`.

## Prerequisites

- Python 3.9+ and [`uv`](https://docs.astral.sh/uv/).
- Kafka client: `confluent-kafka` (installed via `uv sync` from `../../pyproject.toml`).
- A running **KubeMQ Kafka connector** reachable at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`),
  started with **`CONNECTORS_KAFKA_ENABLE=true`** (the connector is disabled by default — gotcha #1).
- `AdvertisedHost` set on the connector for any non-loopback client (gotcha #2).

## How to Run

    cd examples/python
    uv sync
    export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
    uv run python consumer-groups/join_rebalance/main.py

## Expected Output

> The Python suite namespaces its topic with `KUBEMQ_KAFKA_NAME_PREFIX` (default `py`):
> `kafka-py-cgroups-rebalance`. The exact partition split between `c1`/`c2` depends on the assignor.

    === consumer-groups/join-rebalance — topic 'kafka-py-cgroups-rebalance' ===
      bootstrap : localhost:9092
      client    : confluent-kafka (librdkafka; CRC32 default partitioner)
      note      : connector must be started with CONNECTORS_KAFKA_ENABLE=true

      c1 owned partitions: [0, 1]
      c2 owned partitions: [2, 3]
      [OK] the two members COVER every partition
      [OK] the two members' assignments are DISJOINT (no shared partition)
      [OK] all 40 records consumed across the group (40)
      [OK] no record was double-delivered across the rebalance
    Round-trip complete.

## What's Happening

- A 4-partition topic is loaded with 40 records (10 per partition).
- Two consumers share one `group.id`; the group coordinator runs **JoinGroup → SyncGroup →
  Heartbeat** so each partition is owned by exactly one member (disjoint, covering assignment).
- Both consumers are polled round-robin in one process so their heartbeats and the join barrier make
  progress together; the aggregate read is asserted to be exactly-once — no loss, no duplicate.
- Mirrors connector behavior in `connectors/kafka/` (group coordinator Join/Sync/Heartbeat/Leave;
  see `groupoffsets_test.go`).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| FindCoordinator(10), JoinGroup(11), SyncGroup(14), Heartbeat(12), LeaveGroup(13), Fetch(1) | acks=all producer; read_uncommitted | 4 partitions | `join-rebalance-grp` (2 members, subscribe) | offset per partition = STAN Sequence | none | explicit partition on produce | assignments disjoint+covering; no loss across rebalance |

## Related Examples

- Same variant in the other languages: [`../../../go/consumer-groups/join-rebalance/`](../../../go/consumer-groups/join-rebalance/),
  [`../../../java/consumer-groups/join-rebalance/`](../../../java/consumer-groups/join-rebalance/),
  [`../../../javascript/consumer-groups/join-rebalance/`](../../../javascript/consumer-groups/join-rebalance/),
  [`../../../csharp/consumer-groups/join-rebalance/`](../../../csharp/consumer-groups/join-rebalance/),
  [`../../../ruby/consumer-groups/join_rebalance/`](../../../ruby/consumer-groups/join_rebalance/),
  [`../../../rust/consumer-groups/join-rebalance/`](../../../rust/consumer-groups/join-rebalance/).
- [`../commit_and_lag/`](../commit_and_lag/) — offset commit, resume, and lag.
- [`../../../../docs/concepts/consumer-groups.md`](../../../../docs/concepts/consumer-groups.md)

> **Gotcha #4 — keyed order across a rebalance.** Per-key ordering holds only within a fixed partition
> count and a single client family's partitioner. If a producer of a different family (murmur2 vs
> CRC32) writes the same keys, they may land on different partitions and different members. See
> [`../../produce/compression_and_keys/`](../../produce/compression_and_keys/).

> **Auth.** The stock dev connector runs with SASL off. For a secured connector, configure SASL/PLAIN
> or SCRAM and see [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
