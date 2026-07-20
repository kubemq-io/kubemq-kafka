# java — Kafka: Idempotent Producer

An idempotent producer (`enable.idempotence=true`) writes a batch, force-resends it,
and proves the connector's per-`(PID,partition)` dedup stored **each record exactly
once** — no duplicates on retry.

## Prerequisites

- JDK 21+ and Maven 3.9+.
- `org.apache.kafka:kafka-clients 3.9.0` (pinned in `../../pom.xml`), providing the
  `Producer` / `Admin` APIs.
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). **The connector is DISABLED by default — start the
  broker with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1). For non-same-host
  clients also set `CONNECTORS_KAFKA_ADVERTISED_HOST` (gotcha #2).

## How to Run

From `examples/java/`:

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
mvn -q compile
mvn -q exec:exec -Dexec.mainClass=io.kubemq.examples.kafka.produce.idempotent.Main
```

## Expected Output

```
bootstrap.servers = localhost:9092
CreateTopics 'kafka-ex-produce-idempotent-java' (1 partition)
Produce 'rec-0' -> partition=0 offset=0
Produce 'rec-1' -> partition=0 offset=1
Produce 'rec-2' -> partition=0 offset=2
Produce 'rec-3' -> partition=0 offset=3
Produce 'rec-4' -> partition=0 offset=4
Fetch <- unique=5 total=5 (no duplicates after resend)
OK: idempotent producer stored each record exactly once
```

Even though the producer re-sends the batch, the read-back shows `unique == total`:
the connector dedups by `(ProducerId, partition, sequence)`, so a retry never
appends a duplicate.

## What's Happening

Setting `enable.idempotence=true` makes the client issue an `InitProducerId` on the
first `send`, obtaining a Producer ID (PID) and epoch. Every record then carries a
monotonic per-partition sequence number. When the same batch is produced again (an
application-level resend that simulates a network retry), the connector recognizes
the already-seen `(PID, partition, sequence)` tuples and drops them — so a
consumer reads exactly N unique records regardless of how many times the client
sent them. The assertion checks the **unique value set** (not a count of sends).
Idempotence implies `acks=all` and `retries>0` automatically.

The Kafka wire flow is `Metadata → InitProducerId (key 22) → Produce (idempotent
RecordBatch v2) → Fetch`, mirroring connector behavior in `connectors/kafka/`
(idempotent produce / dedup path).

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| InitProducerId(22), Produce(0), Fetch(1), CreateTopics(19) | acks=all (implied by idempotence); read_uncommitted | 1 partition | fresh (throwaway) group | offset = STAN Sequence | none | murmur2 | dedup per `(PID,partition,sequence)`; assert by unique value set, not send count |

## Related Examples

- Same variant in the other 6 languages: [`../../../go/produce/idempotent`](../../../go/produce/idempotent),
  [`../../../python/produce/idempotent`](../../../python/produce/idempotent),
  [`../../../javascript/produce/idempotent`](../../../javascript/produce/idempotent),
  [`../../../csharp/produce/idempotent`](../../../csharp/produce/idempotent),
  [`../../../ruby/produce/idempotent`](../../../ruby/produce/idempotent),
  [`../../../rust/produce/idempotent`](../../../rust/produce/idempotent).
- Docs: [`../../../../docs/guides/producing.md`](../../../../docs/guides/producing.md).
- Related: [`../basic-acks`](../basic-acks), [`../compression-and-keys`](../compression-and-keys).

> **Note — idempotence is ON by default.** `kafka-clients` enables idempotence by
> default (which forces `acks=all`); to use a non-`all` ack level you must
> explicitly disable it. Keep it on for exactly-once-per-partition delivery.

> **Auth.** This example uses the connector's no-auth default posture
> (SHARED-CONVENTIONS §4.3). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md)
> for SASL/PLAIN + SCRAM and TLS/mTLS.
