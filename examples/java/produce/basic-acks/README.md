# java — Kafka: Produce Basic Acks

The Produce round-trip against the KubeMQ Kafka connector across all three ack
levels, plus the oversized → `MESSAGE_TOO_LARGE` rejection:
`CreateTopics → Produce(acks=0|1|all) → Fetch → Produce(2 MiB) → reject`.

## Prerequisites

- JDK 21+ and Maven 3.9+.
- `org.apache.kafka:kafka-clients 3.9.0` (pinned in `../../pom.xml`), which also
  provides the `Admin`/`AdminClient` API used here.
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). **The connector is DISABLED by default — start the
  broker with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1). For any non-same-host
  client, also set `CONNECTORS_KAFKA_ADVERTISED_HOST` or the client connects then
  hangs (gotcha #2).

## How to Run

From `examples/java/`:

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
mvn -q compile
mvn -q exec:exec -Dexec.mainClass=io.kubemq.examples.kafka.produce.basicacks.Main
```

> The `pom.xml` configures `exec:exec` (a **forked JVM**) so the example returns
> the real OS exit code — a failed assertion reliably fails the process.

## Expected Output

```
bootstrap.servers = localhost:9092
CreateTopics 'kafka-ex-produce-acks-java' (1 partition)
Produce acks=0 -> partition=0 offset=-1
Produce acks=1 -> partition=0 offset=1
Produce acks=all -> partition=0 offset=2
Fetch <- offset=0 key=k value=acks=0
Fetch <- offset=1 key=k value=acks=1
Fetch <- offset=2 key=k value=acks=all
Oversized produce rejected -> RecordTooLargeException
OK: produce basic-acks round-trip + MESSAGE_TOO_LARGE guard
```

`acks=0` gives no broker acknowledgement, so the returned `RecordMetadata.offset()`
is `-1`; the round-trip is asserted from the **consumer**, not the producer ack.
The final oversized (2 MiB) record is rejected by the broker with
`MESSAGE_TOO_LARGE` (surfaced in Java as `RecordTooLargeException`).

## What's Happening

The program creates a single-partition topic, then produces one record at each ack
level — `acks=0`, `acks=1`, `acks=all` — reads all three back from the start of the
log and asserts they round-trip in order. On a multi-node deployment an `acks=0`
send to a follower can be silently dropped (gotcha #3), so production code uses
`acks>=1`. Finally it raises the client-side `max.request.size` above the payload
and produces a 2 MiB record so the **broker** (not the client) rejects it,
exceeding the connector's `CONNECTORS_KAFKA_MAX_MESSAGE_BYTES` (1 MiB default), and
asserts a `MESSAGE_TOO_LARGE` result. Any mismatch exits non-zero.

The Kafka wire flow is `Metadata → CreateTopics → Produce (RecordBatch v2) →
Fetch`, mirroring connector behavior in `connectors/kafka/` (produce path;
`produce_test.go`).

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Produce(0), Fetch(1), CreateTopics(19), Metadata(3) | acks 0/1/all; read_uncommitted | 1 partition | fresh (throwaway) group | offset = STAN Sequence, starts at 0 | none | **murmur2** (kafka-clients default) | 2 MiB → `MESSAGE_TOO_LARGE`; raises `max.request.size` so the oversized record reaches the broker |

## Related Examples

- Same variant in the other 6 languages: [`../../../go/produce/basic-acks`](../../../go/produce/basic-acks),
  [`../../../python/produce/basic_acks`](../../../python/produce/basic_acks),
  [`../../../javascript/produce/basic-acks`](../../../javascript/produce/basic-acks),
  [`../../../csharp/produce/basic-acks`](../../../csharp/produce/basic-acks),
  [`../../../ruby/produce/basic_acks`](../../../ruby/produce/basic_acks),
  [`../../../rust/produce/basic-acks`](../../../rust/produce/basic-acks).
- Docs: [`../../../../docs/guides/producing.md`](../../../../docs/guides/producing.md).
- Next: [`../idempotent`](../idempotent), [`../compression-and-keys`](../compression-and-keys).

> **Gotcha #3 — `acks>=1` on multi-node.** `acks=0` gives no delivery guarantee; on
> a multi-node cluster a record sent to a follower can be silently dropped. Use
> `acks=all` (or at least `acks=1`) for durability.

> **Auth.** This example uses the connector's no-auth default posture
> (SHARED-CONVENTIONS §4.3). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md)
> for SASL/PLAIN + SCRAM and TLS/mTLS.
