# javascript — Kafka: Idempotent Producer

Turn on `enable.idempotence` (kafkajs `{ idempotent: true }`), produce a keyed batch, then re-send the
**same** batch with the **same** producer and prove no duplicate lands. The Kafka topic
`kafka-ex-produce-idem` maps onto the Events-Store channel `kafka.kafka-ex-produce-idem`.

## Prerequisites

- Node.js 18+ and `npm install` in `examples/javascript/` (pins `kafkajs` `^2.2.4` — v2+, murmur2
  `DefaultPartitioner`).
- A running KubeMQ server with the Kafka connector **enabled** (`CONNECTORS_KAFKA_ENABLE=true` — the
  connector is **disabled by default**, gotcha #1), reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). For external clients, set `CONNECTORS_KAFKA_ADVERTISED_HOST` (gotcha #2).

## How to Run

```bash
cd examples/javascript
npm install
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
npx tsx produce/idempotent/index.ts
```

## Expected Output

```
Connecting to KubeMQ Kafka connector at localhost:9092 (topic "kafka-ex-produce-idem")
Produced 5 idempotent records (PID assigned via InitProducerId)
Re-sent the SAME batch (simulated retry) — broker must dedupe by (PID, seq)
Read back 5 record(s): [order-a, order-b, order-c, order-d, order-e]

Idempotence proven: re-sent batch produced no duplicates (exactly-once landing)
```

## What's Happening

- `{ idempotent: true }` makes kafkajs perform an `InitProducerId` (key 22) to obtain a Producer ID
  (PID) + epoch, and stamp every RecordBatch with `(PID, epoch, base sequence)`.
- `enable.idempotence` forces `acks=all` and bounds in-flight requests (`maxInFlightRequests: 1` here
  for strict per-partition order).
- The second `producer.send` replays already-persisted sequences; the broker dedupes
  per-`(PID, partition, sequence)`, so the replay is a no-op.
- Consuming back proves exactly `KEYS.length` records, each key unique — no duplicate from the resend.
- Mirrors connector behavior in `connectors/kafka/` (idempotent Produce / `InitProducerId`; see
  `connectors/kafka/produce_test.go`).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| InitProducerId (22), Produce (0), Fetch (1), CreateTopics (19), DeleteTopics (20) | acks=all (forced by idempotence) | `kafka-ex-produce-idem` / 1 partition | ephemeral verify group | offset = STAN Sequence | none | murmur2 (DefaultPartitioner) | per-`(PID,partition,seq)` dedup; resend produces no duplicate |

## Related Examples

- Same variant, other languages: [`../../../go/produce/idempotent`](../../../go/produce/idempotent),
  [`../../../java/produce/idempotent`](../../../java/produce/idempotent),
  [`../../../csharp/produce/idempotent`](../../../csharp/produce/idempotent),
  [`../../../rust/produce/idempotent`](../../../rust/produce/idempotent),
  [`../../../python/produce/idempotent`](../../../python/produce/idempotent),
  [`../../../ruby/produce/idempotent`](../../../ruby/produce/idempotent).
- Doc: [`../../../../docs/guides/producing.md`](../../../../docs/guides/producing.md),
  [`../../../../docs/concepts/transactions-eos.md`](../../../../docs/concepts/transactions-eos.md).
- Next: [`../compression-and-keys/`](../compression-and-keys/), [`../../transactions/eos-commit-abort/`](../../transactions/eos-commit-abort/).

> **Gotcha — idempotence requires `acks=all`.** kafkajs forces `acks=all` when `idempotent: true`; a
> non-`all` ack level would disable the idempotent write. Keep `maxInFlightRequests: 1` for strict order.

> **Auth.** The dev default is no SASL over plain TCP (`:9092`). For a secured connector, configure
> SASL/PLAIN or SASL/SCRAM (and TLS on `:9093`). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
