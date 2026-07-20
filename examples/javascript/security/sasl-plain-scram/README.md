# javascript — Kafka: SASL/PLAIN and SCRAM

Authenticate with SASL/PLAIN or SASL/SCRAM-256/512 and run a produce/consume round-trip over the
authenticated connection, plus the authorization-failure path (`*_AUTHORIZATION_FAILED`). The same
shared factory reads SASL from env, so this is the **same code** as every other variant — only the
connection is authenticated. TLS/mTLS is documented, not a separate program. The Kafka topic
`kafka-ex-security` maps onto the Events-Store channel `kafka.kafka-ex-security`.

## Prerequisites

- Node.js 18+ and `npm install` in `examples/javascript/` (pins `kafkajs` `^2.2.4` — v2+, murmur2
  `DefaultPartitioner`).
- A running KubeMQ server with the Kafka connector **enabled** (`CONNECTORS_KAFKA_ENABLE=true` — the
  connector is **disabled by default**, gotcha #1), reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). For external clients, set `CONNECTORS_KAFKA_ADVERTISED_HOST` (gotcha #2).
- **For the authenticated path:** a broker configured with a Kafka credential store (§4.7). Against a
  stock no-auth dev broker (no SASL env), the example runs an unauthenticated round-trip on the same
  code path and documents how to switch SASL on.

## How to Run

```bash
cd examples/javascript
npm install
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"

# Optional — authenticate (against a broker with a credential store):
export KUBEMQ_KAFKA_SASL_MECHANISM="scram-sha-256"   # or: plain | scram-sha-512
export KUBEMQ_KAFKA_SASL_USERNAME="alice"
export KUBEMQ_KAFKA_SASL_PASSWORD="s3cret"
# Optional — TLS on :9093 (doc-only path, reuses the ssl helper):
export KUBEMQ_KAFKA_TLS="true"
# Optional — exercise the denied path:
export KUBEMQ_KAFKA_DENIED_TOPIC="kafka-ex-forbidden"

npx tsx security/sasl-plain-scram/index.ts
```

## Expected Output

Against the default no-auth dev broker (no SASL env):

```
Connecting to KubeMQ Kafka connector at localhost:9092 using no-auth (dev default) (topic "kafka-ex-security")
Produced 1 record over the authenticated connection
Consumed back: [secure-hello]
Denied-path check skipped (set KUBEMQ_KAFKA_DENIED_TOPIC to exercise *_AUTHORIZATION_FAILED)

Note: ran WITHOUT SASL (dev default). Set KUBEMQ_KAFKA_SASL_MECHANISM=plain|scram-sha-256|scram-sha-512
      + KUBEMQ_KAFKA_SASL_USERNAME/_PASSWORD against a secured broker to exercise authentication.

Security round-trip proven over no-auth (dev default)
```

With SASL set, the first line becomes `... using SASL/SCRAM-SHA-256 (topic "kafka-ex-security")` and, if
`KUBEMQ_KAFKA_DENIED_TOPIC` points at an unauthorized topic, the denied line reads
`Produce to denied topic "..." -> TOPIC_AUTHORIZATION_FAILED`.

## What's Happening

- `newKafka()` reads `KUBEMQ_KAFKA_SASL_MECHANISM` / `_USERNAME` / `_PASSWORD` from env and configures
  kafkajs SASL (`plain`, `scram-sha-256`, `scram-sha-512`); `KUBEMQ_KAFKA_TLS=true` adds the `ssl` path
  against `:9093`.
- The example produces one record and consumes it back over the (authenticated) connection — the same
  produce/Fetch path as every other variant.
- The optional denied path produces to a topic the principal is not authorized to write; the connector
  rejects it with `TOPIC_AUTHORIZATION_FAILED` (or `GROUP_AUTHORIZATION_FAILED` for a denied group).
- Mirrors connector behavior in `connectors/kafka/` (SASL handshake / authorization; see
  `connectors/kafka/sasl_test.go`).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| SaslHandshake (17), SaslAuthenticate (36), Produce (0), Fetch (1), CreateTopics (19) | acks=all | `kafka-ex-security` / 1 partition | ephemeral verify group | offset = STAN Sequence | none | murmur2 (DefaultPartitioner) | SASL/PLAIN + SCRAM-256/512 runnable; TLS/mTLS doc-only; denied → `*_AUTHORIZATION_FAILED` |

## Related Examples

- Same variant, other languages: [`../../../go/security/sasl-plain-scram`](../../../go/security/sasl-plain-scram),
  [`../../../java/security/sasl-plain-scram`](../../../java/security/sasl-plain-scram),
  [`../../../csharp/security/sasl-plain-scram`](../../../csharp/security/sasl-plain-scram),
  [`../../../rust/security/sasl-plain-scram`](../../../rust/security/sasl-plain-scram),
  [`../../../python/security/sasl_plain_scram`](../../../python/security/sasl_plain_scram),
  [`../../../ruby/security/sasl_plain_scram`](../../../ruby/security/sasl_plain_scram).
- Doc: [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).

> **Gotcha #2 — set `AdvertisedHost` (and SAN for TLS).** For any external client, set
> `CONNECTORS_KAFKA_ADVERTISED_HOST`, or the client connects then hangs. For TLS on `:9093`, the broker
> cert's SAN must match the advertised host.

> **Auth.** The dev default is no SASL over plain TCP (`:9092`). This variant is the runnable SASL path;
> TLS/mTLS is documented in
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
