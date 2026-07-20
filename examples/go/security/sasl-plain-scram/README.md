# Go — Kafka: Security SASL PLAIN / SCRAM

Authenticated produce/consume against a SASL-enabled KubeMQ Kafka connector using
SASL/PLAIN or SASL/SCRAM-SHA-256/512, plus the denied-principal path
(`*_AUTHORIZATION_FAILED`). TLS/mTLS on `:9093` is documentation-only.

## Prerequisites

- Go 1.24+ and `github.com/twmb/franz-go v1.21.4` (pinned in `../../go.mod`).
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). **The connector is DISABLED by default — start the
  broker with `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1). For any non-same-host
  client, also set `CONNECTORS_KAFKA_ADVERTISED_HOST` or the client connects then
  hangs (gotcha #2).
- **To exercise the authenticated path**, a broker configured with a Kafka
  credential store and the SASL env vars below. With no credentials the example
  prints a SKIP and passes with nothing to assert.

## How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"

# No credentials -> SKIP path (still exits 0):
go run ./security/sasl-plain-scram

# Authenticated path:
export KAFKA_SASL_USER="app"
export KAFKA_SASL_PASS="s3cret"
export KAFKA_SASL_MECHANISM="SCRAM-SHA-256"   # PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512
# Optional: exercise the denied-principal assertion
export KAFKA_SASL_DENIED_USER="nobody"
export KAFKA_SASL_DENIED_PASS="wrong"
go run ./security/sasl-plain-scram
```

## Expected Output

No credentials (SKIP path):

```
[kubemq-kafka] security/sasl-plain-scram | bootstrap=localhost:9092 partitioner=murmur2(franz-go)
SKIP: SASL example needs a broker with a credential store.
      Set KAFKA_SASL_USER / KAFKA_SASL_PASS (and optionally
      KAFKA_SASL_MECHANISM=PLAIN|SCRAM-SHA-256|SCRAM-SHA-512) and re-run.
PASS: nothing to assert without credentials (see README for setup)
```

Authenticated path (with credentials):

```
[kubemq-kafka] security/sasl-plain-scram | bootstrap=localhost:9092 partitioner=murmur2(franz-go)
SASL: mechanism=SCRAM-SHA-256 user=app
CreateTopic (authenticated): kafka-ex-sec-sasl-<8hex>
Produce (authenticated): ok
Fetch (authenticated): "authenticated payload" round-trip ok
Denied principal "nobody": rejected with <err> (expected)
DeleteTopic: ok
PASS: SASL authenticated round-trip verified
```

> The topic is suffixed with 8 random hex chars so concurrent runs of the other
> language examples against the same connector do not collide.

## What's Happening

If no `KAFKA_SASL_USER`/`KAFKA_SASL_PASS` are set, the example prints a SKIP banner
and exits 0 — a stock dev broker has auth off. With credentials, it builds a SASL
mechanism (PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512) from the env, authenticates,
and runs a full authenticated `CreateTopic → Produce → Fetch` round-trip, asserting
the body survives. If a denied principal is provided, it asserts that producing
with it is rejected with an `*_AUTHORIZATION_FAILED` error — an accepted denied
principal, or a successful auth that should have failed, fails the process.

The wire flow is `SaslHandshake → SaslAuthenticate → Metadata → CreateTopics →
Produce → Fetch`, mirroring connector behavior in `connectors/kafka/sasl_test.go`.

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| SaslHandshake(17), SaslAuthenticate(36), CreateTopics(19), Produce(0), Fetch(1) | acks=all; read_uncommitted | 1 partition | none | offset = STAN Sequence | none | murmur2 (franz-go) | SASL/PLAIN + SCRAM-256/512 runnable; TLS/mTLS on `:9093` doc-only; denied principal → `*_AUTHORIZATION_FAILED`; **gotcha #2** advertised-host still required for external clients |

## Related Examples

- Same variant in other languages: `../../../python/security/sasl_plain_scram`,
  `../../../javascript/security/sasl-plain-scram`,
  `../../../java/security/sasl-plain-scram`,
  `../../../csharp/security/sasl-plain-scram`,
  `../../../ruby/security/sasl_plain_scram`,
  `../../../rust/security/sasl-plain-scram`.
- Docs: `../../../../docs/guides/security-sasl-tls.md`.
- Related: [`../../produce/basic-acks`](../../produce/basic-acks).

> **TLS/mTLS is documentation-only here.** Plain-TCP SASL runs on `:9092`; TLS
> (`security.protocol=SSL`) lives on `:9093` and needs broker certs a stock dev
> broker does not ship, so it is doc-only (see the guide). mTLS derives the
> principal from the verified client-cert CN. **Gotcha #2:** external clients still
> need `CONNECTORS_KAFKA_ADVERTISED_HOST` or they connect-then-hang.

> Auth: this IS the auth example. It runs with no SASL by default (SKIP path) on a
> stock dev broker; supply `KAFKA_SASL_*` to exercise SASL/PLAIN + SCRAM. See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md)
> for mTLS principal derivation.
