# java — Kafka: Security — SASL/PLAIN + SCRAM

SASL/PLAIN and SASL/SCRAM-SHA-256/512 authentication against the connector. On a
stock dev broker (no credential store) it runs in **doc-mode**, emitting the exact
client config; with credentials set it runs a live authenticated round-trip and a
denied bad-credential path. TLS/mTLS is **doc-only**.

## Prerequisites

- JDK 21+ and Maven 3.9+.
- `org.apache.kafka:kafka-clients 3.9.0` (pinned in `../../pom.xml`).
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). **Connector DISABLED by default — start with
  `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1). For SASL, the broker must be
  configured with a Kafka credential store and must set
  `CONNECTORS_KAFKA_ADVERTISED_HOST` (gotcha #2); for TLS the SAN must cover the
  advertised host.
- **Live mode** additionally needs: `KUBEMQ_KAFKA_SASL_USERNAME`,
  `KUBEMQ_KAFKA_SASL_PASSWORD`, optional `KUBEMQ_KAFKA_SASL_MECHANISM`
  (`PLAIN` | `SCRAM-SHA-256` | `SCRAM-SHA-512`, default `PLAIN`) and
  `KUBEMQ_KAFKA_SECURITY_PROTOCOL` (`SASL_PLAINTEXT` default, or `SASL_SSL`).

## How to Run

From `examples/java/`:

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"

# Doc-mode (default — no credentials): prints the client config and exits 0.
mvn -q exec:exec -Dexec.mainClass=io.kubemq.examples.kafka.security.saslplainscram.Main

# Live mode (broker with a Kafka credential store):
export KUBEMQ_KAFKA_SASL_USERNAME="alice"
export KUBEMQ_KAFKA_SASL_PASSWORD="s3cret"
export KUBEMQ_KAFKA_SASL_MECHANISM="SCRAM-SHA-256"   # or PLAIN / SCRAM-SHA-512
mvn -q exec:exec -Dexec.mainClass=io.kubemq.examples.kafka.security.saslplainscram.Main
```

Credentials are read from env — never hard-coded.

## Expected Output

Doc-mode (no credentials set):

```
bootstrap.servers = localhost:9092
SASL not configured (set KUBEMQ_KAFKA_SASL_USERNAME/KUBEMQ_KAFKA_SASL_PASSWORD to run live). Showing client config only.
  PLAIN -> sasl.jaas.config = org.apache.kafka.common.security.plain.PlainLoginModule required username="demo-user" password="demo-secret";
  SCRAM-SHA-256 -> sasl.jaas.config = org.apache.kafka.common.security.scram.ScramLoginModule required username="demo-user" password="demo-secret";
  SCRAM-SHA-512 -> sasl.jaas.config = org.apache.kafka.common.security.scram.ScramLoginModule required username="demo-user" password="demo-secret";
  TLS/mTLS is doc-only: security.protocol=SSL on :9093 (truststore; +keystore for mTLS) — see docs/guides/security-sasl-tls.md
OK: SASL/SCRAM client config emitted (doc-mode, no live broker)
```

Live mode (credentials set):

```
bootstrap.servers = localhost:9092
SASL live mode: mechanism=SCRAM-SHA-256 securityProtocol=SASL_PLAINTEXT user=alice
CreateTopics 'kafka-ex-security-sasl-java' (1 partition)
[auth] produced 'sasl-1752422400000'
[auth] consumed 'sasl-1752422400000'
Bad-cred produce denied -> SaslAuthenticationException
OK: SASL authenticated round-trip + denied bad-cred path
```

## What's Happening

The program builds `sasl.jaas.config` for the chosen mechanism
(`PlainLoginModule` for PLAIN, `ScramLoginModule` for SCRAM-SHA-256/512) together
with `sasl.mechanism` and `security.protocol`. Without credentials it prints the
config for all three mechanisms, self-checks the JAAS string is non-blank, and
exits 0 (doc-mode). With credentials it authenticates an `Admin`, produces and
consumes a record (asserting the round-trip), then retries with a wrong password
and asserts the broker denies it with `SaslAuthenticationException` /
`TopicAuthorizationException` (`*_AUTHORIZATION_FAILED`).

> **TLS/mTLS is doc-only.** A stock dev broker has no certs, so the runnable path is
> SASL over plaintext (`SASL_PLAINTEXT`). For TLS, set `security.protocol=SSL` (or
> `SASL_SSL`) on `:9093` with a truststore (+ keystore for mTLS); the SAN must cover
> `CONNECTORS_KAFKA_ADVERTISED_HOST`. See the security guide.

The Kafka wire flow is `SaslHandshake(17) → SaslAuthenticate(36) → (authenticated)
Metadata/Produce/Fetch`, mirroring connector behavior in `connectors/kafka/`
(`authz_*_test.go`).

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| SaslHandshake(17), SaslAuthenticate(36), Produce(0), Fetch(1) | acks=all; read_uncommitted | 1 partition | fresh (throwaway) group | standard | none | murmur2 | SASL/PLAIN + SCRAM-256/512 runnable; TLS/mTLS doc-only; bad cred → `*_AUTHORIZATION_FAILED`; AdvertisedHost + TLS SAN (gotcha #2) |

## Related Examples

- Same variant in the other 6 languages: [`../../../go/security/sasl-plain-scram`](../../../go/security/sasl-plain-scram),
  [`../../../python/security/sasl_plain_scram`](../../../python/security/sasl_plain_scram),
  [`../../../javascript/security/sasl-plain-scram`](../../../javascript/security/sasl-plain-scram),
  [`../../../csharp/security/sasl-plain-scram`](../../../csharp/security/sasl-plain-scram),
  [`../../../ruby/security/sasl_plain_scram`](../../../ruby/security/sasl_plain_scram),
  [`../../../rust/security/sasl-plain-scram`](../../../rust/security/sasl-plain-scram).
- Docs: [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).

> **Gotcha #2 — `AdvertisedHost` + TLS SAN.** External SASL/TLS clients need the
> broker's `CONNECTORS_KAFKA_ADVERTISED_HOST` set (empty → connect-then-hang), and
> for TLS the certificate SAN must cover that host.

> **Auth.** This is the one example that exercises authentication. See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md)
> for the full SASL/PLAIN + SCRAM and TLS/mTLS setup.
