# Rust — Kafka: SASL/PLAIN & SCRAM

Authenticated produce/consume over SASL/PLAIN or SCRAM-SHA-256/512, plus the authorization-denied path.
TLS/mTLS is doc-only.

## 1. Prerequisites

- Rust 1.75+ + Cargo; `rdkafka` 0.37 (librdkafka, **`ssl-vendored`** for SCRAM HMAC) on `tokio`.
- A KubeMQ Kafka connector with a **Kafka credential store** configured (spec §4.7), at
  `KUBEMQ_KAFKA_BOOTSTRAP`, **`CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1).
- The SASL env vars below. Without `KAFKA_SASL_MECHANISM` the program prints setup instructions and
  exits 0 (documented skip — there is nothing to authenticate against on a stock dev broker).

## 2. How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
export KAFKA_SASL_MECHANISM=SCRAM-SHA-256      # or PLAIN / SCRAM-SHA-512
export KAFKA_SASL_USERNAME=<user>
export KAFKA_SASL_PASSWORD=<pass>
export KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT  # or SASL_SSL for TLS
# optional denied-path check:
# export KAFKA_DENIED_USERNAME=<no-acl-user>
# export KAFKA_DENIED_PASSWORD=<pass>
cargo run -p sasl-plain-scram
```

## 3. Expected Output

```text
[kubemq-kafka] security/sasl-plain-scram bootstrap=localhost:9092 (SASL; connector must be enabled: CONNECTORS_KAFKA_ENABLE=true)
authenticating with mechanism=SCRAM-SHA-256
authenticated produce OK
authenticated consume OK
denied principal correctly rejected: ... (TOPIC/GROUP_AUTHORIZATION_FAILED)
sasl-plain-scram OK: authenticated produce/consume succeeded
```

(Without SASL env set, it prints setup instructions and exits 0.)

## 4. What's Happening

Before any Produce/Fetch, librdkafka performs SaslHandshake (key 17) + SaslAuthenticate (key 36) using
the mechanism and credentials layered by `kafka_common::apply_sasl_from_env`. An authenticated user
round-trips a record; a principal without ACLs is rejected with `TOPIC_AUTHORIZATION_FAILED` /
`GROUP_AUTHORIZATION_FAILED`.

## 5. Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| SaslHandshake(17), SaslAuthenticate(36) | acks=all, read_uncommitted | kafka-ex-security-sasl / 1 | authenticated group | n/a | none | librdkafka CRC32 | denied → *_AUTHORIZATION_FAILED |

## 6. Related Examples

`../../../{go,java,javascript,csharp}/security/sasl-plain-scram`,
`../../../{python,ruby}/security/sasl_plain_scram`. Guide: `../../../../docs/guides/security-sasl-tls.md`.

## 7. Gotcha callout

- **TLS / mTLS is doc-only** in this example — set `KAFKA_SECURITY_PROTOCOL=SASL_SSL` and see the guide.
- **Gotcha #2:** the broker `AdvertisedHost` must match the client's SNI/cert host, or the TLS handshake
  fails after bootstrap.
- **Gotcha #8:** joining a consumer group requires **WRITE** on the Group resource, not just READ.

## 8. Auth

This variant *is* the auth example. See `../../../../docs/guides/security-sasl-tls.md` for the full
credential-store and TLS setup.
