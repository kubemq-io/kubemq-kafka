# C# — Kafka: SASL/PLAIN + SCRAM

Authenticate with **SASL/PLAIN** (or **SCRAM-SHA-256 / SCRAM-SHA-512**), then
produce + consume a record. Wrong credentials or a denied topic surface
`*_AUTHORIZATION_FAILED`. **TLS / mTLS are documented here but doc-only** — the
runnable program covers the SASL mechanisms.

## Prerequisites

- .NET SDK **8.0**
- **Confluent.Kafka 2.6.0** (pinned in `examples/csharp/Directory.Packages.props`).
- A KubeMQ Kafka connector **configured with a Kafka credential store** (SASL
  enabled) at `KUBEMQ_KAFKA_BOOTSTRAP` — **start with `CONNECTORS_KAFKA_ENABLE=true`**
  (gotcha #1) and **`CONNECTORS_KAFKA_ADVERTISED_HOST` set** (gotcha #2, or the
  client hangs before the SASL handshake finishes).

## How to Run

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
export KAFKA_SASL_MECHANISM="PLAIN"          # or SCRAM-SHA-256 / SCRAM-SHA-512
export KAFKA_SASL_USERNAME="alice"
export KAFKA_SASL_PASSWORD="alice-secret"
dotnet run --project security/sasl-plain-scram
```

Without `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` the program explains it needs a
SASL-enabled broker and exits **0** (nothing to assert) rather than falsely failing.

## Expected Output

```
[*] Authenticating with SASL/PLAIN as 'alice' (SecurityProtocol=SaslPlaintext)
[*] Created topic 'kafka-ex-security-sasl-plain-scram' (authenticated)
[x] authenticated produce → kafka-ex-security-sasl-plain-scram [[0]] @0
[v] authenticated consume → 'secure-hello'
[*] [denied] wrong-credential produce rejected: SaslAuthenticationFailed
[*] Cleaned up topic 'kafka-ex-security-sasl-plain-scram'
[ok] SASL/PLAIN authenticated produce+consume round-trip verified
```

## What's Happening

The producer/consumer/admin configs set `SecurityProtocol=SaslPlaintext`,
`SaslMechanism` (from `KAFKA_SASL_MECHANISM`), and the username/password. An
authenticated produce and consume round-trip a record. A second producer with a
**wrong password** is rejected — an authentication / authorization error.

> **TLS / mTLS (doc-only).** For an encrypted listener point `BootstrapServers` at
> the TLS port (`:9093`) and set `SecurityProtocol=SaslSsl` (SASL over TLS) or
> `SecurityProtocol.Ssl` with `SslCaLocation` + client cert/key for mTLS. This
> repo's runnable program stays on `SaslPlaintext` so it needs no cert material;
> the encrypted variants are described in the security guide.
>
> Denied operations surface `TOPIC_AUTHORIZATION_FAILED` /
> `GROUP_AUTHORIZATION_FAILED`; bad credentials surface an authentication failure.

This mirrors the connector's SASL handshake path in `connectors/kafka/`.

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|----------|----------------|------------------|----------------|------------------|-------------|-------------|------------------|
| SaslHandshake, SaslAuthenticate, Metadata, Produce, Fetch | `acks=All` produce | `kafka-ex-security-sasl-plain-scram` / 1 | `cs-sasl-<uuid>` | offset = STAN `Sequence` | none | CRC32 (librdkafka) | **gotcha #2** (AdvertisedHost); `SaslPlaintext` + `PLAIN`/`SCRAM-SHA-256`/`SCRAM-SHA-512` runnable, TLS/mTLS (`:9093`) doc-only; denied → `*_AUTHORIZATION_FAILED` |

## Related Examples

Same variant in the other languages:

- **Go** — [`../../../go/security/sasl-plain-scram`](../../../go/security/sasl-plain-scram)
- **Python** — [`../../../python/security/sasl_plain_scram`](../../../python/security/sasl_plain_scram)
- **Java** — [`../../../java/security/sasl-plain-scram`](../../../java/security/sasl-plain-scram)
- **JS/TS** — [`../../../javascript/security/sasl-plain-scram`](../../../javascript/security/sasl-plain-scram)
- **Ruby** — [`../../../ruby/security/sasl_plain_scram`](../../../ruby/security/sasl_plain_scram)
- **Rust** — [`../../../rust/security/sasl-plain-scram`](../../../rust/security/sasl-plain-scram)

Docs: [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md)

---

> **Auth:** unlike the other examples, this one **requires** a broker with a Kafka
> credential store. See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md).
