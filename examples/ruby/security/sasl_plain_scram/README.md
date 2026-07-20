# Ruby — Kafka: SASL/PLAIN & SCRAM

Authenticated produce+consume over `security.protocol=sasl_plaintext` with SASL/PLAIN or
SCRAM-SHA-256/512; wrong credentials fail with an auth/authorization error. TLS/mTLS on :9093 is
doc-only.

## Prerequisites
- Same Ruby + `rdkafka` toolchain as the other variants.
- **A broker with a Kafka credential store** (spec §4.7). Without one this variant self-skips.
- Credentials from the environment (never hard-coded):
  `KAFKA_SASL_MECHANISM` (PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512), `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`.

## How to Run
```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
export KAFKA_SASL_MECHANISM="PLAIN"          # or SCRAM-SHA-256 / SCRAM-SHA-512
export KAFKA_SASL_USERNAME="app"
export KAFKA_SASL_PASSWORD="s3cret"
bundle exec ruby security/sasl_plain_scram/main.rb
```

## Expected Output
With creds: banner, `Produce(auth) -> ok`, `Consume(auth) -> "authenticated-hello"`,
`Produce(bad) -> rejected: <code> (auth/authorization failure)`, `PASS`.
Without creds: a `SKIP:` block explaining setup, then exit 0.

## What's Happening
librdkafka performs the SASL handshake (SaslHandshake/SaslAuthenticate) before produce/consume. The
negative test uses a wrong password and expects an authentication failure.

> **TLS/mTLS is doc-only.** `security.protocol=ssl` / `sasl_ssl` on :9093 needs broker certificates
> this repo does not ship — see `../../../../docs/guides/security-sasl-tls.md`.
> **N/A note:** some rdkafka-ruby lines use `"sasl.mechanisms"` (plural); adjust if your gem needs it.

## Kafka specifics
| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| SaslHandshake(17), SaslAuthenticate(36), Produce(0), Fetch(1) | acks=all | 1 topic / 1 partition | ephemeral, earliest | offset = STAN Sequence | none | CRC32 | wrong creds → *_AUTHORIZATION_FAILED |

## Related Examples
- `../../../{go,java,javascript,csharp,rust}/security/sasl-plain-scram`, `../../../python/security/sasl_plain_scram`.
- Guide: `../../../../docs/guides/security-sasl-tls.md`.

## Gotcha
**`sasl_plaintext` sends credentials in the clear** — use it only on a trusted network; in
production use `sasl_ssl` on `:9093` (doc-only here, needs broker certs). Wrong credentials surface
as a SASL authentication error and an unauthorized topic as `*_AUTHORIZATION_FAILED` — assert on
`Rdkafka::RdkafkaError#code`, not the message string. The librdkafka key is `sasl.mechanism`
(singular).

## Auth
This variant IS the auth example. For the full matrix see `../../../../docs/guides/security-sasl-tls.md`.
