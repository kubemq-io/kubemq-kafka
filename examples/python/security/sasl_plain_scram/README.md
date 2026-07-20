# Python — Kafka: SASL PLAIN / SCRAM

Authenticate produce/consume over SASL/PLAIN and SASL/SCRAM-SHA-256/512, and prove an unauthorized
action is denied with `*_AUTHORIZATION_FAILED` — against a SASL-enabled KubeMQ Kafka connector using
native `confluent-kafka`. TLS on `:9093` and mTLS principal derivation are **documented, not run**
(a stock dev broker has no certs).

## Prerequisites

- Python 3.9+ and [`uv`](https://docs.astral.sh/uv/).
- Kafka client: `confluent-kafka` (installed via `uv sync` from `../../pyproject.toml`).
- A running **KubeMQ Kafka connector** reachable at `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`),
  started with **`CONNECTORS_KAFKA_ENABLE=true`** (the connector is disabled by default — gotcha #1),
  **and configured with a Kafka credential store** (SASL/PLAIN + SCRAM users). Unlike the other 13
  variants, this one needs credentials; if the broker has no credential store, the program explains the
  skip.
- `AdvertisedHost` set on the connector for any non-loopback client (gotcha #2).

## How to Run

    cd examples/python
    uv sync
    export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
    export KUBEMQ_KAFKA_SASL_USERNAME="alice"
    export KUBEMQ_KAFKA_SASL_PASSWORD="alice-secret"
    uv run python security/sasl_plain_scram/main.py

## Expected Output

> The Python suite namespaces its topic with `KUBEMQ_KAFKA_NAME_PREFIX` (default `py`):
> `kafka-py-security-sasl`.

    === security/sasl-plain-scram — topic 'kafka-py-security-sasl' ===
      bootstrap : localhost:9092
      client    : confluent-kafka (librdkafka; CRC32 default partitioner)
      note      : connector must be started with CONNECTORS_KAFKA_ENABLE=true

      [OK] SASL/PLAIN authenticated produce+consume
      [OK] SASL/SCRAM-SHA-256 authenticated produce+consume
      [OK] SASL/SCRAM-SHA-512 authenticated produce+consume
      [OK] unauthorized action denied (TOPIC_AUTHORIZATION_FAILED)

    Round-trip complete.

## What's Happening

- For each mechanism `PLAIN` / `SCRAM-SHA-256` / `SCRAM-SHA-512`, a producer and consumer are built with
  `security.protocol=SASL_PLAINTEXT`, `sasl.mechanism=<m>`, and the configured username/password; an
  authenticated round-trip succeeds.
- A produce/consume by an unauthorized principal is **denied** — the connector returns
  `TOPIC_AUTHORIZATION_FAILED` / `GROUP_AUTHORIZATION_FAILED`, surfaced as a `KafkaException` / delivery
  error.
- **TLS (`security.protocol=SSL`, `:9093`) and mTLS principal derivation are doc-only** — a stock dev
  broker lacks certs, so there is no runnable TLS path here; see the security guide.
- Mirrors connector behavior in `connectors/kafka/` (SASL handshake + authorization checks).

## Kafka specifics

| API keys | acks/isolation | topic/partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| SaslHandshake(17), SaslAuthenticate(36), Produce(0), Fetch(1), Metadata(3) | acks=all producer; read_uncommitted | 1 partition | `sasl-reader` | offset = STAN Sequence | none | CRC32 (librdkafka default) | denied → `TOPIC_AUTHORIZATION_FAILED` / `GROUP_AUTHORIZATION_FAILED`; TLS/mTLS doc-only |

## Related Examples

- Same variant in the other languages: [`../../../go/security/sasl-plain-scram/`](../../../go/security/sasl-plain-scram/),
  [`../../../java/security/sasl-plain-scram/`](../../../java/security/sasl-plain-scram/),
  [`../../../javascript/security/sasl-plain-scram/`](../../../javascript/security/sasl-plain-scram/),
  [`../../../csharp/security/sasl-plain-scram/`](../../../csharp/security/sasl-plain-scram/),
  [`../../../ruby/security/sasl_plain_scram/`](../../../ruby/security/sasl_plain_scram/),
  [`../../../rust/security/sasl-plain-scram/`](../../../rust/security/sasl-plain-scram/).
- [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md)

> **Gotcha #2 — AdvertisedHost with SASL.** A SASL client that connects but then hangs is almost always
> an unset `CONNECTORS_KAFKA_ADVERTISED_HOST` (empty → pod hostname). Set it for any non-loopback
> client. A denied action returns `*_AUTHORIZATION_FAILED`, not a connection error.

> **Auth.** This variant is the SASL/SCRAM reference. TLS on `:9093` and mTLS principal =
> verified-cert CN are documented in [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md);
> they require broker certs not present on a stock dev broker.
