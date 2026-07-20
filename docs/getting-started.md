# Getting Started

Get a message flowing through the KubeMQ Kafka connector in minutes.

## Connection assumption

This repo assumes a **running KubeMQ server with the Kafka connector enabled**. There is **no
docker-compose and no boot-the-server step** here — point your Kafka client at an existing
connector.

> **Engine requirement (prerequisite).** The Kafka connector runs **only on the `next` storage
> engine** (`store.engine: next`). Enabling Kafka on a legacy-engine cluster is refused at startup
> with a configuration error. On a fresh store the engine auto-selects `next`; pin it explicitly
> with `store.engine: next` / `STORE_ENGINE=next`.

Two things are different from most KubeMQ connectors, and both are one-time setup gotchas:

> **Gotcha #1 — the Kafka connector is DISABLED by default.** Unlike the AMQP and MQTT connectors
> (which are enabled by default), the Kafka listeners are closed until you opt in:
>
> ```bash
> export CONNECTORS_KAFKA_ENABLE=true   # opens :9092 (plain) and :9093 (TLS)
> ```

> **Gotcha #2 — `AdvertisedHost` must be set for any non-loopback client.** On `Metadata`, the
> connector advertises a **single** broker endpoint. If `AdvertisedHost` is empty it advertises
> the pod hostname, and an external client will **connect and then hang** (it dials the advertised
> address on the second round-trip — the "M-23" footgun). Set it to a host the client can reach:
>
> ```bash
> export CONNECTORS_KAFKA_ADVERTISED_HOST=your-broker-host   # e.g. the LB / node DNS name
> ```
>
> For a local loopback client, `localhost` works and can be left unset in practice; for anything
> else, set it explicitly. For TLS, the certificate SAN must cover `AdvertisedHost`.

## Set the bootstrap address

Every example reads a single convenience var, **`KUBEMQ_KAFKA_BOOTSTRAP`** (default
`localhost:9092`), and uses it as the Kafka `bootstrap.servers` value:

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"   # default; used as bootstrap.servers
```

It is named `_BOOTSTRAP` (not `_URL`) because Kafka takes a `host:port` bootstrap list, not a URL
scheme — the honest analog of `bootstrap.servers`.

> **Auth banner.** The runnable examples default to **no SASL** (a stock dev broker with auth off).
> Only the `security/sasl-plain-scram` variant needs credentials. The documented production
> contract is SASL/PLAIN + SASL/SCRAM-SHA-256/512, with the mTLS principal derived from the
> verified client-certificate CN. See [guides/security-sasl-tls.md](guides/security-sasl-tls.md).

## Topic → `kafka.<topic>` channel mapping

Every Kafka topic maps onto an internal KubeMQ **Events Store** channel `kafka.<topic>`; a partition
`p>0` maps to `kafka.<topic>~<p>`. This `kafka.*` namespace is **reserved for the connector** —
native KubeMQ clients cannot read or write it (`Error 443`); the channels are reachable only over
the Kafka wire protocol:

| Kafka topic (partition) | KubeMQ channel |
|-------------------------|----------------|
| `orders` (p0) | `kafka.orders` |
| `orders` (p3) | `kafka.orders~3` |
| `events` (p0) | `kafka.events` |

A Kafka offset is the STAN `Sequence` of the record on its channel — durable, restart-stable, and
identical across cluster nodes. See [architecture.md](architecture.md) and
[concepts/topics-partitions-offsets.md](concepts/topics-partitions-offsets.md).

## Smoke test with `kcat`

`kcat` (librdkafka) is the fastest way to prove the connector is up. Produce one record and read
it back:

```bash
# produce a single record to topic "orders" (auto-created on first produce)
echo "hello kafka" | kcat -b "$KUBEMQ_KAFKA_BOOTSTRAP" -t orders -P

# consume from the beginning and exit after one message
kcat -b "$KUBEMQ_KAFKA_BOOTSTRAP" -t orders -C -o beginning -e
# -> hello kafka
```

If `kcat` connects and then hangs instead of returning, re-check **gotcha #2**
(`CONNECTORS_KAFKA_ADVERTISED_HOST`) — the initial connection succeeds but the client cannot reach
the advertised endpoint.

> `kcat` / librdkafka default to the **CRC32** partitioner; franz-go, Java `kafka-clients`, and
> kafkajs (v2+) default to **murmur2**. The same key can therefore land on a different partition
> depending on which client wrote it — **gotcha #4**. It does not affect this single-partition
> smoke test, but it matters as soon as you use keyed records across clients. See
> [concepts/cross-client-partitioning.md](concepts/cross-client-partitioning.md).

## First round-trip in your language

The `produce/basic-acks` and `consume/from-beginning-latest` variants run the full produce →
consume round-trip. The flow is identical in every language:

1. Producer connects to `bootstrap.servers = KUBEMQ_KAFKA_BOOTSTRAP`.
2. `Produce` a record to topic `orders` with `acks=all` → maps to Events-Store channel
   `kafka.orders`, returns the assigned offset.
3. Consumer `Fetch`es from the beginning (`auto.offset.reset=earliest`) → reads the record back.

Run it in your language of choice:

| Language | Kafka client (floor) | Run command |
|----------|----------------------|-------------|
| Go | `github.com/twmb/franz-go` **(server-test-proven, murmur2)** | `cd examples/go && go run ./produce/basic-acks` |
| Python | `confluent-kafka` (librdkafka; uv) | `cd examples/python && uv run python produce/basic_acks/main.py` |
| Java | `org.apache.kafka:kafka-clients` | `cd examples/java && mvn -q compile exec:java -Dexec.mainClass=...` |
| JavaScript / TS | `kafkajs` | `cd examples/javascript && npx tsx produce/basic-acks/index.ts` |
| C# / .NET | `Confluent.Kafka` | `cd examples/csharp && dotnet run --project produce/basic-acks` |
| Ruby | `rdkafka` (rdkafka-ruby) | `cd examples/ruby && ruby produce/basic_acks/main.rb` |
| Rust | `rdkafka` + `tokio` | `cd examples/rust && cargo run -p basic-acks` |

Each example reads `KUBEMQ_KAFKA_BOOTSTRAP`; override it inline if your connector is elsewhere:

```bash
KUBEMQ_KAFKA_BOOTSTRAP="my-broker:9092" go run ./produce/basic-acks
```

> Only franz-go is the connector's own conformance client (proven by `kubemq-server` tests). The
> other six clients are wire-compatible, and these examples + the burn-in harness are their proof.
> Versions are minimum floors as of 2026-07; bump to the latest stable via `/check-deps` at
> implementation.

> **Gotcha #3 — use `acks>=1` on a multi-node broker.** `acks=0` on a follower is silently dropped
> (the produce is not durably acknowledged). The examples default to `acks=all`. See
> [guides/producing.md](guides/producing.md).

## Next steps

- [architecture.md](architecture.md) — the wire-shim model and channel mapping.
- [configuration.md](configuration.md) — the `CONNECTORS_KAFKA_*` env vars and capacity limits.
- [concepts/topics-partitions-offsets.md](concepts/topics-partitions-offsets.md) — the core mapping.
- [guides/producing.md](guides/producing.md) / [guides/consuming-and-groups.md](guides/consuming-and-groups.md) — the produce and consume surfaces end to end.
- [../examples/README.md](../examples/README.md) — all 13 variants in 7 languages.
