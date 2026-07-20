# Configuration Reference

Field-by-field reference for the `CONNECTORS_KAFKA_*` environment variables and their config
fields. This is the reference companion to the task-oriented
[../configuration.md](../configuration.md) page. Grounded in server docs `docs/24-kafka.md`
(env-mapping table + capacity table) and cross-linked to the authoritative
`kubemq-server` `docs/10-configuration-reference.md#kafkaconfig`.

> **The connector is DISABLED by default (gotcha #1).** Unlike the AMQP and MQTT connectors, Kafka
> is compiled into the default server build (since v3.1, the `kafka` build tag was dropped) but the
> listeners stay closed until you set `CONNECTORS_KAFKA_ENABLE=true`.

> **Engine requirement (DE-57).** The Kafka connector runs **only on the `next` storage engine**.
> Set `STORE_ENGINE=next` (or `store.engine: next`); on a fresh store the engine auto-selects `next`
> when Kafka is enabled. Enabling Kafka on a legacy-engine cluster is refused at startup with a
> configuration error.

## Field Table

| Config field | Env var | Default | Meaning |
|---|---|---|---|
| `Enable` | `CONNECTORS_KAFKA_ENABLE` | `false` | Opens the Kafka listeners. **Must be `true`** for any client to connect. |
| `Port` | `CONNECTORS_KAFKA_PORT` | `"9092"` | Kafka wire protocol, plain TCP. |
| `TlsPort` | `CONNECTORS_KAFKA_TLS_PORT` | `"9093"` | Kafka wire protocol over TLS. |
| `AdvertisedHost` | `CONNECTORS_KAFKA_ADVERTISED_HOST` | `""` | The host returned in `Metadata`. **Must be set for external clients** (gotcha #2). |
| `AdvertisedPort` | `CONNECTORS_KAFKA_ADVERTISED_PORT` | `0` | The port returned in `Metadata`; `0` = use the listener port. |
| `MaxConnections` | `CONNECTORS_KAFKA_MAX_CONNECTIONS` | `1000` | Connections/node cap; `0` = unlimited. |
| `MaxMessageBytes` | `CONNECTORS_KAFKA_MAX_MESSAGE_BYTES` | `1048576` | Max record-batch size (1 MiB); oversized → `MESSAGE_TOO_LARGE`. |
| `MaxGroups` | `CONNECTORS_KAFKA_MAX_GROUPS` | `10000` | Consumer-groups/node cap. |
| `ProduceByteRate` | `CONNECTORS_KAFKA_PRODUCE_BYTE_RATE` | `0` | Per-principal produce quota (token bucket); `0` = unlimited. |
| `FetchByteRate` | `CONNECTORS_KAFKA_FETCH_BYTE_RATE` | `0` | Per-principal fetch quota (token bucket); `0` = unlimited. |

> The first seven fields (`Enable` … `MaxMessageBytes`) are verified exact against the env-mapping
> table in `docs/24-kafka.md`. `MaxGroups`, `ProduceByteRate`, and `FetchByteRate` come from
> `docs/10-configuration-reference.md#kafkaconfig` and the capacity table in `docs/24-kafka.md`.

## `AdvertisedHost` — the M-23 footgun (gotcha #2)

`AdvertisedHost` is the single most common misconfiguration:

- When **empty**, the connector advertises the **pod / container hostname** in its `Metadata`
  response. An external client connects to the bootstrap port, receives that unreachable hostname,
  reconnects to it, and **hangs** ("connect-then-hang").
- **Fix:** set `CONNECTORS_KAFKA_ADVERTISED_HOST` to a hostname/IP the client can actually reach.
- **TLS:** the certificate SAN must cover `AdvertisedHost` (see [below](#tls--mtls)).
- This is a **single-endpoint** model (D6) — there is no Kafka multi-listener
  `advertised.listeners`; one advertised host/port is returned to every client.

## Capacity Limits

| Limit | Value | Knob |
|---|---|---|
| Topics / node | Unbounded (auth-gated) | — |
| Partitions / topic | **256** hard cap | — |
| Connections / node | 1000 (`0` = unlimited) | `CONNECTORS_KAFKA_MAX_CONNECTIONS` |
| Consumer groups / node | 10000 | `CONNECTORS_KAFKA_MAX_GROUPS` |
| Max message bytes | 1 MiB | `CONNECTORS_KAFKA_MAX_MESSAGE_BYTES` |
| Parked fetch long-polls | 1024 | — |
| Produce quota | token bucket (`0` = unlimited) | `CONNECTORS_KAFKA_PRODUCE_BYTE_RATE` |
| Fetch quota | token bucket (`0` = unlimited) | `CONNECTORS_KAFKA_FETCH_BYTE_RATE` |

## TLS / mTLS

TLS termination is on port `9093` (`CONNECTORS_KAFKA_TLS_PORT`). Clients set
`security.protocol=SSL` and point at the TLS port.

- The certificate **SAN must cover `AdvertisedHost`** — otherwise the client's post-`Metadata`
  reconnect fails hostname verification.
- **mTLS principal** = the CN of the verified client-certificate chain; that principal is what the
  Casbin ACL layer authorizes.
- TLS/mTLS is **doc-only** in this repo (no separate runnable example) because it requires broker
  certs not present on a stock dev broker. See
  [../guides/security-sasl-tls.md](../guides/security-sasl-tls.md).

## Minimal Enable Example

```bash
# Open the plain-TCP listener on :9092 and advertise a reachable host.
export CONNECTORS_KAFKA_ENABLE=true
export CONNECTORS_KAFKA_ADVERTISED_HOST=kubemq.internal   # reachable by clients
# optional: raise the message cap to 4 MiB
export CONNECTORS_KAFKA_MAX_MESSAGE_BYTES=4194304
```

Clients then set `bootstrap.servers=kubemq.internal:9092` (the examples read this as
`KUBEMQ_KAFKA_BOOTSTRAP`).

## See Also

- [../getting-started.md](../getting-started.md) — enable + `AdvertisedHost` + smoke test.
- [../configuration.md](../configuration.md) — the task-oriented configuration page.
- [capabilities.md](capabilities.md) — the ✅/🟡/⛔/🔴 surface these knobs gate.
- [../guides/security-sasl-tls.md](../guides/security-sasl-tls.md) — SASL, TLS, mTLS, Casbin ACL.

## Source

Server docs `docs/24-kafka.md` (env-mapping table + capacity table) and
`docs/10-configuration-reference.md#kafkaconfig` (the authoritative `KafkaConfig` schema).
Connector: `connectors/kafka/`.
