# Configuration

The Kafka connector is configured under `Connectors.Kafka` in the KubeMQ server configuration. It
is **disabled by default** (enabling it opens two new listeners). Fields have environment-variable
overrides of the form `CONNECTORS_KAFKA_*`.

## KafkaConfig fields

Use these values **verbatim**. The first seven are verified against the env-mapping table in the
server docs (`24-kafka.md`); `MaxGroups`, `ProduceByteRate`, and `FetchByteRate` come from the
server configuration reference (`10-configuration-reference.md#kafkaconfig`) and the capacity table
in `24-kafka.md`. See [reference/configuration.md](reference/configuration.md) for the field-by-field
reference.

| Env var | Default | Type | Meaning / validation | Config field |
|---------|---------|------|----------------------|--------------|
| `CONNECTORS_KAFKA_ENABLE` | `false` | bool | **Opt-in** — opens the `:9092` and `:9093` listeners. | `Enable` |
| `CONNECTORS_KAFKA_PORT` | `"9092"` | string | Kafka wire protocol, plain TCP. | `Port` |
| `CONNECTORS_KAFKA_TLS_PORT` | `"9093"` | string | Kafka wire protocol, TLS. | `TlsPort` |
| `CONNECTORS_KAFKA_ADVERTISED_HOST` | `""` | string | Advertised broker host. **MUST be set for external clients** (empty → pod hostname → connect-then-hang). | `AdvertisedHost` |
| `CONNECTORS_KAFKA_ADVERTISED_PORT` | `0` | int | Advertised broker port (0 → the actual listener port). | `AdvertisedPort` |
| `CONNECTORS_KAFKA_MAX_CONNECTIONS` | `1000` | int | Per-node connection cap (0 = unlimited). | `MaxConnections` |
| `CONNECTORS_KAFKA_MAX_MESSAGE_BYTES` | `1048576` | int | Max record-batch size (1 MiB); oversized → `MESSAGE_TOO_LARGE`. | `MaxMessageBytes` |
| `CONNECTORS_KAFKA_MAX_GROUPS` | `10000` | int | Max consumer groups per node. | `MaxGroups` |
| `CONNECTORS_KAFKA_PRODUCE_BYTE_RATE` | `0` | int | Per-principal produce quota, bytes/s (0 = unlimited). | `ProduceByteRate` |
| `CONNECTORS_KAFKA_FETCH_BYTE_RATE` | `0` | int | Per-principal fetch quota, bytes/s (0 = unlimited). | `FetchByteRate` |

## Field notes

- **`Enable`** — **opt-in**; `false` by default. Enabling it opens two new TCP listeners, so it
  stays off until you set `CONNECTORS_KAFKA_ENABLE=true` (**gotcha #1** — unlike AMQP/MQTT, which
  are on by default).
- **`Port` / `TlsPort`** — the plain (`9092`) and TLS (`9093`) Kafka listeners. Both must differ
  from any other enabled listener port.
- **`AdvertisedHost`** — the single advertised broker host. The connector uses a **single-endpoint
  model** — there is no Kafka multi-listener `advertised.listeners`. An empty value advertises the
  pod hostname, so external clients connect and then hang (**gotcha #2**, the "M-23" footgun). For
  TLS, the certificate SAN must cover `AdvertisedHost`.
- **`AdvertisedPort`** — the advertised broker port; `0` advertises the actual listener port. Set
  it when a load balancer or NAT remaps the port the client should dial.
- **`MaxConnections`** — per-node connection cap (default `1000`, `0` = unlimited).
- **`MaxMessageBytes`** — max record-batch size (default `1048576` = 1 MiB). A produce that exceeds
  it is rejected with `MESSAGE_TOO_LARGE`.
- **`MaxGroups`** — the per-node consumer-group cap (default `10000`).
- **`ProduceByteRate` / `FetchByteRate`** — per-principal token-bucket quotas in bytes/second;
  `0` (the default) means unlimited. These back the 🟡 quota surface (API keys 48/49).

## Capacity limits

The connector enforces these operational ceilings (from the capacity table in `24-kafka.md`):

| Limit | Value |
|-------|-------|
| Topics per node | Unbounded (auth-gated) |
| Partitions per topic | **256** (hard cap; increase-only via `CreatePartitions`) |
| Connections per node | `1000` (`MaxConnections`, `0` = unlimited) |
| Consumer groups per node | `10000` (`MaxGroups`) |
| Max record-batch size | `1 MiB` (`MaxMessageBytes`) |
| Parked fetch long-polls | `1024` |
| Produce / fetch quota | `{Produce,Fetch}ByteRate` bytes/s (`0` = unlimited) |

Partition count starts at **1** and is **increase-only** (strictly greater, ≤ 256) via
`CreatePartitions` (API key 37); a same-count, decrease, or `>256` request →
`INVALID_PARTITIONS`. See [guides/admin-and-topics.md](guides/admin-and-topics.md).

## TLS / mTLS

TLS is served on `:9093` (`CONNECTORS_KAFKA_TLS_PORT`). The certificate material, CA, and mode
come from the KubeMQ server's shared `Security` block — there is **no Kafka-specific certificate
option** beyond the TLS port. Point a client at `:9093` with `security.protocol=SSL`; for mTLS, the
verified client-certificate CN becomes the connector principal used for Casbin authorization. The
certificate SAN must cover `AdvertisedHost` (gotcha #2). This is a **doc-only** path in this repo —
there is no runnable TLS example, because it requires broker certificates not present on a stock
dev broker. See [guides/security-sasl-tls.md](guides/security-sasl-tls.md).

## Enable / disable

```bash
# Enable the connector (opens :9092 and :9093):
export CONNECTORS_KAFKA_ENABLE=true
export CONNECTORS_KAFKA_ADVERTISED_HOST=your-broker-host   # required for external clients

# Disable it again (config-only rollback; no data migration):
export CONNECTORS_KAFKA_ENABLE=false
```

## Cross-references

- **Disabled by default** — see [getting-started.md](getting-started.md) (gotcha #1).
- **`AdvertisedHost` required for external clients** — see [getting-started.md](getting-started.md)
  and [reference/configuration.md](reference/configuration.md) (gotcha #2).
- **Full field-by-field reference** — see [reference/configuration.md](reference/configuration.md)
  (cross-links the server `10-configuration-reference.md#kafkaconfig`).
- **Capacity / scope** — see [reference/capabilities.md](reference/capabilities.md).

## Source code

The `KafkaConfig` struct and its `defaultKafkaConfig` in the KubeMQ server configuration; the
connector listener wiring in `connectors/kafka/`. Server docs of record: `docs/24-kafka.md`
(ports, env-mapping table, capacity table) and `docs/10-configuration-reference.md#kafkaconfig`.
