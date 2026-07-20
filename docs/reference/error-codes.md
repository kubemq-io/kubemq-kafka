# Error Codes

The connector emits **genuine Kafka error codes** over the Kafka wire protocol, so any standard
Kafka client (franz-go, `kafka-clients`, librdkafka, `kafkajs`, sarama, segmentio) surfaces them as
its normal typed errors. Grounded in `connectors/kafka/` and server docs `docs/24-kafka.md` /
`docs/migration/kafka.md`.

## Error Code Table

| Kafka error (code) | Trigger | Notes |
|---|---|---|
| `MESSAGE_TOO_LARGE` | `Produce` record batch exceeds `CONNECTORS_KAFKA_MAX_MESSAGE_BYTES` (default 1 MiB) | Raise the limit or split the batch. |
| `INVALID_PARTITIONS` | `CreatePartitions` with a count that is **not strictly greater**, or > 256 | Partitions are increase-only, capped at 256. |
| `INVALID_TOPIC_EXCEPTION` (17) | Topic name contains the reserved `~` separator (M8 multi-partition) | Use `~`-free names (gotcha #6). |
| `INVALID_TRANSACTIONAL_ID` → `INVALID_REQUEST` (42) | `transactional.id` contains `/` | `/` is rejected in a `transactional.id` (gotcha #7). |
| `INVALID_PRODUCER_EPOCH` (47) | A fenced producer (older `(PID, epoch)`) issues a transactional op | Producer fencing. |
| `PRODUCER_FENCED` (90) | A newer producer instance has fenced this one | Producer fencing. |
| `TOPIC_AUTHORIZATION_FAILED` | Casbin ACL denies the topic op | See [../guides/security-sasl-tls.md](../guides/security-sasl-tls.md). |
| `GROUP_AUTHORIZATION_FAILED` | Casbin ACL denies the group op (incl. txn offset-commit without Group WRITE, gotcha #8) | Stricter than real Kafka for txn offset-commit (D141). |
| `UNSTABLE_OFFSET_COMMIT` (88) | `OffsetFetch RequireStable` while an offset is below the LSO (open transaction) | Retry once the transaction commits/aborts. |

## Common Triggers by Scenario

| Scenario | Result |
|---|---|
| `Produce` a batch over 1 MiB | `MESSAGE_TOO_LARGE` |
| `CreatePartitions` with the same count | `INVALID_PARTITIONS` |
| `CreatePartitions` decreasing the count | `INVALID_PARTITIONS` |
| `CreatePartitions` above 256 | `INVALID_PARTITIONS` |
| `CreateTopics` / `Produce` a topic name containing `~` (M8) | `INVALID_TOPIC_EXCEPTION(17)` |
| `InitProducerId` with a `transactional.id` containing `/` | `INVALID_TRANSACTIONAL_ID` → `INVALID_REQUEST(42)` |
| A zombie / fenced transactional producer | `INVALID_PRODUCER_EPOCH(47)` or `PRODUCER_FENCED(90)` |
| Produce/consume denied by ACL | `TOPIC_AUTHORIZATION_FAILED` / `GROUP_AUTHORIZATION_FAILED` |
| `TxnOffsetCommit` without Group **WRITE** | `GROUP_AUTHORIZATION_FAILED` (gotcha #8) |
| `OffsetFetch RequireStable` under an open transaction | `UNSTABLE_OFFSET_COMMIT(88)` |
| `Fetch` on an empty topic (long-poll) | _none — parks up to the long-poll deadline, then returns empty_ |
| `read_committed` fetch spanning an aborted txn | _none — aborted records filtered client-side via `AbortedTransactions`_ |
| `DescribeTopicPartitions` (key 75) | _none — falls back to `Metadata`_ |
| An ACL-management op (keys 29/30/31) with security disabled | honest empty view / `SECURITY_DISABLED` |

## Reserved `kafka.*` Namespace (native clients)

This one is **not** a Kafka wire-protocol code — it is a KubeMQ **native** error returned to a
native KubeMQ client (Events, Events-Store, or Queue; gRPC or REST), not to a Kafka client. The
`kafka.*` (and `_KAFKA_*`) Events-Store channel namespace is **reserved for the connector's internal
use**:

| KubeMQ error (code) | Trigger | Notes |
|---|---|---|
| `Error 443` — *channel is reserved for internal connector use* | A native gRPC/REST KubeMQ client subscribes to, reads, or writes a `kafka.*` (or `_KAFKA_*`) channel | Enforced structurally in `services/array/reserved_channel.go` (`checkReservedChannelRead` / `checkReservedChannelWrite`). **Fails safe** and runs **before** authorization; only first-party connector code holds the internal-writer marker. There is **no** native ↔ Kafka bridge. See [../concepts/interop-with-native.md](../concepts/interop-with-native.md). |

## Notes on the Stricter-than-Kafka Cases

- **Group WRITE for txn offset-commit (gotcha #8, D141).** Real Kafka allows `TxnOffsetCommit` with
  group READ; this connector requires group **WRITE**. Grant the transactional principal Group WRITE.
  See [../guides/transactions-eos.md](../guides/transactions-eos.md).
- **`~` and `/` name rules (gotchas #6, #7).** `~` is the partition-channel separator (reserved in
  topic names once M8 is in play); `/` is rejected in a `transactional.id`. Both surface as the
  errors above rather than silently mangling the name.
- **KIP-890 V1 ceiling.** The same-epoch zombie-produce residual (see
  [capabilities.md](capabilities.md)) does **not** raise a distinct error — it is the upstream-shared
  V1 transactional soundness ceiling, not a failure. Every EOS artifact cites it.

## See Also

- [capabilities.md](capabilities.md) — the ✅/🟡/⛔/🔴 surface each error attaches to.
- [channel-mapping.md](channel-mapping.md) — the `~` partition grammar behind `INVALID_TOPIC_EXCEPTION`.
- [../guides/security-sasl-tls.md](../guides/security-sasl-tls.md) — the ACL / authorization-failure path.
- [../guides/transactions-eos.md](../guides/transactions-eos.md) — the transactional / fencing errors.

## Source

`connectors/kafka/` (produce, partitions, txn, offset, and ACL handlers). Verified against server
docs `docs/24-kafka.md` and `docs/migration/kafka.md`. Canonical tests include `txn_rpcs_test.go`,
`groupoffsets_test.go`.
