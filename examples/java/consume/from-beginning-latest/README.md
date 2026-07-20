# java — Kafka: Consume From Beginning / Latest

Two consumers in fresh groups — one with `auto.offset.reset=earliest`, one with
`latest` — prove the two start positions: earliest replays the whole log, latest
sees only records produced after it joined.

## Prerequisites

- JDK 21+ and Maven 3.9+.
- `org.apache.kafka:kafka-clients 3.9.0` (pinned in `../../pom.xml`).
- A running KubeMQ Kafka connector reachable at `KUBEMQ_KAFKA_BOOTSTRAP`
  (default `localhost:9092`). **Connector DISABLED by default — start with
  `CONNECTORS_KAFKA_ENABLE=true`** (gotcha #1); set `CONNECTORS_KAFKA_ADVERTISED_HOST`
  for remote clients (gotcha #2).

## How to Run

From `examples/java/`:

```bash
export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
mvn -q compile
mvn -q exec:exec -Dexec.mainClass=io.kubemq.examples.kafka.consume.frombeginninglatest.Main
```

## Expected Output

```
bootstrap.servers = localhost:9092
CreateTopics 'kafka-ex-consume-reset-java' (1 partition)
Produced 3 'pre' records
[earliest] saw 3 pre records
Produced 4 'post' records after latest joined
[latest] saw values: [post-0, post-1, post-2, post-3]
OK: earliest replays history, latest sees only new records
```

The `earliest` consumer sees the 3 pre-existing records; the `latest` consumer,
which committed its position at the log end before any post-records existed, sees
only the 4 records produced afterward — never the 3 earlier ones.

## What's Happening

The program seeds 3 "pre" records, then subscribes a fresh-group consumer with
`auto.offset.reset=earliest` and asserts it replays all 3. It then subscribes a
second fresh-group consumer with `auto.offset.reset=latest`, lets it establish its
position at the current log end, produces 4 "post" records, and asserts the latest
consumer sees exactly those 4 — proving the reset policy governs where a **new**
group with no committed offset begins. Fresh (unique) group ids per consumer are
essential: `auto.offset.reset` only applies when there is no committed offset.

The Kafka wire flow is `Metadata → FindCoordinator → JoinGroup/SyncGroup → Fetch
(long-poll)`, with the effective start offset chosen by `auto.offset.reset`. This
mirrors connector behavior in `connectors/kafka/` (`fetch_test.go`).

## Kafka specifics

| API keys | acks / isolation | topic / partitions | consumer group | offset semantics | compression | partitioner | special handling |
|---|---|---|---|---|---|---|---|
| Fetch(1), ListOffsets(2), FindCoordinator(10), JoinGroup(11), SyncGroup(14) | acks=all; read_uncommitted | 1 partition | two fresh groups (one earliest, one latest) | `auto.offset.reset` earliest→log-start, latest→HWM | none | murmur2 | fresh group per consumer so the reset actually applies |

## Related Examples

- Same variant in the other 6 languages: [`../../../go/consume/from-beginning-latest`](../../../go/consume/from-beginning-latest),
  [`../../../python/consume/from_beginning_latest`](../../../python/consume/from_beginning_latest),
  [`../../../javascript/consume/from-beginning-latest`](../../../javascript/consume/from-beginning-latest),
  [`../../../csharp/consume/from-beginning-latest`](../../../csharp/consume/from-beginning-latest),
  [`../../../ruby/consume/from_beginning_latest`](../../../ruby/consume/from_beginning_latest),
  [`../../../rust/consume/from-beginning-latest`](../../../rust/consume/from-beginning-latest).
- Docs: [`../../../../docs/guides/consuming-and-groups.md`](../../../../docs/guides/consuming-and-groups.md).
- Next: [`../seek-offsets-timestamps`](../seek-offsets-timestamps).

> **Note — `auto.offset.reset` only fires without a committed offset.** Reuse of a
> group id means "resume from the committed offset", not "apply the reset". This
> example uses a fresh group id per consumer so each reset policy is observable.

> **Auth.** This example uses the connector's no-auth default posture
> (SHARED-CONVENTIONS §4.3). See
> [`../../../../docs/guides/security-sasl-tls.md`](../../../../docs/guides/security-sasl-tls.md)
> for SASL/PLAIN + SCRAM and TLS/mTLS.
