"""KubeMQ Kafka — produce/basic-acks: acks 0/1/all round-trip + oversized reject.

Creates a topic, produces one record at each of acks=0, acks=1, acks=all
(collecting per-record delivery reports / assigned offsets), reads them all back
from partition 0, then proves an oversized record (> the connector's 1 MiB
MaxMessageBytes) is rejected with MESSAGE_TOO_LARGE.

Mirrors the connector Produce path (RecordBatch v2; acks 0/1/all;
oversized -> MESSAGE_TOO_LARGE) in connectors/kafka/ (produce path / produce_test.go).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka import KafkaError, Producer, TopicPartition  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

from _shared import banner, bootstrap, check, make_admin, make_consumer, tname  # noqa: E402

TOPIC = tname("acks", family="produce")
# CONNECTORS_KAFKA_MAX_MESSAGE_BYTES default (§2.1 / §2.7) = 1 MiB.
CONNECTOR_MAX_BYTES = 1024 * 1024


def ensure_topic(admin: AdminClient) -> None:
    """Create the single-partition topic (idempotent across re-runs)."""
    futures = admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])
    for topic, fut in futures.items():
        try:
            fut.result()
            print(f"CreateTopics -> '{topic}' created")
        except Exception as exc:  # noqa: BLE001 — already-exists is fine on a re-run
            print(f"CreateTopics -> '{topic}' ({type(exc).__name__})")
    wait_for_topic(admin, TOPIC)


def wait_for_topic(admin: AdminClient, topic: str, timeout: float = 10.0) -> None:
    """Block until the topic appears in cluster metadata (create is async)."""
    import time

    deadline = time.time() + timeout
    while time.time() < deadline:
        md = admin.list_topics(topic=topic, timeout=5.0)
        tmd = md.topics.get(topic)
        if tmd is not None and tmd.error is None and tmd.partitions:
            return
        time.sleep(0.3)


def produce_with_acks(acks: str) -> None:
    """Produce one record at the given acks level; assert its delivery report fires."""
    delivered: dict = {}

    def on_delivery(err, msg):
        if err is not None:
            print(f"  [FAIL] delivery error at acks={acks}: {err}", file=sys.stderr)
            raise SystemExit(1)
        delivered["offset"] = msg.offset()  # -1 for acks=0 (no broker ack -> no offset)
        delivered["partition"] = msg.partition()

    producer = Producer({"bootstrap.servers": bootstrap(), "acks": acks})
    producer.produce(TOPIC, value=f"acks={acks}".encode(), callback=on_delivery)
    producer.flush(15)
    check(
        "offset" in delivered,
        f"acks={acks} record delivered (partition {delivered.get('partition')}, "
        f"offset {delivered.get('offset')})",
    )


def read_back(expected: set[str]) -> None:
    """Assign partition 0 from offset 0 and prove every produced body is readable."""
    consumer = make_consumer("basic-acks-reader")
    consumer.assign([TopicPartition(TOPIC, 0, 0)])
    seen: set[str] = set()
    for _ in range(100):  # bounded poll loop
        if expected.issubset(seen):
            break
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise SystemExit(f"consume error: {msg.error()}")
        seen.add(msg.value().decode())
    consumer.close()
    check(expected.issubset(seen), f"read back all acks records: {sorted(seen)}")


def oversized_rejected() -> None:
    """A > MaxMessageBytes record must be rejected with MESSAGE_TOO_LARGE."""
    rejected: dict = {}

    def on_delivery(err, msg):
        rejected["err"] = err

    # Bump the CLIENT message.max.bytes ABOVE the connector limit so the oversized
    # record leaves the client and reaches the broker (which returns
    # MESSAGE_TOO_LARGE) instead of librdkafka rejecting it locally first.
    # compression.type=none keeps the on-wire size deterministic (a compressible
    # all-'x' payload could otherwise shrink below the limit and slip through).
    producer = Producer(
        {
            "bootstrap.servers": bootstrap(),
            "acks": "all",
            "message.max.bytes": CONNECTOR_MAX_BYTES * 2,
            "compression.type": "none",
        }
    )
    # 1.5 MiB: ABOVE the connector's 1 MiB MaxMessageBytes but BELOW both the
    # client's raised message.max.bytes (2 MiB) and the ~2 MiB frame cap, so the
    # record leaves the client and the BROKER is the one that rejects it.
    payload = b"x" * (CONNECTOR_MAX_BYTES + 512 * 1024)  # 1_572_864 bytes
    producer.produce(TOPIC, value=payload, callback=on_delivery)
    producer.flush(15)
    err = rejected.get("err")
    # The rejection now originates at the BROKER: librdkafka surfaces
    # MSG_SIZE_TOO_LARGE (code 10, str="Broker: Message size too large").
    rejected_ok = err is not None and (
        err.code() == KafkaError.MSG_SIZE_TOO_LARGE
        or "SIZE_TOO_LARGE" in err.name().upper()
        or "TOO LARGE" in err.str().upper()
    )
    check(rejected_ok, f"oversized record rejected by broker (MESSAGE_TOO_LARGE): {err}")


def main() -> None:
    banner(f"produce/basic-acks — topic '{TOPIC}'")
    ensure_topic(make_admin())

    for acks in ("0", "1", "all"):
        produce_with_acks(acks)

    read_back({"acks=0", "acks=1", "acks=all"})
    oversized_rejected()

    print("\nNote (gotcha #3): on a multi-node connector, acks=0 to a follower can")
    print("silently drop — use acks>=1 for durability.")
    print("Note (gotcha #1): the connector must be enabled with CONNECTORS_KAFKA_ENABLE=true.")
    print("Round-trip complete.")


if __name__ == "__main__":
    main()
