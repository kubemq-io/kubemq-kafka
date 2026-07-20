"""KubeMQ Kafka — produce/idempotent: enable.idempotence + no duplicates.

Enabling the idempotent producer makes librdkafka call InitProducerId to obtain a
producer id (PID) and a starting epoch, then tag every RecordBatch with
(PID, epoch, base-sequence). The broker de-duplicates per (PID, partition) on the
sequence number, so librdkafka's own internal retries can NEVER create a duplicate.

enable.idempotence=True auto-forces acks=all, retries>0, and max.in.flight<=5.
This program produces a fixed batch of uniquely-numbered records, flushes, reads
them all back and asserts the read-back set equals the produced set with no
duplicates (produced count == distinct read-back count == total read-back count).

Mirrors the connector idempotent Produce path (InitProducerId + per-(PID,partition)
sequence dedup) in connectors/kafka/.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka import KafkaError, Producer, TopicPartition  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

from _shared import banner, bootstrap, check, make_admin, make_consumer, tname  # noqa: E402

TOPIC = tname("idem", family="produce")
RECORD_COUNT = 50


def ensure_topic(admin: AdminClient) -> None:
    for topic, fut in admin.create_topics(
        [NewTopic(TOPIC, num_partitions=1, replication_factor=1)]
    ).items():
        try:
            fut.result()
            print(f"CreateTopics -> '{topic}' created")
        except Exception as exc:  # noqa: BLE001 — already-exists is fine on a re-run
            print(f"CreateTopics -> '{topic}' ({type(exc).__name__})")
    deadline = time.time() + 10
    while time.time() < deadline:
        tmd = admin.list_topics(topic=TOPIC, timeout=5.0).topics.get(TOPIC)
        if tmd is not None and tmd.error is None and tmd.partitions:
            return
        time.sleep(0.3)


def produce_idempotent() -> int:
    """Produce RECORD_COUNT records through the idempotent producer; return delivered count."""
    delivered = {"ok": 0}

    def on_delivery(err, msg):
        if err is not None:
            print(f"  [FAIL] delivery error: {err}", file=sys.stderr)
            raise SystemExit(1)
        delivered["ok"] += 1

    # enable.idempotence forces InitProducerId + acks=all + bounded in-flight.
    producer = Producer({"bootstrap.servers": bootstrap(), "enable.idempotence": True})
    for i in range(RECORD_COUNT):
        producer.produce(TOPIC, key=b"k", value=f"msg-{i}".encode(), callback=on_delivery)
        # Pump delivery callbacks periodically so the queue never overflows.
        producer.poll(0)
    producer.flush(20)
    check(
        delivered["ok"] == RECORD_COUNT,
        f"all {RECORD_COUNT} records delivered (acks=all, idempotent)",
    )
    return delivered["ok"]


def read_back() -> list[str]:
    consumer = make_consumer("idempotent-reader")
    consumer.assign([TopicPartition(TOPIC, 0, 0)])
    seen: list[str] = []
    idle = 0
    while idle < 5 and len(seen) < RECORD_COUNT * 2:
        msg = consumer.poll(1.0)
        if msg is None:
            idle += 1
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                idle += 1
                continue
            raise SystemExit(f"consume error: {msg.error()}")
        seen.append(msg.value().decode())
        idle = 0
    consumer.close()
    return seen


def main() -> None:
    banner(f"produce/idempotent — topic '{TOPIC}'")
    ensure_topic(make_admin())

    produced = produce_idempotent()
    seen = read_back()

    distinct = set(seen)
    expected = {f"msg-{i}" for i in range(RECORD_COUNT)}

    check(len(seen) == produced, f"read-back count == produced count ({len(seen)} == {produced})")
    check(len(distinct) == len(seen), f"NO duplicates (all {len(seen)} read-back records distinct)")
    check(distinct == expected, "read-back set exactly matches the produced set")

    print("\nNote: per-(PID,partition) sequence dedup guarantees at-most-once storage")
    print("under librdkafka's internal retries — a retried batch is dropped by the broker.")
    print("Round-trip complete.")


if __name__ == "__main__":
    main()
