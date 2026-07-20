"""KubeMQ Kafka — consumer-groups/commit-and-lag: OffsetCommit/Fetch + resume + lag.

A first consumer reads part of the log and commits its offset synchronously
(OffsetCommit). A second consumer in the same group starts cold, fetches the
committed offset (OffsetFetch) and RESUMES exactly where the first left off — no
re-reading, no gap. We also compute consumer-group LAG client-side as
(high-watermark - committed-offset) and assert it matches the un-consumed tail.

Note: the server also exposes lag as the metric
kubemq_kafka_consumer_group_lag{group,topic,partition}; that read is server-side.
This example computes the same quantity from the client for a self-contained proof.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka import KafkaError, Producer, TopicPartition  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

from _shared import banner, bootstrap, check, make_admin, make_consumer, tname  # noqa: E402

TOPIC = tname("commit", family="cgroups")
GROUP = "commit-lag-grp"
TOTAL = 20
CONSUME_FIRST = 8


def ensure_topic(admin: AdminClient) -> None:
    for _, fut in admin.create_topics(
        [NewTopic(TOPIC, num_partitions=1, replication_factor=1)]
    ).items():
        try:
            fut.result()
        except Exception:  # noqa: BLE001
            pass
    deadline = time.time() + 10
    while time.time() < deadline:
        tmd = admin.list_topics(topic=TOPIC, timeout=5.0).topics.get(TOPIC)
        if tmd is not None and tmd.error is None and tmd.partitions:
            return
        time.sleep(0.3)


def produce(n: int) -> None:
    producer = Producer({"bootstrap.servers": bootstrap(), "acks": "all"})
    for i in range(n):
        producer.produce(TOPIC, value=f"rec-{i}".encode())
    producer.flush(20)


def consume_and_commit() -> int:
    """Read CONSUME_FIRST records, commit sync, return the committed offset."""
    consumer = make_consumer(GROUP)
    consumer.subscribe([TOPIC])
    read = 0
    last_offset = -1
    deadline = time.time() + 30
    while read < CONSUME_FIRST and time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise SystemExit(f"consume error: {msg.error()}")
        last_offset = msg.offset()
        read += 1
    check(read == CONSUME_FIRST, f"first consumer read {CONSUME_FIRST} records")
    # Commit the NEXT offset to read (last consumed + 1) synchronously.
    committed = last_offset + 1
    consumer.commit(offsets=[TopicPartition(TOPIC, 0, committed)], asynchronous=False)
    consumer.close()
    print(f"OffsetCommit -> group '{GROUP}' committed offset {committed}")
    return committed


def resume_and_lag(committed: int) -> None:
    consumer = make_consumer(GROUP)

    # OffsetFetch: what does the group have committed for partition 0?
    fetched = consumer.committed([TopicPartition(TOPIC, 0)], timeout=10.0)[0]
    check(fetched.offset == committed, f"OffsetFetch returns the committed offset ({committed})")

    # Lag = high watermark - committed.
    low, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, 0), timeout=10.0)
    lag = high - committed
    check(lag == TOTAL - committed, f"lag == HWM({high}) - committed({committed}) == {lag}")

    # Resume: the group continues from the committed offset, reading only the tail.
    consumer.subscribe([TOPIC])
    tail: list[str] = []
    deadline = time.time() + 30
    while len(tail) < lag and time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise SystemExit(f"consume error: {msg.error()}")
        tail.append(msg.value().decode())
    consumer.close()

    expected_tail = [f"rec-{i}" for i in range(committed, TOTAL)]
    check(
        tail == expected_tail,
        f"second consumer RESUMED at offset {committed} (read the {lag}-record tail)",
    )


def main() -> None:
    banner(f"consumer-groups/commit-and-lag — topic '{TOPIC}'")
    ensure_topic(make_admin())
    produce(TOTAL)

    committed = consume_and_commit()
    resume_and_lag(committed)

    print("\nRound-trip complete.")


if __name__ == "__main__":
    main()
