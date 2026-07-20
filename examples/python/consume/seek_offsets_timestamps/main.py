"""KubeMQ Kafka — consume/seek-offsets-timestamps: seek by offset + by timestamp.

Two random-access reads over a single partition:

  * seek(offset): assign partition 0, seek to a known offset K, and assert the
    first record read back is exactly record K.
  * offsets_for_times(): ask the broker "what is the first offset at-or-after this
    timestamp?" (Kafka ListOffsets by-timestamp), seek to the returned offset, and
    assert the record read matches the one produced at/after that wall-clock time.

Both are exact, deterministic proofs — a wrong landing offset fails the process.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka import (  # noqa: E402
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

from _shared import banner, bootstrap, check, make_admin, make_consumer, tname  # noqa: E402

TOPIC = tname("seek", family="consume")
COUNT = 10
SEEK_OFFSET = 4


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


def produce_marked() -> int:
    """Produce COUNT records with a small gap; return the ms timestamp of record 6."""
    producer = Producer({"bootstrap.servers": bootstrap(), "acks": "all"})
    mid_ts = 0
    for i in range(COUNT):
        if i == 6:
            mid_ts = int(time.time() * 1000)
        producer.produce(TOPIC, value=f"rec-{i}".encode())
        producer.flush(10)
        time.sleep(0.05)
    return mid_ts


def read_one_at(consumer: Consumer, tp: TopicPartition) -> str:
    # Assign at offset 0, then seek to the target. seek() can transiently return
    # "Local: Erroneous state" before the assignment is live, so retry briefly.
    consumer.assign([TopicPartition(tp.topic, tp.partition, 0)])
    deadline = time.time() + 5
    while True:
        try:
            consumer.seek(tp)
            break
        except KafkaException:
            if time.time() > deadline:
                raise
            consumer.poll(0.2)
    for _ in range(40):
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise SystemExit(f"consume error: {msg.error()}")
        return msg.value().decode()
    raise SystemExit("no record read after seek")


def seek_by_offset() -> None:
    consumer = make_consumer("seek-offset-reader")
    value = read_one_at(consumer, TopicPartition(TOPIC, 0, SEEK_OFFSET))
    consumer.close()
    check(value == f"rec-{SEEK_OFFSET}", f"seek(offset={SEEK_OFFSET}) landed on '{value}'")


def seek_by_timestamp(ts_ms: int) -> None:
    consumer = make_consumer("seek-ts-reader")
    # ListOffsets by-timestamp: first offset with timestamp >= ts_ms.
    resolved = consumer.offsets_for_times([TopicPartition(TOPIC, 0, ts_ms)], timeout=10.0)
    tp = resolved[0]
    check(tp.offset >= 0, f"offsets_for_times resolved ts {ts_ms} -> offset {tp.offset}")
    value = read_one_at(consumer, TopicPartition(TOPIC, 0, tp.offset))
    consumer.close()
    # record 6 was the first produced at-or-after ts_ms.
    check(value == "rec-6", f"timestamp lookup read the first record >= ts: '{value}'")


def main() -> None:
    banner(f"consume/seek-offsets-timestamps — topic '{TOPIC}'")
    ensure_topic(make_admin())

    mid_ts = produce_marked()
    seek_by_offset()
    seek_by_timestamp(mid_ts)

    print("\nRound-trip complete.")


if __name__ == "__main__":
    main()
