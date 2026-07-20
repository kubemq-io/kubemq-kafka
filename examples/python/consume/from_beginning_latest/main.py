"""KubeMQ Kafka — consume/from-beginning-latest: auto.offset.reset earliest vs latest.

A group with NO committed offset resolves its start position from
auto.offset.reset. This program proves both ends of that switch against the
connector's Fetch (long-poll) path:

  * earliest -> the consumer sees records produced BEFORE it joined.
  * latest   -> the consumer sees ONLY records produced AFTER its assignment,
                never the pre-existing ones.

The 'latest' half is race-sensitive: a subscribe does not have partitions until
the first rebalance completes, so we wait for the on_assign callback, then produce
a marker record. Only that marker may be delivered to the latest consumer.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka import Consumer, KafkaError, Producer  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

from _shared import banner, bootstrap, check, make_admin, make_consumer, tname  # noqa: E402

TOPIC = tname("reset", family="consume")
PREEXISTING = [f"pre-{i}" for i in range(5)]
MARKER = "after-latest-joined"


def ensure_topic(admin: AdminClient) -> None:
    for topic, fut in admin.create_topics(
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


def produce(values: list[str]) -> None:
    producer = Producer({"bootstrap.servers": bootstrap(), "acks": "all"})
    for v in values:
        producer.produce(TOPIC, value=v.encode())
    producer.flush(15)


def drain(consumer: Consumer, want: set[str], max_polls: int = 40) -> set[str]:
    seen: set[str] = set()
    for _ in range(max_polls):
        if want and want.issubset(seen):
            break
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise SystemExit(f"consume error: {msg.error()}")
        seen.add(msg.value().decode())
    return seen


def earliest_case() -> None:
    consumer = make_consumer("reset-earliest", **{"auto.offset.reset": "earliest"})
    consumer.subscribe([TOPIC])
    seen = drain(consumer, set(PREEXISTING))
    consumer.close()
    check(
        set(PREEXISTING).issubset(seen),
        f"earliest consumer saw all {len(PREEXISTING)} pre-existing records",
    )


def latest_case() -> None:
    assigned = {"done": False}

    def on_assign(consumer, partitions):
        assigned["done"] = True

    consumer = make_consumer("reset-latest", **{"auto.offset.reset": "latest"})
    consumer.subscribe([TOPIC], on_assign=on_assign)

    # Pump poll() until the assignment lands (latest position is fixed at join).
    deadline = time.time() + 20
    while not assigned["done"] and time.time() < deadline:
        consumer.poll(0.5)
    check(assigned["done"], "latest consumer received its partition assignment")

    # Produce the marker AFTER the latest consumer is positioned at the log end.
    produce([MARKER])
    seen = drain(consumer, {MARKER})
    consumer.close()

    check(MARKER in seen, "latest consumer saw the record produced after it joined")
    check(
        not (set(PREEXISTING) & seen),
        "latest consumer did NOT see any pre-existing record",
    )


def main() -> None:
    banner(f"consume/from-beginning-latest — topic '{TOPIC}'")
    ensure_topic(make_admin())

    produce(PREEXISTING)
    earliest_case()
    latest_case()

    print("\nRound-trip complete.")


if __name__ == "__main__":
    main()
