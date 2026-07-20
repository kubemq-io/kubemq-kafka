"""KubeMQ Kafka — produce/compression-and-keys: every codec + keyed partitioning.

Two things proven here:

1. COMPRESSION round-trip: produce the same value under each of the five Kafka
   codecs (none/gzip/snappy/lz4/zstd) and read every one back intact. The
   connector stores the decompressed record; compression is a wire-format concern.

2. KEYED PARTITIONING + gotcha #4 (the big Python gotcha): confluent-kafka is
   librdkafka-based, so its DEFAULT partitioner is CRC32 ('consistent_random'),
   NOT the murmur2 partitioner used by Java kafka-clients / franz-go / kafkajs.
   For a fixed key that means:
     * the default (CRC32) producer places the key on a STABLE partition across
       every send (same key -> same partition), and
     * a producer configured with partitioner='murmur2_random' places the SAME
       key on the murmur2 partition, which we compute independently with the
       built-in confluent_kafka.murmur2(key, partition_count).
   We assert murmur2 parity exactly, and surface whether the librdkafka default
   landed on a DIFFERENT partition — the cross-client partitioning caveat.

Multi-partition topic (3 partitions) so key placement is observable.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka import KafkaError, Producer, TopicPartition, murmur2  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

from _shared import banner, bootstrap, check, make_admin, make_consumer, tname  # noqa: E402

TOPIC = tname("codec-keys", family="produce")
PARTITIONS = 3
CODECS = ("none", "gzip", "snappy", "lz4", "zstd")
KEY = b"order-42"


def ensure_topic(admin: AdminClient) -> None:
    for topic, fut in admin.create_topics(
        [NewTopic(TOPIC, num_partitions=PARTITIONS, replication_factor=1)]
    ).items():
        try:
            fut.result()
            print(f"CreateTopics -> '{topic}' created ({PARTITIONS} partitions)")
        except Exception as exc:  # noqa: BLE001
            print(f"CreateTopics -> '{topic}' ({type(exc).__name__})")
    deadline = time.time() + 10
    while time.time() < deadline:
        tmd = admin.list_topics(topic=TOPIC, timeout=5.0).topics.get(TOPIC)
        if tmd is not None and tmd.error is None and len(tmd.partitions) >= PARTITIONS:
            return
        time.sleep(0.3)


def produce_all_codecs() -> dict[str, str]:
    """Produce one keyed record per codec; return {codec: value} for read-back checks."""
    values = {codec: f"payload-{codec}-{'A' * 64}" for codec in CODECS}
    delivered = {"ok": 0}

    def on_delivery(err, msg):
        if err is not None:
            print(f"  [FAIL] delivery error: {err}", file=sys.stderr)
            raise SystemExit(1)
        delivered["ok"] += 1

    for codec in CODECS:
        producer = Producer({"bootstrap.servers": bootstrap(), "compression.type": codec})
        producer.produce(TOPIC, key=KEY, value=values[codec].encode(), callback=on_delivery)
        producer.flush(15)
    check(delivered["ok"] == len(CODECS), f"all {len(CODECS)} codec records delivered")
    return values


def read_all_partitions(expected_values: set[str]) -> set[str]:
    consumer = make_consumer("codec-keys-reader")
    consumer.assign([TopicPartition(TOPIC, p, 0) for p in range(PARTITIONS)])
    seen: set[str] = set()
    idle = 0
    while idle < 5 and not expected_values.issubset(seen):
        msg = consumer.poll(1.0)
        if msg is None:
            idle += 1
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                idle += 1
                continue
            raise SystemExit(f"consume error: {msg.error()}")
        seen.add(msg.value().decode())
        idle = 0
    consumer.close()
    return seen


def keyed_placement() -> None:
    """Prove default (CRC32) placement is stable, and murmur2 parity is exact."""
    # --- default (CRC32) partitioner: same key -> same partition twice ---
    default_parts: list[int] = []

    def cap_default(err, msg):
        if err is not None:
            raise SystemExit(f"keyed default delivery error: {err}")
        default_parts.append(msg.partition())

    prod = Producer({"bootstrap.servers": bootstrap()})
    for _ in range(2):
        prod.produce(TOPIC, key=KEY, value=b"crc32-probe", callback=cap_default)
    prod.flush(15)
    check(
        len(default_parts) == 2 and default_parts[0] == default_parts[1],
        f"librdkafka default (CRC32) partitioner is STABLE for the key: partition {default_parts}",
    )
    default_partition = default_parts[0]

    # --- murmur2 parity: force murmur2_random and match the computed partition ---
    murmur_expected = murmur2(KEY, PARTITIONS)
    murmur_parts: list[int] = []

    def cap_murmur(err, msg):
        if err is not None:
            raise SystemExit(f"keyed murmur2 delivery error: {err}")
        murmur_parts.append(msg.partition())

    prod2 = Producer({"bootstrap.servers": bootstrap(), "partitioner": "murmur2_random"})
    prod2.produce(TOPIC, key=KEY, value=b"murmur2-probe", callback=cap_murmur)
    prod2.flush(15)
    check(
        len(murmur_parts) == 1 and murmur_parts[0] == murmur_expected,
        f"murmur2_random lands on the computed murmur2 partition ({murmur_expected})",
    )

    # --- gotcha #4: the two partitioners disagree for this key ---
    print(f"\n  gotcha #4: CRC32(default)->p{default_partition}  murmur2->p{murmur_expected}")
    if default_partition != murmur_expected:
        print("  -> the SAME key lands on a DIFFERENT partition than Java/franz-go/kafkajs.")
    else:
        print("  -> partitions coincide for this key; they diverge for most keys.")


def main() -> None:
    banner(f"produce/compression-and-keys — topic '{TOPIC}'")
    ensure_topic(make_admin())

    values = produce_all_codecs()
    seen = read_all_partitions(set(values.values()))
    for codec, value in values.items():
        check(value in seen, f"codec '{codec}' round-tripped intact")

    keyed_placement()
    print("\nRound-trip complete.")


if __name__ == "__main__":
    main()
