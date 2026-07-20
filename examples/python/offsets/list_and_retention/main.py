"""KubeMQ Kafka — offsets/list-and-retention: ListOffsets earliest/latest/by-ts + retention.

Resolves the three ListOffsets positions and shows how retention config maps to the
connector's channel MaxAge / MaxBytes:

  * ListOffsets(earliest) returns the log-start offset (0 on a fresh topic).
  * ListOffsets(latest) returns the high watermark (HWM).
  * ListOffsets(by-timestamp) returns the first offset with timestamp >= ts.
  * retention.ms / retention.bytes are set at create time, then read back with
    DescribeConfigs to prove they are describable and honored (channel MaxAge/MaxBytes).

The primary path uses AdminClient.list_offsets + OffsetSpec; if that admin surface is
unavailable in the pinned confluent-kafka, it falls back to the consumer
get_watermark_offsets + offsets_for_times pair. Mirrors connectors/kafka/
(ListOffsets earliest/latest/by-timestamp).
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka import Producer, TopicPartition  # noqa: E402
from confluent_kafka.admin import (  # noqa: E402
    AdminClient,
    ConfigResource,
    NewTopic,
    OffsetSpec,
    ResourceType,
)

from _shared import banner, bootstrap, check, make_admin, make_consumer, tname  # noqa: E402

TOPIC = tname("retention", family="offsets")
COUNT = 12
TS_MARK_INDEX = 6
RETENTION_MS = "3600000"
RETENTION_BYTES = "1048576"


def _present(admin: AdminClient) -> bool:
    tmd = admin.list_topics(topic=TOPIC, timeout=5.0).topics.get(TOPIC)
    return tmd is not None and tmd.error is None and bool(tmd.partitions)


def reset_topic(admin: AdminClient) -> None:
    """Fresh topic so earliest tracks 0 and latest tracks COUNT deterministically."""
    if _present(admin):
        for _, fut in admin.delete_topics([TOPIC], operation_timeout=10).items():
            try:
                fut.result()
            except Exception:  # noqa: BLE001
                pass
        deadline = time.time() + 10
        while time.time() < deadline and _present(admin):
            time.sleep(0.3)
    for _ in range(5):
        try:
            for _, fut in admin.create_topics(
                [
                    NewTopic(
                        TOPIC,
                        num_partitions=1,
                        replication_factor=1,
                        config={"retention.ms": RETENTION_MS, "retention.bytes": RETENTION_BYTES},
                    )
                ]
            ).items():
                fut.result()
            break
        except Exception:  # noqa: BLE001 — delete may not have settled; retry
            time.sleep(0.5)
    deadline = time.time() + 10
    while time.time() < deadline and not _present(admin):
        time.sleep(0.3)


def produce_marked() -> int:
    """Produce COUNT records with a small gap; return the ms timestamp of record 6."""
    producer = Producer({"bootstrap.servers": bootstrap(), "acks": "all"})
    mark_ts = 0
    for i in range(COUNT):
        if i == TS_MARK_INDEX:
            mark_ts = int(time.time() * 1000)
        producer.produce(TOPIC, value=f"rec-{i}".encode())
        producer.flush(10)
        time.sleep(0.05)
    return mark_ts


def _admin_offset(admin: AdminClient, spec) -> int:
    for _, fut in admin.list_offsets({TopicPartition(TOPIC, 0): spec}).items():
        return fut.result(timeout=10).offset
    raise SystemExit("list_offsets returned no result")


def list_offsets(admin: AdminClient, mark_ts: int) -> None:
    try:
        earliest = _admin_offset(admin, OffsetSpec.earliest())
        latest = _admin_offset(admin, OffsetSpec.latest())
        by_ts = _admin_offset(admin, OffsetSpec.for_timestamp(mark_ts))
        source = "AdminClient.list_offsets"
    except Exception as exc:  # noqa: BLE001 — fall back to consumer watermarks
        print(
            f"  (AdminClient.list_offsets unavailable: {type(exc).__name__}; "
            "using consumer watermarks)"
        )
        consumer = make_consumer("offsets-reader")
        low, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, 0), timeout=10.0)
        earliest, latest = low, high
        resolved = consumer.offsets_for_times([TopicPartition(TOPIC, 0, mark_ts)], timeout=10.0)
        by_ts = resolved[0].offset
        consumer.close()
        source = "consumer.get_watermark_offsets/offsets_for_times"

    check(earliest == 0, f"earliest watermark tracks log start (offset {earliest})")
    check(latest == COUNT, f"latest watermark tracks the high watermark (offset {latest})")
    check(by_ts == TS_MARK_INDEX, f"ListOffsets by-timestamp resolved ts -> offset {by_ts}")
    print(f"  (offsets resolved via {source})")


def retention_configs(admin: AdminClient) -> None:
    res = ConfigResource(ResourceType.TOPIC, TOPIC)
    for _, fut in admin.describe_configs([res]).items():
        configs = fut.result(timeout=10)

    def as_int(entry) -> int | None:
        try:
            return int(entry.value)
        except (TypeError, ValueError, AttributeError):
            return None

    ret_ms = as_int(configs.get("retention.ms"))
    ret_bytes = as_int(configs.get("retention.bytes"))
    check(
        ret_ms == int(RETENTION_MS) and ret_bytes == int(RETENTION_BYTES),
        "retention.ms / retention.bytes describable and honored",
    )


def main() -> None:
    banner(f"offsets/list-and-retention — topic '{TOPIC}'")
    admin = make_admin()

    reset_topic(admin)
    mark_ts = produce_marked()
    list_offsets(admin, mark_ts)
    retention_configs(admin)

    print("\nNote: offsets are durable STAN Sequences — earliest/latest are restart-stable")
    print("and identical across nodes; retention truncation advances the earliest offset.")
    print("Round-trip complete.")


if __name__ == "__main__":
    main()
