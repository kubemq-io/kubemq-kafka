"""KubeMQ Kafka — admin/partitions-and-configs: grow partitions, alter configs, truncate.

Drives the increase-only partition control plane plus two partial-support (🟡) admin
operations against the connector:

  * CreatePartitions grows the topic 2 -> 4 (strictly-greater, <=256).
  * Same-count, decrease, and >256 CreatePartitions requests are all REJECTED with
    INVALID_PARTITIONS -- partitions are increase-only and hard-capped at 256.
  * IncrementalAlterConfigs applies a subset of topic configs (retention.ms);
    partial support (🟡) -- unrecognized keys are accepted-but-no-op.
  * DeleteRecords truncates the log low-end to a given offset; partial support (🟡).

Growing N re-shards keys across partitions, so per-key order holds only within a
fixed-N epoch (gotcha #5). Mirrors connectors/kafka/ (CreatePartitions / config
alter / DeleteRecords paths).
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka import KafkaError, KafkaException, Producer, TopicPartition  # noqa: E402
from confluent_kafka.admin import (  # noqa: E402
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewPartitions,
    NewTopic,
    ResourceType,
)

from _shared import banner, bootstrap, check, make_admin, tname  # noqa: E402

TOPIC = tname("partitions", family="admin")
START_PARTITIONS = 2
GROWN_PARTITIONS = 4
MAX_PARTITIONS = 256
RECORDS = 10
TRUNCATE_TO = 5


def _partition_count(admin: AdminClient) -> int:
    tmd = admin.list_topics(topic=TOPIC, timeout=5.0).topics.get(TOPIC)
    if tmd is None or tmd.error is not None:
        return 0
    return len(tmd.partitions)


def _await_partition_count(admin: AdminClient, want: int) -> int:
    deadline = time.time() + 10
    last = 0
    while time.time() < deadline:
        last = _partition_count(admin)
        if last >= want:
            return last
        time.sleep(0.3)
    return last


def reset_topic(admin: AdminClient) -> None:
    """Start from a clean 2-partition topic — partition growth is irreversible, so a
    prior run that grew the topic would otherwise break the '2 -> 4' assertion."""
    if _partition_count(admin) > 0:
        for _, fut in admin.delete_topics([TOPIC], operation_timeout=10).items():
            try:
                fut.result()
            except Exception:  # noqa: BLE001 — absent is fine
                pass
        deadline = time.time() + 10
        while time.time() < deadline and _partition_count(admin) > 0:
            time.sleep(0.3)
    for attempt in range(5):
        try:
            for _, fut in admin.create_topics(
                [NewTopic(TOPIC, num_partitions=START_PARTITIONS, replication_factor=1)]
            ).items():
                fut.result()
            break
        except Exception:  # noqa: BLE001 — delete may not have fully settled; retry
            time.sleep(0.5)
    _await_partition_count(admin, START_PARTITIONS)
    print(f"CreateTopics -> '{TOPIC}' created ({START_PARTITIONS} partitions)")


def grow(admin: AdminClient) -> None:
    for _, fut in admin.create_partitions([NewPartitions(TOPIC, GROWN_PARTITIONS)]).items():
        fut.result(timeout=10)
    count = _await_partition_count(admin, GROWN_PARTITIONS)
    check(
        count == GROWN_PARTITIONS,
        f"CreatePartitions increased {START_PARTITIONS} -> {GROWN_PARTITIONS} partitions",
    )


def expect_invalid_partitions(admin: AdminClient, new_total: int, label: str) -> None:
    fut = next(iter(admin.create_partitions([NewPartitions(TOPIC, new_total)]).values()))
    try:
        fut.result(timeout=10)
        check(False, f"{label} CreatePartitions should have been rejected")
    except KafkaException as exc:
        err = exc.args[0]
        code = err.code() if hasattr(err, "code") else None
        check(
            code == KafkaError.INVALID_PARTITIONS,
            f"{label} CreatePartitions rejected (INVALID_PARTITIONS)",
        )


def alter_configs(admin: AdminClient) -> None:
    res = ConfigResource(ResourceType.TOPIC, TOPIC)
    res.add_incremental_config(
        ConfigEntry("retention.ms", "3600000", incremental_operation=AlterConfigOpType.SET)
    )
    for _, fut in admin.incremental_alter_configs([res]).items():
        fut.result(timeout=10)  # 🟡 accepted; unrecognized keys are no-op
    check(True, "IncrementalAlterConfigs accepted (retention.ms) [partial — 🟡]")


def truncate(admin: AdminClient) -> None:
    producer = Producer({"bootstrap.servers": bootstrap(), "acks": "all"})
    for i in range(RECORDS):
        producer.produce(TOPIC, value=f"rec-{i}".encode(), partition=0)
    producer.flush(20)
    for _, fut in admin.delete_records([TopicPartition(TOPIC, 0, TRUNCATE_TO)]).items():
        result = fut.result(timeout=15)  # DeletedRecords
        low = getattr(result, "low_watermark", None)
        check(
            low == TRUNCATE_TO,
            f"DeleteRecords truncated low end to offset {low} [partial — 🟡]",
        )


def main() -> None:
    banner(f"admin/partitions-and-configs — topic '{TOPIC}'")
    admin = make_admin()

    reset_topic(admin)
    grow(admin)
    expect_invalid_partitions(admin, GROWN_PARTITIONS, "same-count")
    expect_invalid_partitions(admin, START_PARTITIONS, "decrease")
    expect_invalid_partitions(admin, MAX_PARTITIONS + 44, ">256")
    alter_configs(admin)
    truncate(admin)

    print("\nNote (gotcha #5): growing N re-shards keys — per-key order holds only within a")
    print("fixed partition-count epoch; decreasing partitions is never allowed.")
    print("Round-trip complete.")


if __name__ == "__main__":
    main()
