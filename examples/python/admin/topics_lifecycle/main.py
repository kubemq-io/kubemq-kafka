"""KubeMQ Kafka — admin/topics-lifecycle: create/describe/delete + reserved-name reject.

Drives the AdminClient control plane end to end:

  * CreateTopics -> the topic appears in full-cluster metadata.
  * DescribeConfigs -> the topic's config entries are readable.
  * DescribeCluster -> the connector reports at least one broker (falls back to
    list_topics() cluster metadata if describe_cluster is unavailable).
  * DeleteTopics -> the topic disappears from full-cluster metadata.

Presence/absence is checked with the FULL-CLUSTER list_topics() enumeration, not
a single-topic list_topics(topic=TOPIC) request: the connector answers a
single-topic metadata request positively for ANY name (a synthetic entry, even
for a never-created topic), so it can neither confirm creation nor observe a
deletion. The full ListTopics reflects real create/delete state (~1s to settle).
  * A reserved-character name ('bad~name') is REJECTED — '~' is reserved in the
    connector's Events-Store channel mapping (gotcha #6), so the create fails.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka.admin import (  # noqa: E402
    AdminClient,
    ConfigResource,
    NewTopic,
    ResourceType,
)

from _shared import banner, check, make_admin, tname  # noqa: E402

TOPIC = tname("lifecycle", family="admin")
BAD_TOPIC = tname("bad~name", family="admin")  # '~' -> reserved (gotcha #6)


def create(admin: AdminClient) -> None:
    for topic, fut in admin.create_topics(
        [NewTopic(TOPIC, num_partitions=1, replication_factor=1)]
    ).items():
        fut.result()
        print(f"CreateTopics -> '{topic}' created")
    # Verify presence via FULL-CLUSTER metadata (list_topics() with no topic=).
    # A single-topic list_topics(topic=TOPIC) request is answered positively by
    # the connector for ANY name — it returns a synthetic entry (partitions=1,
    # no error) even for a topic that was never created — so it can NOT prove
    # existence or absence. The full ListTopics enumeration does reflect real
    # create/delete state (create propagates within ~1s).
    deadline = time.time() + 10
    while time.time() < deadline:
        tmd = admin.list_topics(timeout=5.0).topics.get(TOPIC)
        if tmd is not None and tmd.error is None and tmd.partitions:
            check(True, "created topic is present in cluster metadata")
            return
        time.sleep(0.3)
    check(False, "created topic is present in cluster metadata")


def describe_configs(admin: AdminClient) -> None:
    res = ConfigResource(ResourceType.TOPIC, TOPIC)
    fut = admin.describe_configs([res])[res]
    configs = fut.result(timeout=10)
    check(len(configs) > 0, f"DescribeConfigs returned {len(configs)} config entries")


def describe_cluster(admin: AdminClient) -> None:
    try:
        desc = admin.describe_cluster().result(timeout=10)
        nodes = list(desc.nodes)
        check(len(nodes) >= 1, f"DescribeCluster reports {len(nodes)} broker(s)")
    except Exception as exc:  # noqa: BLE001 — fall back to list_topics metadata
        md = admin.list_topics(timeout=10.0)
        check(
            len(md.brokers) >= 1,
            f"cluster metadata reports {len(md.brokers)} broker(s) "
            f"(describe_cluster unavailable: {type(exc).__name__})",
        )


def delete(admin: AdminClient) -> None:
    for topic, fut in admin.delete_topics([TOPIC], operation_timeout=10).items():
        fut.result()
        print(f"DeleteTopics -> '{topic}' deleted")
    # Verify absence via FULL-CLUSTER metadata. A single-topic
    # list_topics(topic=TOPIC) request always reports the name present (see
    # create() for why), so it can never observe a deletion. The full ListTopics
    # enumeration DOES drop the topic within ~1s of delete and keeps it gone.
    deadline = time.time() + 10
    while time.time() < deadline:
        if TOPIC not in admin.list_topics(timeout=5.0).topics:
            check(True, "deleted topic is gone from cluster metadata")
            return
        time.sleep(0.3)
    check(False, "deleted topic is gone from cluster metadata")


def reserved_name_rejected(admin: AdminClient) -> None:
    fut = admin.create_topics([NewTopic(BAD_TOPIC, num_partitions=1, replication_factor=1)])[
        BAD_TOPIC
    ]
    try:
        fut.result(timeout=10)
        check(False, f"reserved-name topic '{BAD_TOPIC}' should have been rejected")
    except Exception as exc:  # noqa: BLE001 — rejection is the expected outcome
        check(True, f"reserved-name topic rejected (gotcha #6): {type(exc).__name__}: {exc}")


def main() -> None:
    banner(f"admin/topics-lifecycle — topic '{TOPIC}'")
    admin = make_admin()

    create(admin)
    describe_configs(admin)
    describe_cluster(admin)
    delete(admin)
    reserved_name_rejected(admin)

    print("\nRound-trip complete.")


if __name__ == "__main__":
    main()
