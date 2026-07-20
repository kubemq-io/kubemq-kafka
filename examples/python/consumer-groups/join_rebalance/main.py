"""KubeMQ Kafka — consumer-groups/join-rebalance: two members share partitions, no loss.

Two consumers in the SAME group.id subscribe to a multi-partition topic. The group
coordinator runs Join/Sync/Heartbeat so the partitions are split across the two
members. This program proves:

  * every partition is owned by exactly one member (the two assignments are
    DISJOINT and together COVER all partitions), and
  * a full batch produced across all partitions is consumed AT LEAST ONCE in
    aggregate across the two members — no message is LOST by the rebalance.
    Duplicates are acceptable and expected: when the second member joins and the
    coordinator reassigns partitions, a record consumed-but-not-yet-committed by
    the first member is legitimately redelivered (at-least-once delivery). We
    report the duplicate count as informational and only FAIL on message loss.

Both consumers run in one process; we round-robin poll() them so their heartbeats
and the join barrier make progress together.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka import KafkaError, Producer  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

from _shared import banner, bootstrap, check, make_admin, make_consumer, tname  # noqa: E402

TOPIC = tname("rebalance", family="cgroups")
PARTITIONS = 4
GROUP = "join-rebalance-grp"
MSGS_PER_PARTITION = 10
TOTAL = PARTITIONS * MSGS_PER_PARTITION


def ensure_topic(admin: AdminClient) -> None:
    for _, fut in admin.create_topics(
        [NewTopic(TOPIC, num_partitions=PARTITIONS, replication_factor=1)]
    ).items():
        try:
            fut.result()
        except Exception:  # noqa: BLE001
            pass
    deadline = time.time() + 10
    while time.time() < deadline:
        tmd = admin.list_topics(topic=TOPIC, timeout=5.0).topics.get(TOPIC)
        if tmd is not None and tmd.error is None and len(tmd.partitions) >= PARTITIONS:
            return
        time.sleep(0.3)


def produce_across_partitions() -> None:
    producer = Producer({"bootstrap.servers": bootstrap(), "acks": "all"})
    for p in range(PARTITIONS):
        for i in range(MSGS_PER_PARTITION):
            producer.produce(TOPIC, value=f"p{p}-m{i}".encode(), partition=p)
    producer.flush(20)


def main() -> None:
    banner(f"consumer-groups/join-rebalance — topic '{TOPIC}'")
    ensure_topic(make_admin())
    produce_across_partitions()

    assignments: dict[str, set[int]] = {"c1": set(), "c2": set()}

    def track(name):
        def on_assign(consumer, partitions):
            assignments[name] = {tp.partition for tp in partitions}

        def on_revoke(consumer, partitions):
            pass

        return on_assign, on_revoke

    c1 = make_consumer(GROUP)
    c2 = make_consumer(GROUP)
    a1, r1 = track("c1")
    a2, r2 = track("c2")
    c1.subscribe([TOPIC], on_assign=a1, on_revoke=r1)
    c2.subscribe([TOPIC], on_assign=a2, on_revoke=r2)

    seen: set[str] = set()
    dup = {"count": 0}
    deadline = time.time() + 45
    idle = 0
    while time.time() < deadline and len(seen) < TOTAL and idle < 20:
        got = False
        for c in (c1, c2):
            msg = c.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise SystemExit(f"consume error: {msg.error()}")
            v = msg.value().decode()
            if v in seen:
                dup["count"] += 1
            seen.add(v)
            got = True
        idle = 0 if got else idle + 1

    c1.close()
    c2.close()

    union = assignments["c1"] | assignments["c2"]
    overlap = assignments["c1"] & assignments["c2"]
    print(f"\n  c1 owned partitions: {sorted(assignments['c1'])}")
    print(f"  c2 owned partitions: {sorted(assignments['c2'])}")

    check(union == set(range(PARTITIONS)), "the two members COVER every partition")
    check(not overlap, "the two members' assignments are DISJOINT (no shared partition)")
    check(len(seen) == TOTAL, f"all {TOTAL} records consumed across the group ({len(seen)})")
    # Duplicates are EXPECTED under at-least-once: when c2 joins and the
    # coordinator reassigns partitions, any record consumed-but-not-committed by
    # c1 is legitimately redelivered. The proof is no LOSS (every record seen at
    # least once) plus clean COVERAGE/DISJOINTNESS — NOT zero duplicates. Report
    # the duplicate count as informational; do not fail on it.
    print(
        f"  info: {dup['count']} record(s) redelivered across the rebalance "
        f"(acceptable at-least-once behavior)"
    )

    print("Round-trip complete.")


if __name__ == "__main__":
    main()
