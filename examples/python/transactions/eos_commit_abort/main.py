"""KubeMQ Kafka — transactions/eos-commit-abort: transactional commit vs abort.

Runs a transactional producer through the full EOS handshake (InitProducerId ->
AddPartitionsToTxn -> txn Produce -> EndTxn(commit|abort)) and proves, with a
read_committed consumer, that committed records are visible and aborted records are
absent.

KIP-890 V1 ceiling (upstream-shared, NOT a connector defect): the connector
implements the KIP-890 *V1* transaction protocol (no TV2). A same-epoch "zombie"
residual is expected under V1 and is not a failure of this example. A '/' in a
transactional.id is rejected (INVALID_TRANSACTIONAL_ID -> INVALID_REQUEST(42)), so
the id below never uses slashes (gotcha #7); txn offset-commit also requires Group
WRITE (gotcha #8). Mirrors connectors/kafka/ (transaction coordinator RPCs).
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka import KafkaError, Producer, TopicPartition  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

from _shared import banner, bootstrap, check, make_admin, make_consumer, tname  # noqa: E402

TOPIC = tname("eos", family="txn")
TXN_ID = f"{TOPIC}-producer"  # no '/' — gotcha #7
COMMITTED = ["committed-0", "committed-1", "committed-2"]
ABORTED = ["aborted-0", "aborted-1"]


def ensure_topic(admin: AdminClient) -> None:
    for _, fut in admin.create_topics(
        [NewTopic(TOPIC, num_partitions=1, replication_factor=1)]
    ).items():
        try:
            fut.result()
        except Exception:  # noqa: BLE001 — already-exists is fine on a re-run
            pass
    deadline = time.time() + 10
    while time.time() < deadline:
        tmd = admin.list_topics(topic=TOPIC, timeout=5.0).topics.get(TOPIC)
        if tmd is not None and tmd.error is None and tmd.partitions:
            return
        time.sleep(0.3)


def make_txn_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap(),
            "transactional.id": TXN_ID,
            "enable.idempotence": True,
            "acks": "all",
        }
    )


def run_transaction(producer: Producer, bodies: list[str], commit: bool) -> None:
    producer.begin_transaction()
    for body in bodies:
        producer.produce(TOPIC, value=body.encode())
    if commit:
        producer.commit_transaction(30)
    else:
        producer.abort_transaction(30)


def read_committed_bodies() -> set[str]:
    consumer = make_consumer("eos-reader", **{"isolation.level": "read_committed"})
    consumer.assign([TopicPartition(TOPIC, 0, 0)])
    seen: set[str] = set()
    idle = 0
    deadline = time.time() + 30
    while time.time() < deadline and idle < 10:
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


def main() -> None:
    banner(f"transactions/eos-commit-abort — topic '{TOPIC}'")
    ensure_topic(make_admin())

    producer = make_txn_producer()
    producer.init_transactions(30)
    check(True, "init_transactions() obtained a producer id")

    run_transaction(producer, COMMITTED, commit=True)
    run_transaction(producer, ABORTED, commit=False)

    seen = read_committed_bodies()
    check(
        set(COMMITTED).issubset(seen),
        "committed transaction: records visible under read_committed",
    )
    check(
        not (set(ABORTED) & seen),
        "aborted transaction: records ABSENT under read_committed",
    )

    print("\nNote (KIP-890 V1): the connector runs the V1 transaction protocol (no TV2);")
    print("a same-epoch zombie residual is upstream-shared behavior, not a defect.")
    print("Round-trip complete.")


if __name__ == "__main__":
    main()
