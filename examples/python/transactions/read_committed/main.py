"""KubeMQ Kafka — transactions/read-committed: aborted filtering + LSO < HWM.

Proves the consumer-side of EOS isolation:

  * A read_committed consumer never delivers aborted records — the connector returns
    the AbortedTransactions list per Fetch and the client filters them locally
    (gotcha #12); there is no server-side record filter.
  * While a transaction is open, ListOffsets(latest, read_committed) returns the Last
    Stable Offset (LSO), which is strictly below the high watermark (HWM); after
    commit, the LSO advances to the HWM.

KIP-890 V1 ceiling (upstream-shared, NOT a connector defect): EOS runs the KIP-890
*V1* protocol (no TV2). Mirrors connectors/kafka/ (LSO / aborted-txn handling in the
Fetch path).
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka import IsolationLevel, KafkaError, Producer, TopicPartition  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic, OffsetSpec  # noqa: E402

from _shared import banner, bootstrap, check, make_admin, make_consumer, tname  # noqa: E402

TOPIC = tname("readcommitted", family="txn")
TXN_ID = f"{TOPIC}-producer"  # no '/' — gotcha #7
COMMITTED = ["committed-0", "committed-1"]
ABORTED = ["aborted-0", "aborted-1"]
OPEN = ["open-0", "open-1"]


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


def latest_offset(admin: AdminClient, isolation: IsolationLevel) -> int:
    # ⚠ verify at impl: LSO vs HWM depends on the connector honoring the ListOffsets
    # IsolationLevel — read_committed must return the LSO, read_uncommitted the HWM.
    tp = TopicPartition(TOPIC, 0)
    for _, fut in admin.list_offsets({tp: OffsetSpec.latest()}, isolation_level=isolation).items():
        return fut.result(timeout=10).offset
    raise SystemExit("list_offsets returned no result")


def read_committed_bodies() -> set[str]:
    consumer = make_consumer("readcommitted-reader", **{"isolation.level": "read_committed"})
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
    banner(f"transactions/read-committed — topic '{TOPIC}'")
    admin = make_admin()
    ensure_topic(admin)

    producer = make_txn_producer()
    producer.init_transactions(30)

    # One committed and one aborted transaction on the same partition.
    run = _run_txn(producer)
    run(COMMITTED, commit=True)
    run(ABORTED, commit=False)

    # A third transaction, left OPEN so the LSO sits below the HWM.
    producer.begin_transaction()
    for body in OPEN:
        producer.produce(TOPIC, value=body.encode())
    producer.flush(10)  # push the open-txn records to the broker (raises the HWM)

    # read_committed reads up to the LSO: committed visible, aborted filtered,
    # open records beyond the LSO and therefore not delivered.
    seen = read_committed_bodies()
    check(
        not (set(ABORTED) & seen),
        "read_committed consumer never delivered aborted records",
    )
    check(
        set(COMMITTED).issubset(seen),
        "read_committed consumer delivered the committed records",
    )

    hwm = latest_offset(admin, IsolationLevel.READ_UNCOMMITTED)
    lso = latest_offset(admin, IsolationLevel.READ_COMMITTED)
    check(lso < hwm, f"LSO < HWM while a transaction is open (LSO={lso} < HWM={hwm})")

    producer.commit_transaction(30)
    hwm2 = latest_offset(admin, IsolationLevel.READ_UNCOMMITTED)
    lso2 = latest_offset(admin, IsolationLevel.READ_COMMITTED)
    check(
        lso2 == hwm2,
        f"LSO advanced to HWM after the transaction committed (LSO={lso2} == HWM={hwm2})",
    )

    print("\nNote (gotcha #12): read_committed filtering is client-side (AbortedTransactions);")
    print("Note (KIP-890 V1): the connector runs the V1 txn protocol — upstream-shared, not a bug.")
    print("Round-trip complete.")


def _run_txn(producer: Producer):
    def run(bodies: list[str], commit: bool) -> None:
        producer.begin_transaction()
        for body in bodies:
            producer.produce(TOPIC, value=body.encode())
        if commit:
            producer.commit_transaction(30)
        else:
            producer.abort_transaction(30)

    return run


if __name__ == "__main__":
    main()
