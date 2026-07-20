"""KubeMQ Kafka — security/sasl-plain-scram: SASL/PLAIN + SCRAM auth (+ denied path).

Authenticates produce/consume over SASL/PLAIN and SASL/SCRAM-SHA-256/512, then shows
an unauthorized action denied with *_AUTHORIZATION_FAILED.

Credentials come from the environment (KUBEMQ_KAFKA_SASL_USERNAME /
KUBEMQ_KAFKA_SASL_PASSWORD). If they are absent, this program falls back to the
no-auth path (a plain PLAINTEXT round-trip) and prints a documentation note — the
stock dev connector runs with SASL off, so there is nothing to authenticate against.

The denied path runs only when a deliberately-unauthorized principal is supplied via
KUBEMQ_KAFKA_SASL_DENIED_USERNAME / _PASSWORD (broker-ACL dependent); otherwise it is
skipped with a note.

TLS on :9093 (security.protocol=SSL) and mTLS principal derivation are DOC-ONLY: a
stock dev broker has no certs, so there is no runnable TLS path here — see
docs/guides/security-sasl-tls.md. Mirrors connectors/kafka/ (SASL handshake +
authorization checks).
"""

from __future__ import annotations

import os
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

from _shared import banner, bootstrap, check, tname  # noqa: E402

TOPIC = tname("sasl", family="security")
GROUP = "sasl-reader"
RUN = uuid.uuid4().hex[:8]

USERNAME = os.environ.get("KUBEMQ_KAFKA_SASL_USERNAME")
PASSWORD = os.environ.get("KUBEMQ_KAFKA_SASL_PASSWORD")
MECHANISMS = os.environ.get(
    "KUBEMQ_KAFKA_SASL_MECHANISMS", "PLAIN,SCRAM-SHA-256,SCRAM-SHA-512"
).split(",")
DENIED_USERNAME = os.environ.get("KUBEMQ_KAFKA_SASL_DENIED_USERNAME")
DENIED_PASSWORD = os.environ.get("KUBEMQ_KAFKA_SASL_DENIED_PASSWORD")
DENIED_TOPIC = os.environ.get("KUBEMQ_KAFKA_SASL_DENIED_TOPIC", tname("denied", family="security"))


def sasl_config(mechanism: str, username: str, password: str) -> dict:
    return {
        "bootstrap.servers": bootstrap(),
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": mechanism,
        "sasl.username": username,
        "sasl.password": password,
    }


def ensure_topic(base: dict) -> None:
    admin = AdminClient(base)
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


def round_trip(base: dict, tag: str, message: str) -> None:
    body = f"{tag}-{RUN}".encode()
    delivered: dict = {}

    def on_delivery(err, _msg):
        delivered["err"] = err

    producer = Producer({**base, "acks": "all"})
    producer.produce(TOPIC, value=body, callback=on_delivery)
    producer.flush(15)
    if delivered.get("err") is not None:
        check(False, f"{message} (produce failed: {delivered['err']})")
        return

    consumer = Consumer(
        {
            **base,
            "group.id": GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.assign([TopicPartition(TOPIC, 0, 0)])
    got = None
    deadline = time.time() + 20
    while got is None and time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise SystemExit(f"consume error: {msg.error()}")
        if msg.value() == body:
            got = msg.value()
    consumer.close()
    check(got == body, message)


def denied_action(mechanism: str) -> None:
    if not (DENIED_USERNAME and DENIED_PASSWORD):
        print("  (denied-path check skipped: set KUBEMQ_KAFKA_SASL_DENIED_USERNAME/_PASSWORD")
        print("   to a principal without access to exercise *_AUTHORIZATION_FAILED.)")
        return
    base = sasl_config(mechanism, DENIED_USERNAME, DENIED_PASSWORD)
    result: dict = {}

    def on_delivery(err, _msg):
        result["err"] = err

    producer = Producer({**base, "acks": "all"})
    producer.produce(DENIED_TOPIC, value=b"denied-attempt", callback=on_delivery)
    producer.flush(15)
    err = result.get("err")
    code = err.code() if err is not None and hasattr(err, "code") else None
    # ⚠ verify at impl: exact code is broker-ACL dependent — a denied principal
    # surfaces TOPIC/GROUP_AUTHORIZATION_FAILED (valid creds, no ACL) or a SASL
    # authentication failure (bad creds); accept the whole denial family.
    check(
        code
        in (
            KafkaError.TOPIC_AUTHORIZATION_FAILED,
            KafkaError.GROUP_AUTHORIZATION_FAILED,
            KafkaError.SASL_AUTHENTICATION_FAILED,
            KafkaError._AUTHENTICATION,
        ),
        f"unauthorized action denied (TOPIC_AUTHORIZATION_FAILED): {err}",
    )


def doc_note() -> None:
    print("\nDoc-only: TLS (security.protocol=SSL, :9093) and mTLS principal = verified-cert CN")
    print("require broker certs not present on a stock dev broker — see")
    print("docs/guides/security-sasl-tls.md.")


def main() -> None:
    banner(f"security/sasl-plain-scram — topic '{TOPIC}'")

    if not (USERNAME and PASSWORD):
        print("  KUBEMQ_KAFKA_SASL_USERNAME / _PASSWORD not set — running the no-auth path.")
        print("  (SASL/PLAIN, SCRAM-SHA-256/512 and TLS/mTLS are documented, not exercised here;")
        print("   start the connector with a Kafka credential store for the authenticated path.)")
        base = {"bootstrap.servers": bootstrap()}
        ensure_topic(base)
        round_trip(base, "noauth", "no-auth PLAINTEXT produce+consume round-trip")
        doc_note()
        print("\nRound-trip complete.")
        return

    ensure_topic(sasl_config(MECHANISMS[0], USERNAME, PASSWORD))
    for mechanism in MECHANISMS:
        base = sasl_config(mechanism, USERNAME, PASSWORD)
        round_trip(base, mechanism, f"SASL/{mechanism} authenticated produce+consume")
    denied_action(MECHANISMS[0])
    doc_note()
    print("\nRound-trip complete.")


if __name__ == "__main__":
    main()
