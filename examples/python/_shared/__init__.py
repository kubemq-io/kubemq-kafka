"""Shared setup for the KubeMQ Kafka confluent-kafka examples.

Every variant under produce/, consume/, consumer-groups/, admin/, offsets/,
transactions/, and security/ reuses this module to build
confluent-kafka Producer / Consumer / AdminClient objects pointed at the KubeMQ
Kafka connector's bootstrap endpoint instead of a real Kafka cluster.

Connection model (SHARED-CONVENTIONS §4.1): the single KUBEMQ_KAFKA_BOOTSTRAP
var (default localhost:9092), used verbatim as the Kafka bootstrap.servers value.

Gotcha #1: the connector is DISABLED by default — start the server with
CONNECTORS_KAFKA_ENABLE=true or these examples cannot connect.
Gotcha #4 (partitioner): confluent-kafka is librdkafka-based and defaults to the
CRC32 'consistent_random' partitioner, so a keyed record lands on a DIFFERENT
partition than Java / franz-go / kafkajs (murmur2). Pass
partitioner='murmur2_random' to make keyed placement match the murmur2 clients.
"""

from __future__ import annotations

import os
import sys

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient

DEFAULT_BOOTSTRAP = "localhost:9092"


def bootstrap() -> str:
    """Resolve the connector bootstrap endpoint (Kafka bootstrap.servers)."""
    return os.environ.get("KUBEMQ_KAFKA_BOOTSTRAP", DEFAULT_BOOTSTRAP)


def tname(short: str, family: str = "ex") -> str:
    """Build a Kafka-charset-safe, per-suite-namespaced topic name.

    Convention (SHARED-CONVENTIONS §4.2): kafka-ex-<family>-<short>. The prefix
    from KUBEMQ_KAFKA_NAME_PREFIX (default 'py') keeps the Python suite from
    colliding with the other language suites running against the SAME connector
    (connector state persists). Never emits '~' or '/', which are reserved in
    connector topic names (gotchas #6 / #7).
    """
    prefix = os.environ.get("KUBEMQ_KAFKA_NAME_PREFIX", "py")
    return f"kafka-{prefix}-{family}-{short}"


def producer_config(**overrides) -> dict:
    """Base producer config; trimmed timeouts so negative-path examples fail fast."""
    cfg = {
        "bootstrap.servers": bootstrap(),
        "message.timeout.ms": 15000,
        "socket.timeout.ms": 10000,
    }
    cfg.update(overrides)
    return cfg


def make_producer(**overrides) -> Producer:
    """confluent-kafka Producer wired to the connector bootstrap endpoint."""
    return Producer(producer_config(**overrides))


def consumer_config(group_id: str, **overrides) -> dict:
    cfg = {
        "bootstrap.servers": bootstrap(),
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "socket.timeout.ms": 10000,
    }
    cfg.update(overrides)
    return cfg


def make_consumer(group_id: str, **overrides) -> Consumer:
    """confluent-kafka Consumer with manual commits + earliest reset by default."""
    return Consumer(consumer_config(group_id, **overrides))


def make_admin(**overrides) -> AdminClient:
    cfg = {"bootstrap.servers": bootstrap()}
    cfg.update(overrides)
    return AdminClient(cfg)


def banner(title: str) -> None:
    """Print a consistent example header showing the resolved connection."""
    print(f"=== {title} ===")
    print(f"  bootstrap : {bootstrap()}")
    print("  client    : confluent-kafka (librdkafka; CRC32 default partitioner)")
    print("  note      : connector must be started with CONNECTORS_KAFKA_ENABLE=true")
    print()


def check(condition: bool, message: str) -> None:
    """Assert an expected outcome; exit non-zero on failure.

    Examples are runnable PROOFS, not demos — a lost record, a duplicate, an
    out-of-order offset, or an aborted-txn leak must fail the process.
    """
    if condition:
        print(f"  [OK] {message}")
    else:
        print(f"  [FAIL] {message}", file=sys.stderr)
        raise SystemExit(1)
