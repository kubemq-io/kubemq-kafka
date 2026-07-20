# frozen_string_literal: true

require "rdkafka"

# Shared client/config factory for every KubeMQ Kafka Ruby example.
#
# The KubeMQ Kafka connector speaks the real Apache Kafka wire protocol on a
# single bootstrap endpoint (default TCP :9092, TLS :9093). A native rdkafka app
# talks to it by pointing "bootstrap.servers" at the connector — no library swap.
#
# Connection contract (see ../SHARED-CONVENTIONS.md):
#   * Bootstrap — single convenience var KUBEMQ_KAFKA_BOOTSTRAP (default
#                 localhost:9092), used verbatim as librdkafka "bootstrap.servers".
#                 Named *_BOOTSTRAP (not *_URL) because Kafka takes a host:port
#                 bootstrap list, not a URL scheme.
#   * Enable    — the connector is DISABLED BY DEFAULT (gotcha #1). The broker
#                 must be started with CONNECTORS_KAFKA_ENABLE=true, and (for any
#                 non-loopback client) CONNECTORS_KAFKA_ADVERTISED_HOST set
#                 (gotcha #2 — empty advertised host => connect-then-hang).
#   * Auth      — none by default; only security/sasl_plain_scram passes creds.
#
# PARTITIONER (Ruby gotcha #4): librdkafka's default partitioner is
# consistent_random (CRC32) — this is the CRC32 group, which lands a given key on
# a DIFFERENT partition than franz-go / Java kafka-clients / kafkajs (murmur2).
# Keyed examples that must match the murmur2 group set
# "partitioner" => "murmur2_random".
module KafkaClient
  DEFAULT_BOOTSTRAP = "localhost:9092"

  module_function

  # The connector bootstrap endpoint: KUBEMQ_KAFKA_BOOTSTRAP, else the default.
  def bootstrap
    ENV.fetch("KUBEMQ_KAFKA_BOOTSTRAP", DEFAULT_BOOTSTRAP)
  end

  # Build an Rdkafka::Config from the base bootstrap + any per-example overrides.
  # `extra` keys are librdkafka config strings, e.g. "acks", "enable.idempotence",
  # "compression.codec", "group.id", "auto.offset.reset", "isolation.level",
  # "transactional.id", "security.protocol", "sasl.mechanism".
  def config(extra = {})
    Rdkafka::Config.new({ "bootstrap.servers" => bootstrap }.merge(extra))
  end

  # A ready-to-use producer pointed at the connector.
  def producer(extra = {})
    config(extra).producer
  end

  # A ready-to-use consumer. A group.id is REQUIRED by librdkafka even for
  # assign-based (no-subscribe) consumers, so callers always pass one.
  def consumer(group_id, extra = {})
    config({ "group.id" => group_id }.merge(extra)).consumer
  end

  # A ready-to-use admin client (CreateTopics/DeleteTopics/CreatePartitions/...).
  def admin(extra = {})
    config(extra).admin
  end

  # A run-unique topic name so concurrent copies on a shared dev broker don't
  # collide. Convention: kafka-ex-<family>-<short> (Kafka-charset-safe; no ~ or /).
  def topic(family, short)
    "kafka-ex-#{family}-#{short}-#{Process.pid}-#{rand(100_000)}"
  end

  # The standard one-line connection banner every example opens with.
  def banner(extra = "")
    puts "[kafka] bootstrap=#{bootstrap} client=rdkafka (librdkafka, CRC32 partitioner) #{extra}".rstrip
  end
end
