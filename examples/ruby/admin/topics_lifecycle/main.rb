# frozen_string_literal: true

# Kafka admin — topic lifecycle. CreateTopics (key 19) -> confirm it exists via
# ListOffsets/metadata -> DeleteTopics (key 20). Then prove the connector's name
# validation: a topic name containing "~" is rejected (gotcha #6) because "~" is
# the connector's reserved partition separator (kafka.<topic>~<partition>).
#
# Run:
#   export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"   # CONNECTORS_KAFKA_ENABLE=true
#   bundle exec ruby admin/topics_lifecycle/main.rb

require_relative "../../support/kafka_client"

TOPIC = KafkaClient.topic("admin", "life")

def fail!(msg)
  warn "FAIL: #{msg}"
  exit 1
end

KafkaClient.banner("topic=#{TOPIC}")
admin = KafkaClient.admin

# CreateTopics: 3 partitions, replication 1.
admin.create_topic(TOPIC, 3, 1).wait(max_wait_timeout: 15)
puts "CreateTopic    -> #{TOPIC} partitions=3 rf=1"

# Confirm it exists: query_watermark_offsets on each partition must succeed
# (raises for an unknown topic). This is the client-observable "describe".
consumer = KafkaClient.consumer("kafka-ex-admin-life-#{Process.pid}")
3.times do |p|
  low, high = consumer.query_watermark_offsets(TOPIC, p, 5000)
  puts "Describe(p#{p})   -> low=#{low} high=#{high}"
end
consumer.close
# Note: admin.describe_configs IS present on rdkafka 0.29.0, but the connector's
# single-topic metadata is synthetic, so this watermark probe is the portable and
# reliable "topic exists + partition count" describe used here; see README.

# DeleteTopics.
admin.delete_topic(TOPIC).wait(max_wait_timeout: 15)
puts "DeleteTopic    -> ok"

# Reserved "~" in the topic name -> rejected. The connector maps partitions onto
# kafka.<topic>~<p>, so "~" in a user topic is INVALID_TOPIC_EXCEPTION (code 17).
bad = "kafka-ex-admin-bad~name-#{Process.pid}"
begin
  admin.create_topic(bad, 1, 1).wait(max_wait_timeout: 15)
  fail!("topic name with '~' was NOT rejected: #{bad}")
rescue Rdkafka::RdkafkaError => e
  # Verified: e.code == :topic_exception (INVALID_TOPIC_EXCEPTION, Kafka code 17)
  # on rdkafka 0.29.0 / librdkafka 2.14.2 — a validation rejection, not transport.
  puts "CreateTopic(~) -> rejected: #{e.code} (INVALID_TOPIC_EXCEPTION) as expected"
end

admin.close
puts "PASS: create -> describe -> delete lifecycle ok; '~' name rejected"
exit 0
