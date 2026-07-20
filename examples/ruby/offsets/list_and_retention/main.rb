# frozen_string_literal: true

# Kafka offsets — ListOffsets (key 2) earliest/latest, plus the retention story.
# query_watermark_offsets returns [low, high]: low = log-start offset (earliest),
# high = log-end offset (latest). Producing advances `high` by the record count;
# `low` advances only when retention (retention.ms / retention.bytes, mapped onto
# the connector channel's MaxAge / MaxBytes / MaxMsgs) truncates the log start.
#
# Run:
#   export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"   # CONNECTORS_KAFKA_ENABLE=true
#   bundle exec ruby offsets/list_and_retention/main.rb

require_relative "../../support/kafka_client"

TOPIC = KafkaClient.topic("offsets", "list")
BATCH = 7

def fail!(msg)
  warn "FAIL: #{msg}"
  exit 1
end

KafkaClient.banner("topic=#{TOPIC}")

# Create the topic with a retention config so the mapping is visible in metadata.
# retention.ms -> channel MaxAge; retention.bytes -> channel MaxBytes (spec §2.2).
admin = KafkaClient.admin
admin.create_topic(TOPIC, 1, 1, "retention.ms" => "600000", "retention.bytes" => "10485760")
     .wait(max_wait_timeout: 15)
puts "CreateTopic    -> #{TOPIC} retention.ms=600000 retention.bytes=10485760"

consumer = KafkaClient.consumer("kafka-ex-offsets-list-#{Process.pid}")

# Baseline: empty log => earliest == latest == 0.
low0, high0 = consumer.query_watermark_offsets(TOPIC, 0, 5000)
puts "ListOffsets    -> earliest=#{low0} latest=#{high0} (empty)"
fail!("empty topic should have earliest==latest") unless low0 == high0

# Produce BATCH records; latest must advance by exactly BATCH, earliest unchanged.
producer = KafkaClient.producer("acks" => "all")
BATCH.times { |i| producer.produce(topic: TOPIC, payload: "o-#{i}", key: "k", partition: 0).wait(max_wait_timeout: 15) }
producer.close

low1, high1 = consumer.query_watermark_offsets(TOPIC, 0, 5000)
puts "ListOffsets    -> earliest=#{low1} latest=#{high1} (after #{BATCH} produced)"
fail!("latest should advance by #{BATCH}") unless (high1 - high0) == BATCH
fail!("earliest (log-start) should not move without retention truncation") unless low1 == low0

consumer.close
admin.delete_topic(TOPIC).wait(max_wait_timeout: 15)
admin.close
puts "DeleteTopic    -> ok"

puts "PASS: latest tracked produced count (#{BATCH}); earliest held at log-start"
exit 0
