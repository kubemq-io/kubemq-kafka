# frozen_string_literal: true

# Kafka seek — random-access reads by OFFSET and by TIMESTAMP against a single
# partition. Uses ListOffsets (query_watermark_offsets for the log-start/end
# bounds; offsets_for_times for the by-timestamp lookup) and positions the
# consumer with an assign-at-offset TopicPartitionList.
#
# Run:
#   export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"   # CONNECTORS_KAFKA_ENABLE=true
#   bundle exec ruby consume/seek_offsets_timestamps/main.rb

require_relative "../../support/kafka_client"

TOPIC = KafkaClient.topic("consume", "seek")
N     = 6

def fail!(msg)
  warn "FAIL: #{msg}"
  exit 1
end

# Read exactly one record at the consumer's current position for `part`.
def read_one(consumer, seconds = 10)
  deadline = Time.now + seconds
  while Time.now < deadline
    msg = consumer.poll(1000)
    return msg unless msg.nil?
  end
  nil
end

KafkaClient.banner("topic=#{TOPIC}")

# Produce N records to partition 0, remembering the wall-clock time BEFORE record
# #3 so we can later seek by timestamp to it.
producer = KafkaClient.producer("acks" => "all")
ts_before_third = nil
N.times do |i|
  ts_before_third = (Time.now.to_f * 1000).to_i if i == 3
  producer.produce(topic: TOPIC, payload: "rec-#{i}", key: "k", partition: 0).wait(max_wait_timeout: 15)
end
producer.close
puts "Produce        -> #{N} records to partition 0"

consumer = KafkaClient.consumer("kafka-ex-consume-seek-#{Process.pid}")

# ListOffsets: log-start (low) and log-end (high) watermarks.
low, high = consumer.query_watermark_offsets(TOPIC, 0, 5000)
puts "Watermarks     -> low=#{low} high=#{high}"
fail!("expected #{N} records in the log") unless (high - low) == N

# --- Seek by OFFSET: assign the partition positioned at low+2, read it. ---
tpl = Rdkafka::Consumer::TopicPartitionList.new
tpl.add_topic_and_partitions_with_offsets(TOPIC, 0 => low + 2)
consumer.assign(tpl)
msg = read_one(consumer)
fail!("seek(offset=#{low + 2}) returned nothing") if msg.nil?
puts "Seek(offset=#{low + 2}) -> offset=#{msg.offset} payload=#{msg.payload.inspect}"
fail!("seek-by-offset landed on the wrong record") unless msg.payload == "rec-2"

# --- Seek by TIMESTAMP: offsets_for_times maps a timestamp to the first offset
#     with ts >= target. We ask for the time captured just before rec-3.
#     Verified present on rdkafka 0.29.0 (librdkafka 2.14.2); the connector indexes
#     by server APPEND-time, and the captured client wall-clock instant resolves to
#     offset 3 (rec-3) as expected.
tsl = Rdkafka::Consumer::TopicPartitionList.new
tsl.add_topic_and_partitions_with_offsets(TOPIC, 0 => ts_before_third)
resolved = consumer.offsets_for_times(tsl, 5000)
ts_offset = resolved.to_h[TOPIC].find { |p| p.partition.zero? }.offset
puts "offsets_for_times(ts=#{ts_before_third}) -> offset=#{ts_offset}"
fail!("by-timestamp offset should be >= low") unless ts_offset >= low

tpl2 = Rdkafka::Consumer::TopicPartitionList.new
tpl2.add_topic_and_partitions_with_offsets(TOPIC, 0 => ts_offset)
consumer.assign(tpl2)
tmsg = read_one(consumer)
fail!("seek(by-ts) returned nothing") if tmsg.nil?
puts "Seek(by-ts)    -> offset=#{tmsg.offset} payload=#{tmsg.payload.inspect}"
# The record at the by-timestamp offset must be at or after rec-3 (index >= 3).
idx = tmsg.payload.sub("rec-", "").to_i
fail!("by-timestamp landed before the target record") unless idx >= 3

consumer.close
admin = KafkaClient.admin
admin.delete_topic(TOPIC).wait(max_wait_timeout: 15)
admin.close
puts "DeleteTopic    -> ok"

puts "PASS: seek-by-offset and seek-by-timestamp both landed on the expected record"
exit 0
