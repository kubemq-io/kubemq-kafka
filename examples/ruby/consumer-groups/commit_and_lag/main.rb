# frozen_string_literal: true

# Kafka offset commit + lag — a consumer with auto-commit OFF reads part of a
# topic and commits its offset (OffsetCommit, key 8). A SECOND consumer in the
# same group then RESUMES exactly from the committed offset (OffsetFetch, key 9).
# Consumer lag = high-watermark - committed-offset (also exposed server-side as
# the kubemq_kafka_consumer_group_lag metric).
#
# Run:
#   export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"   # CONNECTORS_KAFKA_ENABLE=true
#   bundle exec ruby consumer-groups/commit_and_lag/main.rb

require_relative "../../support/kafka_client"

TOPIC = KafkaClient.topic("cg", "commit")
GROUP = "kafka-ex-cg-commit-#{Process.pid}".freeze
TOTAL = 10
FIRST = 4 # how many the first consumer processes + commits

def fail!(msg)
  warn "FAIL: #{msg}"
  exit 1
end

KafkaClient.banner("topic=#{TOPIC} group=#{GROUP}")

# Produce TOTAL records to a single partition (deterministic offsets 0..TOTAL-1).
producer = KafkaClient.producer("acks" => "all")
TOTAL.times { |i| producer.produce(topic: TOPIC, payload: "n-#{i}", key: "k", partition: 0).wait(max_wait_timeout: 15) }
producer.close
puts "Produce        -> #{TOTAL} records to partition 0"

# --- Consumer #1: manual commit, reads FIRST records, commits synchronously. ---
c1 = KafkaClient.consumer(GROUP,
                          "auto.offset.reset" => "earliest",
                          "enable.auto.commit" => false)
c1.subscribe(TOPIC)
processed = []
deadline = Time.now + 15
while processed.size < FIRST && Time.now < deadline
  msg = c1.poll(1000)
  next if msg.nil?

  processed << msg.payload
end
fail!("consumer #1 could not read #{FIRST} records") unless processed.size == FIRST
c1.commit(nil, false) # synchronous commit of the current position
puts "Consumer#1     -> processed #{processed.inspect}, committed"
c1.close

# --- Consumer #2: same group, resumes strictly AFTER the committed offset. ---
c2 = KafkaClient.consumer(GROUP,
                          "auto.offset.reset" => "earliest",
                          "enable.auto.commit" => false)
c2.subscribe(TOPIC)
resumed = []
deadline = Time.now + 15
while resumed.size < (TOTAL - FIRST) && Time.now < deadline
  msg = c2.poll(1000)
  next if msg.nil?

  resumed << msg.payload
end
puts "Consumer#2     -> resumed #{resumed.inspect}"

# committed offset vs high watermark => lag.
committed_tpl = c2.committed
committed_off = committed_tpl.to_h[TOPIC].find { |p| p.partition.zero? }.offset
_low, high = c2.query_watermark_offsets(TOPIC, 0, 5000)
c2.close

fail!("consumer #2 did not resume from the committed offset") \
  unless resumed == (FIRST...TOTAL).map { |i| "n-#{i}" }
fail!("committed offset should be #{FIRST}, was #{committed_off}") unless committed_off == FIRST

# Lag right after commit#1 (before consumer#2 committed) would be TOTAL-FIRST;
# here we report the current computed lag = high - committed as the metric shape.
puts "Lag            -> high=#{high} committed=#{committed_off} lag=#{high - committed_off}"
puts "Note           -> server also exposes this as metric kubemq_kafka_consumer_group_lag"

admin = KafkaClient.admin
admin.delete_topic(TOPIC).wait(max_wait_timeout: 15)
admin.close
puts "DeleteTopic    -> ok"

puts "PASS: second consumer resumed from committed offset #{FIRST}; lag computed from HWM"
exit 0
