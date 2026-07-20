# frozen_string_literal: true

# Kafka idempotent producer — enable.idempotence turns on InitProducerId (Kafka
# key 22): the client is assigned a Producer ID (PID) and every record carries a
# per-(PID, partition) monotonic sequence number, so a retried batch is
# de-duplicated by the broker instead of appended twice.
#
# What this proves (self-asserting): with enable.idempotence + acks=all, N
# distinct produces to one partition land at N STRICTLY INCREASING, DISTINCT
# offsets with NO gaps and NO duplicates, and a read-back sees exactly N records.
# (librdkafka's actual retry de-dup fires only when the broker forces an internal
# retry of the SAME sequence — that path is internal and not deterministically
# triggerable from a demo, so we assert the observable no-duplicate/no-gap
# invariant that idempotence guarantees. See connectors/kafka/ InitProducerId.)
#
# Run:
#   export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"   # CONNECTORS_KAFKA_ENABLE=true
#   bundle exec ruby produce/idempotent/main.rb

require_relative "../../support/kafka_client"

TOPIC = KafkaClient.topic("produce", "idem")
N     = 5

def fail!(msg)
  warn "FAIL: #{msg}"
  exit 1
end

KafkaClient.banner("topic=#{TOPIC} idempotence=on")

# Idempotence REQUIRES acks=all (librdkafka enforces it). A non-"all" acks with
# enable.idempotence would raise a config error / DisableIdempotentWrite.
producer = KafkaClient.producer(
  "enable.idempotence" => true,
  "acks" => "all"
)

# Produce N records with the SAME key so they all target one partition, waiting
# on each delivery report. Idempotence guarantees each gets a unique sequence.
offsets = []
N.times do |i|
  report = producer.produce(topic: TOPIC, payload: "evt-#{i}", key: "same-key").wait(max_wait_timeout: 15)
  puts "Produce(idem)  -> seq=#{i} partition=#{report.partition} offset=#{report.offset}"
  offsets << report.offset
end
producer.close

# All on one partition, offsets strictly increasing by 1, no duplicates.
fail!("expected #{N} delivery reports, got #{offsets.size}") unless offsets.size == N
fail!("idempotent produce yielded duplicate offsets: #{offsets.inspect}") unless offsets.uniq.size == N
sorted = offsets.sort
fail!("offsets are not gap-free monotonic: #{offsets.inspect}") \
  unless sorted.each_cons(2).all? { |a, b| b == a + 1 }
puts "Assert         -> #{N} unique, gap-free offsets #{sorted.first}..#{sorted.last}"

# Read the partition back: exactly N records, no duplicate payloads.
consumer = KafkaClient.consumer("kafka-ex-produce-idem-#{Process.pid}",
                                "auto.offset.reset" => "earliest")
consumer.subscribe(TOPIC)
seen = []
deadline = Time.now + 15
while seen.size < N && Time.now < deadline
  msg = consumer.poll(1000)
  next if msg.nil?

  seen << msg.payload
end
consumer.close

fail!("expected #{N} records on read-back, saw #{seen.size}") unless seen.size == N
fail!("read-back contained duplicates: #{seen.inspect}") unless seen.uniq.size == N
puts "Fetch          -> read back #{seen.size} unique records (no duplicates)"

admin = KafkaClient.admin
admin.delete_topic(TOPIC).wait(max_wait_timeout: 15)
admin.close
puts "DeleteTopic    -> ok"

puts "PASS: idempotent producer gave #{N} unique gap-free offsets, no duplicates"
exit 0
