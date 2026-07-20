# frozen_string_literal: true

# Kafka compression + keyed partitioning — produce the same keyed record under
# each codec (none/gzip/snappy/lz4/zstd) and prove (a) every codec round-trips
# byte-for-byte and (b) a fixed key always lands on the SAME partition.
#
# GOTCHA #4 (the important Ruby one): rdkafka / librdkafka default partitioner is
# `consistent_random` => CRC32(key) % partitions. That puts rdkafka-ruby in the
# CRC32 GROUP with confluent-kafka (Python), Confluent.Kafka (C#) and Rust
# rdkafka — a given key lands on a DIFFERENT partition than Java kafka-clients /
# franz-go / kafkajs (all murmur2). To match the murmur2 group, set
# "partitioner" => "murmur2_random". See docs/concepts/cross-client-partitioning.md.
#
# Run:
#   export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"   # CONNECTORS_KAFKA_ENABLE=true
#   bundle exec ruby produce/compression_and_keys/main.rb

require_relative "../../support/kafka_client"

TOPIC  = KafkaClient.topic("produce", "codec")
CODECS = %w[none gzip snappy lz4 zstd].freeze
KEY    = "customer-42"

def fail!(msg)
  warn "FAIL: #{msg}"
  exit 1
end

KafkaClient.banner("topic=#{TOPIC} partitioner=CRC32 (librdkafka default)")

# A multi-partition topic so keyed partitioning is observable (1-partition topics
# hide the routing). CreateTopics with 3 partitions, replication 1.
admin = KafkaClient.admin
admin.create_topic(TOPIC, 3, 1).wait(max_wait_timeout: 15)
puts "CreateTopic    -> #{TOPIC} partitions=3"

# Produce the same keyed payload under each codec; record the landing partition.
partitions = []
CODECS.each do |codec|
  producer = KafkaClient.producer("compression.codec" => codec)
  report   = producer.produce(topic: TOPIC, payload: "payload[#{codec}]", key: KEY).wait(max_wait_timeout: 15)
  puts "Produce(#{codec.ljust(6)}) -> partition=#{report.partition} offset=#{report.offset}"
  partitions << report.partition
  producer.close
end

# (b) CRC32 partitioner is deterministic on the key: every codec's record for the
# SAME key landed on the SAME partition.
fail!("keyed records did not all land on one partition (CRC32): #{partitions.inspect}") \
  unless partitions.uniq.size == 1
puts "Assert         -> key #{KEY.inspect} => stable partition #{partitions.first} (CRC32) across all codecs"

# (a) Every codec round-trips: read the whole topic back, each payload survives.
consumer = KafkaClient.consumer("kafka-ex-produce-codec-#{Process.pid}",
                                "auto.offset.reset" => "earliest")
consumer.subscribe(TOPIC)
seen = []
deadline = Time.now + 20
while seen.size < CODECS.size && Time.now < deadline
  msg = consumer.poll(1000)
  next if msg.nil?

  seen << msg.payload
end
consumer.close

CODECS.each do |codec|
  fail!("codec #{codec} did not round-trip") unless seen.include?("payload[#{codec}]")
  puts "Fetch(#{codec.ljust(6)}) -> round-tripped OK"
end

admin.delete_topic(TOPIC).wait(max_wait_timeout: 15)
admin.close
puts "DeleteTopic    -> ok"

puts "PASS: all #{CODECS.size} codecs round-tripped; key => stable CRC32 partition #{partitions.first}"
exit 0
