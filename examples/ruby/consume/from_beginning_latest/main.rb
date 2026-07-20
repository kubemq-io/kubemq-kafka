# frozen_string_literal: true

# Kafka start position — auto.offset.reset controls where a brand-new consumer
# group begins when it has no committed offset: "earliest" replays the log from
# the beginning; "latest" reads only records produced AFTER the subscription is
# live. This is the Fetch long-poll (Kafka key 1) with the two reset policies.
#
# Run:
#   export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"   # CONNECTORS_KAFKA_ENABLE=true
#   bundle exec ruby consume/from_beginning_latest/main.rb

require_relative "../../support/kafka_client"

TOPIC = KafkaClient.topic("consume", "reset")

def fail!(msg)
  warn "FAIL: #{msg}"
  exit 1
end

# Poll until `want` records arrive (or timeout); returns the payloads seen.
def drain(consumer, want, seconds)
  seen = []
  deadline = Time.now + seconds
  while seen.size < want && Time.now < deadline
    msg = consumer.poll(1000)
    next if msg.nil?

    seen << msg.payload
  end
  seen
end

KafkaClient.banner("topic=#{TOPIC}")

# Pre-produce two records BEFORE any consumer exists.
producer = KafkaClient.producer("acks" => "all")
%w[pre-1 pre-2].each { |p| producer.produce(topic: TOPIC, payload: p, key: "k").wait(max_wait_timeout: 15) }
producer.close
puts "Produce        -> pre-1, pre-2 (before any consumer)"

# earliest: a fresh group replays the whole log => sees both pre-produced records.
early = KafkaClient.consumer("kafka-ex-consume-earliest-#{Process.pid}",
                             "auto.offset.reset" => "earliest")
early.subscribe(TOPIC)
early_seen = drain(early, 2, 15)
early.close
puts "earliest       -> saw #{early_seen.inspect}"
fail!("earliest should replay both pre-records") unless early_seen.sort == %w[pre-1 pre-2]

# latest: a fresh group starts at the END, so it must NOT see the pre-records.
# We subscribe, poll once to force the assignment/position to settle at the tail,
# THEN produce a new record and prove only that one is delivered.
late = KafkaClient.consumer("kafka-ex-consume-latest-#{Process.pid}",
                            "auto.offset.reset" => "latest")
late.subscribe(TOPIC)
# Prime the assignment: poll a few times (these should yield nothing new).
5.times { late.poll(500) }

producer = KafkaClient.producer("acks" => "all")
producer.produce(topic: TOPIC, payload: "post-1", key: "k").wait(max_wait_timeout: 15)
producer.close
puts "Produce        -> post-1 (after latest subscribed)"

late_seen = drain(late, 1, 15)
late.close
puts "latest         -> saw #{late_seen.inspect}"
fail!("latest must NOT see pre-records") if late_seen.any? { |p| p.start_with?("pre-") }
fail!("latest should see the post-subscribe record") unless late_seen.include?("post-1")

admin = KafkaClient.admin
admin.delete_topic(TOPIC).wait(max_wait_timeout: 15)
admin.close
puts "DeleteTopic    -> ok"

puts "PASS: earliest replayed the log; latest saw only post-subscribe records"
exit 0
