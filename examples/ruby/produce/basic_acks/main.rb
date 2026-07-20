# frozen_string_literal: true

# Kafka basic acks — Produce at acks=all, acks=1, acks=0 and read the records
# back, then prove an oversized record is rejected with MESSAGE_TOO_LARGE.
#
# Mirrors connector produce behavior in kubemq-server connectors/kafka/ (Produce
# key 0; per-partition ordered append; MaxMessageBytes = 1 MiB oversize guard).
#
# Run:
#   export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"   # connector must be started
#   #   with CONNECTORS_KAFKA_ENABLE=true (disabled by default, gotcha #1)
#   bundle exec ruby produce/basic_acks/main.rb

require_relative "../../support/kafka_client"

TOPIC = KafkaClient.topic("produce", "acks")
BODY  = "order #1138 — 2 widgets"

def fail!(msg)
  warn "FAIL: #{msg}"
  exit 1
end

KafkaClient.banner("topic=#{TOPIC}")

# ---------------------------------------------------------------------------
# 1. Produce one record at each durability level. acks=all waits for the STAN
#    commit; acks=1 for the leader append; acks=0 is fire-and-forget (the
#    delivery report still resolves locally). GOTCHA #3: on a MULTI-NODE broker
#    acks=0 can silently drop if it lands on a follower — always use acks>=1 in
#    production. Topic is auto-created on first Produce (spec §2.3).
# ---------------------------------------------------------------------------
%w[all 1 0].each do |ack|
  producer = KafkaClient.producer("acks" => ack)
  handle   = producer.produce(topic: TOPIC, payload: "#{BODY} [acks=#{ack}]", key: "k1")
  report   = handle.wait(max_wait_timeout: 15) # DeliveryReport: .partition, .offset
  puts "Produce(acks=#{ack.rjust(3)}) -> partition=#{report.partition} offset=#{report.offset}"
  # acks=0 is fire-and-forget: the broker sends no ProduceResponse, so librdkafka
  # reports offset = -1 (RD_KAFKA_OFFSET_INVALID) — a real >=0 offset only exists
  # for acks>=1. The partition is chosen client-side, so it is known either way.
  if ack == "0"
    fail!("acks=0 produced to unexpected partition") if report.partition.nil? || report.partition.negative?
  else
    fail!("acks=#{ack} produced to unexpected offset") if report.offset.nil? || report.offset.negative?
  end
  producer.close
end

# ---------------------------------------------------------------------------
# 2. Read the three records back from the beginning to prove the round-trip.
# ---------------------------------------------------------------------------
consumer = KafkaClient.consumer("kafka-ex-produce-acks-#{Process.pid}",
                                "auto.offset.reset" => "earliest")
consumer.subscribe(TOPIC)

seen = []
deadline = Time.now + 15
while seen.size < 3 && Time.now < deadline
  msg = consumer.poll(1000) # Rdkafka::Consumer::Message or nil
  next if msg.nil?

  # rdkafka returns payloads as ASCII-8BIT (raw bytes); BODY is UTF-8 (em-dash),
  # so decode the known-UTF-8 payload before comparing, else binary != UTF-8.
  payload = msg.payload.dup.force_encoding(Encoding::UTF_8)
  seen << payload
  puts "Fetch          -> partition=#{msg.partition} offset=#{msg.offset} payload=#{payload.inspect}"
end
consumer.close

fail!("expected 3 records, saw #{seen.size}") unless seen.size == 3
%w[all 1 0].each do |ack|
  fail!("missing acks=#{ack} record") unless seen.include?("#{BODY} [acks=#{ack}]")
end

# ---------------------------------------------------------------------------
# 3. Oversized record -> MESSAGE_TOO_LARGE. MaxMessageBytes is 1 MiB (1048576) and
#    the connector's request frame cap is MaxMessageBytes + 1 MiB slack = 2 MiB. A
#    1.5 MiB record is ABOVE the 1 MiB cap but BELOW the 2 MiB frame cap, so it
#    reaches the broker's produce path and is rejected with MESSAGE_TOO_LARGE. A
#    2 MiB record would instead overflow the frame cap and surface as a transport
#    error, not MESSAGE_TOO_LARGE. We raise the CLIENT-side cap ("message.max.bytes")
#    above the payload so the request reaches the broker and the BROKER returns the
#    error, rather than librdkafka rejecting it locally first.
# ---------------------------------------------------------------------------
big = "x" * (1024 * 1024 + 512 * 1024) # 1.5 MiB (over 1 MiB cap, under 2 MiB frame cap)
producer = KafkaClient.producer("acks" => "all", "message.max.bytes" => 10 * 1024 * 1024)
begin
  producer.produce(topic: TOPIC, payload: big, key: "big").wait(max_wait_timeout: 15)
  fail!("oversized record was NOT rejected")
rescue Rdkafka::RdkafkaError => e
  # Broker rejects with MESSAGE_TOO_LARGE (Kafka error code 10; librdkafka symbol
  # :msg_size_too_large — verified against rdkafka 0.29.0 / librdkafka 2.14.2).
  fail!("oversized rejected with unexpected error: #{e.code} (#{e.message})") \
    unless e.code == :msg_size_too_large
  puts "Produce(oversized) -> rejected: #{e.code} (MESSAGE_TOO_LARGE) as expected"
ensure
  producer.close
end

# ---------------------------------------------------------------------------
# 4. Clean up the topic.
# ---------------------------------------------------------------------------
admin = KafkaClient.admin
admin.delete_topic(TOPIC).wait(max_wait_timeout: 15)
admin.close
puts "DeleteTopic    -> ok"

puts "PASS: acks round-trip + oversized rejection succeeded"
exit 0
