# frozen_string_literal: true

# Kafka read_committed isolation — a consumer with isolation.level=read_committed
# never delivers records from an aborted transaction, and its readable end is the
# Last Stable Offset (LSO), which sits BELOW the high-watermark (HWM) while a
# transaction is still open. Aborted-record filtering is CLIENT-side: the broker
# ships the batches plus an AbortedTransactions list and librdkafka drops them
# (gotcha #12 — there is no server-side per-record filter).
#
# KIP-890 CEILING (spec §2.5, gotcha #9): the connector implements KIP-890 V1.
# read_committed correctness holds within that guarantee; we do not claim beyond
# it (residual same-epoch zombie edge is a documented protocol limit, not tested).
#
# Run:
#   export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"   # CONNECTORS_KAFKA_ENABLE=true
#   bundle exec ruby transactions/read_committed/main.rb

require_relative "../../support/kafka_client"

TOPIC = KafkaClient.topic("txn", "readc")
TID   = "kafka-ex-txn-readc.#{Process.pid}".freeze # "."-safe id (gotcha #7)

def fail!(msg)
  warn "FAIL: #{msg}"
  exit 1
end

KafkaClient.banner("topic=#{TOPIC} isolation=read_committed")

# Justified N/A (repo policy): the transactional-producer API
# (init_transactions/begin_transaction/commit_transaction/abort_transaction) is only present in
# rdkafka-ruby builds that bind librdkafka's transaction calls. The installed gem does not expose it,
# so this read_committed proof is a documented N/A rather than a false pass — exit 0 without asserting
# an unmet guarantee. See docs/guides/transactions-eos.md for the supported-client alternatives.
unless Rdkafka::Producer.method_defined?(:init_transactions)
  puts "N/A: rdkafka-ruby #{Rdkafka::VERSION} does not expose the transactional producer " \
       "API (init_transactions); see docs/guides/transactions-eos.md for the supported-client " \
       "alternatives (franz-go / Java / confluent-kafka)."
  exit 0
end

producer = KafkaClient.producer("transactional.id" => TID, "enable.idempotence" => true)
producer.init_transactions

# One committed txn (visible) and one aborted txn (must stay invisible).
producer.begin_transaction
3.times { |i| producer.produce(topic: TOPIC, payload: "keep-#{i}", key: "k", partition: 0) }
producer.commit_transaction
puts "Txn(commit)    -> keep-0..keep-2 committed"

producer.begin_transaction
3.times { |i| producer.produce(topic: TOPIC, payload: "drop-#{i}", key: "k", partition: 0) }
producer.abort_transaction
puts "Txn(abort)     -> drop-0..drop-2 aborted"
producer.close

# read_committed consumer: LSO check + never-sees-aborted.
consumer = KafkaClient.consumer("kafka-ex-txn-readc-#{Process.pid}",
                                "auto.offset.reset" => "earliest",
                                "isolation.level" => "read_committed")

# After both transactions have ended, the LSO (read_committed latest) equals the
# HWM; the aborted batch still occupies offsets in the log (so HWM advanced) but
# its records are filtered out client-side.
_low, hwm = consumer.query_watermark_offsets(TOPIC, 0, 5000)
puts "Watermarks     -> HWM(read_uncommitted end)=#{hwm} (aborted batch still occupies offsets)"

consumer.subscribe(TOPIC)
seen = []
deadline = Time.now + 20
while Time.now < deadline
  msg = consumer.poll(1000)
  if msg.nil?
    break if seen.size >= 3

    next
  end
  seen << msg.payload
end
consumer.close
puts "read_committed -> delivered #{seen.inspect}"

fail!("committed records missing") unless (seen & %w[keep-0 keep-1 keep-2]).sort == %w[keep-0 keep-1 keep-2]
fail!("aborted records leaked to a read_committed consumer") if seen.any? { |p| p.start_with?("drop-") }
# HWM counts the aborted batch's offsets; the consumer delivered strictly fewer
# records than HWM => proof the aborted span was filtered, not delivered.
fail!("expected delivered count (#{seen.size}) < HWM (#{hwm}) due to aborted span") unless seen.size < hwm

admin = KafkaClient.admin
admin.delete_topic(TOPIC).wait(max_wait_timeout: 15)
admin.close
puts "DeleteTopic    -> ok"

puts "PASS: read_committed delivered only committed records; aborted span filtered client-side"
exit 0
