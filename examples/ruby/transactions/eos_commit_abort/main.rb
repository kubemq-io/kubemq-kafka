# frozen_string_literal: true

# Kafka transactions — exactly-once (EOS) commit vs abort. A transactional
# producer runs InitProducerId -> AddPartitionsToTxn -> (txn Produce) -> EndTxn.
# A COMMITTED batch becomes visible to a read_committed consumer; an ABORTED
# batch is never delivered to it.
#
# KIP-890 CEILING (spec §2.5, gotcha #9): the connector implements the KIP-890
# V1 transaction protocol. It closes the classic hanging-transaction window but
# leaves a residual SAME-EPOCH zombie edge (a fenced producer reusing its exact
# epoch within one txn). That is a documented protocol-level limit, NOT an
# assertion this example makes — we never claim EOS beyond the V1 guarantee.
#
# Run:
#   export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"   # CONNECTORS_KAFKA_ENABLE=true
#   bundle exec ruby transactions/eos_commit_abort/main.rb

require_relative "../../support/kafka_client"

TOPIC = KafkaClient.topic("txn", "eos")
# gotcha #7: "/" in a transactional.id -> INVALID_TRANSACTIONAL_ID -> INVALID_REQUEST(42).
# Use a "."-safe id (no "/").
TID = "kafka-ex-txn-eos.#{Process.pid}".freeze

def fail!(msg)
  warn "FAIL: #{msg}"
  exit 1
end

KafkaClient.banner("topic=#{TOPIC} transactional.id=#{TID}")

# Justified N/A (repo policy): the transactional-producer API
# (init_transactions/begin_transaction/commit_transaction/abort_transaction) is only present in
# rdkafka-ruby builds that bind librdkafka's transaction calls. The installed gem does not expose it,
# so this EOS proof is a documented N/A rather than a false pass — exit 0 without asserting an unmet
# guarantee. See docs/guides/transactions-eos.md for the supported-client alternatives.
unless Rdkafka::Producer.method_defined?(:init_transactions)
  puts "N/A: rdkafka-ruby #{Rdkafka::VERSION} does not expose the transactional producer " \
       "API (init_transactions); see docs/guides/transactions-eos.md for the supported-client " \
       "alternatives (franz-go / Java / confluent-kafka)."
  exit 0
end

producer = KafkaClient.producer("transactional.id" => TID, "enable.idempotence" => true)
producer.init_transactions

# --- Committed transaction: two records that MUST be visible. ---
producer.begin_transaction
2.times { |i| producer.produce(topic: TOPIC, payload: "committed-#{i}", key: "k", partition: 0) }
producer.commit_transaction
puts "Txn(commit)    -> produced committed-0, committed-1 and committed"

# --- Aborted transaction: two records that MUST NOT be visible. ---
producer.begin_transaction
2.times { |i| producer.produce(topic: TOPIC, payload: "aborted-#{i}", key: "k", partition: 0) }
producer.abort_transaction
puts "Txn(abort)     -> produced aborted-0, aborted-1 and aborted"
producer.close

# --- read_committed consumer: sees committed-*, never aborted-*. ---
consumer = KafkaClient.consumer("kafka-ex-txn-eos-#{Process.pid}",
                                "auto.offset.reset" => "earliest",
                                "isolation.level" => "read_committed")
consumer.subscribe(TOPIC)
seen = []
deadline = Time.now + 20
# Read until we have both committed records AND a quiet period proves no aborted
# records are coming.
while Time.now < deadline
  msg = consumer.poll(1000)
  if msg.nil?
    break if seen.size >= 2 # committed records in hand; nothing else arriving

    next
  end
  seen << msg.payload
end
consumer.close
puts "read_committed -> saw #{seen.inspect}"

fail!("committed records not all visible") unless (seen & %w[committed-0
                                                             committed-1]).sort == %w[committed-0 committed-1]
fail!("read_committed consumer delivered an ABORTED record") if seen.any? { |p| p.start_with?("aborted-") }

admin = KafkaClient.admin
admin.delete_topic(TOPIC).wait(max_wait_timeout: 15)
admin.close
puts "DeleteTopic    -> ok"

puts "PASS: committed batch visible; aborted batch absent under read_committed (KIP-890 V1)"
exit 0
