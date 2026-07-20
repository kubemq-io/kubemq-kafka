# frozen_string_literal: true

# Kafka consumer-group join + rebalance — two consumers in the SAME group.id
# subscribe to a multi-partition topic; the group coordinator (JoinGroup /
# SyncGroup / Heartbeat) splits the partitions across the two members. Delivery is
# AT LEAST ONCE: every record reaches the group (no loss), but a record consumed
# just before a rebalance and not yet committed is legitimately redelivered after
# it — so duplicates are expected and are NOT a failure.
#
# Run:
#   export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"   # CONNECTORS_KAFKA_ENABLE=true
#   bundle exec ruby consumer-groups/join_rebalance/main.rb

require_relative "../../support/kafka_client"

TOPIC      = KafkaClient.topic("cg", "rebalance")
PARTITIONS = 4
RECORDS    = 40
GROUP      = "kafka-ex-cg-rebalance-#{Process.pid}".freeze

def fail!(msg)
  warn "FAIL: #{msg}"
  exit 1
end

KafkaClient.banner("topic=#{TOPIC} partitions=#{PARTITIONS} group=#{GROUP}")

# Multi-partition topic so the group has something to split.
admin = KafkaClient.admin
admin.create_topic(TOPIC, PARTITIONS, 1).wait(max_wait_timeout: 15)
puts "CreateTopic    -> #{TOPIC} partitions=#{PARTITIONS}"

# Produce RECORDS across all partitions (keys spread by CRC32).
producer = KafkaClient.producer("acks" => "all")
RECORDS.times { |i| producer.produce(topic: TOPIC, payload: "m-#{i}", key: "key-#{i}").wait(max_wait_timeout: 15) }
producer.close
puts "Produce        -> #{RECORDS} records"

# Two members of one group. Both subscribe; the coordinator assigns disjoint
# partition sets. We interleave polls so both get to JoinGroup/SyncGroup.
c1 = KafkaClient.consumer(GROUP, "auto.offset.reset" => "earliest")
c2 = KafkaClient.consumer(GROUP, "auto.offset.reset" => "earliest")
c1.subscribe(TOPIC)
c2.subscribe(TOPIC)

by_member = { c1: [], c2: [] }
seen = {}
deadline = Time.now + 30
# Loop until every UNIQUE record has been observed (not until a fixed poll count):
# under a rebalance a record can be redelivered, so counting raw polls would stop
# early and miss records that only one member still has left to deliver.
while seen.size < RECORDS && Time.now < deadline
  m1 = c1.poll(500)
  if m1
    by_member[:c1] << m1.payload
    seen[m1.payload] = true
  end
  m2 = c2.poll(500)
  if m2
    by_member[:c2] << m2.payload
    seen[m2.payload] = true
  end
end

# Snapshot each member's partition assignment for the log.
a1 = c1.assignment.to_h.fetch(TOPIC, []).map(&:partition).sort
a2 = c2.assignment.to_h.fetch(TOPIC, []).map(&:partition).sort
puts "Assignment     -> member1 partitions=#{a1.inspect} member2 partitions=#{a2.inspect}"
puts "Consumed       -> member1=#{by_member[:c1].size} member2=#{by_member[:c2].size}"

c1.close
c2.close

# No LOSS: the union of both members' records covers every produced record. This is
# AT-LEAST-ONCE — a record consumed-but-not-yet-committed before the rebalance is
# legitimately redelivered afterward, so duplicates are expected and NOT a failure.
all_seen = (by_member[:c1] + by_member[:c2])
expected = (0...RECORDS).map { |i| "m-#{i}" }
dups = all_seen.size - all_seen.uniq.size
fail!("record LOST across rebalance: unique #{all_seen.uniq.size}/#{RECORDS}") \
  unless all_seen.uniq.sort == expected.sort
puts "Duplicates     -> #{dups} (at-least-once: duplicates across a rebalance are expected)" if dups.positive?
# Both members did real work (partitions were actually distributed).
fail!("partitions were not distributed across both members") \
  if by_member[:c1].empty? || by_member[:c2].empty?
puts "Assert         -> all #{RECORDS} records delivered AT LEAST ONCE (no loss), split across 2 members"

admin.delete_topic(TOPIC).wait(max_wait_timeout: 15)
admin.close
puts "DeleteTopic    -> ok"

puts "PASS: 2-member group rebalanced; every record delivered at least once (no loss)"
exit 0
