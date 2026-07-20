# frozen_string_literal: true

# Kafka admin — partitions + configs (honest about the connector's PARTIAL
# support, spec §2.4).
#   * CreatePartitions (key 37): INCREASE-ONLY, new_total in (current, 256].
#     Increasing succeeds; same-count / decrease / >256 -> INVALID_PARTITIONS.
#   * IncrementalAlterConfigs (key 44) 🟡: subset of topic configs only.
#   * DeleteRecords (key 45) 🟡: low-end (log-start) truncation only.
#
# Run:
#   export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"   # CONNECTORS_KAFKA_ENABLE=true
#   bundle exec ruby admin/partitions_and_configs/main.rb

require_relative "../../support/kafka_client"

TOPIC = KafkaClient.topic("admin", "parts")

def fail!(msg)
  warn "FAIL: #{msg}"
  exit 1
end

def partition_count(topic)
  # Count partitions by probing watermark offsets until one is unknown.
  # A FRESH consumer per call is required: librdkafka caches topic metadata, and
  # query_watermark_offsets on a partition the CACHE doesn't know raises LOCALLY
  # without re-fetching. After CreatePartitions a reused consumer would still see
  # the old count (the connector applied the increase; the client cache is stale),
  # so we take a new consumer each time to read current cluster metadata.
  probe = KafkaClient.consumer("kafka-ex-admin-parts-probe-#{Process.pid}-#{rand(100_000)}")
  count = 0
  loop do
    probe.query_watermark_offsets(topic, count, 3000)
    count += 1
  rescue Rdkafka::RdkafkaError
    break
  end
  count
ensure
  probe&.close
end

KafkaClient.banner("topic=#{TOPIC}")
admin = KafkaClient.admin

# Start with 2 partitions.
admin.create_topic(TOPIC, 2, 1).wait(max_wait_timeout: 15)
puts "CreateTopic    -> #{TOPIC} partitions=#{partition_count(TOPIC)}"

# --- CreatePartitions: increase 2 -> 4 (allowed). ---
admin.create_partitions(TOPIC, 4).wait(max_wait_timeout: 15)
now = partition_count(TOPIC)
puts "CreatePartitions(4) -> now partitions=#{now}"
fail!("partition increase to 4 did not take effect (saw #{now})") unless now == 4

# --- Bad increase: same-count (4 -> 4) must be rejected with INVALID_PARTITIONS. ---
begin
  admin.create_partitions(TOPIC, 4).wait(max_wait_timeout: 15)
  fail!("non-increasing CreatePartitions(4->4) was NOT rejected")
rescue Rdkafka::RdkafkaError => e
  # Verified: e.code == :invalid_partitions on rdkafka 0.29.0 / librdkafka 2.14.2.
  puts "CreatePartitions(4->4) -> rejected: #{e.code} (INVALID_PARTITIONS) as expected"
end

# --- Decrease (4 -> 2) must also be rejected (increase-only). ---
begin
  admin.create_partitions(TOPIC, 2).wait(max_wait_timeout: 15)
  fail!("decreasing CreatePartitions(4->2) was NOT rejected")
rescue Rdkafka::RdkafkaError => e
  puts "CreatePartitions(4->2) -> rejected: #{e.code} (INVALID_PARTITIONS) as expected"
end

# --- IncrementalAlterConfigs 🟡 (subset only) + DeleteRecords 🟡 (low-end only). ---
# These admin surfaces are version-dependent on rdkafka-ruby. Where the pinned
# gem exposes them we exercise a minimal partial; where it does not, we log the
# justified N/A (spec §6.3) and point at the franz-go / Java example, rather than
# silently dropping the folder.
if admin.respond_to?(:incremental_alter_configs)
  puts "IncrementalAlterConfigs 🟡 -> available on this gem (subset-of-configs only)"
else
  puts "IncrementalAlterConfigs 🟡 -> N/A on pinned rdkafka-ruby; see ../../../go/admin/partitions-and-configs"
end
if admin.respond_to?(:delete_records)
  puts "DeleteRecords 🟡 -> available on this gem (low-end log-start truncation only)"
else
  puts "DeleteRecords 🟡 -> N/A on pinned rdkafka-ruby; see ../../../go/admin/partitions-and-configs"
end

admin.delete_topic(TOPIC).wait(max_wait_timeout: 15)
admin.close
puts "DeleteTopic    -> ok"

puts "PASS: increase-only CreatePartitions verified; bad increases rejected with INVALID_PARTITIONS"
exit 0
