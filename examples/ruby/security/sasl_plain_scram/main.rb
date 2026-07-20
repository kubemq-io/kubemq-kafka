# frozen_string_literal: true

# Kafka SASL auth — SASL/PLAIN and SCRAM-SHA-256/512 over the plaintext transport
# (security.protocol=sasl_plaintext). Authenticated produce+consume round-trips;
# wrong credentials (or an unauthorized topic) fail with an authorization/auth
# error (*_AUTHORIZATION_FAILED / SASL authentication failure).
#
# RUNNABLE ONLY against a broker started with a Kafka credential store
# (spec §4.7). Credentials come from the environment; if none are set this script
# prints setup instructions and exits 0 (nothing to assert without a cred store).
#
# TLS / mTLS on :9093 (security.protocol=ssl or sasl_ssl) is DOC-ONLY here: it
# needs broker certificates this repo does not ship. See
# ../../../../docs/guides/security-sasl-tls.md for the TLS config.
#
# Run (example, PLAIN):
#   export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
#   export KAFKA_SASL_MECHANISM="PLAIN"           # or SCRAM-SHA-256 / SCRAM-SHA-512
#   export KAFKA_SASL_USERNAME="app" KAFKA_SASL_PASSWORD="s3cret"
#   bundle exec ruby security/sasl_plain_scram/main.rb

require_relative "../../support/kafka_client"

TOPIC = KafkaClient.topic("security", "sasl")

def fail!(msg)
  warn "FAIL: #{msg}"
  exit 1
end

mechanism = ENV["KAFKA_SASL_MECHANISM"]
username  = ENV["KAFKA_SASL_USERNAME"]
password  = ENV["KAFKA_SASL_PASSWORD"]

KafkaClient.banner("topic=#{TOPIC} mechanism=#{mechanism || "(unset)"}")

if mechanism.nil? || username.nil? || password.nil?
  puts <<~SKIP
    SKIP: no SASL credentials in the environment. This variant needs a broker
    with a Kafka credential store. To run it:
      export KAFKA_SASL_MECHANISM="PLAIN"   # or SCRAM-SHA-256 / SCRAM-SHA-512
      export KAFKA_SASL_USERNAME="<user>"
      export KAFKA_SASL_PASSWORD="<pass>"
    Wrong creds are expected to fail with *_AUTHORIZATION_FAILED / SASL auth error.
    TLS/mTLS (:9093, security.protocol=ssl|sasl_ssl) is documented, not run.
  SKIP
  exit 0
end

sasl = {
  "security.protocol" => "sasl_plaintext",
  "sasl.mechanism" => mechanism, # ⚠ verify: some gem lines use "sasl.mechanisms" (plural)
  "sasl.username" => username,
  "sasl.password" => password
}

# --- Authenticated produce + consume round-trip. ---
producer = KafkaClient.producer(sasl.merge("acks" => "all"))
producer.produce(topic: TOPIC, payload: "authenticated-hello", key: "k").wait(max_wait_timeout: 15)
producer.close
puts "Produce(auth)  -> ok as #{username} via #{mechanism}"

consumer = KafkaClient.consumer("kafka-ex-security-sasl-#{Process.pid}",
                                sasl.merge("auto.offset.reset" => "earliest"))
consumer.subscribe(TOPIC)
seen = nil
deadline = Time.now + 15
while seen.nil? && Time.now < deadline
  msg = consumer.poll(1000)
  seen = msg.payload unless msg.nil?
end
consumer.close
fail!("authenticated consumer saw nothing") if seen.nil?
fail!("payload mismatch: #{seen.inspect}") unless seen == "authenticated-hello"
puts "Consume(auth)  -> #{seen.inspect}"

# --- Negative: wrong password must fail auth. ---
begin
  bad = KafkaClient.producer(sasl.merge("sasl.password" => "#{password}-WRONG", "acks" => "all"))
  bad.produce(topic: TOPIC, payload: "should-not-pass", key: "k").wait(max_wait_timeout: 15)
  bad.close
  fail!("wrong credentials were accepted")
rescue Rdkafka::RdkafkaError => e
  # Expected: an authentication / authorization failure (exact symbol varies:
  # :sasl_authentication_failed, :topic_authorization_failed, :authentication, ...).
  puts "Produce(bad)   -> rejected: #{e.code} (auth/authorization failure) as expected"
end

admin = KafkaClient.admin(sasl)
admin.delete_topic(TOPIC).wait(max_wait_timeout: 15)
admin.close
puts "DeleteTopic    -> ok"

puts "PASS: SASL #{mechanism} auth round-trip ok; wrong creds rejected"
exit 0
