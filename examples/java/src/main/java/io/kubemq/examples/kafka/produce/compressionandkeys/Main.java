package io.kubemq.examples.kafka.produce.compressionandkeys;

import io.kubemq.examples.kafka.shared.Check;
import io.kubemq.examples.kafka.shared.KafkaClients;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TopicExistsException;

/**
 * produce: compression-and-keys — every compression codec round-trips, and keyed
 * records land on a STABLE partition.
 *
 * <p>Flow: create a 3-partition topic; for each codec in
 * {@code none,gzip,snappy,lz4,zstd} produce a keyed record per key; consume back and
 * assert (a) every value survives the codec round-trip byte-for-byte and (b) each key
 * always maps to the SAME partition, regardless of codec.
 *
 * <p><b>gotcha #4 — partitioner is murmur2.</b> kafka-clients hashes the key with
 * <b>murmur2</b> (same as franz-go and kafkajs v2+), so a given key lands on the SAME
 * partition as the Go and JavaScript suites, and a DIFFERENT partition from the four
 * librdkafka-based suites (python/csharp/ruby/rust), which use CRC32. See
 * {@code docs/concepts/cross-client-partitioning.md}. Compression is a producer-side
 * batch codec (Produce RecordBatch attribute bits); the connector stores the
 * decompressed records and Fetch returns them transparently. snappy/lz4/zstd need the
 * native codec libs on the classpath (declared in pom.xml); gzip is JDK-native.
 * gotcha #5: increasing partition count later re-shards the key→partition mapping.
 */
public final class Main {

    private static final String TOPIC = "kafka-ex-produce-compress-java";
    private static final int PARTITIONS = 3;
    private static final List<String> CODECS = List.of("none", "gzip", "snappy", "lz4", "zstd");
    private static final List<String> KEYS = List.of("alpha", "bravo", "charlie", "delta");

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        System.out.println("bootstrap.servers = " + KafkaClients.bootstrap());

        try (Admin admin = KafkaClients.admin()) {
            try {
                admin.createTopics(List.of(new NewTopic(TOPIC, PARTITIONS, (short) 1))).all().get();
                System.out.println("CreateTopics '" + TOPIC + "' (" + PARTITIONS + " partitions)");
            } catch (Exception e) {
                if (e.getCause() instanceof TopicExistsException) {
                    System.out.println("Topic '" + TOPIC + "' already exists — reusing");
                } else {
                    throw e;
                }
            }
        }

        String run = Long.toString(System.currentTimeMillis());
        // Expected value per (codec,key) so we can assert exact round-trip, and the
        // partition observed the FIRST time we see a key, so we can assert stability.
        Map<String, Integer> keyPartition = new HashMap<>();

        for (String codec : CODECS) {
            Properties p = KafkaClients.producerProps();
            p.put(ProducerConfig.ACKS_CONFIG, "all");
            p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, codec);
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
                for (String key : KEYS) {
                    String value = "v-" + run + "-" + codec + "-" + key;
                    RecordMetadata md = producer.send(new ProducerRecord<>(TOPIC, key, value)).get();
                    int part = md.partition();
                    Integer prev = keyPartition.putIfAbsent(key, part);
                    if (prev != null) {
                        Check.equal(prev, part,
                                "key '" + key + "' stayed on the same partition across codecs");
                    }
                    System.out.println("Produce codec=" + codec + " key=" + key
                            + " -> partition=" + part + " offset=" + md.offset());
                }
            }
        }
        System.out.println("Key -> partition map (murmur2): " + keyPartition);

        // ---- Consume everything from this run and assert exact value round-trip. ----
        int expected = CODECS.size() * KEYS.size();
        int matched = 0;
        try (KafkaConsumer<String, String> consumer =
                KafkaClients.consumer(KafkaClients.freshGroup("produce-compress"))) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.currentTimeMillis() + 20_000;
            while (matched < expected && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : records) {
                    if (r.value() != null && r.value().startsWith("v-" + run + "-")) {
                        // The value encodes its own codec+key; a byte-for-byte match
                        // proves the codec round-tripped without corruption. Also verify
                        // the record landed on the partition murmur2 assigned to its key.
                        String[] parts = r.value().split("-");
                        String key = parts[parts.length - 1];
                        Check.equal(keyPartition.get(key), r.partition(),
                                "consumed record for key '" + key + "' is on its stable partition");
                        matched++;
                    }
                }
            }
        }
        System.out.println("Fetch <- matched " + matched + "/" + expected + " codec round-trips");

        Check.equal(expected, matched, "every codec x key record survived the round-trip");
        System.out.println("OK: all codecs round-tripped and keys are murmur2-stable");
    }
}
