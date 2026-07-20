package io.kubemq.examples.kafka.shared;

import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Builds native Apache Kafka clients pointed at the KubeMQ Kafka connector.
 *
 * <p>Every example reads the single {@code KUBEMQ_KAFKA_BOOTSTRAP} environment
 * variable (default {@code localhost:9092}) and uses it as the Kafka
 * {@code bootstrap.servers} value. It is a {@code host:port} bootstrap list, NOT
 * a URL — hence {@code _BOOTSTRAP}, the honest analog of {@code bootstrap.servers}
 * (SHARED-CONVENTIONS §4.1).
 *
 * <p><b>The connector is DISABLED by default (gotcha #1):</b> start the server
 * with {@code CONNECTORS_KAFKA_ENABLE=true} (unlike AMQP/MQTT). For external
 * clients the server must also set {@code CONNECTORS_KAFKA_ADVERTISED_HOST}, else
 * clients connect then hang (gotcha #2, the M-23 footgun).
 *
 * <p>No SASL/TLS by default (SHARED-CONVENTIONS §4.3); only
 * {@code security/sasl-plain-scram} adds credentials. Topics map to KubeMQ
 * Events-Store channels {@code kafka.<topic>} (§4.2).
 */
public final class KafkaClients {

    /** Default connector bootstrap endpoint (plain-TCP Kafka port 9092). */
    public static final String DEFAULT_BOOTSTRAP = "localhost:9092";

    private KafkaClients() {
    }

    /** Resolves {@code KUBEMQ_KAFKA_BOOTSTRAP} (default {@link #DEFAULT_BOOTSTRAP}). */
    public static String bootstrap() {
        String b = System.getenv("KUBEMQ_KAFKA_BOOTSTRAP");
        return (b != null && !b.isBlank()) ? b : DEFAULT_BOOTSTRAP;
    }

    /** Base String/String producer props; callers override acks/idempotence/etc. */
    public static Properties producerProps() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return p;
    }

    /** String/String producer with {@code acks=all} (safe default; gotcha #3). */
    public static KafkaProducer<String, String> producer() {
        Properties p = producerProps();
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(p);
    }

    /**
     * Base consumer props for a group. {@code auto.offset.reset=earliest},
     * manual commit off by default ({@code enable.auto.commit=false}) so examples
     * control offset semantics explicitly.
     */
    public static Properties consumerProps(String groupId) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return p;
    }

    /** String/String consumer in {@code groupId} reading from earliest. */
    public static KafkaConsumer<String, String> consumer(String groupId) {
        return new KafkaConsumer<>(consumerProps(groupId));
    }

    /** A fresh unique group id so reruns start clean (avoids stale committed offsets). */
    public static String freshGroup(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    /**
     * A fresh unique topic name so reruns start from a clean log. DeleteTopics on the
     * connector does NOT purge the underlying channel data, so a re-created same-name
     * topic inherits old records/offsets — any offset- or count-based assertion must run
     * against a UNIQUE topic each time to be reliably re-runnable.
     */
    public static String freshTopic(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    /** AdminClient for topic/partition/config operations. */
    public static Admin admin() {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap());
        return Admin.create(p);
    }
}
