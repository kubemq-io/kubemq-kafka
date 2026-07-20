package io.kubemq.examples.kafka.produce.basicacks;

import io.kubemq.examples.kafka.shared.Check;
import io.kubemq.examples.kafka.shared.KafkaClients;
import java.time.Duration;
import java.util.List;
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
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TopicExistsException;

/**
 * produce: basic-acks — Produce at acks 0/1/all and prove the connector's
 * MESSAGE_TOO_LARGE guard.
 *
 * <p>Flow: CreateTopics(kafka-ex-produce-acks-java, 1 partition) -&gt; produce one
 * record each at acks=0, acks=1, acks=all -&gt; consume from earliest and assert all
 * three round-trip in order -&gt; produce a &gt;1 MiB record and assert the broker
 * rejects it with MESSAGE_TOO_LARGE (Java surfaces this as
 * {@link RecordTooLargeException}).
 *
 * <p>Kafka wire flow: Metadata -&gt; (CreateTopics) -&gt; Produce (RecordBatch v2)
 * -&gt; Fetch -&gt; OffsetCommit skipped (we assign, not subscribe). Topic
 * {@code orders} maps to Events-Store channel {@code kafka.orders} (§4.2). Mirrors
 * connector behavior in {@code connectors/kafka/} (produce path). acks&gt;=1 is
 * required on a multi-node broker (gotcha #3): an acks=0 send to a follower is
 * dropped silently.
 */
public final class Main {

    // §4.2 topic naming: kafka-ex-<family>-<short>, language-suffixed so the live
    // examples do not collide with the other 6 language suites on a shared connector.
    private static final String TOPIC = "kafka-ex-produce-acks-java";
    // Connector MaxMessageBytes is 1 MiB and its request frame cap is
    // MaxMessageBytes + 1 MiB slack = 2 MiB. A 1.5 MiB record sits ABOVE the 1 MiB
    // cap but BELOW the 2 MiB frame cap, so it reaches the broker's produce path and
    // is rejected with MESSAGE_TOO_LARGE. A 2 MiB record would instead overflow the
    // frame cap and surface as a transport error, not MESSAGE_TOO_LARGE.
    private static final int OVERSIZED_BYTES = (1024 * 1024) + (512 * 1024); // 1.5 MiB

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        System.out.println("bootstrap.servers = " + KafkaClients.bootstrap());

        // ---- Admin: ensure the topic exists (auto-create also works on Produce). ----
        try (Admin admin = KafkaClients.admin()) {
            try {
                admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1)))
                        .all().get();
                System.out.println("CreateTopics '" + TOPIC + "' (1 partition)");
            } catch (Exception e) {
                if (e.getCause() instanceof TopicExistsException) {
                    System.out.println("Topic '" + TOPIC + "' already exists — reusing");
                } else {
                    throw e;
                }
            }
        }

        // ---- Produce one record at each acks level. ----
        for (String acks : List.of("0", "1", "all")) {
            Properties p = KafkaClients.producerProps();
            p.put(ProducerConfig.ACKS_CONFIG, acks);
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
                String value = "acks=" + acks;
                RecordMetadata md = producer.send(
                        new ProducerRecord<>(TOPIC, "k", value)).get();
                // acks=0 returns a metadata record with offset -1 (no broker ack);
                // acks>=1 returns the assigned offset.
                System.out.println("Produce acks=" + acks
                        + " -> partition=" + md.partition() + " offset=" + md.offset());
            }
        }

        // ---- Consume from the beginning and assert the 3 records round-tripped. ----
        try (KafkaConsumer<String, String> consumer =
                KafkaClients.consumer(KafkaClients.freshGroup("produce-basic-acks"))) {
            consumer.subscribe(List.of(TOPIC));
            int seen = 0;
            long deadline = System.currentTimeMillis() + 15_000;
            while (seen < 3 && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : records) {
                    System.out.println("Fetch <- offset=" + r.offset()
                            + " key=" + r.key() + " value=" + r.value());
                    seen++;
                }
            }
            Check.that(seen >= 3, "all three acks-level records round-tripped (saw " + seen + ")");
        }

        // ---- Oversized record must be rejected with MESSAGE_TOO_LARGE. ----
        Properties big = KafkaClients.producerProps();
        big.put(ProducerConfig.ACKS_CONFIG, "all");
        // Let the oversized record leave the client so the BROKER is the one that
        // rejects it: raise the client-side ceiling above the connector's 1 MiB.
        big.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 8 * 1024 * 1024);
        big.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 16L * 1024 * 1024);
        boolean rejected = false;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(big)) {
            String payload = "x".repeat(OVERSIZED_BYTES);
            producer.send(new ProducerRecord<>(TOPIC, "big", payload)).get();
        } catch (Exception e) {
            // ExecutionException wraps RecordTooLargeException (error code
            // MESSAGE_TOO_LARGE = 10) returned by the connector's produce path.
            Throwable cause = (e.getCause() != null) ? e.getCause() : e;
            rejected = cause instanceof RecordTooLargeException;
            System.out.println("Oversized produce rejected -> " + cause.getClass().getSimpleName());
        }
        Check.that(rejected, "oversized record rejected with MESSAGE_TOO_LARGE / RecordTooLargeException");

        System.out.println("OK: produce basic-acks round-trip + MESSAGE_TOO_LARGE guard");
    }
}
