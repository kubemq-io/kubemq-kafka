package io.kubemq.examples.kafka.consume.frombeginninglatest;

import io.kubemq.examples.kafka.shared.Check;
import io.kubemq.examples.kafka.shared.KafkaClients;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;

/**
 * consume: from-beginning-latest — {@code auto.offset.reset} earliest vs latest.
 *
 * <p>Flow: produce a batch of "pre" records; then two consumers in DISTINCT fresh
 * groups (so each applies its own reset) subscribe to the topic — one with
 * {@code auto.offset.reset=earliest}, one with {@code latest}. The earliest consumer
 * must see the pre-existing records. The latest consumer is positioned at the log end
 * on join, so after we produce a batch of "post" records it must see ONLY those, never
 * the "pre" ones.
 *
 * <p>Kafka wire flow: Metadata -&gt; (FindCoordinator/JoinGroup/SyncGroup) -&gt;
 * ListOffsets(earliest|latest) to resolve the reset -&gt; Fetch (long-poll). Fresh
 * unique group ids are essential: a reused group id "resumes" from its committed
 * offset and the reset never applies. Mirrors {@code connectors/kafka/} fetch path.
 */
public final class Main {

    private static final String TOPIC = "kafka-ex-consume-reset-java";
    private static final int PRE = 3;
    private static final int POST = 4;

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        System.out.println("bootstrap.servers = " + KafkaClients.bootstrap());

        try (Admin admin = KafkaClients.admin()) {
            try {
                admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
                System.out.println("CreateTopics '" + TOPIC + "' (1 partition)");
            } catch (Exception e) {
                if (e.getCause() instanceof TopicExistsException) {
                    System.out.println("Topic '" + TOPIC + "' already exists — reusing");
                } else {
                    throw e;
                }
            }
        }

        String run = Long.toString(System.currentTimeMillis());

        try (KafkaProducer<String, String> producer = KafkaClients.producer()) {
            for (int i = 0; i < PRE; i++) {
                producer.send(new ProducerRecord<>(TOPIC, "k", "pre-" + run + "-" + i)).get();
            }
            System.out.println("Produced " + PRE + " 'pre' records");

            // ---- earliest consumer: must see the pre-existing records. ----
            int earliestSeen = 0;
            try (KafkaConsumer<String, String> earliest = KafkaClients.consumer(
                    KafkaClients.freshGroup("consume-earliest"))) {
                earliest.subscribe(List.of(TOPIC));
                long deadline = System.currentTimeMillis() + 15_000;
                while (earliestSeen < PRE && System.currentTimeMillis() < deadline) {
                    ConsumerRecords<String, String> records = earliest.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> r : records) {
                        if (r.value() != null && r.value().startsWith("pre-" + run + "-")) {
                            earliestSeen++;
                        }
                    }
                }
                System.out.println("[earliest] saw " + earliestSeen + " pre records");
            }
            Check.that(earliestSeen >= PRE, "earliest consumer saw the pre-existing records");

            // ---- latest consumer: position at end BEFORE the post batch. ----
            Properties latestProps = KafkaClients.consumerProps(KafkaClients.freshGroup("consume-latest"));
            latestProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            Set<String> latestValues = new HashSet<>();
            try (KafkaConsumer<String, String> latest = new KafkaConsumer<>(latestProps)) {
                latest.subscribe(List.of(TOPIC));
                // Poll until the group is assigned + positioned at the log end. These
                // polls return nothing (reset=latest) but establish the fetch position.
                long assignDeadline = System.currentTimeMillis() + 10_000;
                while (latest.assignment().isEmpty() && System.currentTimeMillis() < assignDeadline) {
                    latest.poll(Duration.ofMillis(300));
                }
                latest.poll(Duration.ofMillis(500)); // settle the position at end

                // Now produce the 'post' batch — only these should be visible.
                for (int i = 0; i < POST; i++) {
                    producer.send(new ProducerRecord<>(TOPIC, "k", "post-" + run + "-" + i)).get();
                }
                producer.flush();
                System.out.println("Produced " + POST + " 'post' records after latest joined");

                long deadline = System.currentTimeMillis() + 15_000;
                while (latestValues.size() < POST && System.currentTimeMillis() < deadline) {
                    ConsumerRecords<String, String> records = latest.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> r : records) {
                        if (r.value() != null) {
                            latestValues.add(r.value());
                        }
                    }
                }
                System.out.println("[latest] saw values: " + latestValues);
            }

            boolean sawAnyPre = latestValues.stream().anyMatch(v -> v.startsWith("pre-" + run + "-"));
            long postCount = latestValues.stream().filter(v -> v.startsWith("post-" + run + "-")).count();
            Check.that(!sawAnyPre, "latest consumer never saw a pre-existing record");
            Check.equal((long) POST, postCount, "latest consumer saw exactly the post records");
        }

        System.out.println("OK: earliest replays history, latest sees only new records");
    }
}
