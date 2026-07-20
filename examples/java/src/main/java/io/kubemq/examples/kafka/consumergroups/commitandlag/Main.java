package io.kubemq.examples.kafka.consumergroups.commitandlag;

import io.kubemq.examples.kafka.shared.Check;
import io.kubemq.examples.kafka.shared.KafkaClients;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;

/**
 * consumer-groups: commit-and-lag — commit offsets, resume from the commit, and
 * compute lag from Admin offsets.
 *
 * <p>Flow: produce M records to a single partition. Consumer #1 reads the first K,
 * then {@code commitSync} the committed offset K (OffsetCommit). A brand-new consumer
 * #2 in the SAME group then {@code subscribe}s and resumes from offset K — it reads
 * exactly the remaining M-K records, and its first record's offset is K (OffsetFetch
 * on join). Independently, we compute consumer-group LAG from the Admin API:
 * {@code listConsumerGroupOffsets} gives the committed offset, {@code listOffsets}
 * (latest) gives the high-water mark (HWM), and lag = HWM - committed = M - K.
 *
 * <p>Kafka wire flow: Produce -&gt; Fetch -&gt; OffsetCommit(K) -&gt; (new consumer)
 * OffsetFetch -&gt; Fetch from K. API keys 8/9 (§2.3). Lag is derived from an Admin
 * offsets diff; scraping {@code kubemq_kafka_consumer_group_lag} is doc-only. Mirrors
 * {@code connectors/kafka/} group-offset path.
 */
public final class Main {

    private static final String TOPIC = "kafka-ex-cg-commit-java";
    private static final int M = 10;
    private static final int K = 4;

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        System.out.println("bootstrap.servers = " + KafkaClients.bootstrap());
        TopicPartition tp = new TopicPartition(TOPIC, 0);
        String group = KafkaClients.freshGroup("cg-commit-lag");

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

            // Produce M records; capture the base offset so assertions are absolute.
            long base;
            try (KafkaProducer<String, String> producer = KafkaClients.producer()) {
                long first = -1;
                for (int i = 0; i < M; i++) {
                    var md = producer.send(new ProducerRecord<>(TOPIC, "k", "m-" + i)).get();
                    if (first < 0) {
                        first = md.offset();
                    }
                }
                base = first;
            }
            System.out.println("Produced " + M + " records from base offset " + base);

            // ---- Consumer #1: read K, then commit offset (base+K). ----
            long committedTarget = base + K;
            try (KafkaConsumer<String, String> c1 = KafkaClients.consumer(group)) {
                c1.assign(List.of(tp));
                c1.seek(tp, base);
                int read = 0;
                long deadline = System.currentTimeMillis() + 15_000;
                while (read < K && System.currentTimeMillis() < deadline) {
                    ConsumerRecords<String, String> records = c1.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> r : records) {
                        if (read < K) {
                            read++;
                        }
                    }
                }
                Check.equal(K, read, "consumer #1 read the first K records");
                c1.commitSync(Map.of(tp, new OffsetAndMetadata(committedTarget)));
                System.out.println("[c1] committed offset " + committedTarget);
            }

            // ---- Lag from Admin: HWM - committed. ----
            long committed = admin.listConsumerGroupOffsets(group)
                    .partitionsToOffsetAndMetadata().get().get(tp).offset();
            ListOffsetsResult latest = admin.listOffsets(Map.of(tp, OffsetSpec.latest()));
            long hwm = latest.all().get().get(tp).offset();
            long lag = hwm - committed;
            System.out.println("Admin: committed=" + committed + " HWM=" + hwm + " lag=" + lag);
            Check.equal(committedTarget, committed, "committed offset matches what consumer #1 wrote");
            Check.equal((long) (M - K), lag, "lag (HWM - committed) equals the unread records");

            // ---- Consumer #2: same group resumes from the committed offset. ----
            int resumed = 0;
            long firstResumedOffset = -1;
            try (KafkaConsumer<String, String> c2 = KafkaClients.consumer(group)) {
                c2.subscribe(List.of(TOPIC));
                long deadline = System.currentTimeMillis() + 15_000;
                while (resumed < (M - K) && System.currentTimeMillis() < deadline) {
                    ConsumerRecords<String, String> records = c2.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> r : records) {
                        if (firstResumedOffset < 0) {
                            firstResumedOffset = r.offset();
                        }
                        resumed++;
                    }
                }
            }
            System.out.println("[c2] resumed at offset " + firstResumedOffset
                    + " and read " + resumed + " records");
            Check.equal(committedTarget, firstResumedOffset, "consumer #2 resumed exactly at the commit");
            Check.equal(M - K, resumed, "consumer #2 read exactly the remaining records");
        }

        System.out.println("OK: commit/resume works and lag matches HWM - committed");
    }
}
