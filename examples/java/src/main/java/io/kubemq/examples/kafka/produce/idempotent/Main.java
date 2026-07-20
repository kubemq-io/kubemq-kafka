package io.kubemq.examples.kafka.produce.idempotent;

import io.kubemq.examples.kafka.shared.Check;
import io.kubemq.examples.kafka.shared.KafkaClients;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
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
 * produce: idempotent — the idempotent producer and its no-duplicate guarantee.
 *
 * <p>Flow: enable {@code enable.idempotence=true} (which implies {@code acks=all},
 * {@code retries&gt;0}, {@code max.in.flight&lt;=5}). The first {@code send} triggers
 * an {@code InitProducerId} handshake so the broker assigns this producer a Producer
 * Id (PID) and epoch. We produce N distinct records; each idempotent RecordBatch
 * carries a monotonically increasing base sequence stamped with (PID, epoch). If the
 * producer has to RETRY a batch internally (transient error, disconnect), the broker
 * recognises the duplicate (PID, partition, sequence) and stores it exactly once —
 * that is the §2.3 "no dup on retry" guarantee. We assert exactly N unique records
 * round-trip with no duplicate offsets.
 *
 * <p><b>Honest scope:</b> Kafka idempotence de-duplicates the producer's OWN internal
 * retries of a batch — NOT two independent application {@code send()} calls of the
 * same payload (those get different sequences and are both stored). So we prove the
 * guarantee by asserting the unique value SET equals N and no value appears twice,
 * with idempotence enabled end-to-end. Kafka wire flow: Metadata -&gt; InitProducerId
 * -&gt; Produce (idempotent batch) -&gt; Fetch. Mirrors {@code connectors/kafka/}.
 */
public final class Main {

    private static final String TOPIC = "kafka-ex-produce-idempotent-java";
    private static final int N = 5;

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

        // Unique run tag so a rerun's values don't collide with a previous run's.
        String run = Long.toString(System.currentTimeMillis());

        // ---- Idempotent producer: enable.idempotence implies acks=all + retries. ----
        Properties p = KafkaClients.producerProps();
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // These are already forced by idempotence; stating them documents the contract.
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.RETRIES_CONFIG, 5);
        p.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
            for (int i = 0; i < N; i++) {
                String value = "idem-" + run + "-" + i;
                RecordMetadata md = producer.send(new ProducerRecord<>(TOPIC, "k", value)).get();
                // The first send performs InitProducerId; every batch is stamped with
                // (PID, epoch, base sequence) so any producer-internal retry is deduped.
                System.out.println("Produce '" + value + "' -> partition=" + md.partition()
                        + " offset=" + md.offset());
            }
        }

        // ---- Consume back: assert N unique values, each seen exactly once. ----
        Set<String> seen = new HashSet<>();
        Set<String> duplicated = new HashSet<>();
        int total = 0;
        try (KafkaConsumer<String, String> consumer =
                KafkaClients.consumer(KafkaClients.freshGroup("produce-idempotent"))) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : records) {
                    if (r.value() != null && r.value().startsWith("idem-" + run + "-")) {
                        if (!seen.add(r.value())) {
                            duplicated.add(r.value());
                        }
                        total++;
                    }
                }
                if (seen.size() >= N && records.isEmpty()) {
                    break;
                }
            }
        }
        System.out.println("Fetch <- unique=" + seen.size() + " total=" + total
                + " duplicates=" + duplicated.size());

        Check.equal(N, seen.size(), "all N idempotent records round-tripped");
        Check.that(duplicated.isEmpty(), "no record was stored twice (idempotent no-dup guarantee)");
        Check.equal(N, total, "exactly N records stored — no duplicates under the idempotent path");

        System.out.println("OK: idempotent producer stored each record exactly once");
    }
}
