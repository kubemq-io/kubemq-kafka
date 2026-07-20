package io.kubemq.examples.kafka.transactions.eoscommitabort;

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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;

/**
 * transactions: eos-commit-abort — a committed transaction is visible and an aborted
 * one is absent under {@code read_committed}.
 *
 * <p>Flow: a transactional producer ({@code transactional.id} set) calls
 * {@code initTransactions} (InitProducerId with a transactional PID/epoch). It then
 * runs two transactions: the first produces "committed" records and calls
 * {@code commitTransaction}; the second produces "aborted" records and calls
 * {@code abortTransaction}. A {@code read_committed} consumer then reads the topic and
 * must see EVERY committed record and NONE of the aborted ones.
 *
 * <p>Kafka wire flow: InitProducerId -&gt; (per txn) AddPartitionsToTxn -&gt; Produce
 * -&gt; EndTxn(commit|abort). API key EndTxn (§2.4). <b>KIP-890 ceiling (gotcha #9):</b>
 * the connector implements the transaction-coordinator V1 protocol only — it does NOT
 * implement KIP-890 V2 (the hardened "transaction verification" epoch-bump path). Use
 * transactions within that V1 envelope; do not rely on V2-only guarantees. <b>gotcha
 * #7:</b> a {@code /} in {@code transactional.id} is rejected as INVALID_REQUEST(42) —
 * use dots/hyphens (as below). <b>gotcha #8:</b> committing consumer offsets inside a
 * txn additionally needs Group WRITE. Mirrors {@code connectors/kafka/} txn path.
 */
public final class Main {

    private static final String TOPIC = "kafka-ex-txn-eos-java";
    // gotcha #7: dots/hyphens only in transactional.id — never '/'.
    private static final String TXN_ID = "kafka-ex.txn-eos.java";
    private static final int COMMITTED = 4;
    private static final int ABORTED = 3;

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

        Properties p = KafkaClients.producerProps();
        p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TXN_ID);
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // required for txns
        p.put(ProducerConfig.ACKS_CONFIG, "all");

        Set<String> committedValues = new HashSet<>();
        Set<String> abortedValues = new HashSet<>();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
            producer.initTransactions();
            System.out.println("initTransactions() -> transactional.id=" + TXN_ID);

            // ---- Transaction 1: commit. ----
            producer.beginTransaction();
            for (int i = 0; i < COMMITTED; i++) {
                String v = "committed-" + run + "-" + i;
                committedValues.add(v);
                producer.send(new ProducerRecord<>(TOPIC, "k", v));
            }
            producer.commitTransaction();
            System.out.println("commitTransaction() -> " + COMMITTED + " records");

            // ---- Transaction 2: abort. ----
            producer.beginTransaction();
            for (int i = 0; i < ABORTED; i++) {
                String v = "aborted-" + run + "-" + i;
                abortedValues.add(v);
                producer.send(new ProducerRecord<>(TOPIC, "k", v));
            }
            producer.abortTransaction();
            System.out.println("abortTransaction() -> " + ABORTED + " records discarded");
        }

        // ---- read_committed consumer: committed visible, aborted absent. ----
        Properties cp = KafkaClients.consumerProps(KafkaClients.freshGroup("txn-eos"));
        cp.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        Set<String> seen = new HashSet<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cp)) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.currentTimeMillis() + 20_000;
            // Drain until we have the committed set AND a poll comes back empty (the
            // read_committed LSO is fully consumed). Do NOT stop at COMMITTED count:
            // aborted records live at HIGHER offsets, so a connector that wrongly
            // leaked them would only surface if we keep fetching past the committed run.
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : records) {
                    if (r.value() != null && r.value().contains(run)) {
                        seen.add(r.value());
                    }
                }
                if (seen.size() >= COMMITTED && records.isEmpty()) {
                    break;
                }
            }
        }
        System.out.println("read_committed saw: " + seen);

        for (String v : committedValues) {
            Check.that(seen.contains(v), "committed record visible under read_committed: " + v);
        }
        for (String v : abortedValues) {
            Check.that(!seen.contains(v), "aborted record NOT visible under read_committed: " + v);
        }

        System.out.println("OK: committed txn visible, aborted txn absent (read_committed)");
    }
}
