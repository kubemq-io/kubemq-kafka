package io.kubemq.examples.kafka.transactions.readcommitted;

import io.kubemq.examples.kafka.shared.Check;
import io.kubemq.examples.kafka.shared.KafkaClients;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;

/**
 * transactions: read-committed — an open transaction is invisible to read_committed,
 * and the Last Stable Offset (LSO) sits below the high-water mark while it is open.
 *
 * <p>Flow: a transactional producer commits a baseline of B records, then opens a
 * SECOND transaction, produces U records and {@code flush}es them to the log but does
 * NOT commit. While that txn is open:
 * <ul>
 *   <li>{@code listOffsets(latest, READ_UNCOMMITTED)} = HWM = base + B + U (the
 *       uncommitted records are physically in the log);</li>
 *   <li>{@code listOffsets(latest, READ_COMMITTED)} = LSO = base + B (read_committed
 *       stops at the first open transaction) — so LSO &lt; HWM;</li>
 *   <li>a {@code read_committed} consumer sees only the B committed records, never the
 *       U uncommitted ones.</li>
 * </ul>
 * We then {@code abortTransaction}; the consumer still never sees the aborted records.
 *
 * <p><b>gotcha #12:</b> {@code read_committed} filtering is done CLIENT-SIDE from the
 * {@code AbortedTransactions} list the broker returns in the Fetch response — there is
 * no server-side record filter. <b>gotcha #9:</b> KIP-890 V2 is not implemented
 * (V1-only transaction coordinator). {@code UNSTABLE_OFFSET_COMMIT(88)} may surface for
 * offset reads over an open txn (see docs/reference/error-codes.md). Mirrors
 * {@code connectors/kafka/} txn/offset path.
 */
public final class Main {

    private static final String TOPIC = "kafka-ex-txn-readcommitted-java";
    private static final String TXN_ID = "kafka-ex.txn-readcommitted.java";
    private static final int BASELINE = 3;
    private static final int UNCOMMITTED = 4;

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        System.out.println("bootstrap.servers = " + KafkaClients.bootstrap());
        TopicPartition tp = new TopicPartition(TOPIC, 0);

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

            String run = Long.toString(System.currentTimeMillis());
            Set<String> committedValues = new HashSet<>();
            Set<String> uncommittedValues = new HashSet<>();

            Properties p = KafkaClients.producerProps();
            p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TXN_ID);
            p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            p.put(ProducerConfig.ACKS_CONFIG, "all");

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
                producer.initTransactions();

                // ---- Baseline: commit B records. ----
                producer.beginTransaction();
                for (int i = 0; i < BASELINE; i++) {
                    String v = "committed-" + run + "-" + i;
                    committedValues.add(v);
                    producer.send(new ProducerRecord<>(TOPIC, "k", v));
                }
                producer.commitTransaction();
                System.out.println("Committed baseline of " + BASELINE + " records");

                long earliest = admin.listOffsets(Map.of(tp, OffsetSpec.earliest()))
                        .all().get().get(tp).offset();
                long expectedLso = earliest + BASELINE;

                // ---- Open a second txn, flush but DO NOT commit. ----
                producer.beginTransaction();
                for (int i = 0; i < UNCOMMITTED; i++) {
                    String v = "uncommitted-" + run + "-" + i;
                    uncommittedValues.add(v);
                    producer.send(new ProducerRecord<>(TOPIC, "k", v));
                }
                producer.flush(); // records reach the log; txn still open
                System.out.println("Opened a txn with " + UNCOMMITTED + " uncommitted (flushed) records");

                // ---- LSO (read_committed) < HWM (read_uncommitted). ----
                long hwm = admin.listOffsets(Map.of(tp, OffsetSpec.latest()),
                                new ListOffsetsOptions(IsolationLevel.READ_UNCOMMITTED))
                        .all().get().get(tp).offset();
                long lso = admin.listOffsets(Map.of(tp, OffsetSpec.latest()),
                                new ListOffsetsOptions(IsolationLevel.READ_COMMITTED))
                        .all().get().get(tp).offset();
                System.out.println("HWM(read_uncommitted)=" + hwm + " LSO(read_committed)=" + lso);
                Check.that(lso < hwm, "LSO sits below HWM while the transaction is open");
                Check.equal(expectedLso, lso, "LSO equals the first offset of the open txn");
                Check.equal(earliest + BASELINE + UNCOMMITTED, hwm, "HWM includes the uncommitted records");

                // ---- read_committed consumer sees only the committed baseline. ----
                Properties cp = KafkaClients.consumerProps(KafkaClients.freshGroup("txn-readcommitted"));
                cp.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
                Set<String> seen = new HashSet<>();
                try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cp)) {
                    consumer.subscribe(List.of(TOPIC));
                    long deadline = System.currentTimeMillis() + 12_000;
                    while (System.currentTimeMillis() < deadline) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                        for (ConsumerRecord<String, String> r : records) {
                            if (r.value() != null && r.value().contains(run)) {
                                seen.add(r.value());
                            }
                        }
                        // Once we have the baseline and polls go quiet, stop; the open
                        // txn records must never appear.
                        if (seen.size() >= BASELINE && records.isEmpty()) {
                            break;
                        }
                    }
                }
                System.out.println("read_committed saw: " + seen);
                for (String v : committedValues) {
                    Check.that(seen.contains(v), "committed baseline visible: " + v);
                }
                for (String v : uncommittedValues) {
                    Check.that(!seen.contains(v), "open-txn record invisible to read_committed: " + v);
                }

                // ---- Abort: the records are gone for good. ----
                producer.abortTransaction();
                System.out.println("abortTransaction() -> the " + UNCOMMITTED + " records are discarded");
            }
        }

        System.out.println("OK: read_committed never saw the open/aborted txn; LSO < HWM while open");
    }
}
