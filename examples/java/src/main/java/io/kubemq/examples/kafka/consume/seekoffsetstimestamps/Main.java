package io.kubemq.examples.kafka.consume.seekoffsetstimestamps;

import io.kubemq.examples.kafka.shared.Check;
import io.kubemq.examples.kafka.shared.KafkaClients;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;

/**
 * consume: seek-offsets-timestamps — {@code seek} by offset and
 * {@code offsetsForTimes} (ListOffsets by-timestamp).
 *
 * <p>Flow: produce N records to a single partition, each with an explicit, evenly
 * spaced record timestamp. Then, on a consumer that {@code assign}s the partition
 * (not {@code subscribe} — so seeks are deterministic, no group coordination):
 * <ol>
 *   <li><b>seek by offset:</b> {@code seek(tp, 2)} then poll; the first record read
 *       must be the record originally produced at offset 2.</li>
 *   <li><b>seek by timestamp:</b> {@code offsetsForTimes(tp -&gt; ts)} resolves the
 *       first offset whose timestamp is &gt;= ts (ListOffsets by-timestamp, §2.3);
 *       we {@code seek} there and assert the first record read is at/after ts.</li>
 * </ol>
 *
 * <p>Kafka wire flow: Metadata -&gt; Produce -&gt; ListOffsets(by-timestamp) -&gt;
 * Fetch from the sought offset. Using {@code assign} keeps positions explicit and
 * repeatable. Mirrors {@code connectors/kafka/} offset/fetch path.
 *
 * <p><b>by-timestamp indexes server APPEND time.</b> The connector's ListOffsets
 * by-timestamp resolves against the wall-clock instant each record was APPENDED, not
 * a client-supplied {@code CreateTime}. So we do NOT fabricate spaced record
 * timestamps (a query against those resolves to offset 0 because every record was
 * appended at roughly "now"); instead we space the sends in real time and capture a
 * real wall-clock instant BETWEEN two appends, then query with that instant.
 */
public final class Main {

    private static final String TOPIC_PREFIX = "kafka-ex-consume-seek-java";
    private static final int N = 6;
    // The record whose append we query for by timestamp (0-based). offsetsForTimes
    // resolves to the first record appended at/after the captured instant = this one.
    private static final int PIVOT = 4;
    private static final long STEP_MS = 150L;

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        System.out.println("bootstrap.servers = " + KafkaClients.bootstrap());

        // A fresh topic each run so the log starts empty (baseOffset==0) and this run's
        // appends are the newest records the by-timestamp query can resolve to.
        String topic = KafkaClients.freshTopic(TOPIC_PREFIX);
        try (Admin admin = KafkaClients.admin()) {
            try {
                admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
                System.out.println("CreateTopics '" + topic + "' (1 partition)");
            } catch (Exception e) {
                if (e.getCause() instanceof TopicExistsException) {
                    System.out.println("Topic '" + topic + "' already exists — reusing");
                } else {
                    throw e;
                }
            }
        }

        // Produce N records spaced in REAL wall-clock time (default CreateTime), and
        // capture a wall-clock instant that falls strictly between the append of
        // rec-(PIVOT-1) and rec-(PIVOT). offsetsForTimes(queryTs) must resolve to PIVOT.
        long baseOffset;
        long queryTs = -1;
        try (KafkaProducer<String, String> producer = KafkaClients.producer()) {
            long firstOffset = -1;
            for (int i = 0; i < N; i++) {
                if (i == PIVOT) {
                    queryTs = System.currentTimeMillis();
                    Thread.sleep(60); // ensure rec-PIVOT is appended strictly after queryTs
                }
                RecordMetadata md = producer.send(
                        new ProducerRecord<>(topic, 0, "k", "rec-" + i)).get();
                if (firstOffset < 0) {
                    firstOffset = md.offset();
                }
                System.out.println("Produce rec-" + i + " -> offset=" + md.offset()
                        + " ts=" + md.timestamp());
                Thread.sleep(STEP_MS); // real spacing so append times are ordered/distinct
            }
            baseOffset = firstOffset;
        }

        TopicPartition tp = new TopicPartition(topic, 0);

        try (KafkaConsumer<String, String> consumer =
                KafkaClients.consumer(KafkaClients.freshGroup("consume-seek"))) {
            consumer.assign(List.of(tp));

            // ---- 1) seek by offset: jump straight to the 3rd produced record. ----
            long targetOffset = baseOffset + 2;
            consumer.seek(tp, targetOffset);
            ConsumerRecord<String, String> first = pollOne(consumer);
            Check.that(first != null, "a record was read after seek(offset)");
            Check.equal(targetOffset, first.offset(), "seek(offset) landed on the exact offset");
            Check.equal("rec-2", first.value(), "seek(offset) returned the 3rd produced record");
            System.out.println("seek(offset=" + targetOffset + ") -> value=" + first.value());

            // ---- 2) seek by timestamp: first record APPENDED at/after queryTs. ----
            Map<TopicPartition, OffsetAndTimestamp> resolved =
                    consumer.offsetsForTimes(Map.of(tp, queryTs));
            OffsetAndTimestamp oat = resolved.get(tp);
            Check.that(oat != null, "offsetsForTimes resolved an offset for the timestamp");
            System.out.println("offsetsForTimes(ts=" + queryTs + ") -> offset=" + oat.offset()
                    + " ts=" + oat.timestamp());
            Check.equal(baseOffset + PIVOT, oat.offset(),
                    "offsetsForTimes resolved to the first record appended at/after queryTs");
            consumer.seek(tp, oat.offset());
            ConsumerRecord<String, String> byTs = pollOne(consumer);
            Check.that(byTs != null, "a record was read after seek(by-timestamp)");
            Check.that(byTs.timestamp() >= queryTs,
                    "by-timestamp seek landed on the first record at/after the timestamp");
            Check.equal("rec-" + PIVOT, byTs.value(), "by-timestamp seek returned the expected record");
            System.out.println("seek(by-ts) -> value=" + byTs.value() + " ts=" + byTs.timestamp());
        }

        System.out.println("OK: seek by offset and by timestamp both landed correctly");
    }

    /** Polls until one record is available or a short deadline elapses. */
    private static ConsumerRecord<String, String> pollOne(KafkaConsumer<String, String> consumer) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> r : records) {
                return r;
            }
        }
        return null;
    }
}
