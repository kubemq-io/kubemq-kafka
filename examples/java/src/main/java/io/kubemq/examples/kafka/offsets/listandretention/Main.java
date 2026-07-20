package io.kubemq.examples.kafka.offsets.listandretention;

import io.kubemq.examples.kafka.shared.Check;
import io.kubemq.examples.kafka.shared.KafkaClients;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;

/**
 * offsets: list-and-retention — ListOffsets (earliest/latest/by-timestamp) and
 * retention config.
 *
 * <p>Flow: create a single-partition topic that carries {@code retention.ms} and
 * {@code retention.bytes} at creation time; produce N records spaced in real
 * wall-clock time. Then assert, via {@code Admin.listOffsets}:
 * <ul>
 *   <li>{@code OffsetSpec.earliest()} tracks the log-start offset (base);</li>
 *   <li>{@code OffsetSpec.latest()} tracks the high-water mark (base + N);</li>
 *   <li>{@code OffsetSpec.forTimestamp(ts)} resolves to the first offset whose record
 *       was APPENDED at/after ts.</li>
 * </ul>
 * and confirm the retention config was accepted via {@code describeConfigs}.
 *
 * <p><b>Fresh topic per run (gotcha: DeleteTopics does not purge channel data).</b>
 * The {@code earliest == base} assertion only holds on a log that starts empty; a
 * REUSED same-name topic keeps {@code earliest} at the old log-start while {@code base}
 * advances past it every rerun. So each run creates a UNIQUE topic
 * ({@link KafkaClients#freshTopic}).
 *
 * <p><b>by-timestamp indexes server APPEND time</b>, not a client {@code CreateTime}.
 * So records are spaced in real time and the query uses a real wall-clock instant
 * captured between two appends (a query against fabricated past CreateTimes resolves
 * to offset 0, because every record is appended at roughly "now").
 *
 * <p>Kafka wire flow: Metadata -&gt; CreateTopics(configs) -&gt; Produce -&gt;
 * ListOffsets(earliest|latest|by-ts). retention.ms/bytes map to the Events-Store
 * channel MaxAge/MaxBytes/MaxMsgs (§2.2). Mirrors {@code connectors/kafka/} offset path.
 */
public final class Main {

    private static final String TOPIC_PREFIX = "kafka-ex-offsets-retention-java";
    private static final int N = 6;
    // The record whose append we query for by timestamp (0-based).
    private static final int PIVOT = 3;
    private static final long STEP_MS = 150L;

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        System.out.println("bootstrap.servers = " + KafkaClients.bootstrap());
        String topic = KafkaClients.freshTopic(TOPIC_PREFIX);
        TopicPartition tp = new TopicPartition(topic, 0);

        try (Admin admin = KafkaClients.admin()) {
            Map<String, String> retention = Map.of(
                    "retention.ms", "86400000",       // 1 day
                    "retention.bytes", "1073741824"); // 1 GiB
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1).configs(retention)))
                    .all().get();
            System.out.println("CreateTopics '" + topic + "' with retention.ms/retention.bytes");

            // Produce N records spaced in REAL wall-clock time (default append-time
            // stamping), capturing an instant strictly between rec-(PIVOT-1) and
            // rec-(PIVOT) so forTimestamp(queryTs) resolves to base + PIVOT.
            long base = -1;
            long queryTs = -1;
            try (KafkaProducer<String, String> producer = KafkaClients.producer()) {
                for (int i = 0; i < N; i++) {
                    if (i == PIVOT) {
                        queryTs = System.currentTimeMillis();
                        Thread.sleep(60); // rec-PIVOT appended strictly after queryTs
                    }
                    RecordMetadata md = producer.send(
                            new ProducerRecord<>(topic, 0, "k", "o-" + i)).get();
                    if (base < 0) {
                        base = md.offset();
                    }
                    Thread.sleep(STEP_MS);
                }
            }
            System.out.println("Produced " + N + " records from base offset " + base);

            // ---- ListOffsets: earliest / latest / by-timestamp. ----
            long earliest = admin.listOffsets(Map.of(tp, OffsetSpec.earliest()))
                    .all().get().get(tp).offset();
            long latest = admin.listOffsets(Map.of(tp, OffsetSpec.latest()))
                    .all().get().get(tp).offset();
            System.out.println("ListOffsets earliest=" + earliest + " latest=" + latest);
            Check.equal(base, earliest, "earliest tracks the log-start offset");
            Check.equal(base + N, latest, "latest tracks the high-water mark");

            ListOffsetsResult byTs = admin.listOffsets(Map.of(tp, OffsetSpec.forTimestamp(queryTs)));
            long tsOffset = byTs.all().get().get(tp).offset();
            System.out.println("ListOffsets forTimestamp(" + queryTs + ") -> offset=" + tsOffset);
            Check.equal(base + PIVOT, tsOffset, "by-timestamp resolves to the first record at/after ts");

            // ---- Confirm retention config was accepted. ----
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Config cfg = admin.describeConfigs(List.of(resource)).all().get().get(resource);
            Check.that(cfg.get("retention.ms") != null, "retention.ms present in topic config");
            Check.that(cfg.get("retention.bytes") != null, "retention.bytes present in topic config");
            System.out.println("Retention config accepted: retention.ms="
                    + cfg.get("retention.ms").value()
                    + " retention.bytes=" + cfg.get("retention.bytes").value());
        }

        System.out.println("OK: ListOffsets earliest/latest/by-timestamp + retention config verified");
    }
}
