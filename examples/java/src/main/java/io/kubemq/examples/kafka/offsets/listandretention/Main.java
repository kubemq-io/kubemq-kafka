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
import org.apache.kafka.common.errors.TopicExistsException;

/**
 * offsets: list-and-retention — ListOffsets (earliest/latest/by-timestamp) and
 * retention config.
 *
 * <p>Flow: create a single-partition topic that carries {@code retention.ms} and
 * {@code retention.bytes} at creation time; produce N records with explicit, evenly
 * spaced timestamps. Then assert, via {@code Admin.listOffsets}:
 * <ul>
 *   <li>{@code OffsetSpec.earliest()} tracks the log-start offset (base);</li>
 *   <li>{@code OffsetSpec.latest()} tracks the high-water mark (base + N);</li>
 *   <li>{@code OffsetSpec.forTimestamp(ts)} resolves to the first offset whose record
 *       timestamp is &gt;= ts.</li>
 * </ul>
 * and confirm the retention config was accepted via {@code describeConfigs}.
 *
 * <p>Kafka wire flow: Metadata -&gt; CreateTopics(configs) -&gt; Produce -&gt;
 * ListOffsets(earliest|latest|by-ts). retention.ms/bytes map to the Events-Store
 * channel MaxAge/MaxBytes/MaxMsgs (§2.2). Time-based expiry is not raced in a fast
 * example — we assert the config is accepted plus the offset semantics. Mirrors
 * {@code connectors/kafka/} offset path.
 */
public final class Main {

    private static final String TOPIC = "kafka-ex-offsets-retention-java";
    private static final int N = 6;
    private static final long TS_STEP_MS = 1000L;

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        System.out.println("bootstrap.servers = " + KafkaClients.bootstrap());
        TopicPartition tp = new TopicPartition(TOPIC, 0);

        try (Admin admin = KafkaClients.admin()) {
            Map<String, String> retention = Map.of(
                    "retention.ms", "86400000",       // 1 day
                    "retention.bytes", "1073741824"); // 1 GiB
            // Recreate a FRESH topic each run so this run's first produced offset IS the
            // log-start offset. The earliest==base assertion below only holds on a topic
            // that starts empty: a REUSED topic keeps earliest at the old log-start while
            // base advances past it on every rerun, which would fail the check spuriously
            // even against a healthy connector. Mirrors admin/partitions-and-configs.
            recreate(admin, TOPIC, retention);
            System.out.println("CreateTopics '" + TOPIC + "' with retention.ms/retention.bytes");

            // Produce N records with explicit timestamps.
            long baseTs = System.currentTimeMillis() - (N * TS_STEP_MS);
            long base = -1;
            long[] tsByIndex = new long[N];
            try (KafkaProducer<String, String> producer = KafkaClients.producer()) {
                for (int i = 0; i < N; i++) {
                    long ts = baseTs + i * TS_STEP_MS;
                    tsByIndex[i] = ts;
                    RecordMetadata md = producer.send(
                            new ProducerRecord<>(TOPIC, 0, ts, "k", "o-" + i)).get();
                    if (base < 0) {
                        base = md.offset();
                    }
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

            long queryTs = tsByIndex[3];
            ListOffsetsResult byTs = admin.listOffsets(Map.of(tp, OffsetSpec.forTimestamp(queryTs)));
            long tsOffset = byTs.all().get().get(tp).offset();
            System.out.println("ListOffsets forTimestamp(" + queryTs + ") -> offset=" + tsOffset);
            Check.equal(base + 3, tsOffset, "by-timestamp resolves to the first record at/after ts");

            // ---- Confirm retention config was accepted. ----
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC);
            Config cfg = admin.describeConfigs(List.of(resource)).all().get().get(resource);
            Check.that(cfg.get("retention.ms") != null, "retention.ms present in topic config");
            Check.that(cfg.get("retention.bytes") != null, "retention.bytes present in topic config");
            System.out.println("Retention config accepted: retention.ms="
                    + cfg.get("retention.ms").value()
                    + " retention.bytes=" + cfg.get("retention.bytes").value());
        }

        System.out.println("OK: ListOffsets earliest/latest/by-timestamp + retention config verified");
    }

    /** Creates {@code topic} fresh (deleting a leftover first) so it starts empty. */
    private static void recreate(Admin admin, String topic, Map<String, String> configs)
            throws Exception {
        NewTopic newTopic = new NewTopic(topic, 1, (short) 1).configs(configs);
        try {
            admin.createTopics(List.of(newTopic)).all().get();
        } catch (Exception e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw e;
            }
            admin.deleteTopics(List.of(topic)).all().get();
            // Small settle loop: recreate once the delete has propagated.
            for (int i = 0; i < 20; i++) {
                try {
                    admin.createTopics(List.of(newTopic)).all().get();
                    return;
                } catch (Exception again) {
                    if (again.getCause() instanceof TopicExistsException) {
                        Thread.sleep(250);
                    } else {
                        throw again;
                    }
                }
            }
            throw e;
        }
    }
}
