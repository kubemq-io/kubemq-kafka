package io.kubemq.examples.kafka.admin.partitionsandconfigs;

import io.kubemq.examples.kafka.shared.Check;
import io.kubemq.examples.kafka.shared.KafkaClients;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.TopicExistsException;

/**
 * admin: partitions-and-configs — CreatePartitions (increase-only), an incremental
 * config change, DeleteRecords (low-end truncation), and the bad-increase guard.
 *
 * <p>Flow: create a 2-partition topic; {@code createPartitions} increaseTo(3) and
 * assert the topic now has 3 partitions; {@code incrementalAlterConfigs} SET
 * {@code retention.ms} and assert the value took (🟡 subset-only: many keys are
 * no-ops on the connector, but the accepted subset applies); produce records to
 * partition 0 and {@code deleteRecords} beforeOffset(base+2), then assert the
 * earliest offset advanced (🟡 low-end truncation only). Finally, assert an INVALID
 * partition change (increaseTo the SAME count) is rejected with
 * {@link InvalidPartitionsException}.
 *
 * <p>Kafka wire flow: Metadata -&gt; CreatePartitions -&gt; IncrementalAlterConfigs
 * -&gt; Produce -&gt; DeleteRecords -&gt; ListOffsets(earliest). CreatePartitions is
 * increase-only and capped at 256 (§2.4). Mirrors {@code connectors/kafka/} admin path.
 */
public final class Main {

    private static final String TOPIC = "kafka-ex-admin-partitions-java";

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        System.out.println("bootstrap.servers = " + KafkaClients.bootstrap());
        TopicPartition tp0 = new TopicPartition(TOPIC, 0);

        try (Admin admin = KafkaClients.admin()) {
            recreate(admin, TOPIC, 2);
            System.out.println("CreateTopics '" + TOPIC + "' (2 partitions)");

            // ---- CreatePartitions: increase 2 -> 3. ----
            admin.createPartitions(Map.of(TOPIC, NewPartitions.increaseTo(3))).all().get();
            TopicDescription td = admin.describeTopics(List.of(TOPIC)).allTopicNames().get().get(TOPIC);
            System.out.println("CreatePartitions increaseTo(3) -> now " + td.partitions().size());
            Check.equal(3, td.partitions().size(), "partition count increased to 3");

            // ---- IncrementalAlterConfigs: SET retention.ms (subset applies, 🟡). ----
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC);
            String retentionMs = "3600000";
            AlterConfigOp op = new AlterConfigOp(
                    new ConfigEntry("retention.ms", retentionMs), AlterConfigOp.OpType.SET);
            admin.incrementalAlterConfigs(Map.of(resource, List.of(op))).all().get();
            Config after = admin.describeConfigs(List.of(resource)).all().get().get(resource);
            String observed = after.get("retention.ms") != null ? after.get("retention.ms").value() : null;
            System.out.println("IncrementalAlterConfigs retention.ms -> " + observed);
            Check.equal(retentionMs, observed, "retention.ms config change applied");

            // ---- Produce then DeleteRecords beforeOffset(base+2) (low-end trunc, 🟡). ----
            long base;
            try (KafkaProducer<String, String> producer = KafkaClients.producer()) {
                long first = -1;
                for (int i = 0; i < 5; i++) {
                    var md = producer.send(new ProducerRecord<>(TOPIC, 0, "k", "r-" + i)).get();
                    if (first < 0) {
                        first = md.offset();
                    }
                }
                base = first;
            }
            long deleteBefore = base + 2;
            admin.deleteRecords(Map.of(tp0, RecordsToDelete.beforeOffset(deleteBefore))).all().get();
            long earliest = admin.listOffsets(Map.of(tp0, OffsetSpec.earliest()))
                    .all().get().get(tp0).offset();
            System.out.println("DeleteRecords beforeOffset(" + deleteBefore
                    + ") -> earliest offset now " + earliest);
            Check.equal(deleteBefore, earliest, "low-end truncation advanced the earliest offset");

            // ---- Bad increase: same count -> InvalidPartitionsException. ----
            boolean rejected = false;
            try {
                admin.createPartitions(Map.of(TOPIC, NewPartitions.increaseTo(3))).all().get();
            } catch (Exception e) {
                Throwable cause = (e.getCause() != null) ? e.getCause() : e;
                rejected = cause instanceof InvalidPartitionsException;
                System.out.println("createPartitions increaseTo(3) again rejected -> "
                        + cause.getClass().getSimpleName());
            }
            Check.that(rejected, "non-increasing partition change rejected as INVALID_PARTITIONS");
        }

        System.out.println("OK: partitions increase + config change + truncation + bad-increase guard");
    }

    private static void recreate(Admin admin, String topic, int partitions) throws Exception {
        try {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
        } catch (Exception e) {
            if (e.getCause() instanceof TopicExistsException) {
                admin.deleteTopics(List.of(topic)).all().get();
                // Small settle loop: recreate once the delete has propagated.
                for (int i = 0; i < 20; i++) {
                    try {
                        admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
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
            } else {
                throw e;
            }
        }
    }
}
