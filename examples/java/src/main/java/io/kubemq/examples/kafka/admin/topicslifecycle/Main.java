package io.kubemq.examples.kafka.admin.topicslifecycle;

import io.kubemq.examples.kafka.shared.Check;
import io.kubemq.examples.kafka.shared.KafkaClients;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;

/**
 * admin: topics-lifecycle — CreateTopics / DescribeTopics / DescribeConfigs /
 * DescribeCluster / DeleteTopics, plus the invalid-name guard.
 *
 * <p>Flow: {@code createTopics} a fresh 2-partition topic; {@code describeTopics}
 * and assert the partition count; {@code describeConfigs} on the topic resource and
 * print a config; {@code describeCluster} and assert at least one broker node; then
 * {@code deleteTopics} and confirm it is gone. Finally, attempt to create a topic
 * whose name contains {@code ~} and assert the broker rejects it with
 * {@link InvalidTopicException} (gotcha #6, INVALID_TOPIC_EXCEPTION=17).
 *
 * <p>Kafka wire flow: Metadata -&gt; CreateTopics(19) -&gt; (Describe)Metadata -&gt;
 * DescribeConfigs(32) -&gt; DescribeCluster(60) -&gt; DeleteTopics(20). Mirrors
 * {@code connectors/kafka/} admin path.
 */
public final class Main {

    private static final String TOPIC = "kafka-ex-admin-lifecycle-java";
    private static final String BAD_TOPIC = "kafka-ex-admin~bad-java";
    private static final int PARTITIONS = 2;

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        System.out.println("bootstrap.servers = " + KafkaClients.bootstrap());

        try (Admin admin = KafkaClients.admin()) {
            // ---- CreateTopics ----
            try {
                admin.createTopics(List.of(new NewTopic(TOPIC, PARTITIONS, (short) 1))).all().get();
                System.out.println("CreateTopics '" + TOPIC + "' (" + PARTITIONS + " partitions)");
            } catch (Exception e) {
                if (e.getCause() instanceof TopicExistsException) {
                    // A leftover from a previous run — delete and recreate for a clean state.
                    admin.deleteTopics(List.of(TOPIC)).all().get();
                    admin.createTopics(List.of(new NewTopic(TOPIC, PARTITIONS, (short) 1))).all().get();
                    System.out.println("Recreated '" + TOPIC + "' after clearing a leftover");
                } else {
                    throw e;
                }
            }

            // ---- DescribeTopics ----
            Map<String, TopicDescription> described =
                    admin.describeTopics(List.of(TOPIC)).allTopicNames().get();
            TopicDescription td = described.get(TOPIC);
            Check.that(td != null, "describeTopics returned the topic");
            System.out.println("DescribeTopics '" + TOPIC + "' -> partitions=" + td.partitions().size());
            Check.equal(PARTITIONS, td.partitions().size(), "described partition count matches");

            // ---- DescribeConfigs ----
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC);
            Config config = admin.describeConfigs(List.of(resource)).all().get().get(resource);
            Check.that(config != null && !config.entries().isEmpty(), "describeConfigs returned entries");
            System.out.println("DescribeConfigs -> " + config.entries().size() + " entries (e.g. "
                    + firstConfigName(config) + ")");

            // ---- DescribeCluster ----
            DescribeClusterResult cluster = admin.describeCluster();
            int nodes = cluster.nodes().get().size();
            String clusterId = cluster.clusterId().get();
            System.out.println("DescribeCluster -> clusterId=" + clusterId + " nodes=" + nodes);
            Check.that(nodes >= 1, "cluster reports at least one broker node");

            // ---- DeleteTopics ----
            admin.deleteTopics(List.of(TOPIC)).all().get();
            System.out.println("DeleteTopics '" + TOPIC + "'");
            // Verify absence via a FULL-CLUSTER listTopics enumeration, NOT a
            // single-topic describeTopics: the connector answers a single-topic
            // metadata request with a synthetic entry (it reports any requested name
            // as present), so describeTopics can never observe a deletion. Retry to
            // absorb the brief metadata-propagation lag after DeleteTopics.
            boolean gone = false;
            long goneDeadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < goneDeadline) {
                if (!admin.listTopics().names().get().contains(TOPIC)) {
                    gone = true;
                    break;
                }
                Thread.sleep(200);
            }
            Check.that(gone, "topic is gone after DeleteTopics");

            // ---- Invalid name guard: '~' -> InvalidTopicException (gotcha #6). ----
            boolean rejected = false;
            try {
                admin.createTopics(List.of(new NewTopic(BAD_TOPIC, 1, (short) 1))).all().get();
            } catch (Exception e) {
                Throwable cause = (e.getCause() != null) ? e.getCause() : e;
                rejected = cause instanceof InvalidTopicException;
                System.out.println("CreateTopics '" + BAD_TOPIC + "' rejected -> "
                        + cause.getClass().getSimpleName());
            }
            Check.that(rejected, "topic name with '~' rejected as INVALID_TOPIC_EXCEPTION");
        }

        System.out.println("OK: topic lifecycle + invalid-name guard verified");
    }

    private static String firstConfigName(Config config) {
        return config.entries().iterator().next().name();
    }
}
