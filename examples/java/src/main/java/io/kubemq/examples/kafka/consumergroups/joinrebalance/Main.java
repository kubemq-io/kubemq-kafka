package io.kubemq.examples.kafka.consumergroups.joinrebalance;

import io.kubemq.examples.kafka.shared.Check;
import io.kubemq.examples.kafka.shared.KafkaClients;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;

/**
 * consumer-groups: join-rebalance — two consumers in one group rebalance a
 * multi-partition topic with no record loss.
 *
 * <p>Flow: create a 2-partition topic and produce M keyed records spread across both
 * partitions. Start consumer A (group {@code G}); it is assigned BOTH partitions.
 * Start consumer B in the same group; the classic group protocol
 * (JoinGroup/SyncGroup/Heartbeat) rebalances so A and B end up with one partition
 * each. Both consumers run on their own thread and pool their consumed values into a
 * shared set. We assert (a) a rebalance actually redistributed the partitions — both
 * members observe an assignment and together cover both partitions — and (b) NO record
 * is lost across the rebalance: the union of consumed values equals the produced set.
 *
 * <p>Kafka wire flow: FindCoordinator -&gt; JoinGroup -&gt; SyncGroup -&gt; Heartbeat
 * -&gt; (rebalance on B's join) -&gt; Fetch; LeaveGroup on close. API keys 10/11/14/12/13
 * (§2.3). <b>Classic protocol only</b> — do NOT set {@code group.protocol=consumer}
 * (KIP-848 next-gen groups are unsupported, §2.6). Mirrors {@code connectors/kafka/}
 * group path. (Offsets are not committed here, so a partition reassigned mid-run may be
 * re-read from earliest — that yields at-least-once, i.e. possible duplicates but never
 * loss, which is exactly what the no-loss assertion checks.)
 */
public final class Main {

    private static final String TOPIC = "kafka-ex-cg-rebalance-java";
    private static final int PARTITIONS = 2;
    private static final int M = 20;

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        System.out.println("bootstrap.servers = " + KafkaClients.bootstrap());

        try (Admin admin = KafkaClients.admin()) {
            try {
                admin.createTopics(List.of(new NewTopic(TOPIC, PARTITIONS, (short) 1))).all().get();
                System.out.println("CreateTopics '" + TOPIC + "' (" + PARTITIONS + " partitions)");
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
            for (int i = 0; i < M; i++) {
                producer.send(new ProducerRecord<>(TOPIC, "key-" + i, "msg-" + run + "-" + i)).get();
            }
            producer.flush();
        }
        System.out.println("Produced " + M + " records across " + PARTITIONS + " partitions");

        String group = KafkaClients.freshGroup("cg-join-rebalance");
        Set<String> collected = ConcurrentHashMap.newKeySet();
        // Partitions each consumer has held at least once (proves the rebalance spread).
        Set<Integer> assignedA = ConcurrentHashMap.newKeySet();
        Set<Integer> assignedB = ConcurrentHashMap.newKeySet();
        AtomicBoolean running = new AtomicBoolean(true);

        Thread a = new Thread(() -> consumeLoop("A", group, run, collected, assignedA, running), "consumer-A");
        a.start();
        // Let A settle as sole member (owning both partitions) before B joins.
        Thread.sleep(4000);
        Thread b = new Thread(() -> consumeLoop("B", group, run, collected, assignedB, running), "consumer-B");
        b.start();

        // Wait until every produced value is collected, or a hard deadline.
        long deadline = System.currentTimeMillis() + 40_000;
        while (collected.size() < M && System.currentTimeMillis() < deadline) {
            Thread.sleep(250);
        }
        running.set(false);
        a.join(10_000);
        b.join(10_000);

        System.out.println("Collected " + collected.size() + "/" + M + " unique values");
        System.out.println("A held partitions " + assignedA + " | B held partitions " + assignedB);

        Check.equal(M, collected.size(), "no record lost across the rebalance");
        Check.that(!assignedA.isEmpty() && !assignedB.isEmpty(),
                "both consumers received a partition assignment (rebalance happened)");
        Set<Integer> union = ConcurrentHashMap.newKeySet();
        union.addAll(assignedA);
        union.addAll(assignedB);
        Check.equal(PARTITIONS, union.size(), "the two members together covered every partition");

        System.out.println("OK: group rebalanced across 2 members with no record loss");
    }

    private static void consumeLoop(String name, String group, String run,
            Set<String> collected, Set<Integer> assigned, AtomicBoolean running) {
        try (KafkaConsumer<String, String> consumer = KafkaClients.consumer(group)) {
            consumer.subscribe(List.of(TOPIC), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> parts) {
                    System.out.println("[" + name + "] revoked " + parts);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> parts) {
                    for (TopicPartition tp : parts) {
                        assigned.add(tp.partition());
                    }
                    System.out.println("[" + name + "] assigned " + parts);
                }
            });
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(400));
                for (ConsumerRecord<String, String> r : records) {
                    if (r.value() != null && r.value().startsWith("msg-" + run + "-")) {
                        collected.add(r.value());
                    }
                }
            }
        }
    }
}
