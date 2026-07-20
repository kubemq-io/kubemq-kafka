/**
 * consumer-groups/join-rebalance — two members share a topic; partitions rebalance; no loss.
 *
 * Creates a 2-partition topic and joins TWO consumers to the SAME group. The group
 * coordinator (Join/Sync/Heartbeat/Leave — keys 11/14/12/13) assigns one partition to each
 * member. We observe the assignment via the GROUP_JOIN event, then produce a keyed batch and
 * assert every produced record is consumed AT LEAST ONCE across the two members (no loss,
 * both members cover the group). Duplicates across a rebalance are valid at-least-once
 * behavior (a record consumed-but-not-committed before a rebalance is legitimately redelivered),
 * so they are reported for information, NOT asserted away.
 *
 * The topic/group are unique per run: DeleteTopics does not purge the connector's Events-Store
 * channel, so a reused topic would replay prior runs' records (fromBeginning) and inflate the
 * counts. A fresh topic makes fromBeginning read exactly this run's records.
 *
 * Kafka topic "kafka-ex-cg-rebalance-<run>" <-> Events-Store channel "kafka.kafka-ex-cg-rebalance-<run>".
 *
 * Run: npx tsx consumer-groups/join-rebalance/index.ts
 */
import { Kafka } from 'kafkajs';
import { newKafka, newProducer, newConsumer, newAdmin, bootstrap, assert, sleep, runExample } from '../../shared/client.js';

const RUN = Date.now();
const TOPIC = `kafka-ex-cg-rebalance-${RUN}`; // fresh per run so fromBeginning reads only this run
const GROUP = `kafka-ex-cg-rebalance-grp-${RUN}`;
const PARTITIONS = 2;

async function main(): Promise<void> {
  const kafka: Kafka = newKafka();
  console.log(`Connecting to KubeMQ Kafka connector at ${bootstrap()} (topic "${TOPIC}", ${PARTITIONS} partitions, group "${GROUP}")`);

  const admin = newAdmin(kafka);
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: TOPIC, numPartitions: PARTITIONS }], waitForLeaders: true });

  // Two members of the same group.
  const assignments: Record<string, number[]> = { m1: [], m2: [] };
  const consumedBy: Record<string, string[]> = { m1: [], m2: [] };

  const makeMember = async (name: string) => {
    const c = newConsumer(kafka, GROUP);
    c.on(c.events.GROUP_JOIN, (e) => {
      const parts = e.payload.memberAssignment[TOPIC] ?? [];
      assignments[name] = parts;
      console.log(`${name} GROUP_JOIN -> assigned partitions [${parts.join(', ')}]`);
    });
    await c.connect();
    await c.subscribe({ topic: TOPIC, fromBeginning: true });
    await c.run({ eachMessage: async ({ message }) => { consumedBy[name].push(message.value?.toString() ?? ''); } });
    return c;
  };

  const m1 = await makeMember('m1');
  await sleep(1500); // let m1 own both partitions first
  const m2 = await makeMember('m2'); // triggers a rebalance -> partitions split
  await sleep(3000); // allow the rebalance to settle

  // Produce keyed records spread across both partitions (after the rebalance settles).
  const producer = newProducer(kafka);
  await producer.connect();
  const values = Array.from({ length: 10 }, (_, i) => `msg-${RUN}-${i}`);
  await producer.send({ topic: TOPIC, acks: -1, messages: values.map((v, i) => ({ key: `k${i}`, value: v })) });
  console.log(`Produced ${values.length} records across ${PARTITIONS} partitions`);

  // Wait until every produced value has been consumed at least once (allow extra time for dups).
  const deadline = Date.now() + 15_000;
  const consumedSet = () => new Set([...consumedBy.m1, ...consumedBy.m2].filter((v) => v.startsWith(`msg-${RUN}-`)));
  while (consumedSet().size < values.length && Date.now() < deadline) await sleep(150);

  await m1.stop(); await m1.disconnect();
  await m2.stop(); await m2.disconnect();
  await producer.disconnect();
  await admin.deleteTopics({ topics: [TOPIC] });
  await admin.disconnect();

  const all = [...consumedBy.m1, ...consumedBy.m2].filter((v) => v.startsWith(`msg-${RUN}-`));
  const unique = new Set(all);
  const dups = all.length - unique.size;
  console.log(`m1 consumed ${consumedBy.m1.length}, m2 consumed ${consumedBy.m2.length}, total ${all.length}, unique ${unique.size}, duplicates ${dups} (dups are valid at-least-once)`);

  // Both members joined the group and were assigned a partition (rebalance distributed the load).
  assert(assignments.m1.length >= 1 && assignments.m2.length >= 1, 'rebalance did not distribute partitions to both members');
  // AT-LEAST-ONCE: every produced record was consumed by the group (no loss). Duplicates allowed.
  assert(unique.size === values.length, `expected all ${values.length} records at least once, saw ${unique.size} unique (loss)`);
  for (const v of values) assert(unique.has(v), `record ${v} was never consumed (loss)`);

  console.log('\nRebalance proven: 2 members split the partitions and consumed every record at least once (no loss; duplicates are valid EOS-off behavior)');
}

runExample(main);
