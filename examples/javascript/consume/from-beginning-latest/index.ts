/**
 * consume/from-beginning-latest — auto.offset.reset earliest vs latest.
 *
 * Produces a batch of "pre-existing" records, then starts two fresh consumer groups:
 *   - one subscribed with { fromBeginning: true }  (== auto.offset.reset=earliest)
 *   - one subscribed with { fromBeginning: false } (== auto.offset.reset=latest)
 * The earliest consumer sees the pre-existing records; the latest consumer sees only
 * records produced AFTER it joined. Fetch is the long-poll bounded read (key 1).
 *
 * Kafka topic "kafka-ex-consume-reset" <-> Events-Store channel "kafka.kafka-ex-consume-reset".
 *
 * Run: npx tsx consume/from-beginning-latest/index.ts
 */
import { Kafka } from 'kafkajs';
import { newKafka, newProducer, newAdmin, bootstrap, assert, sleep, runExample } from '../../shared/client.js';

const TOPIC = 'kafka-ex-consume-reset';

async function main(): Promise<void> {
  const kafka: Kafka = newKafka();
  console.log(`Connecting to KubeMQ Kafka connector at ${bootstrap()} (topic "${TOPIC}")`);

  const admin = newAdmin(kafka);
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: TOPIC, numPartitions: 1 }], waitForLeaders: true });

  const producer = newProducer(kafka);
  await producer.connect();

  // Pre-existing records (before either consumer subscribes).
  const pre = ['pre-1', 'pre-2', 'pre-3'];
  await producer.send({ topic: TOPIC, acks: -1, messages: pre.map((v) => ({ value: v })) });
  console.log(`Produced ${pre.length} pre-existing records: [${pre.join(', ')}]`);

  // Earliest consumer — should replay the pre-existing records.
  const earliestSeen: string[] = [];
  const earliest = kafka.consumer({ groupId: `reset-earliest-${Date.now()}` });
  await earliest.connect();
  await earliest.subscribe({ topic: TOPIC, fromBeginning: true });
  await earliest.run({ eachMessage: async ({ message }) => { earliestSeen.push(message.value?.toString() ?? ''); } });

  // Latest consumer — should NOT see the pre-existing records, only post-join ones.
  const latestSeen: string[] = [];
  const latest = kafka.consumer({ groupId: `reset-latest-${Date.now()}` });
  await latest.connect();
  await latest.subscribe({ topic: TOPIC, fromBeginning: false });
  await latest.run({ eachMessage: async ({ message }) => { latestSeen.push(message.value?.toString() ?? ''); } });

  // Let the latest consumer establish its committed position at the log end before we produce more.
  await sleep(2000);

  const post = ['post-1', 'post-2'];
  await producer.send({ topic: TOPIC, acks: -1, messages: post.map((v) => ({ value: v })) });
  console.log(`Produced ${post.length} post-subscribe records: [${post.join(', ')}]`);

  // Wait for propagation.
  const deadline = Date.now() + 10_000;
  while ((earliestSeen.length < pre.length + post.length || latestSeen.length < post.length) && Date.now() < deadline) {
    await sleep(100);
  }

  await earliest.stop(); await earliest.disconnect();
  await latest.stop(); await latest.disconnect();
  await producer.disconnect();
  await admin.deleteTopics({ topics: [TOPIC] });
  await admin.disconnect();

  console.log(`earliest consumer saw: [${earliestSeen.join(', ')}]`);
  console.log(`latest   consumer saw: [${latestSeen.join(', ')}]`);

  // earliest sees pre + post; latest sees only post.
  for (const v of pre) assert(earliestSeen.includes(v), `earliest consumer missed pre-existing record ${v}`);
  for (const v of post) assert(earliestSeen.includes(v), `earliest consumer missed post record ${v}`);
  for (const v of pre) assert(!latestSeen.includes(v), `latest consumer wrongly saw pre-existing record ${v}`);
  for (const v of post) assert(latestSeen.includes(v), `latest consumer missed post record ${v}`);

  console.log('\nOffset reset proven: fromBeginning=true replays history; fromBeginning=false starts at the log end');
}

runExample(main);
