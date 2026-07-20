/**
 * produce/basic-acks — Produce with acks 0/1/all + oversized -> MESSAGE_TOO_LARGE.
 *
 * Produces the same payload three times at acks=all (-1), acks=1 (leader), and
 * acks=0 (fire-and-forget), then fetches them back to prove round-trip. Finally
 * produces an oversized record and asserts the connector rejects it with
 * MESSAGE_TOO_LARGE (MaxMessageBytes default 1 MiB, §2.7).
 *
 * Kafka topic "kafka-ex-produce-acks" <-> Events-Store channel "kafka.kafka-ex-produce-acks".
 * Mirrors connector behavior in connectors/kafka/ (Produce key 0; RecordBatch v2, acks 0/1/all).
 *
 * Gotcha #3: on a multi-node broker, acks=0 on a follower can silently drop — use
 * acks>=1 for any delivery guarantee.
 *
 * Run: npx tsx produce/basic-acks/index.ts
 */
import { Kafka } from 'kafkajs';
import {
  newKafka,
  newProducer,
  newAdmin,
  bootstrap,
  channelForTopic,
  assert,
  runExample,
} from '../../shared/client.js';

const TOPIC = 'kafka-ex-produce-acks';

async function main(): Promise<void> {
  const kafka: Kafka = newKafka();
  console.log(
    `Connecting to KubeMQ Kafka connector at ${bootstrap()} ` +
      `(topic "${TOPIC}" -> channel "${channelForTopic(TOPIC)}")`,
  );

  const admin = newAdmin(kafka);
  await admin.connect();
  // Auto-create is on (Metadata/Produce), but create explicitly for a deterministic start.
  await admin.createTopics({ topics: [{ topic: TOPIC, numPartitions: 1 }], waitForLeaders: true });

  const producer = newProducer(kafka);
  await producer.connect();

  // 1..3: produce the same value at each ack level. acks: -1 = all, 1 = leader, 0 = none.
  const ackLevels: Array<{ label: string; acks: -1 | 0 | 1 }> = [
    { label: 'all', acks: -1 },
    { label: 'leader', acks: 1 },
    { label: 'none', acks: 0 },
  ];
  for (const { label, acks } of ackLevels) {
    const md = await producer.send({
      topic: TOPIC,
      acks,
      messages: [{ value: `order#42 acks=${label}` }],
    });
    // acks=0 returns no offset metadata (fire-and-forget); acks>=1 returns baseOffset.
    const base = md[0]?.baseOffset;
    console.log(`Produce acks=${label.padEnd(6)} -> partition=${md[0]?.partition ?? 0} baseOffset=${base ?? '(none, acks=0)'}`);
    if (acks !== 0) {
      assert(base !== undefined && base !== null, `acks=${label} should return a baseOffset (offset = STAN Sequence)`);
    }
  }

  // 4: fetch the acks>=1 records back to prove round-trip (a short-lived consumer from beginning).
  const consumer = kafka.consumer({ groupId: `kafka-ex-produce-acks-verify-${Date.now()}` });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
  const seen: string[] = [];
  await consumer.run({
    eachMessage: async ({ message }) => {
      const v = message.value?.toString() ?? '';
      if (v.startsWith('order#42')) seen.push(v);
    },
  });
  // Wait until the two guaranteed (acks>=1) records are read back (acks=0 is best-effort).
  const deadline = Date.now() + 10_000;
  while (seen.filter((v) => v.includes('acks=all') || v.includes('acks=leader')).length < 2 && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 100));
  }
  await consumer.stop();
  await consumer.disconnect();
  console.log(`Fetch          -> read back ${seen.length} record(s); acks>=1 records present`);
  assert(seen.some((v) => v.includes('acks=all')), 'acks=all record not read back');
  assert(seen.some((v) => v.includes('acks=leader')), 'acks=leader record not read back');

  // 5: oversized record -> MESSAGE_TOO_LARGE. MaxMessageBytes default 1 MiB (§2.7).
  // 1.5 MiB: over the 1 MiB message cap but UNDER the 2 MiB frame cap (MaxMessageBytes +
  // 1 MiB), so the record actually reaches the broker and comes back as a clean
  // MESSAGE_TOO_LARGE protocol error — a full 2 MiB record instead overflows the frame
  // cap and dies as a raw connection reset (EPIPE/ECONNRESET) before the broker replies.
  const oversized = Buffer.alloc(1572864, 0x41); // 1.5 MiB: > 1 MiB msg cap, < 2 MiB frame cap
  let rejected = false;
  try {
    await producer.send({ topic: TOPIC, acks: -1, messages: [{ value: oversized }] });
  } catch (err) {
    // kafkajs surfaces a KafkaJSProtocolError with .type === 'MESSAGE_TOO_LARGE'
    const t = (err as { type?: string }).type ?? String(err);
    rejected = t === 'MESSAGE_TOO_LARGE' || /MESSAGE_TOO_LARGE|too large/i.test(String(err));
    console.log(`Produce 1.5MiB -> rejected: ${t}`);
  }
  assert(rejected, 'oversized record was NOT rejected with MESSAGE_TOO_LARGE');

  // cleanup
  await producer.disconnect();
  await admin.deleteTopics({ topics: [TOPIC] });
  await admin.disconnect();

  console.log('\nRound-trip complete: acks 0/1/all produced, acks>=1 read back, oversized rejected OK');
}

runExample(main);
