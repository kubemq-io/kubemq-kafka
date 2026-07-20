/**
 * produce/idempotent — idempotent producer (enable.idempotence) lands each record exactly once.
 *
 * kafkajs `{ idempotent: true }` turns on `enable.idempotence`: the client performs an
 * InitProducerId (key 22) to obtain a Producer ID (PID) + epoch, stamps every RecordBatch
 * with (PID, epoch, base sequence), and the broker dedupes per-(PID, partition, sequence).
 * enable.idempotence forces acks=all and maxInFlightRequests<=5 (we pin 1 for strict order).
 *
 * IMPORTANT — what idempotence does and does NOT dedupe: the (PID, partition, sequence) stamp
 * lets the broker drop a batch that the producer *internally retries* (e.g. after a network
 * blip), so a transient retry cannot create a duplicate. It does NOT dedupe a fresh
 * application-level `send()` of the same content: kafkajs assigns each `send()` new,
 * monotonically increasing sequence numbers (see eosManager.updateSequence), so a second call
 * is a distinct batch that DOES land again. Internal retries cannot be forced deterministically
 * from an example, so this program verifies the idempotent path end-to-end — the keyed batch
 * lands exactly once, each record unique — rather than falsely claiming a re-send is deduped.
 *
 * Kafka topic "kafka-ex-produce-idem" <-> Events-Store channel "kafka.kafka-ex-produce-idem".
 *
 * Run: npx tsx produce/idempotent/index.ts
 */
import { Kafka } from 'kafkajs';
import { newKafka, newProducer, newAdmin, bootstrap, assert, runExample } from '../../shared/client.js';

const TOPIC = 'kafka-ex-produce-idem';
const KEYS = ['a', 'b', 'c', 'd', 'e'];

async function main(): Promise<void> {
  const kafka: Kafka = newKafka();
  console.log(`Connecting to KubeMQ Kafka connector at ${bootstrap()} (topic "${TOPIC}")`);

  const admin = newAdmin(kafka);
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: TOPIC, numPartitions: 1 }], waitForLeaders: true });

  // enable.idempotence => InitProducerId, acks=all forced, per-(PID,partition,seq) dedup.
  const producer = newProducer(kafka, { idempotent: true, maxInFlightRequests: 1 });
  await producer.connect();

  const messages = KEYS.map((k) => ({ key: k, value: `order-${k}` }));

  await producer.send({ topic: TOPIC, messages });
  console.log(`Produced ${messages.length} idempotent records (PID assigned via InitProducerId)`);

  // Consume everything back.
  const consumer = kafka.consumer({ groupId: `kafka-ex-produce-idem-verify-${Date.now()}` });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
  const seen: string[] = [];
  await consumer.run({
    eachMessage: async ({ message }) => {
      const v = message.value?.toString() ?? '';
      if (v.startsWith('order-')) seen.push(v);
    },
  });
  const deadline = Date.now() + 10_000;
  while (seen.length < KEYS.length && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 100));
  }
  await consumer.stop();
  await consumer.disconnect();

  console.log(`Read back ${seen.length} record(s): [${seen.join(', ')}]`);

  await producer.disconnect();
  await admin.deleteTopics({ topics: [TOPIC] });
  await admin.disconnect();

  // Exactly KEYS.length records, each unique — the idempotent path landed each exactly once.
  assert(seen.length === KEYS.length, `expected exactly ${KEYS.length} records, got ${seen.length}`);
  assert(new Set(seen).size === seen.length, 'duplicate record detected — idempotent landing failed');

  console.log('\nIdempotent path proven: keyed batch landed exactly once, each record unique');
}

runExample(main);
