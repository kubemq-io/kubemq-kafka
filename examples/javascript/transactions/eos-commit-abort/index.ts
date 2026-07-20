/**
 * transactions/eos-commit-abort — transactional produce with commit and abort.
 *
 * Uses a transactional producer ({ transactionalId, idempotent: true, maxInFlightRequests: 1 }):
 *   InitProducerId -> AddPartitionsToTxn -> transactional Produce -> EndTxn(commit|abort).
 * One transaction is committed and one is aborted; a read_committed consumer must see the
 * committed records and NEVER the aborted ones.
 *
 * 🟡 EOS V1 (§2.4). KIP-890 CEILING (§2.5, gotcha #9): this connector implements the EOS V1
 *   transaction protocol. The KIP-890 "transactions server-side" improvements (the newer
 *   AddPartitionsToTxn verification flow / TransactionV2) are NOT in scope — do not rely on
 *   behavior beyond spec section 2. Transactional guarantees here are exactly EOS V1.
 * gotcha #7: a `transactional.id` containing `/` is rejected with INVALID_TRANSACTIONAL_ID.
 *
 * Kafka topic "kafka-ex-txn-eos" <-> Events-Store channel "kafka.kafka-ex-txn-eos".
 *
 * Run: npx tsx transactions/eos-commit-abort/index.ts
 */
import { Kafka } from 'kafkajs';
import { newKafka, newProducer, newAdmin, bootstrap, assert, sleep, runExample } from '../../shared/client.js';

const TOPIC = 'kafka-ex-txn-eos';
const TXN_ID = 'kafka-ex-txn-eos-tid'; // no '/' (gotcha #7)

async function main(): Promise<void> {
  const kafka: Kafka = newKafka();
  console.log(`Connecting to KubeMQ Kafka connector at ${bootstrap()} (topic "${TOPIC}", transactional.id "${TXN_ID}")`);

  const admin = newAdmin(kafka);
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: TOPIC, numPartitions: 1 }], waitForLeaders: true });

  const producer = newProducer(kafka, { transactionalId: TXN_ID, idempotent: true, maxInFlightRequests: 1 });
  await producer.connect(); // InitProducerId

  // ---- Transaction 1: COMMIT. ----
  const t1 = await producer.transaction();
  try {
    await t1.send({ topic: TOPIC, messages: [{ value: 'committed-1' }, { value: 'committed-2' }] });
    await t1.commit();
    console.log('Txn 1 -> committed 2 records (committed-1, committed-2)');
  } catch (err) {
    await t1.abort();
    throw err;
  }

  // ---- Transaction 2: ABORT. ----
  const t2 = await producer.transaction();
  await t2.send({ topic: TOPIC, messages: [{ value: 'aborted-1' }, { value: 'aborted-2' }] });
  await t2.abort();
  console.log('Txn 2 -> aborted 2 records (aborted-1, aborted-2)');

  await producer.disconnect();

  // ---- read_committed consumer: sees committed, never aborted. ----
  const consumer = kafka.consumer({ groupId: `txn-eos-verify-${Date.now()}`, readUncommitted: false });
  const seen: string[] = [];
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
  await consumer.run({ eachMessage: async ({ message }) => { seen.push(message.value?.toString() ?? ''); } });
  const deadline = Date.now() + 10_000;
  while (seen.length < 2 && Date.now() < deadline) await sleep(100);
  await sleep(1000); // give any (wrongly) aborted records a chance to show up — they must not
  await consumer.stop(); await consumer.disconnect();

  await admin.deleteTopics({ topics: [TOPIC] });
  await admin.disconnect();

  console.log(`read_committed consumer saw: [${seen.join(', ')}]`);
  assert(seen.includes('committed-1') && seen.includes('committed-2'), 'committed records not visible under read_committed');
  assert(!seen.some((v) => v.startsWith('aborted-')), 'aborted records were visible under read_committed (EOS violation)');

  console.log('\nEOS proven: committed records visible, aborted records absent under read_committed (EOS V1; KIP-890 out of scope)');
}

runExample(main);
