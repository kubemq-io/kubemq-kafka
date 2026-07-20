/**
 * transactions/read-committed — read_committed isolation, LSO < HWM, aborted-record filtering.
 *
 * Produces a committed transaction and an open-then-aborted transaction on one partition, and
 * proves the three EOS-consumer guarantees against this connector:
 *
 *   1. LAST STABLE OFFSET (LSO) vs HIGH WATER MARK (HWM). While a transaction is OPEN its
 *      records ARE appended to the log (the HWM advances past them — verified below with a
 *      read_uncommitted consumer that reads them), but the LSO — the furthest a read_committed
 *      consumer may advance — stays put until the txn resolves. So LSO < HWM while a txn is
 *      open; after it resolves the LSO catches up. This is real Kafka / franz-go behavior.
 *
 *      ⚠ kafkajs quirk: `admin.fetchTopicOffsets(topic).high` returns the READ_COMMITTED LSO
 *        (kafkajs's admin has no isolation-level knob), NOT the read_uncommitted HWM. That is
 *        why this example reads the LSO from admin but demonstrates the HWM side with a
 *        read_uncommitted CONSUMER (which the connector lets read past the LSO).
 *
 *   2. A read_committed consumer delivers the committed records — never the aborted ones
 *      (client-side filtering via the AbortedTransactions list — gotcha #12).
 *
 * 🟡 KIP-890 CEILING (§2.5): EOS V1 semantics only.
 *
 * Offsets/values are anchored/tagged per run: DeleteTopics does not purge the connector's
 * Events-Store channel, so the topic keeps prior runs' committed/aborted records.
 *
 * Kafka topic "kafka-ex-txn-rc" <-> Events-Store channel "kafka.kafka-ex-txn-rc".
 *
 * Run: npx tsx transactions/read-committed/index.ts
 */
import { Kafka, Admin, Consumer } from 'kafkajs';
import { newKafka, newProducer, newAdmin, bootstrap, assert, sleep, runExample } from '../../shared/client.js';

const TOPIC = 'kafka-ex-txn-rc';
const TXN_ID = 'kafka-ex-txn-rc-tid';
const RUN = Date.now(); // per-run tag so the delivery proof is about THIS run's records

/** admin.fetchTopicOffsets returns the READ_COMMITTED Last Stable Offset on this connector. */
async function lastStableOffset(admin: Admin): Promise<number> {
  return Number((await admin.fetchTopicOffsets(TOPIC)).find((o) => o.partition === 0)!.high);
}

/**
 * Collect delivered values until `want` are all seen (or timeout). When `base` is a number the
 * consumer is parked at the log end and we seek back to `base`; when `base` is null the consumer
 * was subscribed fromBeginning and reads the whole (small) log forward. The read_committed reader
 * uses fromBeginning so kafkajs's aborted-record filter sees each transaction's commit/abort
 * marker in order and filters correctly.
 */
async function collectValues(consumer: Consumer, base: number | null, want: string[], timeoutMs: number): Promise<string[]> {
  const seen: string[] = [];
  await consumer.run({ eachMessage: async ({ message }) => { seen.push(message.value?.toString() ?? ''); } });
  const deadline = Date.now() + timeoutMs;
  let reseekAt = 0;
  while (!want.every((v) => seen.includes(v)) && Date.now() < deadline) {
    if (base !== null && seen.length === 0 && Date.now() >= reseekAt) {
      consumer.seek({ topic: TOPIC, partition: 0, offset: String(base) });
      reseekAt = Date.now() + 1500;
    }
    await sleep(100);
  }
  return seen;
}

async function main(): Promise<void> {
  const kafka: Kafka = newKafka();
  console.log(`Connecting to KubeMQ Kafka connector at ${bootstrap()} (topic "${TOPIC}")`);

  const admin = newAdmin(kafka);
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: TOPIC, numPartitions: 1 }], waitForLeaders: true });

  const committedVals = [`rc-committed-${RUN}-1`, `rc-committed-${RUN}-2`];
  const openVals = [`rc-open-${RUN}-1`, `rc-open-${RUN}-2`];

  const producer = newProducer(kafka, { transactionalId: TXN_ID, idempotent: true, maxInFlightRequests: 1 });
  await producer.connect();

  // This run's records start here (robust to prior runs left in the non-purged channel).
  const base = await lastStableOffset(admin);

  // Committed transaction (2 records) -> the LSO advances (2 records + commit marker).
  const committed = await producer.transaction();
  await committed.send({ topic: TOPIC, messages: committedVals.map((value) => ({ value })) });
  await committed.commit();
  const lsoCommitted = await lastStableOffset(admin);
  console.log(`Committed txn -> ${committedVals.join(', ')}; LSO ${base} -> ${lsoCommitted}`);
  assert(lsoCommitted > base, `committed txn should advance the LSO past ${base}, was ${lsoCommitted}`);

  // Open a second transaction and leave it OPEN. Its records ARE appended (HWM advances), but
  // the LSO must NOT advance past them until the txn resolves.
  const open = await producer.transaction();
  await open.send({ topic: TOPIC, messages: openVals.map((value) => ({ value })) });
  await sleep(800);
  const lsoOpen = await lastStableOffset(admin);
  console.log(`Opened txn (uncommitted) -> ${openVals.join(', ')}; LSO stays ${lsoOpen} (read_committed cannot pass it)`);
  assert(lsoOpen === lsoCommitted, `LSO must NOT advance past an open txn (expected ${lsoCommitted}, was ${lsoOpen})`);

  // HWM side of LSO < HWM: a read_uncommitted consumer reads PAST the LSO and sees the open
  // records, proving they are in the log (HWM advanced) while the txn is still open.
  const ru = kafka.consumer({ groupId: `txn-rc-uncommitted-${RUN}`, readUncommitted: true });
  await ru.connect();
  await ru.subscribe({ topic: TOPIC, fromBeginning: false });
  const ruSeen = await collectValues(ru, base, [...committedVals, ...openVals], 10_000);
  await ru.stop(); await ru.disconnect();
  console.log(`read_uncommitted consumer (txn still OPEN) saw: [${ruSeen.filter((v) => v.includes(String(RUN))).join(', ')}]`);
  assert(
    openVals.every((v) => ruSeen.includes(v)),
    'read_uncommitted consumer did not see the open records — HWM did not advance past the LSO (expected LSO < HWM)',
  );

  // Abort: the (now-resolved) records + an abort marker are stable, so the LSO advances past
  // them, but a read_committed consumer filters the aborted records via AbortedTransactions.
  await open.abort();
  await producer.disconnect();
  const lsoAfterAbort = await lastStableOffset(admin);
  console.log(`Aborted the open txn -> LSO ${lsoOpen} -> ${lsoAfterAbort} (advances once the txn resolves)`);
  assert(lsoAfterAbort > lsoOpen, `abort should let the LSO advance, ${lsoOpen} -> ${lsoAfterAbort}`);

  // read_committed consumer. Reads fromBeginning (not seek — see collectValues) and picks out
  // THIS run's records by tag; the whole accumulated log is small and reads in well under a second.
  const rc = kafka.consumer({ groupId: `txn-rc-verify-${RUN}`, readUncommitted: false });
  await rc.connect();
  await rc.subscribe({ topic: TOPIC, fromBeginning: true });
  const rcSeen = await collectValues(rc, null, committedVals, 12_000);
  await sleep(1000); // give any (wrongly) un-filtered aborted record a chance to surface
  await rc.stop(); await rc.disconnect();

  await admin.disconnect();

  console.log(`read_committed consumer saw (this run): [${rcSeen.filter((v) => v.includes(String(RUN))).join(', ')}]`);
  assert(committedVals.every((v) => rcSeen.includes(v)), 'committed records not delivered under read_committed');
  assert(!rcSeen.some((v) => openVals.includes(v)), 'aborted (open-then-aborted) records were delivered under read_committed');

  console.log('\nread_committed proven: LSO < HWM while a txn is open (read_uncommitted reads past the LSO);');
  console.log('committed records delivered, aborted records never delivered (EOS V1; KIP-890 out of scope).');
}

runExample(main);
