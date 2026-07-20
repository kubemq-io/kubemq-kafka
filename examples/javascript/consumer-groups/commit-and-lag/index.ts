/**
 * consumer-groups/commit-and-lag — manual OffsetCommit, resume-from-committed, and lag.
 *
 * Produces N records, then a first consumer (autoCommit off) reads HALF and explicitly
 * commitOffsets() at that point (OffsetCommit — key 8). A second consumer in the SAME group
 * starts fresh and must RESUME from the committed offset (OffsetFetch — key 9), reading only
 * the remaining half (no re-read). Finally we compute lag as HWM - committed using
 * admin.fetchTopicOffsets (log-end) and admin.fetchOffsets (committed).
 *
 * The connector also exposes kubemq_kafka_consumer_group_lag{group,topic,partition} as a
 * server-side cross-check of the same number.
 *
 * Two correctness points this example must get right (both were latent bugs):
 *   - autoCommit is a `run()` option, NOT a consumer-constructor option. Passing it to
 *     `kafka.consumer({ autoCommit: false })` is silently ignored, so `run()` defaulted to
 *     auto-commit ON and committed the HIGH watermark (kafkajs resolves every fetched
 *     message, even the ones eachMessage skips) — committed read back as N, not HALF, and
 *     the resumed consumer then read nothing. autoCommit:false MUST go on `run()`.
 *   - Offsets are computed RELATIVE to this run's base offset. DeleteTopics does not purge
 *     the connector's Events-Store channel, so a reused topic keeps growing; assuming the
 *     records sit at 0..N-1 would break committed/lag on every rerun. We read the current
 *     HWM as the base and seek the first consumer to it, and each consumer uses a fresh
 *     group so a stale committed offset from a prior run can't be delivered first.
 *
 * Kafka topic "kafka-ex-cg-commit" <-> Events-Store channel "kafka.kafka-ex-cg-commit".
 *
 * Run: npx tsx consumer-groups/commit-and-lag/index.ts
 */
import { Kafka } from 'kafkajs';
import { newKafka, newProducer, newConsumer, newAdmin, bootstrap, assert, sleep, runExample } from '../../shared/client.js';

const TOPIC = 'kafka-ex-cg-commit';
const GROUP = `kafka-ex-cg-commit-grp-${Date.now()}`; // fresh group per run: no stale committed offset
const N = 10;
const HALF = N / 2;

async function main(): Promise<void> {
  const kafka: Kafka = newKafka();
  console.log(`Connecting to KubeMQ Kafka connector at ${bootstrap()} (topic "${TOPIC}", group "${GROUP}")`);

  const admin = newAdmin(kafka);
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: TOPIC, numPartitions: 1 }], waitForLeaders: true });

  // This run's records land at [base .. base+N-1]. Anchor here — DeleteTopics does not
  // purge the underlying Events-Store channel, so the log may already hold prior records.
  const base = Number((await admin.fetchTopicOffsets(TOPIC)).find((o) => o.partition === 0)!.offset);

  const producer = newProducer(kafka);
  await producer.connect();
  await producer.send({ topic: TOPIC, acks: -1, messages: Array.from({ length: N }, (_, i) => ({ value: `e-${i}` })) });
  console.log(`Produced ${N} records at offsets ${base}..${base + N - 1}`);

  // ---- First consumer: read HALF of THIS run's records, commit, then leave. ----
  const first = newConsumer(kafka, GROUP);
  const firstSeen: string[] = [];
  await first.connect();
  // Parked at the log end; seek(base) below rewinds onto this run's first record.
  await first.subscribe({ topic: TOPIC, fromBeginning: false });
  await first.run({
    autoCommit: false, // MUST be here (run config), not on the consumer constructor.
    eachMessage: async ({ topic, partition, message }) => {
      if (firstSeen.length >= HALF) return;
      firstSeen.push(message.value?.toString() ?? '');
      // Commit the NEXT offset to read (Kafka committed offset = last-processed + 1).
      await first.commitOffsets([{ topic, partition, offset: String(Number(message.offset) + 1) }]);
    },
  });
  // seek requires the run loop to be live and the partition assigned; re-issue until delivery starts.
  const d1 = Date.now() + 10_000;
  let reseekAt = 0;
  while (firstSeen.length < HALF && Date.now() < d1) {
    if (firstSeen.length === 0 && Date.now() >= reseekAt) {
      first.seek({ topic: TOPIC, partition: 0, offset: String(base) });
      reseekAt = Date.now() + 1500;
    }
    await sleep(100);
  }
  await first.stop(); await first.disconnect();
  console.log(`First consumer read ${firstSeen.length} and committed: [${firstSeen.join(', ')}]`);

  // Lag after committing HALF: HWM - committed.
  const hwm = Number((await admin.fetchTopicOffsets(TOPIC)).find((o) => o.partition === 0)!.offset);
  const committedResp = await admin.fetchOffsets({ groupId: GROUP, topics: [TOPIC] });
  const committed = Number(committedResp[0].partitions.find((p) => p.partition === 0)!.offset);
  const lag = hwm - committed;
  console.log(`HWM=${hwm} committed=${committed} lag=${lag}`);

  // ---- Second consumer: same group, must resume from committed (only the remaining half). ----
  const second = newConsumer(kafka, GROUP);
  const secondSeen: string[] = [];
  await second.connect();
  await second.subscribe({ topic: TOPIC, fromBeginning: true }); // ignored: committed offset wins for an existing group
  await second.run({ autoCommit: false, eachMessage: async ({ message }) => { secondSeen.push(message.value?.toString() ?? ''); } });
  const d2 = Date.now() + 8000;
  while (secondSeen.length < N - HALF && Date.now() < d2) await sleep(100);
  await second.stop(); await second.disconnect();
  console.log(`Second consumer resumed and read ${secondSeen.length}: [${secondSeen.join(', ')}]`);

  await producer.disconnect();
  await admin.deleteTopics({ topics: [TOPIC] });
  await admin.disconnect();

  assert(firstSeen.length === HALF, `first consumer should read ${HALF}, read ${firstSeen.length}`);
  assert(committed === base + HALF, `committed offset should be ${base + HALF} (base+HALF), was ${committed}`);
  assert(lag === N - HALF, `lag should be ${N - HALF}, was ${lag}`);
  // Resume means NO overlap with what the first consumer already committed.
  for (const v of firstSeen) assert(!secondSeen.includes(v), `second consumer re-read already-committed record ${v}`);
  assert(secondSeen.length === N - HALF, `second consumer should read the remaining ${N - HALF}, read ${secondSeen.length}`);

  console.log('\nCommit + lag proven: resumed from committed offset with no re-read; lag = HWM - committed');
}

runExample(main);
