/**
 * consume/seek-offsets-timestamps — seek by offset and by timestamp.
 *
 * Produces a numbered sequence, then:
 *   1. seek({ offset }) — repositions the consumer to a known offset; it must next
 *      deliver exactly the record at that offset.
 *   2. admin.fetchTopicOffsetsByTimestamp(topic, ts) — ListOffsets by-timestamp (key 2)
 *      returns the offset of the first record with timestamp >= ts; seeking there lands
 *      on the expected record.
 *
 * kafkajs requires the consumer's run loop to be active before seek() takes effect, so we
 * start run() first, then seek.
 *
 * Two things this example must get right (both were latent bugs):
 *   - Offsets are computed RELATIVE to the current high-watermark. The connector maps a
 *     topic to a persistent Events-Store channel, so DeleteTopics does NOT purge records —
 *     a reused topic keeps growing and retention can even advance the log-start offset.
 *     Seeking to an absolute low offset (e.g. 3) from a prior run may be out of range and
 *     deliver nothing, so we anchor every seek to this run's own base offset.
 *   - Records are produced at REAL wall-clock time (a short gap between sends), NOT with
 *     synthetic future timestamps. The connector resolves ListOffsets-by-timestamp against
 *     the record's server-side append time, so the by-timestamp query has to use a real
 *     wall-clock instant captured between two sends (the same approach the Python/confluent
 *     example uses). A synthetic CreateTime in the future is stored and returned on Fetch
 *     but is NOT what the by-timestamp index compares against, so it never resolves.
 *
 * Kafka topic "kafka-ex-consume-seek" <-> Events-Store channel "kafka.kafka-ex-consume-seek".
 *
 * Run: npx tsx consume/seek-offsets-timestamps/index.ts
 */
import { Kafka, Consumer } from 'kafkajs';
import { newKafka, newProducer, newAdmin, bootstrap, assert, sleep, runExample } from '../../shared/client.js';

const TOPIC = 'kafka-ex-consume-seek';
const N = 6;
const SEEK_OFFSET = 3; // seek-by-offset target -> record-3
const TS_TARGET = 4; // seek-by-timestamp target -> record-4

/**
 * Seek to `offset` and return the first record delivered. kafkajs applies seek() only once
 * the partition is assigned, so we re-issue it periodically until a record arrives — this
 * removes the "seek lost before the group assignment was live" flake.
 */
async function seekAndReadFirst(consumer: Consumer, offset: string, timeoutMs: number): Promise<string | undefined> {
  const seen: string[] = [];
  await consumer.run({ eachMessage: async ({ message }) => { seen.push(message.value?.toString() ?? ''); } });
  const deadline = Date.now() + timeoutMs;
  let reseekAt = 0;
  while (seen.length === 0 && Date.now() < deadline) {
    if (Date.now() >= reseekAt) {
      consumer.seek({ topic: TOPIC, partition: 0, offset });
      reseekAt = Date.now() + 1500;
    }
    await sleep(100);
  }
  return seen[0];
}

async function main(): Promise<void> {
  const kafka: Kafka = newKafka();
  console.log(`Connecting to KubeMQ Kafka connector at ${bootstrap()} (topic "${TOPIC}")`);

  const admin = newAdmin(kafka);
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: TOPIC, numPartitions: 1 }], waitForLeaders: true });

  // This run's records land at [base .. base+N-1]. Anchor here rather than assuming 0,
  // because DeleteTopics does not purge the underlying Events-Store channel.
  const baseOffset = Number((await admin.fetchTopicOffsets(TOPIC)).find((o) => o.partition === 0)!.offset);
  console.log(`Current high-watermark = ${baseOffset}; this run's records will occupy ${baseOffset}..${baseOffset + N - 1}`);

  const producer = newProducer(kafka);
  await producer.connect();

  // Produce records 0..N-1 at real wall-clock time, with a gap between sends so their
  // append timestamps are ordered and distinguishable. Capture the wall-clock instant
  // just before record-TS_TARGET is sent: it sits strictly between record-(TS_TARGET-1)'s
  // and record-TS_TARGET's append time, so the by-timestamp lookup resolves to record-TS_TARGET.
  let targetTs = 0;
  for (let i = 0; i < N; i++) {
    if (i === TS_TARGET) targetTs = Date.now();
    await producer.send({ topic: TOPIC, acks: -1, messages: [{ value: `record-${i}` }] });
    await sleep(100);
  }
  console.log(`Produced ${N} records; captured timestamp ${targetTs} just before record-${TS_TARGET}`);

  // ---- 1: seek by offset -> next delivered record is the one at that offset. ----
  const c1 = kafka.consumer({ groupId: `seek-offset-${Date.now()}` });
  await c1.connect();
  // Start at the log END (not fromBeginning): parked at the end nothing is delivered until
  // seek() repositions the consumer backward onto this run's record-SEEK_OFFSET.
  await c1.subscribe({ topic: TOPIC, fromBeginning: false });
  const byOffset = await seekAndReadFirst(c1, String(baseOffset + SEEK_OFFSET), 8000);
  await c1.stop(); await c1.disconnect();
  console.log(`seek(offset=${baseOffset + SEEK_OFFSET}) -> first delivered: "${byOffset}"`);

  // ---- 2: seek by timestamp -> ListOffsets(by-ts) then seek. ----
  const offsets = await admin.fetchTopicOffsetsByTimestamp(TOPIC, targetTs);
  const tsOffset = offsets.find((o) => o.partition === 0)!.offset;
  console.log(`fetchTopicOffsetsByTimestamp(${targetTs}) -> offset ${tsOffset} (expected ${baseOffset + TS_TARGET})`);

  const c2 = kafka.consumer({ groupId: `seek-ts-${Date.now()}` });
  await c2.connect();
  await c2.subscribe({ topic: TOPIC, fromBeginning: false }); // start at log end; seek repositions backward
  const byTs = await seekAndReadFirst(c2, tsOffset, 8000);
  await c2.stop(); await c2.disconnect();
  console.log(`seek(ts-offset=${tsOffset}) -> first delivered: "${byTs}"`);

  await producer.disconnect();
  await admin.deleteTopics({ topics: [TOPIC] });
  await admin.disconnect();

  assert(byOffset === `record-${SEEK_OFFSET}`, `seek-by-offset landed on "${byOffset}", expected record-${SEEK_OFFSET}`);
  assert(Number(tsOffset) === baseOffset + TS_TARGET, `by-timestamp resolved offset ${tsOffset}, expected ${baseOffset + TS_TARGET}`);
  assert(byTs === `record-${TS_TARGET}`, `seek-by-timestamp landed on "${byTs}", expected record-${TS_TARGET}`);

  console.log('\nSeek proven: seek-by-offset and ListOffsets-by-timestamp both land on the expected record');
}

runExample(main);
