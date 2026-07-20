/**
 * offsets/list-and-retention — ListOffsets earliest/latest/by-timestamp + retention config.
 *
 * Creates a topic with a retention.ms/retention.bytes config (mapped to the channel's
 * MaxAge / MaxBytes, §2.2), produces a numbered sequence, then queries offsets:
 *   - fetchTopicOffsets(topic)        -> earliest (low = log-start) and latest (high = HWM)
 *   - fetchTopicOffsetsByTimestamp(ts) -> offset of first record with ts >= target (key 2)
 *
 * Two things this example must get right (both were latent bugs):
 *   - Offsets are anchored to this run's base HWM. DeleteTopics does not purge the connector's
 *     Events-Store channel, so a reused topic keeps growing; asserting the records sit at 0..N-1
 *     (low=0, high=N) breaks on every rerun. We read the current HWM as the base and assert the
 *     N records advanced the HWM by exactly N.
 *   - The by-timestamp query uses a REAL wall-clock instant captured between two sends, not a
 *     synthetic future timestamp. The connector resolves ListOffsets-by-timestamp against the
 *     record's server-side APPEND time, so a synthetic CreateTime in the future is stored and
 *     returned on Fetch but never resolves via the by-timestamp index (same finding as
 *     consume/seek-offsets-timestamps).
 *
 * Kafka topic "kafka-ex-offsets" <-> Events-Store channel "kafka.kafka-ex-offsets".
 *
 * Run: npx tsx offsets/list-and-retention/index.ts
 */
import { Kafka } from 'kafkajs';
import { newKafka, newProducer, newAdmin, bootstrap, assert, sleep, runExample } from '../../shared/client.js';

const TOPIC = 'kafka-ex-offsets';
const N = 5;
const TS_TARGET = 2; // by-timestamp query should resolve to this run's record o-2

async function main(): Promise<void> {
  const kafka: Kafka = newKafka();
  console.log(`Connecting to KubeMQ Kafka connector at ${bootstrap()} (topic "${TOPIC}")`);

  const admin = newAdmin(kafka);
  await admin.connect();
  // retention.ms -> channel MaxAge, retention.bytes -> channel MaxBytes (§2.2).
  await admin.createTopics({
    topics: [{ topic: TOPIC, numPartitions: 1, configEntries: [
      { name: 'retention.ms', value: '86400000' },
      { name: 'retention.bytes', value: '1048576' },
    ] }],
    waitForLeaders: true,
  });
  console.log('Created topic with retention.ms=86400000, retention.bytes=1048576');

  // This run's records land at [base .. base+N-1]. Anchor here — the channel is not purged on delete.
  const base = Number((await admin.fetchTopicOffsets(TOPIC)).find((o) => o.partition === 0)!.offset);

  // Produce N records at real wall-clock time (gap between sends), capturing the instant just
  // before record TS_TARGET so the by-timestamp lookup (server append-time) resolves to it.
  const producer = newProducer(kafka);
  await producer.connect();
  let targetTs = 0;
  for (let i = 0; i < N; i++) {
    if (i === TS_TARGET) targetTs = Date.now();
    await producer.send({ topic: TOPIC, acks: -1, messages: [{ value: `o-${i}` }] });
    await sleep(100);
  }
  await producer.disconnect();
  console.log(`Produced ${N} records at offsets ${base}..${base + N - 1}; captured ts ${targetTs} before o-${TS_TARGET}`);

  // earliest / latest.
  const offs = await admin.fetchTopicOffsets(TOPIC);
  const p0 = offs.find((o) => o.partition === 0)!;
  console.log(`fetchTopicOffsets -> low(earliest)=${p0.low} high(latest/HWM)=${p0.high} (this run base=${base})`);
  assert(Number(p0.high) === base + N, `latest offset (HWM) should be ${base + N}, was ${p0.high}`);
  assert(Number(p0.low) >= 0 && Number(p0.low) <= base, `earliest offset should be a valid log-start <= ${base}, was ${p0.low}`);

  // by-timestamp: first record with append-time >= targetTs is this run's o-TS_TARGET at base+TS_TARGET.
  const byTs = await admin.fetchTopicOffsetsByTimestamp(TOPIC, targetTs);
  const tsOffset = Number(byTs.find((o) => o.partition === 0)!.offset);
  console.log(`fetchTopicOffsetsByTimestamp(${targetTs}) -> offset ${tsOffset} (expect ${base + TS_TARGET} = record o-${TS_TARGET})`);
  assert(tsOffset === base + TS_TARGET, `by-timestamp offset should be ${base + TS_TARGET}, was ${tsOffset}`);

  await admin.deleteTopics({ topics: [TOPIC] });
  await admin.disconnect();

  console.log('\nListOffsets proven: latest=base+N, earliest is the log-start, by-timestamp resolves the first record >= ts; retention config accepted');
}

runExample(main);
