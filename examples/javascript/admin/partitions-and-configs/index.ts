/**
 * admin/partitions-and-configs — CreatePartitions (increase-only), DescribeConfigs,
 * DeleteRecords (partial low-end truncation).
 *
 * 🟡 Partial support (§2.4):
 *   - CreatePartitions: increase-only, capped at 256. A same-count / decrease / >256 request
 *     is rejected with INVALID_PARTITIONS. Growing N re-shards keys (gotcha #5).
 *   - DescribeConfigs: topic configs are readable.
 *   - deleteTopicRecords (DeleteRecords): low-end truncation — advances the log-start offset.
 *
 * ⚠ kafkajs limitation — config ALTER is not exercisable here. The connector implements
 *   IncrementalAlterConfigs (API key 44) only; it does NOT advertise the legacy AlterConfigs
 *   (key 33). kafkajs 2.2.4's `admin.alterConfigs()` speaks legacy AlterConfigs and offers no
 *   incremental variant, so the call is rejected with UNSUPPORTED_VERSION — and that failed
 *   request also drops the admin's broker socket, so it is attempted LAST (after DeleteRecords)
 *   and treated as a documented no-op rather than a proof. The Python/Go/Rust suites reach the
 *   supported IncrementalAlterConfigs path via librdkafka; kafkajs cannot.
 *
 * Kafka topic "kafka-ex-admin-parts" <-> Events-Store channel "kafka.kafka-ex-admin-parts".
 *
 * Run: npx tsx admin/partitions-and-configs/index.ts
 */
import { Kafka } from 'kafkajs';
// kafkajs is CommonJS and its `ConfigResourceTypes` enum is not detected as a
// named ESM export by tsx/esbuild (the `...errors` spread in its module.exports
// defeats the CJS-interop lexer), so read it off the default import instead.
import kafkajs from 'kafkajs';
const { ConfigResourceTypes } = kafkajs;
import { newKafka, newProducer, newAdmin, bootstrap, assert, sleep, runExample } from '../../shared/client.js';
import type { Admin } from 'kafkajs';

const TOPIC = 'kafka-ex-admin-parts';
const START_PARTITIONS = 2;

async function partitionCount(admin: Admin): Promise<number> {
  try {
    const md = await admin.fetchTopicMetadata({ topics: [TOPIC] });
    return md.topics[0]?.partitions.length ?? 0;
  } catch {
    return 0; // topic absent
  }
}

/**
 * Start from a clean 2-partition topic. Partition growth is IRREVERSIBLE, so a prior run
 * that already grew the topic to 4 would make the "2 -> 4" increase a rejected same-count
 * request ("Number of partitions is invalid"). DeleteTopics resets the partition count (it
 * does not purge records), so delete then recreate at 2 partitions and wait for it to settle.
 */
async function resetTopic(admin: Admin): Promise<void> {
  if ((await partitionCount(admin)) > 0) {
    try {
      await admin.deleteTopics({ topics: [TOPIC] });
    } catch {
      /* absent is fine */
    }
    const gone = Date.now() + 10_000;
    while ((await partitionCount(admin)) > 0 && Date.now() < gone) await sleep(300);
  }
  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      await admin.createTopics({ topics: [{ topic: TOPIC, numPartitions: START_PARTITIONS }], waitForLeaders: true });
      break;
    } catch {
      await sleep(500); // delete may not have fully settled; retry
    }
  }
  const ready = Date.now() + 10_000;
  while ((await partitionCount(admin)) < START_PARTITIONS && Date.now() < ready) await sleep(300);
}

async function main(): Promise<void> {
  const kafka: Kafka = newKafka();
  console.log(`Connecting to KubeMQ Kafka connector at ${bootstrap()} (topic "${TOPIC}")`);

  const admin = newAdmin(kafka);
  await admin.connect();
  await resetTopic(admin);
  console.log(`Reset topic to ${START_PARTITIONS} partitions (partition growth is irreversible, so a clean start is required)`);

  // ---- CreatePartitions: increase 2 -> 4 (allowed). ----
  await admin.createPartitions({ topicPartitions: [{ topic: TOPIC, count: 4 }] });
  const md = await admin.fetchTopicMetadata({ topics: [TOPIC] });
  const count = md.topics[0].partitions.length;
  console.log(`CreatePartitions 2 -> 4: now ${count} partitions`);
  assert(count === 4, `expected 4 partitions after increase, got ${count}`);

  // ---- CreatePartitions: decrease 4 -> 2 (rejected INVALID_PARTITIONS). ----
  let badRejected = false;
  try {
    await admin.createPartitions({ topicPartitions: [{ topic: TOPIC, count: 2 }] });
  } catch (err) {
    const type = (err as { type?: string }).type ?? String(err);
    badRejected = /INVALID_PARTITIONS/i.test(type) || /INVALID_PARTITIONS|invalid partitions/i.test(String(err));
    console.log(`CreatePartitions 4 -> 2 (decrease): rejected ${type}`);
  }
  assert(badRejected, 'decreasing partition count was NOT rejected with INVALID_PARTITIONS');

  // ---- DescribeConfigs: topic configs are readable (this API IS supported). ----
  const cfg = await admin.describeConfigs({
    includeSynonyms: false,
    resources: [{ type: ConfigResourceTypes.TOPIC, name: TOPIC, configNames: ['retention.ms'] }],
  });
  const retention = cfg.resources[0].configEntries.find((e) => e.configName === 'retention.ms')?.configValue;
  console.log(`DescribeConfigs retention.ms -> ${retention}`);
  assert(retention !== undefined, 'DescribeConfigs did not return retention.ms');

  // ---- DeleteRecords: low-end truncation on partition 0. ----
  // Produce a batch, then truncate to a point INSIDE the current live window. Two connector
  // realities make an exact absolute assertion fragile: DeleteTopics does not purge the channel,
  // and the channel's own retention independently advances the log-start (producing can evict
  // older records). So we read the live [low, high) window right before truncating, pick a target
  // inside it, and assert the DeleteRecords CONTRACT: the log-start ends up at least at the target
  // (records below it are gone) — which holds whether DeleteRecords or retention did the removal.
  const producer = newProducer(kafka);
  await producer.connect();
  for (let i = 0; i < 8; i++) {
    await producer.send({ topic: TOPIC, acks: -1, messages: [{ partition: 0, value: `r-${i}` }] });
  }
  await producer.disconnect();

  const pre = (await admin.fetchTopicOffsets(TOPIC)).find((o) => o.partition === 0)!;
  const preLow = Number(pre.low);
  const preHigh = Number(pre.high);
  // A target strictly forward of the current log-start when the window allows it.
  const truncateTo = preHigh - preLow >= 2 ? preLow + Math.floor((preHigh - preLow) / 2) : preHigh;
  await admin.deleteTopicRecords({ topic: TOPIC, partitions: [{ partition: 0, offset: String(truncateTo) }] });
  const postLow = Number((await admin.fetchTopicOffsets(TOPIC)).find((o) => o.partition === 0)!.low);
  console.log(`deleteTopicRecords(offset=${truncateTo}) -> partition 0 log-start (low): ${preLow} -> ${postLow}`);
  assert(postLow >= truncateTo, `log-start should advance to >= ${truncateTo} after truncation, was ${postLow}`);

  // ---- Config ALTER (documented kafkajs limitation — attempted LAST; see file header). ----
  // The connector only implements IncrementalAlterConfigs (key 44); kafkajs 2.2.4 can only
  // send legacy AlterConfigs (key 33), which the connector does not advertise, so this is
  // rejected with UNSUPPORTED_VERSION. The failed request also drops the admin socket, which
  // is why it runs after every supported operation. Not a connector defect and not fixable in
  // the example — surfaced here, not silently skipped.
  try {
    await admin.alterConfigs({
      validateOnly: false,
      resources: [{ type: ConfigResourceTypes.TOPIC, name: TOPIC, configEntries: [{ name: 'retention.ms', value: '7200000' }] }],
    });
    console.log('alterConfigs (legacy) -> unexpectedly accepted');
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.log(`alterConfigs (legacy key 33) -> rejected as expected: ${msg}`);
    console.log('  NOTE: connector supports only IncrementalAlterConfigs (key 44); kafkajs cannot send it.');
  }

  await admin.disconnect();

  console.log('\nPartitions + configs proven: increase-only enforced, configs readable, low-end truncation done');
  console.log('(config ALTER is a documented kafkajs limitation — connector is IncrementalAlterConfigs-only)');
}

runExample(main);
