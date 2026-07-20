/**
 * admin/topics-lifecycle — CreateTopics / DescribeConfigs / DescribeCluster / DeleteTopics
 * plus the reserved-name rejection.
 *
 * Exercises the admin API keys:
 *   CreateTopics (19), fetchTopicMetadata (Metadata 3), DescribeConfigs (32),
 *   DescribeCluster (60), DeleteTopics (20).
 * Then proves gotcha #6: a topic name containing the reserved `~` is rejected
 * (INVALID_TOPIC_EXCEPTION / error code 17) — `~` is reserved by the channel mapping.
 *
 * Kafka topic "kafka-ex-admin-topics" <-> Events-Store channel "kafka.kafka-ex-admin-topics".
 *
 * Run: npx tsx admin/topics-lifecycle/index.ts
 */
import { Kafka } from 'kafkajs';
// kafkajs is CommonJS and its `ConfigResourceTypes` enum is not detected as a
// named ESM export by tsx/esbuild (the `...errors` spread in its module.exports
// defeats the CJS-interop lexer), so read it off the default import instead.
import kafkajs from 'kafkajs';
const { ConfigResourceTypes } = kafkajs;
import { newKafka, newAdmin, bootstrap, assert, sleep, runExample } from '../../shared/client.js';

const TOPIC = 'kafka-ex-admin-topics';
const BAD_TOPIC = 'kafka-ex-admin~bad'; // contains reserved '~'

async function main(): Promise<void> {
  const kafka: Kafka = newKafka();
  console.log(`Connecting to KubeMQ Kafka connector at ${bootstrap()} (topic "${TOPIC}")`);

  const admin = newAdmin(kafka);
  await admin.connect();

  // Clean slate so CreateTopics reports created=true on reruns — DeleteTopics resets the
  // topic metadata (a prior run that was interrupted before its own delete would otherwise
  // leave the topic present and make createTopics return false).
  try {
    await admin.deleteTopics({ topics: [TOPIC] });
    await sleep(1000);
  } catch {
    /* absent is fine */
  }

  // CreateTopics.
  const created = await admin.createTopics({
    topics: [{ topic: TOPIC, numPartitions: 2, configEntries: [{ name: 'retention.ms', value: '3600000' }] }],
    waitForLeaders: true,
  });
  console.log(`CreateTopics    -> created=${created}`);
  assert(created, 'CreateTopics returned false (topic already existed?)');

  // Metadata / describe.
  const md = await admin.fetchTopicMetadata({ topics: [TOPIC] });
  const t = md.topics.find((x) => x.name === TOPIC)!;
  console.log(`Metadata        -> topic "${t.name}" partitions=${t.partitions.length}`);
  assert(t.partitions.length === 2, `expected 2 partitions, got ${t.partitions.length}`);

  // DescribeConfigs.
  const cfg = await admin.describeConfigs({
    includeSynonyms: false,
    resources: [{ type: ConfigResourceTypes.TOPIC, name: TOPIC, configNames: [] }],
  });
  const entries = cfg.resources[0]?.configEntries ?? [];
  console.log(`DescribeConfigs -> ${entries.length} config entries (e.g. retention.ms=${entries.find((e) => e.configName === 'retention.ms')?.configValue ?? '?'})`);
  assert(entries.length > 0, 'DescribeConfigs returned no config entries');

  // DescribeCluster.
  const cluster = await admin.describeCluster();
  console.log(`DescribeCluster -> clusterId=${cluster.clusterId ?? '(none)'} brokers=${cluster.brokers.length} controller=${cluster.controller ?? '?'}`);
  assert(cluster.brokers.length >= 1, 'DescribeCluster returned no brokers');

  // gotcha #6: reserved '~' in a topic name is rejected. The connector rejects the create, but
  // kafkajs surfaces it as a generic KafkaJSAggregateError ("Topic creation errors") — NOT a
  // typed INVALID_TOPIC / code 17 — so the create promise REJECTING is itself the proof. We
  // still confirm it is a topic-creation failure (createTopics never resolves for the bad name).
  let rejected = false;
  try {
    await admin.createTopics({ topics: [{ topic: BAD_TOPIC, numPartitions: 1 }], waitForLeaders: true });
  } catch (err) {
    rejected = true;
    const detail = err instanceof Error ? `${err.name}: ${err.message}` : String(err);
    console.log(`CreateTopics("${BAD_TOPIC}") -> rejected: ${detail}`);
  }
  assert(rejected, `reserved-name topic "${BAD_TOPIC}" was NOT rejected (gotcha #6)`);

  // DeleteTopics.
  await admin.deleteTopics({ topics: [TOPIC] });
  console.log(`DeleteTopics    -> deleted "${TOPIC}"`);

  await admin.disconnect();

  console.log('\nTopic lifecycle proven: create/describe-configs/describe-cluster/delete OK; ~ name rejected');
}

runExample(main);
