/**
 * security/sasl-plain-scram — SASL/PLAIN + SCRAM-SHA-256/512 authenticated produce/consume,
 * and the authorization-failure path.
 *
 * The shared helper's newKafka() reads SASL from env, so this example is the SAME code as
 * every other variant — only the connection is authenticated:
 *   KUBEMQ_KAFKA_SASL_MECHANISM = plain | scram-sha-256 | scram-sha-512
 *   KUBEMQ_KAFKA_SASL_USERNAME  = <user>
 *   KUBEMQ_KAFKA_SASL_PASSWORD  = <password>
 * Requires a broker configured with a credential store (spec §4.7). Against the default
 * no-auth dev broker (no SASL env), the example runs an UNAUTHENTICATED round-trip on the
 * same code path and documents how to switch SASL on — it still asserts a full round-trip.
 *
 * Denied path: set KUBEMQ_KAFKA_DENIED_TOPIC to a topic the principal is NOT authorized to
 * write; the example asserts the produce fails with TOPIC_AUTHORIZATION_FAILED (or
 * GROUP_AUTHORIZATION_FAILED for a denied group).
 *
 * TLS / mTLS is DOC-ONLY (README + guides/security-sasl-tls.md): set KUBEMQ_KAFKA_TLS=true to
 * use the ssl path in the helper against :9093 — not a separate program here.
 *
 * Kafka topic "kafka-ex-security" <-> Events-Store channel "kafka.kafka-ex-security".
 *
 * Run: npx tsx security/sasl-plain-scram/index.ts
 */
import { Kafka } from 'kafkajs';
import { newKafka, newProducer, newAdmin, bootstrap, assert, sleep, runExample } from '../../shared/client.js';

const TOPIC = 'kafka-ex-security';

async function main(): Promise<void> {
  const mechanism = process.env.KUBEMQ_KAFKA_SASL_MECHANISM;
  const tls = process.env.KUBEMQ_KAFKA_TLS === 'true';
  const authMode = mechanism ? `SASL/${mechanism.toUpperCase()}${tls ? ' + TLS' : ''}` : 'no-auth (dev default)';
  const kafka: Kafka = newKafka();
  console.log(`Connecting to KubeMQ Kafka connector at ${bootstrap()} using ${authMode} (topic "${TOPIC}")`);

  const admin = newAdmin(kafka);
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: TOPIC, numPartitions: 1 }], waitForLeaders: true });

  // Authenticated (or dev no-auth) produce/consume round-trip on the same code path.
  const producer = newProducer(kafka);
  await producer.connect();
  await producer.send({ topic: TOPIC, acks: -1, messages: [{ value: 'secure-hello' }] });
  console.log('Produced 1 record over the authenticated connection');

  const consumer = kafka.consumer({ groupId: `security-verify-${Date.now()}` });
  const seen: string[] = [];
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
  await consumer.run({ eachMessage: async ({ message }) => { seen.push(message.value?.toString() ?? ''); } });
  const deadline = Date.now() + 8000;
  while (seen.length < 1 && Date.now() < deadline) await sleep(100);
  await consumer.stop(); await consumer.disconnect();
  console.log(`Consumed back: [${seen.join(', ')}]`);
  assert(seen.includes('secure-hello'), 'authenticated round-trip failed: record not read back');

  // Denied path (opt-in): produce to a topic the principal is not authorized to write.
  const deniedTopic = process.env.KUBEMQ_KAFKA_DENIED_TOPIC;
  if (deniedTopic) {
    let denied = false;
    try {
      await producer.send({ topic: deniedTopic, acks: -1, messages: [{ value: 'should-be-denied' }] });
    } catch (err) {
      const type = (err as { type?: string }).type ?? String(err);
      denied = /AUTHORIZATION_FAILED/i.test(type) || /AUTHORIZATION_FAILED/i.test(String(err));
      console.log(`Produce to denied topic "${deniedTopic}" -> ${type}`);
    }
    assert(denied, `produce to "${deniedTopic}" was NOT rejected with *_AUTHORIZATION_FAILED`);
  } else {
    console.log('Denied-path check skipped (set KUBEMQ_KAFKA_DENIED_TOPIC to exercise *_AUTHORIZATION_FAILED)');
  }

  await producer.disconnect();
  await admin.deleteTopics({ topics: [TOPIC] });
  await admin.disconnect();

  if (!mechanism) {
    console.log('\nNote: ran WITHOUT SASL (dev default). Set KUBEMQ_KAFKA_SASL_MECHANISM=plain|scram-sha-256|scram-sha-512');
    console.log('      + KUBEMQ_KAFKA_SASL_USERNAME/_PASSWORD against a secured broker to exercise authentication.');
  }
  console.log(`\nSecurity round-trip proven over ${authMode}`);
}

runExample(main);
