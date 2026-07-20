/**
 * Shared client/config helper for every KubeMQ Kafka example.
 *
 * The KubeMQ Kafka connector speaks the real Apache Kafka wire protocol on a
 * dedicated listener (plain TCP :9092, TLS :9093). Examples connect by pointing
 * kafkajs `brokers` at the connector's bootstrap address — no library swap, no
 * code change versus a real-Kafka app. The same code works against a real broker.
 *
 *  - Reads KUBEMQ_KAFKA_BOOTSTRAP (default localhost:9092) and uses it as the single
 *    entry in kafkajs `brokers`. This is the honest analog of `bootstrap.servers`
 *    (a host:port list, NOT a URL — hence _BOOTSTRAP, not _URL).
 *  - The connector is DISABLED by default: the broker must be started with
 *    CONNECTORS_KAFKA_ENABLE=true, and AdvertisedHost must be set for external
 *    clients (empty -> pod hostname -> connect-then-hang; gotchas #1 and #2).
 *  - kafkajs producers are pinned to the murmur2 DefaultPartitioner (Java/franz-go
 *    compatible), NOT the librdkafka CRC32 partitioner — see gotcha #4. A pre-2.0
 *    kafkajs would default to LegacyPartitioner, so this repo floors kafkajs at 2.x.
 */
import { Kafka, Partitioners, logLevel } from 'kafkajs';
import type { KafkaConfig, Producer, Consumer, Admin, SASLOptions } from 'kafkajs';

/** The connector bootstrap address, e.g. localhost:9092 (the `bootstrap.servers` value). */
export function bootstrap(): string {
  return process.env.KUBEMQ_KAFKA_BOOTSTRAP ?? 'localhost:9092';
}

/** A stable client id so connector-side metrics/logs attribute this suite. */
export function clientId(): string {
  return process.env.KUBEMQ_KAFKA_CLIENT_ID ?? 'kubemq-kafka-examples-js';
}

/**
 * Optional SASL/TLS config, read from env so `security/sasl-plain-scram` and the
 * documented TLS path reuse the same factory. Returns undefined for the default
 * no-auth dev broker (spec §4.3).
 *   KUBEMQ_KAFKA_SASL_MECHANISM = plain | scram-sha-256 | scram-sha-512
 *   KUBEMQ_KAFKA_SASL_USERNAME / _PASSWORD
 *   KUBEMQ_KAFKA_TLS = "true" -> security.protocol=SSL against :9093
 */
function saslFromEnv(): SASLOptions | undefined {
  const mechanism = process.env.KUBEMQ_KAFKA_SASL_MECHANISM;
  if (!mechanism) return undefined;
  return {
    mechanism: mechanism as SASLOptions['mechanism'],
    username: process.env.KUBEMQ_KAFKA_SASL_USERNAME ?? '',
    password: process.env.KUBEMQ_KAFKA_SASL_PASSWORD ?? '',
  } as SASLOptions;
}

/**
 * Every kafkajs client (producer/consumer/admin) keeps sockets and background
 * timers alive, so a process that stops after a *failed* assertion — without
 * disconnecting its clients — hangs until the outer `timeout` kills it (~35s).
 * We track every client created from a handle returned by `newKafka` and tear
 * them all down in `runExample`'s finally block so a failing run reports in ~1s.
 */
type Disconnectable = { disconnect: () => Promise<unknown> };
const trackedClients: Disconnectable[] = [];

function track<T extends Disconnectable>(client: T): T {
  trackedClients.push(client);
  return client;
}

/** Await `p`, but give up after `ms`. The timer is cleared on settle and unref'd so it
 *  can never, by itself, keep the process alive (which would defeat the whole point). */
function withTimeout(p: Promise<unknown>, ms: number): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    if (typeof (timer as { unref?: () => void }).unref === 'function') (timer as { unref: () => void }).unref();
    p.then(
      () => { clearTimeout(timer); resolve(); },
      () => { clearTimeout(timer); resolve(); },
    );
  });
}

async function disconnectAll(): Promise<void> {
  const clients = trackedClients.splice(0, trackedClients.length);
  await Promise.allSettled(
    // Bound each disconnect so a wedged socket can't reintroduce the hang.
    clients.map((c) => withTimeout(Promise.resolve().then(() => c.disconnect()), 3000)),
  );
}

/** Build a kafkajs Kafka handle pointed at the KubeMQ Kafka connector. */
export function newKafka(overrides: Partial<KafkaConfig> = {}): Kafka {
  const sasl = saslFromEnv();
  const cfg: KafkaConfig = {
    clientId: clientId(),
    brokers: [bootstrap()],
    ssl: process.env.KUBEMQ_KAFKA_TLS === 'true' || undefined,
    sasl,
    logLevel: logLevel.NOTHING, // examples print their own progress; silence kafkajs chatter
    ...overrides,
  };
  const kafka = new Kafka(cfg);
  // Auto-track every client this handle creates — including consumers built via
  // `kafka.consumer(...)` directly — so `runExample` can always disconnect them.
  const origConsumer = kafka.consumer.bind(kafka);
  kafka.consumer = (config) => track(origConsumer(config));
  const origProducer = kafka.producer.bind(kafka);
  kafka.producer = (config) => track(origProducer(config));
  const origAdmin = kafka.admin.bind(kafka);
  kafka.admin = (config) => track(origAdmin(config));
  return kafka;
}

/**
 * Build a producer pinned to the murmur2 DefaultPartitioner (Java/franz-go compatible).
 * Pass { idempotent: true } / { transactionalId } for variants 2 and 11/12.
 */
export function newProducer(kafka: Kafka, opts: Record<string, unknown> = {}): Producer {
  return kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner, // murmur2, NOT CRC32 (gotcha #4)
    ...opts,
  });
}

/** Build a consumer for `groupId`. */
export function newConsumer(kafka: Kafka, groupId: string, opts: Record<string, unknown> = {}): Consumer {
  return kafka.consumer({ groupId, ...opts });
}

/** Build an AdminClient (CreateTopics / DeleteTopics / CreatePartitions / offsets / configs). */
export function newAdmin(kafka: Kafka): Admin {
  return kafka.admin();
}

/** Native KubeMQ gRPC broker address for the interop native peer (default localhost:50000). */
export function brokerAddress(): string {
  return process.env.KUBEMQ_BROKER_ADDRESS ?? 'localhost:50000';
}

/** Kafka topic <-> Events-Store channel `kafka.<topic>` (channel prefix "kafka."). */
export function channelForTopic(topic: string): string {
  return `kafka.${topic}`;
}

/**
 * Assertion helper. Examples are runnable PROOFS, not demos: a failed assertion
 * must fail the process (exit non-zero). On failure this throws; `runExample(main)`
 * at each entrypoint prints the error, disconnects every client, and exits 1.
 */
export function assert(condition: unknown, message: string): asserts condition {
  if (!condition) {
    throw new Error(`ASSERTION FAILED: ${message}`);
  }
}

/** Sleep for `ms` milliseconds. */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Standard failure handler for example entrypoints: print + exit non-zero. */
export function fail(err: unknown): void {
  console.error('\nFAILED:', err instanceof Error ? err.message : err);
  process.exitCode = 1;
}

/**
 * Entrypoint wrapper every example uses instead of `main().catch(fail)`.
 *
 * Runs `main`; on any error (including a failed assertion) prints it via `fail`,
 * then — pass OR fail — disconnects every kafkajs client created through
 * `newKafka`. On failure it forces `process.exit(1)` so a leftover socket/timer
 * can't keep the process alive until the outer `timeout` fires. On success it
 * returns and lets the event loop drain naturally.
 */
export async function runExample(main: () => Promise<void>): Promise<void> {
  try {
    await main();
  } catch (err) {
    fail(err);
  } finally {
    await disconnectAll();
    if (process.exitCode && process.exitCode !== 0) {
      process.exit(process.exitCode);
    }
  }
}
