//! produce/compression-and-keys — round-trip a keyed record under every codec
//! (none/gzip/snappy/lz4/zstd) and prove keyed partitioning is STABLE per key.
//!
//! Gotcha #4 (the load-bearing Rust caveat): `rdkafka` is librdkafka-based, so its default partitioner
//! is `consistent_random` = **CRC32(key) % partitions**. Java kafka-clients, franz-go, and kafkajs v2+
//! default to **murmur2**, so THE SAME KEY LANDS ON A DIFFERENT PARTITION from those clients. We assert
//! only that this client's own keyed partitioning is deterministic (same key -> same partition) — we do
//! NOT force murmur2, because this example demonstrates native Rust behavior. See
//! `docs/concepts/cross-client-partitioning.md`.
//!
//! `zstd` requires the rdkafka `zstd` feature (declared in the workspace deps); gzip/snappy/lz4 are
//! bundled with librdkafka.
//!
//! Exits 0 when every codec round-trips and each key maps to a single stable partition.

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::collections::HashMap;
use std::error::Error;
use std::process::ExitCode;
use std::time::Duration;

const TOPIC_PREFIX: &str = "kafka-ex-produce-compkeys";
const GROUP: &str = "kafka-ex-produce-compkeys-grp";
const PARTITIONS: i32 = 4;
const CODECS: [&str; 5] = ["none", "gzip", "snappy", "lz4", "zstd"];
const KEYS: [&str; 3] = ["alpha", "beta", "gamma"];

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("FAILED: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    kafka_common::print_banner("produce/compression-and-keys");

    // Unique per-run topic so the round-trip fetches THIS run's records (a fixed name accumulates
    // prior runs, so the count-based check would pass while reading stale records).
    let topic = kafka_common::unique_topic(TOPIC_PREFIX);

    // 1. Create a multi-partition topic so keyed partitioning is observable (auto-create gives 1).
    let admin: AdminClient<DefaultClientContext> = kafka_common::base_config().create()?;
    let opts = AdminOptions::new();
    let _ = admin
        .create_topics(
            &[NewTopic::new(&topic, PARTITIONS, TopicReplication::Fixed(1))],
            &opts,
        )
        .await; // ignore "already exists" on re-run
    println!("topic '{topic}' ready with {PARTITIONS} partitions");

    // 2. One producer per codec: each sends a keyed record and must round-trip.
    let mut key_partition: HashMap<String, i32> = HashMap::new();
    for codec in CODECS {
        let mut cfg = kafka_common::base_config();
        cfg.set("compression.type", codec);
        let producer: FutureProducer = cfg.create()?;

        for key in KEYS {
            let payload = format!("{codec}:{key}");
            let record = FutureRecord::to(&topic).key(key).payload(&payload);
            let (partition, offset) = match producer
                .send(record, Timeout::After(Duration::from_secs(10)))
                .await
            {
                Ok(po) => po,
                Err((e, _)) => return Err(format!("produce codec={codec} key={key}: {e}").into()),
            };
            println!("codec={codec} key={key} -> partition={partition} offset={offset}");

            // Keyed partitioning must be STABLE: same key -> same partition every time (CRC32).
            match key_partition.get(key) {
                None => {
                    key_partition.insert(key.to_string(), partition);
                }
                Some(&prev) if prev != partition => {
                    return Err(format!(
                        "key '{key}' landed on partition {partition}, previously {prev} — \
                         keyed partitioning is not stable"
                    )
                    .into());
                }
                _ => {}
            }
        }
    }
    println!(
        "keyed partitioning stable (CRC32, librdkafka default): {:?}",
        key_partition
    );

    // 3. Fetch every record back (all codecs decode transparently on the consumer side).
    let consumer: StreamConsumer = {
        let mut c = kafka_common::base_config();
        c.set("group.id", GROUP);
        c.set("auto.offset.reset", "earliest");
        c.set("enable.auto.commit", "false");
        c.create()?
    };
    consumer.subscribe(&[&topic])?;

    let expected = CODECS.len() * KEYS.len();
    let mut received = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while received < expected && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), consumer.recv()).await {
            Ok(Ok(m)) => {
                let body = m
                    .payload()
                    .map(|b| String::from_utf8_lossy(b).into_owned())
                    .unwrap_or_default();
                println!(
                    "fetched partition={} offset={} body='{body}'",
                    m.partition(),
                    m.offset()
                );
                received += 1;
            }
            Ok(Err(e)) => return Err(format!("fetch error: {e}").into()),
            Err(_) => break,
        }
    }
    if received < expected {
        return Err(format!("expected {expected} records, fetched {received}").into());
    }
    println!("compression-and-keys OK: all {expected} records round-tripped across 5 codecs");
    Ok(())
}
