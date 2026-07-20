//! produce/idempotent — idempotent producer: enable.idempotence forces a per-(PID,partition,seq)
//! dedup on the broker so producer-internal retries never create duplicates.
//!
//! Kafka wire flow: InitProducerId (key 22) assigns a Producer ID; every Produce carries the PID +
//! a monotonic base sequence; the broker drops a re-delivered batch with a sequence it already saw.
//! We cannot force an internal retry deterministically from user code, so the honest, verifiable
//! assertion is EXACTLY-N delivery: produce N distinct records on an idempotent producer, Fetch back,
//! and assert exactly N arrive (no duplication, no gap). librdkafka assigns the PID internally and,
//! with `enable.idempotence=true`, forces `acks=all` and bounded in-flight.
//!
//! Exits 0 when exactly N records round-trip; non-zero on any duplicate/gap/failure.

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::collections::HashSet;
use std::error::Error;
use std::process::ExitCode;
use std::time::Duration;

const TOPIC_PREFIX: &str = "kafka-ex-produce-idem";
const GROUP: &str = "kafka-ex-produce-idem-grp";
const N: usize = 5;

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
    kafka_common::print_banner("produce/idempotent");

    // Unique per-run topic so the exactly-N / no-duplicate check reads THIS run's records. A fixed name
    // accumulates prior runs, so the consumer would stop after N stale records and never observe a
    // duplicate created in the current run.
    let topic = kafka_common::unique_topic(TOPIC_PREFIX);

    // 1. Idempotent producer: enable.idempotence => InitProducerId + acks=all + dedup on retry.
    let mut cfg = kafka_common::base_config();
    cfg.set("enable.idempotence", "true");
    let producer: FutureProducer = cfg.create()?;
    println!(
        "idempotent producer created (enable.idempotence=true; PID assigned via InitProducerId)"
    );

    // 2. Produce N distinct, keyed records. Any internal retry is deduped by (PID, partition, seq).
    for i in 0..N {
        let payload = format!("evt-{i}");
        let record = FutureRecord::to(&topic).key("orders").payload(&payload);
        match producer
            .send(record, Timeout::After(Duration::from_secs(10)))
            .await
        {
            Ok((p, o)) => println!("produced '{payload}' -> partition={p} offset={o}"),
            Err((e, _)) => return Err(format!("produce '{payload}' failed: {e}").into()),
        }
    }

    // 3. Fetch back and assert EXACTLY N distinct payloads (no duplicate from any retry).
    let consumer: StreamConsumer = {
        let mut c = kafka_common::base_config();
        c.set("group.id", GROUP);
        c.set("auto.offset.reset", "earliest");
        c.set("enable.auto.commit", "false");
        c.create()?
    };
    consumer.subscribe(&[&topic])?;

    let mut seen: Vec<String> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while seen.len() < N && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), consumer.recv()).await {
            Ok(Ok(m)) => {
                let body = m
                    .payload()
                    .map(|b| String::from_utf8_lossy(b).into_owned())
                    .unwrap_or_default();
                println!("fetched offset={} body='{body}'", m.offset());
                seen.push(body);
            }
            Ok(Err(e)) => return Err(format!("fetch error: {e}").into()),
            Err(_) => break,
        }
    }

    let unique: HashSet<&String> = seen.iter().collect();
    if seen.len() != N {
        return Err(format!("expected exactly {N} records, fetched {}", seen.len()).into());
    }
    if unique.len() != N {
        return Err(format!(
            "duplicate detected: {} records but only {} unique — idempotence dedup failed",
            seen.len(),
            unique.len()
        )
        .into());
    }
    println!("idempotent OK: exactly {N} distinct records, no duplicates");
    Ok(())
}
