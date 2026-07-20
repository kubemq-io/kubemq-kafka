//! consume/from-beginning-latest — `auto.offset.reset` earliest vs latest, driven by Fetch long-poll.
//!
//! Kafka wire flow: Metadata -> (Find/JoinGroup) -> Fetch (bounded long-poll). A brand-new group with
//! no committed offset applies `auto.offset.reset`: `earliest` starts at the log start, `latest` starts
//! at the current high-water mark (only records produced AFTER the consumer joins).
//!
//! Plan: seed 3 records; an `earliest` group sees all 3; a `latest` group that subscribes first, THEN
//! sees only records produced after it joined.
//!
//! Exits 0 when earliest sees the seeded records and latest sees only the post-join records.

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::error::Error;
use std::process::ExitCode;
use std::time::Duration;

const TOPIC: &str = "kafka-ex-consume-reset";

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

fn consumer(group: &str, reset: &str) -> Result<StreamConsumer, Box<dyn Error>> {
    let mut c = kafka_common::base_config();
    c.set("group.id", group);
    c.set("auto.offset.reset", reset);
    c.set("enable.auto.commit", "false");
    Ok(c.create()?)
}

async fn produce(producer: &FutureProducer, payload: &str) -> Result<(), Box<dyn Error>> {
    let record = FutureRecord::to(TOPIC).key("k").payload(payload);
    producer
        .send(record, Timeout::After(Duration::from_secs(10)))
        .await
        .map_err(|(e, _)| format!("produce '{payload}': {e}"))?;
    Ok(())
}

/// Drain up to `max` records within `secs`, returning their payloads.
async fn drain(c: &StreamConsumer, max: usize, secs: u64) -> Result<Vec<String>, Box<dyn Error>> {
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while out.len() < max && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(3), c.recv()).await {
            Ok(Ok(m)) => out.push(
                m.payload()
                    .map(|b| String::from_utf8_lossy(b).into_owned())
                    .unwrap_or_default(),
            ),
            Ok(Err(e)) => return Err(format!("fetch error: {e}").into()),
            Err(_) => {} // idle poll; keep waiting until deadline
        }
    }
    Ok(out)
}

async fn run() -> Result<(), Box<dyn Error>> {
    kafka_common::print_banner("consume/from-beginning-latest");

    let producer: FutureProducer = kafka_common::base_config().create()?;

    // 1. Seed 3 pre-existing records.
    for i in 0..3 {
        produce(&producer, &format!("seed-{i}")).await?;
    }
    println!("seeded 3 records");

    // 2. earliest group: must see all 3 pre-existing records.
    let early = consumer("kafka-ex-consume-reset-early", "earliest")?;
    early.subscribe(&[TOPIC])?;
    let early_seen = drain(&early, 3, 15).await?;
    println!(
        "earliest group saw {} records: {early_seen:?}",
        early_seen.len()
    );
    if early_seen.len() < 3 {
        return Err(format!("earliest expected ≥3 records, got {}", early_seen.len()).into());
    }

    // 3. latest group: subscribe + poll to force the join at the current HWM, THEN produce 2 more.
    let late = consumer("kafka-ex-consume-reset-late", "latest")?;
    late.subscribe(&[TOPIC])?;
    // Poll for ~3s so the group actually joins and the "latest" position is pinned at the current
    // end BEFORE the post-records are produced. (A 0-max `drain` would return instantly without
    // ever polling, leaving the join to happen only on the first real recv() below — after the
    // post-records exist — so `latest` would resolve past them and the group would see nothing.)
    let warmup = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < warmup {
        let _ = tokio::time::timeout(Duration::from_secs(1), late.recv()).await;
    }
    for i in 0..2 {
        produce(&producer, &format!("post-{i}")).await?;
    }
    let late_seen = drain(&late, 2, 15).await?;
    println!(
        "latest group saw {} records: {late_seen:?}",
        late_seen.len()
    );

    // latest must see ONLY the post-join records, never the seeded ones.
    if late_seen.iter().any(|s| s.starts_with("seed-")) {
        return Err(
            "latest group saw a pre-existing 'seed-*' record; expected post-join only".into(),
        );
    }
    if !late_seen.iter().any(|s| s.starts_with("post-")) {
        return Err("latest group saw no post-join records".into());
    }
    println!("from-beginning-latest OK: earliest saw seeded, latest saw only post-join");
    Ok(())
}
