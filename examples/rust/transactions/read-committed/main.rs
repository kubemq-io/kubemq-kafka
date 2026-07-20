//! transactions/read-committed — a `read_committed` consumer never delivers aborted records, and the
//! delivered set is smaller than the high-water mark (the aborted record + txn control markers occupy
//! offsets a read_committed reader skips).
//!
//! Kafka wire flow: Fetch (key 1) returns records plus an `AbortedTransactions` list; the CLIENT filters
//! aborted records out (gotcha #12 — filtering is client-side, there is no server-side record filter).
//! ListOffsets(latest) under read_committed returns the Last Stable Offset (LSO), which lags the HWM
//! while a transaction is open.
//!
//! Gotcha #9 (KIP-890 V1 ceiling) cited — see the README; no guarantee is claimed beyond spec §2.
//!
//! Exits 0 when only committed records are delivered and delivered_count < HWM.

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use std::error::Error;
use std::process::ExitCode;
use std::time::Duration;

const TOPIC: &str = "kafka-ex-readcommitted";
const GROUP: &str = "kafka-ex-readcommitted-grp";
const TXN_ID: &str = "kafka-ex-readcommitted-rust";
const PARTITION: i32 = 0;

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
    kafka_common::print_banner("transactions/read-committed");

    // 1. Transactional producer: one committed txn (2 records), one aborted txn (1 record).
    let mut cfg = kafka_common::base_config();
    cfg.set("transactional.id", TXN_ID);
    let producer: FutureProducer = cfg.create()?;
    producer.init_transactions(Timeout::After(Duration::from_secs(30)))?;

    producer.begin_transaction()?;
    for i in 0..2 {
        producer
            .send(
                FutureRecord::to(TOPIC)
                    .key("k")
                    .payload(&format!("commit-{i}")),
                Timeout::After(Duration::from_secs(10)),
            )
            .await
            .map_err(|(e, _)| format!("produce commit-{i}: {e}"))?;
    }
    producer.commit_transaction(Timeout::After(Duration::from_secs(30)))?;
    println!("committed 2 records (commit-0, commit-1)");

    producer.begin_transaction()?;
    producer
        .send(
            FutureRecord::to(TOPIC).key("k").payload("abort-0"),
            Timeout::After(Duration::from_secs(10)),
        )
        .await
        .map_err(|(e, _)| format!("produce abort-0: {e}"))?;
    producer.abort_transaction(Timeout::After(Duration::from_secs(30)))?;
    println!("aborted 1 record (abort-0)");

    // 2. read_committed consumer: collect everything it delivers.
    let consumer: StreamConsumer = {
        let mut c = kafka_common::base_config();
        c.set("group.id", GROUP);
        c.set("auto.offset.reset", "earliest");
        c.set("enable.auto.commit", "false");
        c.set("isolation.level", "read_committed");
        c.create()?
    };
    consumer.subscribe(&[TOPIC])?;

    // Drain to the END of the log (not just until the 2 committed records): the aborted record
    // lands at a HIGHER offset than the committed ones, so we must read PAST them to prove it is
    // never delivered. Stopping at 2 would let a broken read_committed leak the aborted record
    // undetected (plan §8: an aborted-txn leak MUST fail the process).
    let mut delivered: Vec<String> = Vec::new();
    let mut idle = 0u8;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(4), consumer.recv()).await {
            Ok(Ok(m)) => {
                let body = m
                    .payload()
                    .map(|b| String::from_utf8_lossy(b).into_owned())
                    .unwrap_or_default();
                println!(
                    "read_committed delivered offset={} body='{body}'",
                    m.offset()
                );
                if body == "abort-0" {
                    return Err(
                        "read_committed delivered the ABORTED record — isolation broken".into(),
                    );
                }
                delivered.push(body);
                idle = 0;
            }
            Ok(Err(e)) => return Err(format!("fetch error: {e}").into()),
            Err(_) => {
                idle += 1;
                if idle >= 2 {
                    break; // reached the log end (LSO) with nothing more to deliver
                }
            }
        }
    }

    // 3. Assertions: both committed records delivered (the abort-leak check above ran over the whole
    //    log), and HWM exceeds the delivered count (aborted record + txn markers occupy the gap).
    //    Presence-based (not `== 2`) so a re-run against a non-empty topic stays green.
    if !delivered.iter().any(|s| s == "commit-0") || !delivered.iter().any(|s| s == "commit-1") {
        return Err(format!(
            "expected committed records [commit-0, commit-1] among delivered, got {delivered:?}"
        )
        .into());
    }
    let (_low, high) =
        consumer.fetch_watermarks(TOPIC, PARTITION, Timeout::After(Duration::from_secs(10)))?;
    println!(
        "delivered={} HWM={high} (aborted record + txn markers occupy the gap)",
        delivered.len()
    );
    if high <= delivered.len() as i64 {
        return Err(format!(
            "expected HWM > delivered ({}), got HWM={high} — aborted record/markers should advance HWM",
            delivered.len()
        )
        .into());
    }
    println!(
        "read-committed OK: aborted never delivered; delivered {} < HWM {high}",
        delivered.len()
    );
    Ok(())
}
