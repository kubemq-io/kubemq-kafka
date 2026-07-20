//! transactions/eos-commit-abort — exactly-once producer transactions: a committed record is visible
//! to a read_committed consumer, an aborted record is not.
//!
//! Kafka wire flow: InitProducerId (key 22, with a transactional.id) -> begin -> AddPartitionsToTxn
//! (key 24) -> Produce -> EndTxn (key 26, commit|abort). librdkafka drives AddPartitionsToTxn/EndTxn
//! internally; user code calls init/begin/commit/abort.
//!
//! Gotcha #7: a `/` in `transactional.id` is rejected with `INVALID_REQUEST` (Kafka error 42) — the
//! connector maps the id to a channel and `/` is illegal there. We use a safe id below.
//!
//! Gotcha #9 (KIP-890 V1 ceiling) — HONEST SCOPE: the connector implements the KIP-890 *V1* transaction
//! protocol. A same-epoch "zombie" producer can, in a narrow window, still append after a fence; this
//! residual is SHARED WITH UPSTREAM KAFKA at the V1 protocol level, NOT a connector defect. Full
//! hardening needs the V2 (KIP-890 part 2) protocol. Do not claim guarantees beyond spec §2.
//!
//! Exits 0 when the committed record is visible under read_committed and the aborted record is absent.

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use std::error::Error;
use std::process::ExitCode;
use std::time::Duration;

const TOPIC: &str = "kafka-ex-eos";
const GROUP: &str = "kafka-ex-eos-grp";
const TXN_ID: &str = "kafka-ex-eos-rust"; // no '/' -> avoids gotcha #7 INVALID_REQUEST
const COMMITTED: &str = "committed-record";
const ABORTED: &str = "aborted-record";

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
    kafka_common::print_banner("transactions/eos-commit-abort");

    // 1. Transactional producer (transactional.id forces idempotence + acks=all).
    let mut cfg = kafka_common::base_config();
    cfg.set("transactional.id", TXN_ID);
    let producer: FutureProducer = cfg.create()?;
    producer.init_transactions(Timeout::After(Duration::from_secs(30)))?; // InitProducerId (txn)
    println!("transactional producer initialized (transactional.id={TXN_ID})");

    // 2. Committed transaction: begin -> produce -> commit.
    producer.begin_transaction()?;
    producer
        .send(
            FutureRecord::to(TOPIC).key("k").payload(COMMITTED),
            Timeout::After(Duration::from_secs(10)),
        )
        .await
        .map_err(|(e, _)| format!("produce committed: {e}"))?;
    producer.commit_transaction(Timeout::After(Duration::from_secs(30)))?; // EndTxn(commit)
    println!("committed transaction with '{COMMITTED}'");

    // 3. Aborted transaction: begin -> produce -> abort.
    producer.begin_transaction()?;
    producer
        .send(
            FutureRecord::to(TOPIC).key("k").payload(ABORTED),
            Timeout::After(Duration::from_secs(10)),
        )
        .await
        .map_err(|(e, _)| format!("produce aborted: {e}"))?;
    producer.abort_transaction(Timeout::After(Duration::from_secs(30)))?; // EndTxn(abort)
    println!("aborted transaction with '{ABORTED}'");

    // 4. read_committed consumer must see COMMITTED and never ABORTED.
    let consumer: StreamConsumer = {
        let mut c = kafka_common::base_config();
        c.set("group.id", GROUP);
        c.set("auto.offset.reset", "earliest");
        c.set("enable.auto.commit", "false");
        c.set("isolation.level", "read_committed");
        c.create()?
    };
    consumer.subscribe(&[TOPIC])?;

    // Drain to the END of the log (not just until the first committed record): the ABORTED record
    // lands at a HIGHER offset than the COMMITTED one, so we must read PAST the committed record to
    // prove the aborted one is truly absent. Stopping early would let a broken read_committed leak
    // the aborted record undetected (plan §8: an aborted-txn leak MUST fail the process).
    let mut saw_committed = false;
    let mut idle = 0u8;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(4), consumer.recv()).await {
            Ok(Ok(m)) => {
                let body = m
                    .payload()
                    .map(|b| String::from_utf8_lossy(b).into_owned())
                    .unwrap_or_default();
                println!("read_committed fetched offset={} body='{body}'", m.offset());
                if body == ABORTED {
                    return Err(
                        "read_committed consumer saw the ABORTED record — isolation broken".into(),
                    );
                }
                if body == COMMITTED {
                    saw_committed = true;
                }
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
    if !saw_committed {
        return Err("read_committed consumer never saw the COMMITTED record".into());
    }
    println!("eos-commit-abort OK: committed visible, aborted absent under read_committed");
    Ok(())
}
