//! produce/basic-acks — Produce under acks 0/1/all, then a Fetch round-trip; plus the oversized-record
//! rejection path (`MESSAGE_TOO_LARGE`).
//!
//! Kafka wire flow: Metadata (auto-create topic) -> Produce (RecordBatch v2, acks negotiated) ->
//! Fetch (bounded long-poll). Mirrors connector behavior in `connectors/kafka/` (Produce key 0,
//! Fetch key 1; oversized -> MESSAGE_TOO_LARGE per docs 24-kafka.md ✅-Full surface, §2.3).
//!
//! Gotcha #3: on a MULTI-NODE deployment `acks=0` on a follower can silently drop — always use
//! `acks>=1` (here `acks=all`) for durability. On a single dev node acks=0 is fine for the demo.
//!
//! Exits 0 on a clean round-trip + rejected oversized; non-zero on any failed assertion.

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::util::Timeout;
use std::error::Error;
use std::process::ExitCode;
use std::time::Duration;

const TOPIC_PREFIX: &str = "kafka-ex-produce-acks"; // spec §4.2 naming: kafka-ex-<family>-<short>
const GROUP: &str = "kafka-ex-produce-acks-grp";

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

fn producer(acks: &str) -> Result<FutureProducer, KafkaError> {
    let mut cfg: ClientConfig = kafka_common::base_config();
    cfg.set("acks", acks);
    cfg.create()
}

async fn run() -> Result<(), Box<dyn Error>> {
    kafka_common::print_banner("produce/basic-acks");

    // Unique per-run topic so the round-trip fetches THIS run's 3 records (a fixed name accumulates
    // prior-run records, so a count-only check would pass while reading stale records).
    let topic = kafka_common::unique_topic(TOPIC_PREFIX);

    // 1. Produce the same record under acks 0, 1, and all — each must be accepted by the connector.
    for acks in ["0", "1", "all"] {
        let p = producer(acks)?;
        let payload = format!("order-42 acks={acks}");
        let record = FutureRecord::to(&topic).key("k1").payload(&payload);
        // FutureProducer::send returns Result<(partition, offset), (KafkaError, OwnedMessage)>.
        match p
            .send(record, Timeout::After(Duration::from_secs(10)))
            .await
        {
            Ok((partition, offset)) => {
                println!("produced acks={acks} -> partition={partition} offset={offset}");
            }
            Err((e, _)) => return Err(format!("produce acks={acks} failed: {e}").into()),
        }
    }

    // 2. Fetch the records back with a fresh consumer group reading from the beginning.
    let consumer: StreamConsumer = {
        let mut cfg = kafka_common::base_config();
        cfg.set("group.id", GROUP);
        cfg.set("auto.offset.reset", "earliest");
        cfg.set("enable.auto.commit", "false");
        cfg.create()?
    };
    consumer.subscribe(&[&topic])?;

    let mut received = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while received < 3 && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), consumer.recv()).await {
            Ok(Ok(m)) => {
                let body = m
                    .payload()
                    .map(|b| String::from_utf8_lossy(b).into_owned())
                    .unwrap_or_default();
                println!("fetched offset={} body='{body}'", m.offset());
                received += 1;
            }
            Ok(Err(e)) => return Err(format!("fetch error: {e}").into()),
            Err(_) => break, // poll timeout
        }
    }
    if received < 3 {
        return Err(format!("expected to fetch 3 records, got {received}").into());
    }
    println!("round-trip OK: produced+fetched 3 records under acks 0/1/all");

    // 3. Oversized record must be rejected by the BROKER with MESSAGE_TOO_LARGE (connector
    //    MaxMessageBytes = 1 MiB). librdkafka's own default `message.max.bytes` (~1 MB) would reject a
    //    2 MiB record client-side before it ever reaches the wire, so we deliberately RAISE the client
    //    cap to 2 MiB and send a 1.5 MiB record: it leaves the client, fits inside the 2 MiB frame cap,
    //    reaches the broker, and the broker's guard returns MESSAGE_TOO_LARGE -> rdkafka
    //    `MessageSizeTooLarge`. (Compression stays off so the on-wire size is the payload size.)
    let p: FutureProducer = {
        let mut cfg: ClientConfig = kafka_common::base_config();
        cfg.set("acks", "all");
        cfg.set("message.max.bytes", "2097152"); // 2 MiB — above the broker's 1 MiB, below the frame cap
        cfg.create()?
    };
    let huge = vec![b'x'; 1_572_864]; // 1.5 MiB: > broker 1 MiB cap, < 2 MiB frame cap -> broker rejects
    let oversized = FutureRecord::to(&topic).key("big").payload(&huge);
    match p
        .send(oversized, Timeout::After(Duration::from_secs(10)))
        .await
    {
        Ok(_) => return Err("oversized record was accepted; expected MESSAGE_TOO_LARGE".into()),
        Err((KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge), _)) => {
            println!("oversized 1.5 MiB record correctly rejected by the broker: MESSAGE_TOO_LARGE");
        }
        Err((e, _)) => {
            let s = e.to_string();
            if s.contains("MessageSizeTooLarge") || s.contains("MSG_SIZE_TOO_LARGE") {
                println!("oversized 1.5 MiB record correctly rejected: MESSAGE_TOO_LARGE ({s})");
            } else {
                return Err(format!("oversized rejected with unexpected error: {e}").into());
            }
        }
    }

    println!("basic-acks complete");
    Ok(())
}
