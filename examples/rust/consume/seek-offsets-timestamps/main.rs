//! consume/seek-offsets-timestamps — reposition a consumer by explicit offset (`seek`) and by
//! timestamp (`offsets_for_times` / ListOffsets by-timestamp).
//!
//! Kafka wire flow: assign a partition, then Fetch from an arbitrary position. `seek(offset)` moves the
//! next Fetch to a specific log offset; `offsets_for_times` issues ListOffsets(-3, by timestamp) and
//! returns the first offset whose record timestamp is >= the query time.
//!
//! Plan: produce a first batch, mark a wall-clock boundary, produce a second batch; reposition to offset
//! 2 (set at assign time) and assert the next record is offset 2; then resolve the boundary timestamp to
//! an offset and `seek()` there (the consumer is active by now), asserting the next record is the first
//! of the second batch.
//!
//! Exits 0 when both repositioning paths land on the expected record.

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{Offset, TopicPartitionList};
use std::error::Error;
use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const TOPIC_PREFIX: &str = "kafka-ex-consume-seek";
const GROUP: &str = "kafka-ex-consume-seek-grp";
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

async fn produce(producer: &FutureProducer, topic: &str, payload: &str) -> Result<(), Box<dyn Error>> {
    let record = FutureRecord::to(topic).key("k").payload(payload);
    producer
        .send(record, Timeout::After(Duration::from_secs(10)))
        .await
        .map_err(|(e, _)| format!("produce '{payload}': {e}"))?;
    Ok(())
}

async fn recv_one(c: &StreamConsumer) -> Result<(i64, String), Box<dyn Error>> {
    match tokio::time::timeout(Duration::from_secs(10), c.recv()).await {
        Ok(Ok(m)) => Ok((
            m.offset(),
            m.payload()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .unwrap_or_default(),
        )),
        Ok(Err(e)) => Err(format!("fetch error: {e}").into()),
        Err(_) => Err("timed out waiting for a record".into()),
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    kafka_common::print_banner("consume/seek-offsets-timestamps");

    // Unique per-run topic so offsets 0..4 are THIS run's records (fixed names accumulate across runs
    // and break the offset/timestamp assertions on re-run).
    let topic = kafka_common::unique_topic(TOPIC_PREFIX);

    let producer: FutureProducer = kafka_common::base_config().create()?;

    // 1. First batch (offsets 0,1,2 on a fresh topic).
    for i in 0..3 {
        produce(&producer, &topic, &format!("rec-{i}")).await?;
    }
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let boundary_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    // 2. Second batch (offsets 3,4) — all with timestamps >= boundary_ms.
    for i in 3..5 {
        produce(&producer, &topic, &format!("rec-{i}")).await?;
    }
    println!("produced 5 records; boundary timestamp = {boundary_ms} (between rec-2 and rec-3)");

    // 3. Reposition by explicit offset. Set the start position AT ASSIGN time (add_partition_offset)
    //    rather than assign()+seek(): librdkafka only allows seek() on a partition whose fetch has
    //    already been activated by a poll — seeking immediately after assign() yields
    //    `Local: Erroneous state`. Repositioning at assignment needs no post-assign seek.
    let consumer: StreamConsumer = {
        let mut c = kafka_common::base_config();
        c.set("group.id", GROUP);
        c.set("enable.auto.commit", "false");
        c.create()?
    };
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic, PARTITION, Offset::Offset(2))?;
    consumer.assign(&tpl)?;

    let (off, body) = recv_one(&consumer).await?;
    println!("assigned at offset=2 -> next record offset={off} body='{body}'");
    if off != 2 {
        return Err(format!("seek by offset: expected offset 2, got {off}").into());
    }

    // 4. Resolve the boundary timestamp to an offset via ListOffsets-by-timestamp, then seek there.
    //    The consumer is now active (the recv above pumped the assignment), so seek() is valid.
    let mut query = TopicPartitionList::new();
    query.add_partition_offset(&topic, PARTITION, Offset::Offset(boundary_ms))?;
    let resolved = consumer.offsets_for_times(query, Timeout::After(Duration::from_secs(10)))?;
    let elem = resolved
        .find_partition(&topic, PARTITION)
        .ok_or("offsets_for_times returned no entry for the partition")?;
    let ts_offset = match elem.offset() {
        Offset::Offset(n) => n,
        other => {
            return Err(format!("offsets_for_times gave a non-numeric offset: {other:?}").into())
        }
    };
    println!("offsets_for_times(boundary) -> offset {ts_offset}");

    consumer.seek(
        &topic,
        PARTITION,
        Offset::Offset(ts_offset),
        Timeout::After(Duration::from_secs(10)),
    )?;
    let (off2, body2) = recv_one(&consumer).await?;
    println!("seek(by-timestamp) -> next record offset={off2} body='{body2}'");
    // The first record at/after the boundary is rec-3 (offset 3).
    if off2 < 3 {
        return Err(format!(
            "by-timestamp: expected first record >= boundary (offset 3), got {off2}"
        )
        .into());
    }
    if body2 != "rec-3" {
        return Err(format!("by-timestamp: expected body 'rec-3', got '{body2}'").into());
    }
    println!("seek-offsets-timestamps OK: offset seek and timestamp seek both landed correctly");
    Ok(())
}
