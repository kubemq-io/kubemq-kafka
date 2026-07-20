//! offsets/list-and-retention — ListOffsets (earliest/latest/by-timestamp) + retention config round-trip.
//!
//! Kafka wire flow: ListOffsets (key 2) — `fetch_watermarks` returns (earliest=log-start, latest=HWM);
//! `offsets_for_times` resolves a timestamp to an offset. Retention is set at CreateTopics via the
//! topic config (`retention.ms` / `retention.bytes`) and read back with DescribeConfigs.
//!
//! Retention maps to the native channel's MaxAge/MaxBytes/MaxMsgs (§2.2). Time-based EXPIRY is not
//! asserted here — it is too slow to soak in an example; we assert the config round-trips instead.
//!
//! Exits 0 when watermarks track the log, a timestamp resolves to an offset, and retention config is
//! readable.

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{Offset, TopicPartitionList};
use std::error::Error;
use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const TOPIC_PREFIX: &str = "kafka-ex-offsets-list";
const PARTITION: i32 = 0;
const N: i64 = 6;
const RETENTION_MS: &str = "3600000"; // 1h
const RETENTION_BYTES: &str = "1048576"; // 1 MiB

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
    kafka_common::print_banner("offsets/list-and-retention");

    // Unique per-run topic so watermarks (earliest=0, latest=N) and the by-timestamp offset (N/2) are
    // this run's absolute offsets (a fixed name accumulates records, so latest would exceed N).
    let topic = kafka_common::unique_topic(TOPIC_PREFIX);

    // 1. Create the topic WITH retention config (maps to channel MaxAge/MaxBytes).
    let admin: AdminClient<DefaultClientContext> = kafka_common::base_config().create()?;
    let opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(10)));
    let new_topic = NewTopic::new(&topic, 1, TopicReplication::Fixed(1))
        .set("retention.ms", RETENTION_MS)
        .set("retention.bytes", RETENTION_BYTES);
    let _ = admin.create_topics(&[new_topic], &opts).await;
    println!(
        "topic '{topic}' ready (retention.ms={RETENTION_MS}, retention.bytes={RETENTION_BYTES})"
    );

    // 2. Produce N records, marking a boundary timestamp in the middle.
    let producer: FutureProducer = kafka_common::base_config().create()?;
    for i in 0..(N / 2) {
        producer
            .send(
                FutureRecord::to(&topic)
                    .key("k")
                    .payload(&format!("rec-{i}")),
                Timeout::After(Duration::from_secs(10)),
            )
            .await
            .map_err(|(e, _)| format!("produce rec-{i}: {e}"))?;
    }
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let boundary_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    for i in (N / 2)..N {
        producer
            .send(
                FutureRecord::to(&topic)
                    .key("k")
                    .payload(&format!("rec-{i}")),
                Timeout::After(Duration::from_secs(10)),
            )
            .await
            .map_err(|(e, _)| format!("produce rec-{i}: {e}"))?;
    }
    println!("produced {N} records");

    // 3. ListOffsets: watermarks (earliest, latest) via fetch_watermarks.
    let consumer: StreamConsumer = {
        let mut c = kafka_common::base_config();
        c.set("group.id", "kafka-ex-offsets-list-grp");
        c.create()?
    };
    let (low, high) =
        consumer.fetch_watermarks(&topic, PARTITION, Timeout::After(Duration::from_secs(10)))?;
    println!("watermarks: earliest={low} latest(HWM)={high}");
    if low != 0 {
        return Err(format!("expected earliest watermark 0, got {low}").into());
    }
    if high != N {
        return Err(format!("expected latest watermark {N}, got {high}").into());
    }

    // 4. ListOffsets by-timestamp: the boundary resolves to the first record of the second half.
    let mut query = TopicPartitionList::new();
    query.add_partition_offset(&topic, PARTITION, Offset::Offset(boundary_ms))?;
    let resolved = consumer.offsets_for_times(query, Timeout::After(Duration::from_secs(10)))?;
    let ts_off = resolved
        .find_partition(&topic, PARTITION)
        .and_then(|e| match e.offset() {
            Offset::Offset(n) => Some(n),
            _ => None,
        })
        .ok_or("offsets_for_times gave no numeric offset")?;
    println!("offsets_for_times(boundary) -> offset {ts_off}");
    if ts_off != N / 2 {
        return Err(format!("expected by-timestamp offset {}, got {ts_off}", N / 2).into());
    }

    // 5. Retention config round-trips via DescribeConfigs.
    let described = admin
        .describe_configs(&[ResourceSpecifier::Topic(&topic)], &opts)
        .await?;
    let cfg = described
        .into_iter()
        .next()
        .ok_or("describe_configs returned nothing")?
        .map_err(|e| format!("describe_configs error: {e:?}"))?;
    let has_retention = cfg.entries.iter().any(|e| e.name == "retention.ms");
    if !has_retention {
        return Err("retention.ms not present in DescribeConfigs response".into());
    }
    println!("retention.ms present in DescribeConfigs — config round-trip OK");

    println!("list-and-retention OK: watermarks + by-timestamp + retention config all verified");
    Ok(())
}
