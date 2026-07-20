//! consumer-groups/join-rebalance — two consumers in one group split the partitions of a topic; the
//! union of what they consume covers every record (no loss across the rebalance).
//!
//! Kafka wire flow: FindCoordinator -> JoinGroup -> SyncGroup -> Heartbeat -> (LeaveGroup). With a
//! 2-partition topic and 2 members, the group leader assigns one partition to each member. This example
//! asserts the load-bearing property: **no record is lost** when the group rebalances.
//!
//! Requires a multi-partition topic (created here with N=2). On a single-partition topic only one member
//! can hold the partition — see the README note.
//!
//! Exits 0 when the two members together consume every produced record and each is assigned a partition.

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::collections::HashSet;
use std::error::Error;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

const TOPIC: &str = "kafka-ex-cg-rebalance";
const GROUP: &str = "kafka-ex-cg-rebalance-grp";
const PARTITIONS: i32 = 2;
const N: usize = 12;

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

/// Subscribe and drain until idle; record every payload into the shared set. Returns (payloads seen by
/// this member, partitions assigned to this member at the end).
async fn member(
    label: &'static str,
    c: StreamConsumer,
    seen: Arc<Mutex<HashSet<String>>>,
) -> Result<(usize, usize), Box<dyn Error + Send + Sync>> {
    c.subscribe(&[TOPIC]).map_err(|e| e.to_string())?;
    let mut count = 0usize;
    let mut idle = 0u8;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), c.recv()).await {
            Ok(Ok(m)) => {
                let body = m
                    .payload()
                    .map(|b| String::from_utf8_lossy(b).into_owned())
                    .unwrap_or_default();
                seen.lock().await.insert(body);
                count += 1;
                idle = 0;
            }
            Ok(Err(e)) => return Err(e.to_string().into()),
            Err(_) => {
                idle += 1;
                if idle >= 4 {
                    break; // ~8s idle -> done
                }
            }
        }
    }
    let assigned = c.assignment().map_err(|e| e.to_string())?.count();
    println!("[{label}] consumed {count} records, assigned {assigned} partition(s)");
    Ok((count, assigned))
}

fn make_consumer() -> Result<StreamConsumer, Box<dyn Error>> {
    let mut c = kafka_common::base_config();
    c.set("group.id", GROUP);
    c.set("auto.offset.reset", "earliest");
    c.set("enable.auto.commit", "false");
    Ok(c.create()?)
}

async fn run() -> Result<(), Box<dyn Error>> {
    kafka_common::print_banner("consumer-groups/join-rebalance");

    // 1. Multi-partition topic so two members can each own a partition.
    let admin: AdminClient<DefaultClientContext> = kafka_common::base_config().create()?;
    let _ = admin
        .create_topics(
            &[NewTopic::new(TOPIC, PARTITIONS, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await;
    println!("topic '{TOPIC}' ready with {PARTITIONS} partitions");

    // 2. Produce N records spread across keys (so both partitions receive traffic).
    let producer: FutureProducer = kafka_common::base_config().create()?;
    for i in 0..N {
        let key = format!("key-{}", i % 6);
        let payload = format!("msg-{i}");
        let record = FutureRecord::to(TOPIC).key(&key).payload(&payload);
        producer
            .send(record, Timeout::After(Duration::from_secs(10)))
            .await
            .map_err(|(e, _)| format!("produce {payload}: {e}"))?;
    }
    println!("produced {N} records");

    // 3. Two members join the same group concurrently and split the partitions.
    let seen = Arc::new(Mutex::new(HashSet::<String>::new()));
    let a = member("A", make_consumer()?, seen.clone());
    let b = member("B", make_consumer()?, seen.clone());
    let (ra, rb) = tokio::join!(a, b);
    let (_ca, aa) = ra.map_err(|e| e.to_string())?;
    let (_cb, ab) = rb.map_err(|e| e.to_string())?;

    // 4. Assertions: no record lost, and each member ended up with a partition.
    let union = seen.lock().await;
    if union.len() != N {
        return Err(format!(
            "record loss across rebalance: produced {N}, group saw {} distinct",
            union.len()
        )
        .into());
    }
    if aa == 0 || ab == 0 {
        return Err(format!("expected each member to own a partition, got A={aa} B={ab}").into());
    }
    if aa + ab != PARTITIONS as usize {
        return Err(format!(
            "expected the {PARTITIONS} partitions split across members, got A={aa} B={ab}"
        )
        .into());
    }
    println!("join-rebalance OK: {N}/{N} records covered, partitions split {aa}+{ab}");
    Ok(())
}
