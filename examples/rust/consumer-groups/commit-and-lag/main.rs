//! consumer-groups/commit-and-lag — manual offset commit, resume-from-committed, and consumer lag.
//!
//! Kafka wire flow: OffsetCommit (key 8) persists the group's position; a later member OffsetFetch
//! (key 9) resumes there. Lag = high-water mark (latest offset) − committed offset.
//!
//! Plan: produce N; consumer #1 reads the first half and commits; consumer #2 (same group) resumes from
//! the committed offset (not from 0); lag is computed as HWM − committed and checked against the split.
//!
//! Exits 0 when the resume starts at the committed offset and the computed lag matches (HWM − committed).
//!
//! CONNECTOR NOTE (consumer teardown): closing a *subscribed* consumer against the KubeMQ Kafka
//! connector completes the group *close* (final OffsetCommit + LeaveGroup succeed — verified with
//! `debug=cgrp,consumer,broker`), but librdkafka's subsequent client *destroy* hangs: once the group
//! is left, the coordinator's nodename is cleared to "" and the `GroupCoordinator` broker thread spins
//! in "broker has no address yet: postponing connect", which `rd_kafka_destroy` waits on forever. The
//! group leave itself is done, so the demonstration is unaffected — we release each consumer on a
//! detached thread (the leave lands; the process exits before the stuck destroy matters) instead of
//! blocking on `drop`. This is a teardown-only quirk, not a correctness issue in commit/resume/lag.

use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::Offset;
use std::error::Error;
use std::process::ExitCode;
use std::time::Duration;

const TOPIC_PREFIX: &str = "kafka-ex-cg-commit";
const GROUP_PREFIX: &str = "kafka-ex-cg-commit-grp";
const N: i64 = 8;
const HALF: i64 = 4;

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

fn consumer(group: &str) -> Result<StreamConsumer, Box<dyn Error>> {
    let mut c = kafka_common::base_config();
    c.set("group.id", group);
    c.set("auto.offset.reset", "earliest");
    c.set("enable.auto.commit", "false"); // manual commit — the whole point
    Ok(c.create()?)
}

async fn run() -> Result<(), Box<dyn Error>> {
    kafka_common::print_banner("consumer-groups/commit-and-lag");

    // Unique per-run topic so the batch starts at offset 0 and the committed offset is HALF (fixed
    // names accumulate prior-run records, so this run's msg-0 would land past 0 and the committed
    // offset would exceed HALF).
    let topic = kafka_common::unique_topic(TOPIC_PREFIX);
    // Unique per-run group id too, for parity with the other-language examples (Go/C# suffix the group
    // with a uuid) and so the group's committed-offset store starts empty each run rather than
    // accumulating stale entries for every prior run's unique topic. (Consumer teardown is handled
    // separately — see the CONNECTOR NOTE at the top of the file.)
    let group = kafka_common::unique_topic(GROUP_PREFIX);

    // 1. Produce N records on a single partition.
    let producer: FutureProducer = kafka_common::base_config().create()?;
    for i in 0..N {
        let payload = format!("msg-{i}");
        let record = FutureRecord::to(&topic).key("orders").payload(&payload);
        producer
            .send(record, Timeout::After(Duration::from_secs(10)))
            .await
            .map_err(|(e, _)| format!("produce {payload}: {e}"))?;
    }
    println!("produced {N} records");

    // 2. Consumer #1: read the first HALF and commit synchronously on the last one read.
    let c1 = consumer(&group)?;
    c1.subscribe(&[&topic])?;
    let mut read = 0i64;
    let mut part = 0i32;
    while read < HALF {
        match tokio::time::timeout(Duration::from_secs(10), c1.recv()).await {
            Ok(Ok(m)) => {
                part = m.partition();
                let body = m
                    .payload()
                    .map(|b| String::from_utf8_lossy(b).into_owned())
                    .unwrap_or_default();
                read += 1;
                println!("[c1] read offset={} body='{body}'", m.offset());
                if read == HALF {
                    c1.commit_message(&m, CommitMode::Sync)?; // OffsetCommit key 8
                    println!("[c1] committed through offset {}", m.offset());
                }
            }
            Ok(Err(e)) => return Err(format!("[c1] fetch error: {e}").into()),
            Err(_) => return Err("[c1] timed out before reading first half".into()),
        }
    }

    // 3. Lag = HWM − committed, taken while the second half is still unread.
    let (_low, high) = c1.fetch_watermarks(&topic, part, Timeout::After(Duration::from_secs(10)))?;
    let committed_tpl = c1.committed(Timeout::After(Duration::from_secs(10)))?;
    let committed = committed_tpl
        .find_partition(&topic, part)
        .and_then(|e| match e.offset() {
            Offset::Offset(n) => Some(n),
            _ => None,
        })
        .ok_or("no committed offset found for the partition")?;
    let lag = high - committed;
    println!("HWM={high} committed={committed} lag={lag}");
    if committed != HALF {
        return Err(format!("expected committed offset {HALF}, got {committed}").into());
    }
    if lag != N - HALF {
        return Err(format!("expected lag {}, got {lag}", N - HALF).into());
    }
    // Release c1's membership. The consumer *close* (final OffsetCommit + LeaveGroup) runs on this
    // detached thread and completes quickly; we DON'T join it because librdkafka's subsequent client
    // *destroy* hangs against this connector — after the group leaves, the coordinator's nodename is
    // cleared and the GroupCoordinator broker thread spins in "no address yet: postponing connect",
    // which rd_kafka_destroy never finishes waiting on. See CONNECTOR NOTE at the top of run().
    std::thread::spawn(move || drop(c1));
    // Give the LeaveGroup a moment to land at the connector before c2 rejoins the (now-empty) group.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 4. Consumer #2 (same group): must RESUME from the committed offset, not from 0.
    let c2 = consumer(&group)?;
    c2.subscribe(&[&topic])?;
    let mut remaining = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while (remaining.len() as i64) < N - HALF && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), c2.recv()).await {
            Ok(Ok(m)) => {
                if m.offset() < committed {
                    return Err(format!(
                        "[c2] re-read committed record at offset {} — resume did not honor commit",
                        m.offset()
                    )
                    .into());
                }
                let body = m
                    .payload()
                    .map(|b| String::from_utf8_lossy(b).into_owned())
                    .unwrap_or_default();
                println!("[c2] resumed offset={} body='{body}'", m.offset());
                remaining.push(body);
            }
            Ok(Err(e)) => return Err(format!("[c2] fetch error: {e}").into()),
            Err(_) => break,
        }
    }
    if (remaining.len() as i64) != N - HALF {
        return Err(format!(
            "[c2] expected {} remaining records, got {}",
            N - HALF,
            remaining.len()
        )
        .into());
    }
    if remaining.first().map(String::as_str) != Some("msg-4") {
        return Err(format!(
            "[c2] expected to resume at 'msg-4', got {:?}",
            remaining.first()
        )
        .into());
    }
    println!("commit-and-lag OK: resumed from committed offset {committed}, lag was {lag}");
    // Detach c2's teardown for the same connector reason as c1 (see the CONNECTOR NOTE): its close
    // (LeaveGroup) runs on the detached thread, and the process exits before the hanging destroy.
    std::thread::spawn(move || drop(c2));
    Ok(())
}
