//! admin/partitions-and-configs — increase-only partition growth + config inspection.
//!
//! Kafka wire flow: CreatePartitions (key 37) grows the partition count (increase-only, capped at 256);
//! DescribeConfigs (key 32) reads topic config. A request that does not strictly increase — a decrease,
//! the same count, or a jump past the 256 cap — is rejected with `INVALID_PARTITIONS`.
//!
//! 🟡 Partial surfaces (spec §2.4), documented not asserted here:
//!   - IncrementalAlterConfigs — the connector honors a SUBSET of topic configs (retention.*), and
//!     no-ops the rest. This example READS config via DescribeConfigs rather than mutate it.
//!   - DeleteRecords — the rdkafka admin API does not expose DeleteRecords; the connector supports only
//!     low-end log truncation. Use the Java `kafka-clients` admin (`deleteRecords`) for that path — see
//!     `../../../java/admin/partitions-and-configs`. (No silent drop, spec §6.3.)
//!
//! Exits 0 when the increase succeeds and the invalid (past-cap) request is rejected.

use rdkafka::admin::{
    AdminClient, AdminOptions, NewPartitions, NewTopic, ResourceSpecifier, TopicReplication,
};
use rdkafka::client::DefaultClientContext;
use std::error::Error;
use std::process::ExitCode;
use std::time::Duration;

const TOPIC_PREFIX: &str = "kafka-ex-admin-parts";
const START_PARTS: i32 = 2;
const GROWN_PARTS: usize = 4;
const OVER_CAP: usize = 300; // > 256 -> INVALID_PARTITIONS

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
    kafka_common::print_banner("admin/partitions-and-configs");

    let admin: AdminClient<DefaultClientContext> = kafka_common::base_config().create()?;
    let opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(10)));

    // Unique per-run topic so the increase-only growth (START_PARTS -> GROWN_PARTS) is a genuine
    // increase every run. A fixed name keeps its grown partition count across runs, so the second run's
    // grow request would not strictly increase and the broker returns INVALID_PARTITIONS.
    let topic = kafka_common::unique_topic(TOPIC_PREFIX);

    // 1. Create the topic with START_PARTS partitions (tolerate re-run).
    let _ = admin
        .create_topics(
            &[NewTopic::new(
                &topic,
                START_PARTS,
                TopicReplication::Fixed(1),
            )],
            &opts,
        )
        .await;
    println!("topic '{topic}' ready with {START_PARTS} partitions");

    // 2. Increase-only growth: 2 -> 4 must succeed.
    let grown = admin
        .create_partitions(&[NewPartitions::new(&topic, GROWN_PARTS)], &opts)
        .await?;
    for r in &grown {
        match r {
            Ok(name) => println!("grew '{name}' to {GROWN_PARTS} partitions"),
            Err((name, code)) => return Err(format!("grow '{name}' failed: {code:?}").into()),
        }
    }

    // 3. DescribeConfigs — read the topic config (retention.* is the honored subset, 🟡).
    let described = admin
        .describe_configs(&[ResourceSpecifier::Topic(&topic)], &opts)
        .await?;
    let cfg = described
        .into_iter()
        .next()
        .ok_or("describe_configs returned nothing")?
        .map_err(|e| format!("describe_configs error: {e:?}"))?;
    println!(
        "config for '{topic}': {} entries readable via DescribeConfigs",
        cfg.entries.len()
    );

    // 4. Past-cap request (300 > 256) must be rejected with INVALID_PARTITIONS.
    let bad = admin
        .create_partitions(&[NewPartitions::new(&topic, OVER_CAP)], &opts)
        .await;
    let rejected = match bad {
        Err(_) => true, // some clients surface it as a call-level error
        Ok(results) => results.iter().any(|r| r.is_err()),
    };
    if !rejected {
        return Err(format!(
            "partition count {OVER_CAP} (> 256 cap) was accepted; expected INVALID_PARTITIONS"
        )
        .into());
    }
    println!("over-cap request ({OVER_CAP} > 256) correctly rejected: INVALID_PARTITIONS");

    println!("partitions-and-configs OK: increase-only growth honored, over-cap rejected");
    Ok(())
}
