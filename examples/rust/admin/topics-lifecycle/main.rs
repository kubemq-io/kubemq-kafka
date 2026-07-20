//! admin/topics-lifecycle — CreateTopics -> DescribeConfigs -> DeleteTopics, plus the invalid-name
//! rejection path.
//!
//! Kafka wire flow: CreateTopics (key 19), DescribeConfigs (key 32), DescribeCluster/Metadata,
//! DeleteTopics (key 20). Mirrors the connector admin surface in `connectors/kafka/`.
//!
//! Gotcha #6: a topic name containing `~` (the KubeMQ channel separator) is rejected with
//! `INVALID_TOPIC_EXCEPTION` (Kafka error 17) — the connector cannot map it to a native channel.
//!
//! Exits 0 when the valid topic is created, described, and deleted, and the `~` name is rejected.

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication};
use rdkafka::client::DefaultClientContext;
use std::error::Error;
use std::process::ExitCode;
use std::time::Duration;

const TOPIC: &str = "kafka-ex-admin-topics";
const BAD_TOPIC: &str = "kafka-ex-admin~bad"; // '~' is the native channel separator -> rejected

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
    kafka_common::print_banner("admin/topics-lifecycle");

    let admin: AdminClient<DefaultClientContext> = kafka_common::base_config().create()?;
    let opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(10)));

    // 1. CreateTopics — valid name must succeed.
    let created = admin
        .create_topics(
            &[NewTopic::new(TOPIC, 1, TopicReplication::Fixed(1))],
            &opts,
        )
        .await?;
    for r in &created {
        match r {
            Ok(name) => println!("created topic '{name}'"),
            // Tolerate "already exists" on re-run; any other error fails.
            Err((name, code)) => {
                let s = format!("{code:?}");
                if s.contains("Exists") || s.contains("EXISTS") {
                    println!("topic '{name}' already exists (re-run) — ok");
                } else {
                    return Err(format!("create '{name}' failed: {code:?}").into());
                }
            }
        }
    }

    // 2. DescribeConfigs — the topic's config must be readable.
    let described = admin
        .describe_configs(&[ResourceSpecifier::Topic(TOPIC)], &opts)
        .await?;
    let cfg = described
        .into_iter()
        .next()
        .ok_or("describe_configs returned nothing")?
        .map_err(|e| format!("describe_configs error: {e:?}"))?;
    println!(
        "described topic '{TOPIC}' ({} config entries)",
        cfg.entries.len()
    );

    // 3. Invalid name with '~' must be rejected (INVALID_TOPIC_EXCEPTION, gotcha #6).
    let bad = admin
        .create_topics(
            &[NewTopic::new(BAD_TOPIC, 1, TopicReplication::Fixed(1))],
            &opts,
        )
        .await?;
    let rejected = bad.iter().any(|r| r.is_err());
    if !rejected {
        return Err(format!(
            "topic name '{BAD_TOPIC}' was accepted; expected INVALID_TOPIC_EXCEPTION"
        )
        .into());
    }
    if let Some(Err((name, code))) = bad.into_iter().next() {
        println!("invalid name '{name}' correctly rejected: {code:?} (INVALID_TOPIC_EXCEPTION, gotcha #6)");
    }

    // 4. DeleteTopics — clean up the valid topic.
    let deleted = admin.delete_topics(&[TOPIC], &opts).await?;
    for r in &deleted {
        match r {
            Ok(name) => println!("deleted topic '{name}'"),
            Err((name, code)) => return Err(format!("delete '{name}' failed: {code:?}").into()),
        }
    }

    println!("topics-lifecycle OK: create -> describe -> delete, invalid name rejected");
    Ok(())
}
