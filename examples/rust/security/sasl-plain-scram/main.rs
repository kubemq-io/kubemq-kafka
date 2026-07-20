//! security/sasl-plain-scram — authenticated produce/consume over SASL/PLAIN or SCRAM-SHA-256/512,
//! plus the authorization-denied path.
//!
//! Kafka wire flow: SaslHandshake (key 17) + SaslAuthenticate (key 36) before any Produce/Fetch. The
//! shared helper `kafka_common::apply_sasl_from_env` (called by `base_config`) layers
//! `security.protocol` + `sasl.mechanism`/`sasl.username`/`sasl.password` from the environment.
//!
//! RUNNABLE ONLY against a broker with a Kafka credential store (spec §4.7). SCRAM needs the rdkafka
//! `ssl-vendored` feature (HMAC). If `KAFKA_SASL_MECHANISM` is unset, this program prints how to
//! configure it and exits 0 (nothing to authenticate against — documented, not a failure).
//!
//! TLS / mTLS is DOC-ONLY here — see `docs/guides/security-sasl-tls.md`. Gotcha #2 (AdvertisedHost must
//! match the client's SNI/cert) and gotcha #8 (Group resource needs WRITE for join) apply.
//!
//! Env:
//!   KAFKA_SASL_MECHANISM   = PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512   (gates the whole run)
//!   KAFKA_SASL_USERNAME / KAFKA_SASL_PASSWORD
//!   KAFKA_SECURITY_PROTOCOL = SASL_PLAINTEXT (default) | SASL_SSL
//!   KAFKA_DENIED_USERNAME / KAFKA_DENIED_PASSWORD  (optional: a principal without ACLs -> denied path)
//!
//! Exits 0 when the authenticated round-trip succeeds (and, if configured, the denied user is rejected).

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::error::Error;
use std::process::ExitCode;
use std::time::Duration;

const TOPIC: &str = "kafka-ex-security-sasl";
const GROUP: &str = "kafka-ex-security-sasl-grp";

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
    kafka_common::print_banner("security/sasl-plain-scram");

    let mech = match std::env::var("KAFKA_SASL_MECHANISM") {
        Ok(m) => m,
        Err(_) => {
            println!("KAFKA_SASL_MECHANISM is not set — this variant needs a SASL-enabled broker.");
            println!("To run it:");
            println!("  export KAFKA_SASL_MECHANISM=SCRAM-SHA-256   # or PLAIN / SCRAM-SHA-512");
            println!("  export KAFKA_SASL_USERNAME=<user>");
            println!("  export KAFKA_SASL_PASSWORD=<pass>");
            println!("  export KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT   # or SASL_SSL for TLS");
            println!("TLS / mTLS is doc-only — see docs/guides/security-sasl-tls.md.");
            println!(
                "sasl-plain-scram: nothing to authenticate against, exiting 0 (documented skip)."
            );
            return Ok(());
        }
    };
    println!("authenticating with mechanism={mech}");

    // 1. Authenticated round-trip. base_config() already layered the SASL creds from env.
    let producer: FutureProducer = kafka_common::base_config().create()?;
    let payload = "authenticated-payload";
    producer
        .send(
            FutureRecord::to(TOPIC).key("k").payload(payload),
            Timeout::After(Duration::from_secs(15)),
        )
        .await
        .map_err(|(e, _)| format!("authenticated produce failed: {e}"))?;
    println!("authenticated produce OK");

    let consumer: StreamConsumer = {
        let mut c = kafka_common::base_config();
        c.set("group.id", GROUP);
        c.set("auto.offset.reset", "earliest");
        c.set("enable.auto.commit", "false");
        c.create()?
    };
    consumer.subscribe(&[TOPIC])?;
    let mut got = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while !got && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), consumer.recv()).await {
            Ok(Ok(m)) => {
                let body = m
                    .payload()
                    .map(|b| String::from_utf8_lossy(b).into_owned())
                    .unwrap_or_default();
                if body == payload {
                    got = true;
                }
            }
            Ok(Err(e)) => return Err(format!("authenticated fetch error: {e}").into()),
            Err(_) => break,
        }
    }
    if !got {
        return Err("authenticated consumer did not receive the record".into());
    }
    println!("authenticated consume OK");

    // 2. Optional denied path: a principal without ACLs -> *_AUTHORIZATION_FAILED.
    if let (Ok(du), Ok(dp)) = (
        std::env::var("KAFKA_DENIED_USERNAME"),
        std::env::var("KAFKA_DENIED_PASSWORD"),
    ) {
        let proto = std::env::var("KAFKA_SECURITY_PROTOCOL")
            .unwrap_or_else(|_| "SASL_PLAINTEXT".to_string());
        let denied: FutureProducer = {
            let mut c = rdkafka::config::ClientConfig::new();
            c.set("bootstrap.servers", kafka_common::bootstrap());
            c.set("security.protocol", proto);
            c.set("sasl.mechanism", &mech);
            c.set("sasl.username", du);
            c.set("sasl.password", dp);
            c.create()?
        };
        match denied
            .send(
                FutureRecord::to(TOPIC).key("k").payload("nope"),
                Timeout::After(Duration::from_secs(10)),
            )
            .await
        {
            Ok(_) => {
                return Err(
                    "denied principal was allowed to produce; expected TOPIC_AUTHORIZATION_FAILED"
                        .into(),
                )
            }
            Err((e, _)) => {
                let s = e.to_string();
                if s.contains("AUTHORIZATION") || s.contains("Authorization") || s.contains("acl") {
                    println!("denied principal correctly rejected: {s} (TOPIC/GROUP_AUTHORIZATION_FAILED)");
                } else {
                    return Err(
                        format!("denied principal rejected with unexpected error: {e}").into(),
                    );
                }
            }
        }
    } else {
        println!("(no KAFKA_DENIED_USERNAME set — skipping the authorization-denied sub-check)");
    }

    println!("sasl-plain-scram OK: authenticated produce/consume succeeded");
    Ok(())
}
