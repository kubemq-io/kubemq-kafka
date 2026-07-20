//! Shared client/config helper for every kubemq-kafka Rust example.
//!
//! All variants connect the SAME way: point a standard `rdkafka` ClientConfig at the KubeMQ Kafka
//! wire-protocol connector by setting `bootstrap.servers`. This crate centralizes that one decision
//! so the variant programs stay focused on the Kafka behavior they demonstrate.
//!
//! Connection model (see `../../SHARED-CONVENTIONS.md` §4.1):
//! - **Bootstrap** — read `KUBEMQ_KAFKA_BOOTSTRAP` (default `localhost:9092`), used verbatim as the
//!   Kafka `bootstrap.servers` value. (Named `_BOOTSTRAP` not `_URL` because Kafka takes a host:port
//!   bootstrap list, not a URL scheme — the honest analog of `bootstrap.servers`.)
//! - **Auth — none by default.** A stock dev broker runs with auth off, so the base config sets no
//!   SASL. Only `security/sasl-plain-scram` opts in, via `sasl_config_from_env` below.
//! - **The connector is DISABLED by default** — the broker must be started with
//!   `CONNECTORS_KAFKA_ENABLE=true` (repo gotcha #1) for these examples to connect.
//!
//! Partitioner note (gotcha #4): librdkafka's default partitioner is `consistent_random` (CRC32 of
//! the key). Java `kafka-clients`, franz-go, and kafkajs v2+ default to **murmur2**. So a keyed
//! record from THIS client may land on a DIFFERENT partition than the same key from those clients.
//! `produce/compression-and-keys` calls this out; do not "fix" it by silently forcing murmur2.

use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Default connector bootstrap endpoint (plain TCP :9092).
pub const DEFAULT_BOOTSTRAP: &str = "localhost:9092";

/// Build a per-run UNIQUE topic name: `"{prefix}-{suffix}"` where the suffix is unique per call.
///
/// Every other language's kubemq-kafka examples use a random per-run topic suffix so each run starts
/// against a fresh topic (offsets from 0). The Rust examples historically used fixed `const TOPIC`
/// names, which accumulate records/offsets across runs and break any offset/watermark/count-based
/// assertion on the second run (e.g. `expected committed offset 4, got 14`, or `expected latest
/// watermark 6, got 12`). This helper restores parity with the other languages.
///
/// The suffix combines a nanosecond wall-clock timestamp, the process id, and a monotonic per-process
/// counter — unique per call across concurrent processes and repeated calls, without pulling a
/// `uuid`/`rand` crate into this dependency-light helper. Hex-encoded so the result stays within
/// Kafka's `[a-zA-Z0-9._-]` topic-name charset and 249-char limit.
pub fn unique_topic(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let pid = std::process::id();
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{nanos:x}-{pid:x}-{seq:x}")
}

/// The `bootstrap.servers` value every example uses (`KUBEMQ_KAFKA_BOOTSTRAP`, default
/// `localhost:9092`).
pub fn bootstrap() -> String {
    std::env::var("KUBEMQ_KAFKA_BOOTSTRAP").unwrap_or_else(|_| DEFAULT_BOOTSTRAP.to_string())
}

/// A base `ClientConfig` with `bootstrap.servers` set (and any SASL/TLS pulled from env). Producers,
/// consumers, and the admin client all start from this and layer their own keys on top.
pub fn base_config() -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", bootstrap());
    cfg.set_log_level(RDKafkaLogLevel::Warning);
    apply_sasl_from_env(&mut cfg);
    cfg
}

/// If `KAFKA_SASL_MECHANISM` is set, layer SASL creds onto `cfg`. Used only by the
/// `security/sasl-plain-scram` variant against a broker with a Kafka credential store; a no-op for
/// every other variant (stock dev broker, auth off).
///
/// Env (all default-unset except mechanism which gates the block):
///   KAFKA_SASL_MECHANISM  = PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512
///   KAFKA_SASL_USERNAME   = <user>
///   KAFKA_SASL_PASSWORD   = <pass>
///   KAFKA_SECURITY_PROTOCOL = SASL_PLAINTEXT (default) | SASL_SSL
pub fn apply_sasl_from_env(cfg: &mut ClientConfig) {
    if let Ok(mech) = std::env::var("KAFKA_SASL_MECHANISM") {
        let proto = std::env::var("KAFKA_SECURITY_PROTOCOL")
            .unwrap_or_else(|_| "SASL_PLAINTEXT".to_string());
        cfg.set("security.protocol", proto);
        cfg.set("sasl.mechanism", mech);
        if let Ok(u) = std::env::var("KAFKA_SASL_USERNAME") {
            cfg.set("sasl.username", u);
        }
        if let Ok(p) = std::env::var("KAFKA_SASL_PASSWORD") {
            cfg.set("sasl.password", p);
        }
    }
}

/// Print the one-line banner every example shows on startup.
pub fn print_banner(variant: &str) {
    let auth = if std::env::var("KAFKA_SASL_MECHANISM").is_ok() {
        "SASL"
    } else {
        "no-auth"
    };
    println!(
        "[kubemq-kafka] {variant} bootstrap={} ({auth}; connector must be enabled: CONNECTORS_KAFKA_ENABLE=true)",
        bootstrap(),
    );
}
