# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This repository ships **documentation, runnable examples, and a burn-in soak harness** for the
`kubemq-kafka` connector — there is **no published package**. Version numbers track the
state of the docs and examples in this repo, not a client library release.

## [Unreleased]

### Removed

- The `interop/kafka-to-native` example (all 7 languages) — native access to the `kafka.*` channel
  namespace is **reserved by the broker and rejected with `Error 443`**, so there is no
  cross-protocol interop to demonstrate. The now-unused native KubeMQ SDK dependencies
  (`kubemq-go`, `kubemq`, `kubemq-sdk-Java`, `KubeMQ.SDK.csharp`, `kubemq-js`) were dropped from the
  example manifests, leaving **13 variants × 7 languages**. See
  [`docs/concepts/interop-with-native.md`](docs/concepts/interop-with-native.md) for the reservation.

### Fixed

- Docs: corrected the Kafka scope matrix to match the server source of truth (`docs/24-kafka.md`) —
  log compaction (GA on `next`), Kafka Connect/Streams (supported, they rely on compacted internal
  topics), and OAUTHBEARER (✅ SUPPORTED, SASL_SSL / OIDC only) were mis-marked as ⛔/🔴; added the
  `next`-engine requirement (DE-57).

## [1.0.0] - 2026-07-13

### Added

- Initial release of the `kubemq-kafka` direct-connect documentation and examples repository.
- Connector overview: bridging the **Apache Kafka wire protocol** onto KubeMQ's native messaging
  via the embedded connector (dedicated listeners on TCP **9092** / TLS **9093**), driven by
  standard unmodified Kafka clients with only a `bootstrap.servers` repoint. The connector is
  **disabled by default** — set `CONNECTORS_KAFKA_ENABLE=true` to open the listeners.
- Channel model: Kafka topics map onto KubeMQ **Events-Store** channels (`kafka.<topic>`; partition
  p>0 onto `kafka.<topic>~<p>`); the Kafka offset is the durable STAN `Sequence`.
- `docs/`: architecture, getting-started, configuration, per-concept pages (topics/partitions/offsets,
  consumer-groups, transactions-EOS, cross-client-partitioning, interop-with-native), guides
  (producing, consuming-and-groups, admin-and-topics, security-SASL/TLS, transactions-EOS), and
  reference (capabilities, channel-mapping, error-codes, configuration, migration-from-kafka) —
  every behavioral claim traced to connector source or the server docs, honest ✅/🟡/⛔/🔴 scope.
- `examples/`: per-concept runnable examples across 7 languages (Go, Python, Java, JS/TS, C#, Ruby,
  Rust) — 13 variants × 7 languages (~84-91 examples) — using standard native Kafka client libraries
  only (no KubeMQ proto bindings). Variants cover
  produce (basic-acks, idempotent, compression-and-keys), consume (from-beginning-latest,
  seek-offsets-timestamps), consumer-groups (join-rebalance, commit-and-lag), admin
  (topics-lifecycle, partitions-and-configs), offsets (list-and-retention), transactions
  (eos-commit-abort, read-committed), and security (sasl-plain-scram).
- `burnin/`: standalone Go soak-test harness (franz-go transport) exercising the connector under
  sustained load — one worker per Kafka operation family including a full EOS/transactions worker,
  boots idle on control/metrics port **8898**.
- `examples/SHARED-CONVENTIONS.md`: the per-language build/lint/run loop, the `KUBEMQ_KAFKA_BOOTSTRAP`
  convention, the channel-mapping mental model, and the per-example README template.

[Unreleased]: https://github.com/kubemq-io/kubemq-kafka/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/kubemq-io/kubemq-kafka/releases/tag/v1.0.0
