# Dependencies — kubemq-kafka/burnin

Module: `github.com/kubemq-io/kubemq-kafka/burnin` (Go 1.25).

The transport drives **franz-go** (`pkg/kgo` + `pkg/kadm` + `pkg/kmsg`) — the
server's own Kafka conformance client (murmur2 partitioner).

| Module | Used by |
|---|---|
| `github.com/twmb/franz-go` (`pkg/kgo`) | transport/, worker/ (produce/consume/txn client) |
| `github.com/twmb/franz-go/pkg/kadm` | transport/ops.go (CreateTopics/DeleteTopics/CreatePartitions, offsets, lag) |
| `github.com/twmb/franz-go/pkg/kmsg` | transport/ (error-code / config request-response types) |
| `github.com/HdrHistogram/hdrhistogram-go` | metrics/ latency histograms |
| `github.com/prometheus/client_golang` | metrics/, server/ |
| `golang.org/x/time` | worker/ rate limiting |
| `gopkg.in/yaml.v3` | config/ |

## Connectivity

- **Kafka bootstrap** `KUBEMQ_BROKER_ADDRESS` (default `localhost:9092`, the Kafka
  wire listener — NOT `KUBEMQ_KAFKA_BOOTSTRAP`, which is the examples var).
- **Fixed control/metrics port `8898`** (collision-free: mqtt/amqp/stomp=8896,
  ce=8895, aws=8897, gcp=8899). The dashboard drives it concurrently with no
  port-rewrite step.
- The **connector is DISABLED by default** — run the server with
  `CONNECTORS_KAFKA_ENABLE=true` (gotcha #1).

## Notes

- franz-go is pure-Go (no librdkafka), so `CGO_ENABLED=0` + Alpine build works
  unchanged from aws — no build-toolchain additions.
- **KIP-890 V1 EOS ceiling** applies to the `transactions_eos` worker: the
  same-epoch zombie residual is upstream-shared, RECORDED for transparency
  (`kip890_residual`), and EXCLUDED from `max_eos_violations` — it is NOT a soak
  failure (spec §2.5).
