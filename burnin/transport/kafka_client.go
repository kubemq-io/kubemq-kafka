package transport

import (
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// KafkaClientConfig captures everything needed to build a franz-go client against
// the KubeMQ Kafka connector. Bootstrap is host:port (KUBEMQ_BROKER_ADDRESS), NOT
// a URL.
type KafkaClientConfig struct {
	Bootstrap      string
	ClientID       string
	Acks           string
	Compression    string
	IsolationLevel string
	Idempotent     bool

	// Transaction / consumer-group options set per-worker.
	TransactionalID   string   // non-empty => transactional producer (EOS worker)
	ConsumerGroup     string   // non-empty => group consumer
	ConsumeTopics     []string // nil => produce-only client
	TxnTimeoutMS      int
	FetchMaxWaitMS    int
	DisableAutoCommit bool // offset_commit_lag drives manual CommitRecords

	SASLMechanism string
	SASLUsername  string
	SASLPassword  string
	TLS           bool
}

// NewClient builds a franz-go client. Producer-side, consumer-side, and
// transactional options are all expressed through kgo.Opt; a nil ConsumeTopics
// yields a produce-only client.
func NewClient(cfg KafkaClientConfig) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(BootstrapAddress(cfg.Bootstrap)),
		kgo.ClientID(cfg.ClientID),
		kgo.RequiredAcks(acksFor(cfg.Acks)),
		kgo.ProducerBatchCompression(compressionFor(cfg.Compression)),
	}

	// franz-go's idempotent producer (default ON) requires acks=all; disable it
	// when acks<all, or when idempotence is explicitly turned off.
	if cfg.Acks != "all" || !cfg.Idempotent {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}

	if cfg.TransactionalID != "" {
		opts = append(opts, kgo.TransactionalID(cfg.TransactionalID))
		if cfg.TxnTimeoutMS > 0 {
			opts = append(opts, kgo.TransactionTimeout(msDuration(cfg.TxnTimeoutMS)))
		}
	}

	if len(cfg.ConsumeTopics) > 0 {
		opts = append(opts, kgo.ConsumeTopics(cfg.ConsumeTopics...))
		if cfg.ConsumerGroup != "" {
			opts = append(opts, kgo.ConsumerGroup(cfg.ConsumerGroup))
			if cfg.DisableAutoCommit {
				opts = append(opts, kgo.DisableAutoCommit())
			}
		}
		opts = append(opts, kgo.FetchIsolationLevel(isolationFor(cfg.IsolationLevel)))
		if cfg.FetchMaxWaitMS > 0 {
			opts = append(opts, kgo.FetchMaxWait(msDuration(cfg.FetchMaxWaitMS)))
		}
	}

	if cfg.SASLMechanism != "" {
		m, err := saslMechanism(cfg)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(m))
	}
	if cfg.TLS {
		opts = append(opts, kgo.DialTLS()) // :9093 SSL; doc-only in examples, config-gated here
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("build kafka client: %w", err)
	}
	return cl, nil
}

// NewAdmin wraps a client in a kadm.Client for topic/partition/offset admin.
func NewAdmin(cl *kgo.Client) *kadm.Client { return kadm.NewClient(cl) }

// acksFor maps the acks string to a franz-go Acks value: "all"->AllISRAcks (-1),
// "1"->LeaderAck, "0"->NoAck.
func acksFor(s string) kgo.Acks {
	switch s {
	case "0":
		return kgo.NoAck()
	case "1":
		return kgo.LeaderAck()
	default:
		return kgo.AllISRAcks()
	}
}

// compressionFor maps the compression string to a franz-go codec.
func compressionFor(s string) kgo.CompressionCodec {
	switch s {
	case "gzip":
		return kgo.GzipCompression()
	case "snappy":
		return kgo.SnappyCompression()
	case "lz4":
		return kgo.Lz4Compression()
	case "zstd":
		return kgo.ZstdCompression()
	default:
		return kgo.NoCompression()
	}
}

// isolationFor maps the isolation string to a franz-go IsolationLevel.
func isolationFor(s string) kgo.IsolationLevel {
	if s == "read_uncommitted" {
		return kgo.ReadUncommitted()
	}
	return kgo.ReadCommitted()
}

// saslMechanism builds the SASL mechanism from the configured credentials.
func saslMechanism(cfg KafkaClientConfig) (sasl.Mechanism, error) {
	switch cfg.SASLMechanism {
	case "PLAIN":
		return plain.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("unsupported sasl mechanism %q", cfg.SASLMechanism)
	}
}

func msDuration(ms int) time.Duration { return time.Duration(ms) * time.Millisecond }
