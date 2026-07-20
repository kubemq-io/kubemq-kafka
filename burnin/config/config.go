// Package config defines the burn-in harness configuration model, defaults,
// loading, validation, and duration parsing. It mirrors kubemq-aws/burnin/config
// recast for Kafka (spec §7.5): the aws block is replaced by a kafka block, the
// 14 AWS workers by the 6 Kafka workers (produce_round_trip, keyed_ordering,
// consumer_group, offset_commit_lag, admin_topic_churn, transactions_eos), and
// the broker var is KUBEMQ_BROKER_ADDRESS (default localhost:9092, the Kafka wire
// listener; NOT KUBEMQ_KAFKA_BOOTSTRAP, which is the examples var).
package config

import (
	"bytes"
	cryptorand "crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// ConfigVersion is the required config schema version.
const ConfigVersion = "2"

// Worker name constants (spec §7.3).
const (
	WorkerProduceRoundTrip = "produce_round_trip"
	WorkerKeyedOrdering    = "keyed_ordering"
	WorkerConsumerGroup    = "consumer_group"
	WorkerOffsetCommitLag  = "offset_commit_lag"
	WorkerAdminTopicChurn  = "admin_topic_churn"
	WorkerTransactionsEOS  = "transactions_eos"
)

// AllWorkerNames lists all Kafka burn-in workers in a stable order.
var AllWorkerNames = []string{
	WorkerProduceRoundTrip,
	WorkerKeyedOrdering,
	WorkerConsumerGroup,
	WorkerOffsetCommitLag,
	WorkerAdminTopicChurn,
	WorkerTransactionsEOS,
}

// Worker predicates the engine/verdict use to branch cyclic / eos workers
// (mirror aws IsFifoWorker/IsMgmtWorker helpers).
func IsTransactionsWorker(name string) bool  { return name == WorkerTransactionsEOS }
func IsAdminWorker(name string) bool         { return name == WorkerAdminTopicChurn }
func IsOffsetLagWorker(name string) bool     { return name == WorkerOffsetCommitLag }
func IsKeyedWorker(name string) bool         { return name == WorkerKeyedOrdering }
func IsConsumerGroupWorker(name string) bool { return name == WorkerConsumerGroup }

// BrokerConfig holds the broker address (host:port for the Kafka wire listener).
type BrokerConfig struct {
	Address        string `yaml:"address" json:"address"`
	ClientIDPrefix string `yaml:"client_id_prefix" json:"client_id_prefix"`
}

// KafkaConfig is the Kafka knob block (spec §7.5) replacing the AWS block. The
// fields drive franz-go client options; SASL/TLS default off (doc-only) and
// become runnable when a credential store is configured.
type KafkaConfig struct {
	Acks              string `yaml:"acks" json:"acks"`                             // "all"|"1"|"0"  -> kgo.RequiredAcks
	Compression       string `yaml:"compression" json:"compression"`               // none|gzip|snappy|lz4|zstd
	Partitions        int    `yaml:"partitions" json:"partitions"`                 // topic partition count (>=1, <=256)
	ReplicationFactor int    `yaml:"replication_factor" json:"replication_factor"` // 1 (single-node connector)
	IsolationLevel    string `yaml:"isolation_level" json:"isolation_level"`       // read_committed|read_uncommitted
	Idempotent        bool   `yaml:"idempotent" json:"idempotent"`                 // enable.idempotence (default true)
	Transactional     bool   `yaml:"transactional" json:"transactional"`           // EOS worker on/off gate helper
	GroupPrefix       string `yaml:"group_prefix" json:"group_prefix"`             // consumer group id prefix
	TopicPrefix       string `yaml:"topic_prefix" json:"topic_prefix"`             // burnin.<worker>.<idx> topic namespace root
	FetchMaxWaitMS    int    `yaml:"fetch_max_wait_ms" json:"fetch_max_wait_ms"`   // PollFetches long-poll budget
	TxnTimeoutMS      int    `yaml:"txn_timeout_ms" json:"txn_timeout_ms"`         // transaction.timeout.ms (EOS worker)

	// SASL (doc-only default off; runnable when a credential store is configured).
	SASLMechanism string `yaml:"sasl_mechanism" json:"sasl_mechanism"` // ""|PLAIN|SCRAM-SHA-256|SCRAM-SHA-512
	SASLUsername  string `yaml:"sasl_username" json:"sasl_username"`
	SASLPassword  string `yaml:"sasl_password" json:"sasl_password"`
	TLS           bool   `yaml:"tls" json:"tls"` // security.protocol=SSL against :9093
}

// WorkerConfig holds the per-worker concurrency + rate knobs. ProducersPerChannel
// is the number of producers per channel; ConsumersPerChannel the number of
// consumers. Subscribers is unused for Kafka but kept for struct-parity with the
// aws api overlay so the RunConfig overlay marshals cleanly.
type WorkerConfig struct {
	Enabled             bool `yaml:"enabled" json:"enabled"`
	Channels            int  `yaml:"channels" json:"channels"`
	ProducersPerChannel int  `yaml:"producers_per_channel" json:"producers_per_channel"`
	ConsumersPerChannel int  `yaml:"consumers_per_channel" json:"consumers_per_channel"`
	Subscribers         int  `yaml:"subscribers" json:"subscribers"`
	Rate                int  `yaml:"rate" json:"rate"`
}

// WorkersConfig groups the 6 Kafka worker blocks.
type WorkersConfig struct {
	ProduceRoundTrip WorkerConfig `yaml:"produce_round_trip" json:"produce_round_trip"`
	KeyedOrdering    WorkerConfig `yaml:"keyed_ordering" json:"keyed_ordering"`
	ConsumerGroup    WorkerConfig `yaml:"consumer_group" json:"consumer_group"`
	OffsetCommitLag  WorkerConfig `yaml:"offset_commit_lag" json:"offset_commit_lag"`
	AdminTopicChurn  WorkerConfig `yaml:"admin_topic_churn" json:"admin_topic_churn"`
	TransactionsEOS  WorkerConfig `yaml:"transactions_eos" json:"transactions_eos"`
}

// MessageConfig holds payload sizing knobs (CRC32 + sequence stamped into Kafka
// record headers, body padded to size).
type MessageConfig struct {
	SizeMode         string `yaml:"size_mode" json:"size_mode"`
	SizeBytes        int    `yaml:"size_bytes" json:"size_bytes"`
	SizeDistribution string `yaml:"size_distribution" json:"size_distribution"`
	ReorderWindow    int    `yaml:"reorder_window" json:"reorder_window"`
}

// MetricsConfig holds the control HTTP port and report interval.
type MetricsConfig struct {
	Port           int    `yaml:"port" json:"port"`
	ReportInterval string `yaml:"report_interval" json:"report_interval"`
}

// LoggingConfig holds log format and level.
type LoggingConfig struct {
	Format string `yaml:"format" json:"format"`
	Level  string `yaml:"level" json:"level"`
}

// ForcedDisconnConfig drives the connection-churn injector (recreates the
// franz-go clients to exercise rejoin and at-least-once redelivery).
type ForcedDisconnConfig struct {
	Interval string `yaml:"interval" json:"interval"`
	Duration string `yaml:"duration" json:"duration"`
}

// RecoveryConfig holds reconnect backoff knobs.
type RecoveryConfig struct {
	ReconnectInterval    string  `yaml:"reconnect_interval" json:"reconnect_interval"`
	ReconnectMaxInterval string  `yaml:"reconnect_max_interval" json:"reconnect_max_interval"`
	ReconnectMultiplier  float64 `yaml:"reconnect_multiplier" json:"reconnect_multiplier"`
}

// ShutdownConfig holds the drain timeout.
type ShutdownConfig struct {
	DrainTimeoutSeconds int `yaml:"drain_timeout_seconds" json:"drain_timeout_seconds"`
}

// OutputConfig holds report output knobs.
type OutputConfig struct {
	ReportFile string `yaml:"report_file" json:"report_file"`
	SDKVersion string `yaml:"sdk_version" json:"sdk_version"`
}

// ThresholdsConfig holds pass/fail thresholds (spec §7.4). It keeps the standard
// gates (latency P50/P95/P99/P999, throughput, error-rate, memory-growth,
// downtime, max_duration) and adds the 6 Kafka zero-tolerance keys. The KIP-890
// V1 same-epoch residual is EXCLUDED from max_eos_violations (spec §2.5).
type ThresholdsConfig struct {
	MaxLossPct                  float64 `yaml:"max_loss_pct" json:"max_loss_pct"`
	MaxDuplicationPct           float64 `yaml:"max_duplication_pct" json:"max_duplication_pct"`
	MaxOffsetOrderViolations    int     `yaml:"max_offset_order_violations" json:"max_offset_order_violations"`
	MaxGroupLossAcrossRebalance int     `yaml:"max_group_loss_across_rebalance" json:"max_group_loss_across_rebalance"`
	MaxEOSViolations            int     `yaml:"max_eos_violations" json:"max_eos_violations"`
	MaxLagAccuracyErrorMsgs     int     `yaml:"max_lag_accuracy_error_msgs" json:"max_lag_accuracy_error_msgs"`

	MaxP50LatencyMS       float64 `yaml:"max_p50_latency_ms" json:"max_p50_latency_ms"`
	MaxP95LatencyMS       float64 `yaml:"max_p95_latency_ms" json:"max_p95_latency_ms"`
	MaxP99LatencyMS       float64 `yaml:"max_p99_latency_ms" json:"max_p99_latency_ms"`
	MaxP999LatencyMS      float64 `yaml:"max_p999_latency_ms" json:"max_p999_latency_ms"`
	MinThroughputPct      float64 `yaml:"min_throughput_pct" json:"min_throughput_pct"`
	MaxErrorRatePct       float64 `yaml:"max_error_rate_pct" json:"max_error_rate_pct"`
	MaxMemoryGrowthFactor float64 `yaml:"max_memory_growth_factor" json:"max_memory_growth_factor"`
	MaxDowntimePct        float64 `yaml:"max_downtime_pct" json:"max_downtime_pct"`
	MaxDuration           string  `yaml:"max_duration" json:"max_duration"`
}

// WarmupConfig holds warmup parallelism + per-channel timeout.
type WarmupConfig struct {
	MaxParallelChannels int `yaml:"max_parallel_channels" json:"max_parallel_channels"`
	TimeoutPerChannelMs int `yaml:"timeout_per_channel_ms" json:"timeout_per_channel_ms"`
}

// CORSConfig holds the allowed origins for the control API.
type CORSConfig struct {
	Origins string `yaml:"origins" json:"origins"`
}

// Config is the full burn-in configuration.
type Config struct {
	Version          string              `yaml:"version" json:"version"`
	Broker           BrokerConfig        `yaml:"broker" json:"broker"`
	Mode             string              `yaml:"mode" json:"mode"`
	Duration         string              `yaml:"duration" json:"duration"`
	RunID            string              `yaml:"run_id" json:"run_id"`
	WarmupDuration   string              `yaml:"warmup_duration" json:"warmup_duration"`
	Kafka            KafkaConfig         `yaml:"kafka" json:"kafka"`
	Workers          WorkersConfig       `yaml:"workers" json:"workers"`
	Message          MessageConfig       `yaml:"message" json:"message"`
	Metrics          MetricsConfig       `yaml:"metrics" json:"metrics"`
	Logging          LoggingConfig       `yaml:"logging" json:"logging"`
	ForcedDisconnect ForcedDisconnConfig `yaml:"forced_disconnect" json:"forced_disconnect"`
	Recovery         RecoveryConfig      `yaml:"recovery" json:"recovery"`
	Shutdown         ShutdownConfig      `yaml:"shutdown" json:"shutdown"`
	Output           OutputConfig        `yaml:"output" json:"output"`
	Thresholds       ThresholdsConfig    `yaml:"thresholds" json:"thresholds"`
	Warmup           WarmupConfig        `yaml:"warmup" json:"warmup"`
	CORS             CORSConfig          `yaml:"cors" json:"cors"`

	DurationParsed        time.Duration `yaml:"-" json:"-"`
	WarmupDurationParsed  time.Duration `yaml:"-" json:"-"`
	ReportIntervalParsed  time.Duration `yaml:"-" json:"-"`
	ForcedDisconnInterval time.Duration `yaml:"-" json:"-"`
	ForcedDisconnDuration time.Duration `yaml:"-" json:"-"`
	ReconnectInterval     time.Duration `yaml:"-" json:"-"`
	ReconnectMaxInterval  time.Duration `yaml:"-" json:"-"`
	MaxDurationParsed     time.Duration `yaml:"-" json:"-"`
	Warnings              []string      `yaml:"-" json:"-"`
}

// DefaultConfig returns the built-in default configuration.
func DefaultConfig() *Config {
	c := &Config{}
	c.Version = ConfigVersion
	c.Broker.Address = "localhost:9092"
	c.Broker.ClientIDPrefix = "burnin-kafka"
	c.Mode = "soak"
	c.Duration = "1h"

	c.Kafka = KafkaConfig{
		Acks:              "all",
		Compression:       "lz4",
		Partitions:        3,
		ReplicationFactor: 1,
		IsolationLevel:    "read_committed",
		Idempotent:        true,
		Transactional:     true,
		GroupPrefix:       "burnin-kafka-grp",
		TopicPrefix:       "burnin",
		FetchMaxWaitMS:    500,
		TxnTimeoutMS:      60000,
		SASLMechanism:     "",
	}

	// All 6 Kafka workers ON by default (Kafka's core surface, unlike aws's opt-in
	// set). Rates: produce 100, keyed 50, group 50, offset-lag 50, admin-churn 10
	// (cyclic, low rate), eos 30.
	c.Workers = WorkersConfig{
		ProduceRoundTrip: WorkerConfig{
			Enabled: true, Channels: 1,
			ProducersPerChannel: 1, ConsumersPerChannel: 2,
			Rate: 100,
		},
		KeyedOrdering: WorkerConfig{
			Enabled: true, Channels: 1,
			ProducersPerChannel: 1, ConsumersPerChannel: 1,
			Rate: 50,
		},
		ConsumerGroup: WorkerConfig{
			Enabled: true, Channels: 1,
			ProducersPerChannel: 1, ConsumersPerChannel: 3,
			Rate: 50,
		},
		OffsetCommitLag: WorkerConfig{
			Enabled: true, Channels: 1,
			ProducersPerChannel: 1, ConsumersPerChannel: 1,
			Rate: 50,
		},
		AdminTopicChurn: WorkerConfig{
			Enabled: true, Channels: 1,
			ProducersPerChannel: 1, ConsumersPerChannel: 1,
			Rate: 10,
		},
		TransactionsEOS: WorkerConfig{
			Enabled: true, Channels: 1,
			ProducersPerChannel: 1, ConsumersPerChannel: 1,
			Rate: 30,
		},
	}

	c.Message = MessageConfig{
		SizeMode:         "fixed",
		SizeBytes:        1024,
		SizeDistribution: "256:80,4096:15,65536:5",
		ReorderWindow:    10_000,
	}

	c.Metrics = MetricsConfig{
		Port:           8898,
		ReportInterval: "30s",
	}

	c.Logging = LoggingConfig{Format: "text", Level: "info"}

	c.ForcedDisconnect = ForcedDisconnConfig{
		Interval: "0",
		Duration: "5s",
	}

	c.Recovery = RecoveryConfig{
		ReconnectInterval:    "1s",
		ReconnectMaxInterval: "30s",
		ReconnectMultiplier:  2.0,
	}

	c.Shutdown.DrainTimeoutSeconds = 10

	c.Thresholds = ThresholdsConfig{
		MaxLossPct:                  0.0,
		MaxDuplicationPct:           0.0,
		MaxOffsetOrderViolations:    0,
		MaxGroupLossAcrossRebalance: 0,
		MaxEOSViolations:            0,
		MaxLagAccuracyErrorMsgs:     1,
		MaxP50LatencyMS:             2000,
		MaxP95LatencyMS:             5000,
		MaxP99LatencyMS:             8000,
		MaxP999LatencyMS:            15000,
		MinThroughputPct:            80,
		MaxErrorRatePct:             1.0,
		MaxMemoryGrowthFactor:       2.0,
		MaxDowntimePct:              10,
		MaxDuration:                 "168h",
	}

	c.Warmup = WarmupConfig{
		MaxParallelChannels: 10,
		TimeoutPerChannelMs: 5000,
	}

	c.CORS.Origins = "*"

	return c
}

// GetWorkerConfig returns a pointer to the named worker's config block.
func (c *Config) GetWorkerConfig(name string) *WorkerConfig {
	switch name {
	case WorkerProduceRoundTrip:
		return &c.Workers.ProduceRoundTrip
	case WorkerKeyedOrdering:
		return &c.Workers.KeyedOrdering
	case WorkerConsumerGroup:
		return &c.Workers.ConsumerGroup
	case WorkerOffsetCommitLag:
		return &c.Workers.OffsetCommitLag
	case WorkerAdminTopicChurn:
		return &c.Workers.AdminTopicChurn
	case WorkerTransactionsEOS:
		return &c.Workers.TransactionsEOS
	default:
		return nil
	}
}

// GetWorkerRate returns the configured rate for a worker (fallback 100).
func (c *Config) GetWorkerRate(name string) int {
	if wc := c.GetWorkerConfig(name); wc != nil {
		return wc.Rate
	}
	return 100
}

// GetWorkerChannels returns the configured channel count for a worker (min 1).
func (c *Config) GetWorkerChannels(name string) int {
	if wc := c.GetWorkerConfig(name); wc != nil && wc.Channels > 0 {
		return wc.Channels
	}
	return 1
}

// TotalChannelCount sums enabled worker channel counts.
func (c *Config) TotalChannelCount() int {
	total := 0
	for _, name := range AllWorkerNames {
		if wc := c.GetWorkerConfig(name); wc != nil && wc.Enabled {
			total += wc.Channels
		}
	}
	return total
}

// Load reads and parses the config file (or just defaults when path == ""),
// applies env overrides, parses durations, and mints a run ID.
func Load(path string) (*Config, error) {
	c := DefaultConfig()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read config file %s: %w", path, err)
		}

		decoder := yaml.NewDecoder(bytes.NewReader(data))
		decoder.KnownFields(true)
		if err := decoder.Decode(c); err != nil {
			// Re-parse tolerantly so unknown fields warn rather than fail.
			c2 := DefaultConfig()
			if err2 := yaml.Unmarshal(data, c2); err2 != nil {
				return nil, fmt.Errorf("parse config file %s: %w", path, err2)
			}
			*c = *c2
			c.Warnings = append(c.Warnings, fmt.Sprintf("config has unknown fields: %v", err))
		}
	}

	applyEnvOverrides(c)

	if err := parseDurations(c); err != nil {
		return nil, err
	}

	if c.RunID == "" {
		c.RunID = RandomRunID()
	}

	return c, nil
}

// FindConfigFile resolves the config path search order (spec §7.5):
// CLI flag → ./burnin-config.yaml → /etc/burnin/config.yaml.
func FindConfigFile(cliPath string) string {
	if cliPath != "" {
		return cliPath
	}
	if _, err := os.Stat("./burnin-config.yaml"); err == nil {
		return "./burnin-config.yaml"
	}
	if _, err := os.Stat("/etc/burnin/config.yaml"); err == nil {
		return "/etc/burnin/config.yaml"
	}
	return ""
}

// Validate checks the config and returns a slice of errors. Entries prefixed
// "WARNING:" are advisory and do not fail validation.
func (c *Config) Validate() []error {
	var errs []error

	if c.Version != ConfigVersion {
		errs = append(errs, fmt.Errorf("version must be %q, got %q", ConfigVersion, c.Version))
	}

	if c.Broker.Address == "" {
		errs = append(errs, fmt.Errorf("broker.address is required"))
	}

	// Kafka knob validation (spec §7.5).
	if c.Kafka.Acks != "all" && c.Kafka.Acks != "1" && c.Kafka.Acks != "0" {
		errs = append(errs, fmt.Errorf("kafka.acks must be all|1|0, got %q", c.Kafka.Acks))
	}
	switch c.Kafka.Compression {
	case "none", "gzip", "snappy", "lz4", "zstd":
	default:
		errs = append(errs, fmt.Errorf("kafka.compression must be none|gzip|snappy|lz4|zstd, got %q", c.Kafka.Compression))
	}
	if c.Kafka.Partitions < 1 || c.Kafka.Partitions > 256 {
		errs = append(errs, fmt.Errorf("kafka.partitions must be 1-256 (connector hard cap 256), got %d", c.Kafka.Partitions))
	}
	if c.Kafka.ReplicationFactor < 1 {
		errs = append(errs, fmt.Errorf("kafka.replication_factor must be >= 1, got %d", c.Kafka.ReplicationFactor))
	}
	if c.Kafka.IsolationLevel != "read_committed" && c.Kafka.IsolationLevel != "read_uncommitted" {
		errs = append(errs, fmt.Errorf("kafka.isolation_level must be read_committed|read_uncommitted, got %q", c.Kafka.IsolationLevel))
	}
	if c.Workers.TransactionsEOS.Enabled && c.Kafka.TxnTimeoutMS < 1000 {
		errs = append(errs, fmt.Errorf("kafka.txn_timeout_ms must be >= 1000 when transactions_eos is enabled, got %d", c.Kafka.TxnTimeoutMS))
	}
	if c.Kafka.FetchMaxWaitMS < 0 {
		errs = append(errs, fmt.Errorf("kafka.fetch_max_wait_ms must be >= 0, got %d", c.Kafka.FetchMaxWaitMS))
	}
	if m := c.Kafka.SASLMechanism; m != "" && m != "PLAIN" && m != "SCRAM-SHA-256" && m != "SCRAM-SHA-512" {
		errs = append(errs, fmt.Errorf("kafka.sasl_mechanism must be ''|PLAIN|SCRAM-SHA-256|SCRAM-SHA-512, got %q", m))
	}

	enabledCount := 0
	totalWorkers := 0

	for _, name := range AllWorkerNames {
		wc := c.GetWorkerConfig(name)
		if wc == nil || !wc.Enabled {
			continue
		}
		enabledCount++

		if wc.Channels < 1 || wc.Channels > 1000 {
			errs = append(errs, fmt.Errorf("%s.channels: must be 1-1000, got %d", name, wc.Channels))
		}
		if wc.Rate <= 0 {
			errs = append(errs, fmt.Errorf("%s.rate: must be > 0, got %d", name, wc.Rate))
		}
		if wc.ProducersPerChannel < 1 {
			errs = append(errs, fmt.Errorf("%s.producers_per_channel: must be >= 1, got %d", name, wc.ProducersPerChannel))
		}
		if wc.ConsumersPerChannel < 1 {
			errs = append(errs, fmt.Errorf("%s.consumers_per_channel: must be >= 1, got %d", name, wc.ConsumersPerChannel))
		}
		totalWorkers += wc.Channels * (wc.ProducersPerChannel + wc.ConsumersPerChannel)
	}

	if enabledCount == 0 {
		errs = append(errs, fmt.Errorf("at least one worker must be enabled"))
	}

	if c.Message.SizeMode != "fixed" && c.Message.SizeMode != "distribution" {
		errs = append(errs, fmt.Errorf("message.size_mode must be 'fixed' or 'distribution', got %q", c.Message.SizeMode))
	}
	if c.Message.SizeMode == "fixed" && c.Message.SizeBytes < 64 {
		errs = append(errs, fmt.Errorf("message.size_bytes: must be >= 64, got %d", c.Message.SizeBytes))
	}
	if c.Message.SizeMode == "fixed" && c.Message.SizeBytes > 1048576 {
		errs = append(errs, fmt.Errorf("message.size_bytes: must be <= 1048576 (Kafka 1 MiB default max), got %d", c.Message.SizeBytes))
	}
	if c.Message.ReorderWindow < 100 {
		errs = append(errs, fmt.Errorf("message.reorder_window: must be >= 100, got %d", c.Message.ReorderWindow))
	}

	if c.Shutdown.DrainTimeoutSeconds <= 0 {
		errs = append(errs, fmt.Errorf("shutdown.drain_timeout_seconds: must be > 0, got %d", c.Shutdown.DrainTimeoutSeconds))
	}
	if c.Metrics.Port < 1 || c.Metrics.Port > 65535 {
		errs = append(errs, fmt.Errorf("metrics.port: must be 1-65535, got %d", c.Metrics.Port))
	}

	if c.Thresholds.MaxLossPct < 0 || c.Thresholds.MaxLossPct > 100 {
		errs = append(errs, fmt.Errorf("thresholds.max_loss_pct: must be 0-100"))
	}
	if c.Thresholds.MaxDuplicationPct < 0 || c.Thresholds.MaxDuplicationPct > 100 {
		errs = append(errs, fmt.Errorf("thresholds.max_duplication_pct: must be 0-100"))
	}
	if c.Thresholds.MaxOffsetOrderViolations < 0 {
		errs = append(errs, fmt.Errorf("thresholds.max_offset_order_violations: must be >= 0"))
	}
	if c.Thresholds.MaxGroupLossAcrossRebalance < 0 {
		errs = append(errs, fmt.Errorf("thresholds.max_group_loss_across_rebalance: must be >= 0"))
	}
	if c.Thresholds.MaxEOSViolations < 0 {
		errs = append(errs, fmt.Errorf("thresholds.max_eos_violations: must be >= 0"))
	}
	if c.Thresholds.MaxLagAccuracyErrorMsgs < 0 {
		errs = append(errs, fmt.Errorf("thresholds.max_lag_accuracy_error_msgs: must be >= 0"))
	}
	for label, v := range map[string]float64{
		"max_p50_latency_ms":  c.Thresholds.MaxP50LatencyMS,
		"max_p95_latency_ms":  c.Thresholds.MaxP95LatencyMS,
		"max_p99_latency_ms":  c.Thresholds.MaxP99LatencyMS,
		"max_p999_latency_ms": c.Thresholds.MaxP999LatencyMS,
	} {
		if v <= 0 {
			errs = append(errs, fmt.Errorf("thresholds.%s: must be > 0", label))
		}
	}
	if c.Thresholds.MinThroughputPct <= 0 || c.Thresholds.MinThroughputPct > 100 {
		errs = append(errs, fmt.Errorf("thresholds.min_throughput_pct: must be > 0 and <= 100"))
	}
	if c.Thresholds.MaxErrorRatePct < 0 || c.Thresholds.MaxErrorRatePct > 100 {
		errs = append(errs, fmt.Errorf("thresholds.max_error_rate_pct: must be 0-100"))
	}
	if c.Thresholds.MaxMemoryGrowthFactor < 1.0 {
		errs = append(errs, fmt.Errorf("thresholds.max_memory_growth_factor: must be >= 1.0"))
	}
	if c.Thresholds.MaxDowntimePct < 0 || c.Thresholds.MaxDowntimePct > 100 {
		errs = append(errs, fmt.Errorf("thresholds.max_downtime_pct: must be 0-100"))
	}
	if c.Recovery.ReconnectMultiplier < 1.0 {
		errs = append(errs, fmt.Errorf("recovery.reconnect_multiplier: must be >= 1.0, got %f", c.Recovery.ReconnectMultiplier))
	}

	if totalWorkers > 1000 {
		errs = append(errs, fmt.Errorf("WARNING: high worker count: %d -- may impact system resources", totalWorkers))
	}

	return errs
}

// LogResourceWarnings logs any advisory (WARNING:-prefixed) validation entries.
func (c *Config) LogResourceWarnings(logger *slog.Logger) {
	for _, e := range c.Validate() {
		if strings.HasPrefix(e.Error(), "WARNING:") {
			logger.Warn(e.Error())
		}
	}
}

func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("KUBEMQ_BROKER_ADDRESS"); v != "" {
		cfg.Broker.Address = v
	}

	envNames := map[string]string{
		WorkerProduceRoundTrip: "PRODUCE_ROUND_TRIP",
		WorkerKeyedOrdering:    "KEYED_ORDERING",
		WorkerConsumerGroup:    "CONSUMER_GROUP",
		WorkerOffsetCommitLag:  "OFFSET_COMMIT_LAG",
		WorkerAdminTopicChurn:  "ADMIN_TOPIC_CHURN",
		WorkerTransactionsEOS:  "TRANSACTIONS_EOS",
	}

	for name, env := range envNames {
		wc := cfg.GetWorkerConfig(name)
		if wc == nil {
			continue
		}
		if v := os.Getenv("BURNIN_" + env + "_RATE"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				wc.Rate = n
			}
		}
		if v := os.Getenv("BURNIN_" + env + "_CHANNELS"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				wc.Channels = n
			}
		}
		if v := os.Getenv("BURNIN_" + env + "_ENABLED"); v != "" {
			wc.Enabled = v == "true" || v == "1"
		}
	}
}

func parseDurations(c *Config) error {
	var err error

	if c.Duration != "" && c.Duration != "0" {
		c.DurationParsed, err = parseDuration(c.Duration)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", c.Duration, err)
		}
	}
	if c.WarmupDuration != "" {
		c.WarmupDurationParsed, err = parseDuration(c.WarmupDuration)
		if err != nil {
			return fmt.Errorf("invalid warmup_duration %q: %w", c.WarmupDuration, err)
		}
	}
	if c.Metrics.ReportInterval != "" {
		c.ReportIntervalParsed, err = parseDuration(c.Metrics.ReportInterval)
		if err != nil {
			return fmt.Errorf("invalid metrics.report_interval %q: %w", c.Metrics.ReportInterval, err)
		}
	}
	if c.ForcedDisconnect.Interval != "" && c.ForcedDisconnect.Interval != "0" {
		c.ForcedDisconnInterval, err = parseDuration(c.ForcedDisconnect.Interval)
		if err != nil {
			return fmt.Errorf("invalid forced_disconnect.interval %q: %w", c.ForcedDisconnect.Interval, err)
		}
	}
	if c.ForcedDisconnect.Duration != "" {
		c.ForcedDisconnDuration, err = parseDuration(c.ForcedDisconnect.Duration)
		if err != nil {
			return fmt.Errorf("invalid forced_disconnect.duration %q: %w", c.ForcedDisconnect.Duration, err)
		}
	}
	if c.Recovery.ReconnectInterval != "" {
		c.ReconnectInterval, err = parseDuration(c.Recovery.ReconnectInterval)
		if err != nil {
			return fmt.Errorf("invalid recovery.reconnect_interval %q: %w", c.Recovery.ReconnectInterval, err)
		}
	}
	if c.Recovery.ReconnectMaxInterval != "" {
		c.ReconnectMaxInterval, err = parseDuration(c.Recovery.ReconnectMaxInterval)
		if err != nil {
			return fmt.Errorf("invalid recovery.reconnect_max_interval %q: %w", c.Recovery.ReconnectMaxInterval, err)
		}
	}
	if c.Thresholds.MaxDuration != "" {
		c.MaxDurationParsed, err = parseDuration(c.Thresholds.MaxDuration)
		if err != nil {
			return fmt.Errorf("invalid thresholds.max_duration %q: %w", c.Thresholds.MaxDuration, err)
		}
	}

	return nil
}

func parseDuration(s string) (time.Duration, error) {
	if strings.HasSuffix(s, "d") {
		s = strings.TrimSuffix(s, "d")
		days, err := strconv.Atoi(s)
		if err != nil {
			return 0, fmt.Errorf("invalid day duration: %s", s)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}

// RandomRunID returns an 8-hex-char random run identifier.
func RandomRunID() string {
	b := make([]byte, 4)
	_, _ = cryptorand.Read(b)
	return fmt.Sprintf("%08x", b)
}

// ParseDurationsPublic re-parses durations on a config (used after API overlay).
func ParseDurationsPublic(cfg *Config) error {
	return parseDurations(cfg)
}
