// Package worker implements the six Kafka burn-in workers (spec §7.3):
// produce_round_trip, keyed_ordering, consumer_group, offset_commit_lag,
// admin_topic_churn, and transactions_eos. They drive the franz-go (kgo + kadm)
// client — the server's own conformance client — against the KubeMQ Kafka
// wire-protocol connector. Each worker records loss/dup/latency/throughput plus
// its own Kafka fidelity counters via the shared BaseWorker. Mirrors
// kubemq-aws/burnin/worker recast for franz-go.
package worker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/kubemq-io/kubemq-kafka/burnin/config"
	"github.com/kubemq-io/kubemq-kafka/burnin/metrics"
	"github.com/kubemq-io/kubemq-kafka/burnin/payload"
	"github.com/kubemq-io/kubemq-kafka/burnin/tracker"
	"github.com/kubemq-io/kubemq-kafka/burnin/transport"
)

// Worker is the lifecycle contract every burn-in worker satisfies. The engine
// drives Start (provision topics + start consumers) → wait ready →
// StartProducers (measurement window) → StopProducers → drain → StopConsumers.
type Worker interface {
	Name() string
	ChannelName() string
	ChannelIndex() int

	// Start provisions topics and brings up consumers, signalling ConsumerReady
	// once they are polling.
	Start(ctx context.Context) error
	StartProducers()
	StopProducers()
	StopConsumers()
	DisconnectConsumers()
	ConsumerReady() <-chan struct{}

	Tracker() *tracker.Tracker
	LatencyAccumulator() *metrics.LatencyAccumulator
	PeakRate() *metrics.PeakRateTracker
	RateWindow() *metrics.SlidingRateWindow

	SentCount() uint64
	ReceivedCount() uint64
	ErrorCount() uint64
	CorruptedCount() uint64
	ReconnectionCount() uint64
	DowntimeSeconds() float64
	DuplicatedCount() uint64
	DeletedCount() uint64

	// Kafka-specific fidelity counters (spec §7.4). Workers that do not drive a
	// given operation inherit the zero-valued BaseWorker getters.
	OffsetOrderViolations() uint64
	GroupLossRebalance() uint64
	EOSViolations() uint64
	LagAccuracyErrMax() uint64
	AdminOpFailures() uint64
	AdminInvalidRejected() uint64
	KIP890Residual() uint64

	AdvanceRateWindows()
	ResetAfterWarmup()
}

// BaseWorker holds the shared state and helpers for all Kafka workers.
type BaseWorker struct {
	name         string
	channelName  string
	channelIndex int
	cfg          *config.Config
	workerCfg    *config.WorkerConfig
	logger       *slog.Logger

	sizeDistrib *payload.SizeDistribution

	trk        *tracker.Tracker
	latAccum   *metrics.LatencyAccumulator
	peakRate   *metrics.PeakRateTracker
	rateWindow *metrics.SlidingRateWindow

	limiter *rate.Limiter

	producerCtx    context.Context
	producerCancel context.CancelFunc
	consumerCtx    context.Context
	consumerCancel context.CancelFunc
	producerWG     sync.WaitGroup
	consumerWG     sync.WaitGroup
	consumerReady  chan struct{}
	readyOnce      sync.Once

	// disconnectGen is bumped by DisconnectConsumers to signal receiver loops to
	// rebuild their franz-go clients (forced-churn injection + rebalance churn).
	disconnectGen atomic.Uint64

	sent          atomic.Uint64
	received      atomic.Uint64
	errors        atomic.Uint64
	corrupted     atomic.Uint64
	reconnections atomic.Uint64
	downtime      atomic.Uint64 // nanoseconds
	duplicated    atomic.Uint64
	deleted       atomic.Uint64

	// Kafka-specific fidelity counters (spec §7.4). All are SUM-folded across a
	// worker group EXCEPT lagAccuracyErrMax, which is a running MAX. kip890Residual
	// is RECORDED for transparency but EXCLUDED from eosViolations and every gate
	// (spec §2.5).
	offsetOrderViolations atomic.Uint64
	groupLossRebalance    atomic.Uint64
	eosViolations         atomic.Uint64
	lagAccuracyErrMax     atomic.Uint64
	adminOpFailures       atomic.Uint64
	adminInvalidRejected  atomic.Uint64
	kip890Residual        atomic.Uint64
}

// NewBaseWorker constructs the shared worker scaffolding.
func NewBaseWorker(name, channelName string, channelIndex int, cfg *config.Config, logger *slog.Logger) *BaseWorker {
	workerCfg := cfg.GetWorkerConfig(name)

	targetRate := float64(workerCfg.Rate)
	burst := int(targetRate)
	if burst < 1 {
		burst = 1
	}

	var sizeDistrib *payload.SizeDistribution
	if cfg.Message.SizeMode == "distribution" {
		sizeDistrib, _ = payload.ParseDistribution(cfg.Message.SizeDistribution)
	}

	return &BaseWorker{
		name:         name,
		channelName:  channelName,
		channelIndex: channelIndex,
		cfg:          cfg,
		workerCfg:    workerCfg,
		logger:       logger.With("worker", name, "channel", channelName),

		sizeDistrib: sizeDistrib,

		trk:        tracker.New(cfg.Message.ReorderWindow),
		latAccum:   metrics.NewLatencyAccumulator(),
		peakRate:   metrics.NewPeakRateTracker(),
		rateWindow: metrics.NewSlidingRateWindow(),

		limiter:       rate.NewLimiter(rate.Limit(targetRate), burst),
		consumerReady: make(chan struct{}),
	}
}

// --- Accessors ---

func (b *BaseWorker) Name() string                                    { return b.name }
func (b *BaseWorker) ChannelName() string                             { return b.channelName }
func (b *BaseWorker) ChannelIndex() int                               { return b.channelIndex }
func (b *BaseWorker) ConsumerReady() <-chan struct{}                  { return b.consumerReady }
func (b *BaseWorker) Tracker() *tracker.Tracker                       { return b.trk }
func (b *BaseWorker) LatencyAccumulator() *metrics.LatencyAccumulator { return b.latAccum }
func (b *BaseWorker) PeakRate() *metrics.PeakRateTracker              { return b.peakRate }
func (b *BaseWorker) RateWindow() *metrics.SlidingRateWindow          { return b.rateWindow }

func (b *BaseWorker) SentCount() uint64         { return b.sent.Load() }
func (b *BaseWorker) ReceivedCount() uint64     { return b.received.Load() }
func (b *BaseWorker) ErrorCount() uint64        { return b.errors.Load() }
func (b *BaseWorker) CorruptedCount() uint64    { return b.corrupted.Load() }
func (b *BaseWorker) ReconnectionCount() uint64 { return b.reconnections.Load() }
func (b *BaseWorker) DuplicatedCount() uint64   { return b.duplicated.Load() }
func (b *BaseWorker) DeletedCount() uint64      { return b.deleted.Load() }

func (b *BaseWorker) OffsetOrderViolations() uint64 { return b.offsetOrderViolations.Load() }
func (b *BaseWorker) GroupLossRebalance() uint64    { return b.groupLossRebalance.Load() }
func (b *BaseWorker) EOSViolations() uint64         { return b.eosViolations.Load() }
func (b *BaseWorker) LagAccuracyErrMax() uint64     { return b.lagAccuracyErrMax.Load() }
func (b *BaseWorker) AdminOpFailures() uint64       { return b.adminOpFailures.Load() }
func (b *BaseWorker) AdminInvalidRejected() uint64  { return b.adminInvalidRejected.Load() }
func (b *BaseWorker) KIP890Residual() uint64        { return b.kip890Residual.Load() }

func (b *BaseWorker) DowntimeSeconds() float64 {
	return float64(b.downtime.Load()) / float64(time.Second)
}

// --- Client-config builders ---

// kafkaClientCfg returns a transport.KafkaClientConfig pre-populated from the
// shared kafka block; callers layer ConsumeTopics/ConsumerGroup/TransactionalID/
// DisableAutoCommit on top. suffix disambiguates the franz-go ClientID.
func (b *BaseWorker) kafkaClientCfg(suffix string) transport.KafkaClientConfig {
	clientID := b.cfg.Broker.ClientIDPrefix + "-" + b.channelName
	if suffix != "" {
		clientID += "-" + suffix
	}
	return transport.KafkaClientConfig{
		Bootstrap:      b.cfg.Broker.Address,
		ClientID:       clientID,
		Acks:           b.cfg.Kafka.Acks,
		Compression:    b.cfg.Kafka.Compression,
		IsolationLevel: b.cfg.Kafka.IsolationLevel,
		Idempotent:     b.cfg.Kafka.Idempotent,
		FetchMaxWaitMS: b.cfg.Kafka.FetchMaxWaitMS,
		TxnTimeoutMS:   b.cfg.Kafka.TxnTimeoutMS,
		SASLMechanism:  b.cfg.Kafka.SASLMechanism,
		SASLUsername:   b.cfg.Kafka.SASLUsername,
		SASLPassword:   b.cfg.Kafka.SASLPassword,
		TLS:            b.cfg.Kafka.TLS,
	}
}

// groupID builds the consumer group id for this channel: <group_prefix>-<channel>
// [-suffix]. Distinct suffixes keep independent groups from sharing offsets.
func (b *BaseWorker) groupID(suffix string) string {
	g := b.cfg.Kafka.GroupPrefix + "-" + b.channelName
	if suffix != "" {
		g += "-" + suffix
	}
	return g
}

// --- Counter helpers (used by concrete workers) ---

func (b *BaseWorker) recordSent(bytes int) {
	b.sent.Add(1)
	b.rateWindow.Record()
	b.peakRate.Record()
	metrics.IncSent(b.name, b.channelName)
	metrics.RecordBytesSent(b.name, bytes)
}

func (b *BaseWorker) recordReceived(bytes int) {
	b.received.Add(1)
	metrics.IncReceived(b.name, b.channelName)
	metrics.RecordBytesReceived(b.name, bytes)
}

func (b *BaseWorker) recordDeleted() {
	b.deleted.Add(1)
	metrics.IncDelete(b.name)
}

func (b *BaseWorker) recordError(errType string) {
	b.errors.Add(1)
	metrics.IncError(b.name, errType)
}

func (b *BaseWorker) recordCorrupted() {
	b.corrupted.Add(1)
	metrics.IncCorrupted(b.name)
}

func (b *BaseWorker) recordReconnection() {
	b.reconnections.Add(1)
	metrics.IncReconnection(b.name)
}

func (b *BaseWorker) recordLatency(d time.Duration) {
	b.latAccum.Record(d)
	metrics.ObserveLatency(b.name, d)
}

// --- Kafka-specific fidelity counter helpers (spec §7.4) ---

func (b *BaseWorker) recordOffsetOrderViolation() {
	b.offsetOrderViolations.Add(1)
	metrics.IncOffsetOrderViolation(b.name)
}

func (b *BaseWorker) recordGroupLossRebalance(n uint64) {
	if n == 0 {
		return
	}
	b.groupLossRebalance.Add(n)
	for i := uint64(0); i < n; i++ {
		metrics.IncGroupLossRebalance(b.name)
	}
}

func (b *BaseWorker) recordEOSViolation() {
	b.eosViolations.Add(1)
	metrics.IncEOSViolation(b.name)
}

func (b *BaseWorker) recordAdminOpFailure() {
	b.adminOpFailures.Add(1)
	metrics.IncAdminOpFailure(b.name)
}

func (b *BaseWorker) recordAdminInvalidRejected() {
	b.adminInvalidRejected.Add(1)
	metrics.IncAdminInvalidRejected(b.name)
}

// recordKIP890Residual records a KIP-890 V1 same-epoch zombie admission. This is
// RECORDED for transparency ONLY — it is EXCLUDED from eosViolations and every
// verdict gate (spec §2.5).
func (b *BaseWorker) recordKIP890Residual() {
	b.kip890Residual.Add(1)
	metrics.IncKIP890Residual(b.name)
}

// setLagAccuracyErr folds an observed group-lag accuracy error (|reported − true|
// in messages) into the running MAX and mirrors it to the gauge.
func (b *BaseWorker) setLagAccuracyErr(v uint64) {
	for {
		cur := b.lagAccuracyErrMax.Load()
		if v <= cur {
			return
		}
		if b.lagAccuracyErrMax.CompareAndSwap(cur, v) {
			metrics.SetLagAccuracyError(b.name, float64(v))
			return
		}
	}
}

// recordTracked feeds a received (producer-id, seq) into the tracker and records
// duplicates / out-of-order in metrics. Returns whether it was a duplicate so
// callers can distinguish a legal at-least-once redelivery.
func (b *BaseWorker) recordTracked(producerID string, seq uint64) (isDuplicate bool) {
	dup, oo := b.trk.Record(producerID, seq)
	if dup {
		b.duplicated.Add(1)
		metrics.IncDuplicated(b.name)
	}
	if oo {
		metrics.IncOutOfOrder(b.name)
	}
	return dup
}

// --- Rate control ---

func (b *BaseWorker) waitForRate(ctx context.Context) error {
	return b.limiter.Wait(ctx)
}

// --- Message size ---

func (b *BaseWorker) selectMessageSize() int {
	if b.cfg.Message.SizeMode == "distribution" && b.sizeDistrib != nil {
		return b.sizeDistrib.SelectSize()
	}
	return b.cfg.Message.SizeBytes
}

// --- Downtime accounting ---

func (b *BaseWorker) addDowntime(d time.Duration) {
	b.downtime.Add(uint64(d))
	metrics.AddDowntime(b.name, d.Seconds())
}

// --- Rate windows ---

func (b *BaseWorker) AdvanceRateWindows() {
	b.rateWindow.Advance()
	b.peakRate.Advance()
}

// --- Warmup reset ---

func (b *BaseWorker) ResetAfterWarmup() {
	b.trk.Reset()
	b.latAccum.Reset()
	b.peakRate = metrics.NewPeakRateTracker()
	b.rateWindow.Reset()

	b.sent.Store(0)
	b.received.Store(0)
	b.errors.Store(0)
	b.corrupted.Store(0)
	b.reconnections.Store(0)
	b.downtime.Store(0)
	b.duplicated.Store(0)
	b.deleted.Store(0)

	b.offsetOrderViolations.Store(0)
	b.groupLossRebalance.Store(0)
	b.eosViolations.Store(0)
	b.lagAccuracyErrMax.Store(0)
	b.adminOpFailures.Store(0)
	b.adminInvalidRejected.Store(0)
	b.kip890Residual.Store(0)
}

// --- Ready signalling ---

func (b *BaseWorker) signalReady() {
	b.readyOnce.Do(func() { close(b.consumerReady) })
}

// --- Forced disconnect ---

// DisconnectConsumers bumps the disconnect generation so receiver loops rebuild
// their franz-go clients (and, for group consumers, force a rejoin/rebalance).
// The default churn-free run never calls this.
func (b *BaseWorker) DisconnectConsumers() {
	b.disconnectGen.Add(1)
}

// disconnectGeneration returns the current churn generation; receiver loops
// compare against a captured value to detect a forced disconnect.
func (b *BaseWorker) disconnectGeneration() uint64 {
	return b.disconnectGen.Load()
}

// --- Lifecycle helpers shared by concrete workers ---

func (b *BaseWorker) StopProducers() {
	if b.producerCancel != nil {
		b.producerCancel()
	}
	b.producerWG.Wait()
}

func (b *BaseWorker) StopConsumers() {
	if b.consumerCancel != nil {
		b.consumerCancel()
	}
	b.consumerWG.Wait()
}
