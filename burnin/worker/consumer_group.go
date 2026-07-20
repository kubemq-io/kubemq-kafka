package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kubemq-io/kubemq-kafka/burnin/config"
	"github.com/kubemq-io/kubemq-kafka/burnin/payload"
	"github.com/kubemq-io/kubemq-kafka/burnin/transport"
)

// rebalanceInterval is how often the consumer_group worker churns one group
// member to force a rebalance during the measurement window.
const rebalanceInterval = 8 * time.Second

// ConsumerGroupWorker (worker 3, spec §7.3) runs N consumers in ONE group
// (consumers_per_channel ≥ 2) and periodically kills+respawns one member to force
// a rebalance. The producer streams keyed records. After each rebalance the group
// must resume from committed offsets with NO loss. Any tracker loss delta observed
// across a rebalance is counted as group-rebalance loss. Invariant:
// max_group_loss_across_rebalance 0.
type ConsumerGroupWorker struct {
	*BaseWorker
	topic   string
	group   string
	seq     atomic.Uint64
	prodCli *kgo.Client

	numConsumers int
	consumerGen  []atomic.Uint64 // per-member churn generation
	attributed   atomic.Uint64   // tracker loss already folded into groupLossRebalance
}

// NewConsumerGroupWorker creates a consumer_group worker.
func NewConsumerGroupWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	topic := transport.TopicName(config.WorkerConsumerGroup, idx)
	channel := transport.MappedChannel(topic)
	n := cfg.GetWorkerConfig(config.WorkerConsumerGroup).ConsumersPerChannel
	if n < 2 {
		n = 2 // a group needs ≥2 members for a meaningful rebalance
	}
	w := &ConsumerGroupWorker{
		BaseWorker:   NewBaseWorker(config.WorkerConsumerGroup, channel, idx, cfg, logger),
		topic:        topic,
		numConsumers: n,
		consumerGen:  make([]atomic.Uint64, n),
	}
	w.group = w.groupID("")
	return w
}

// Start creates the topic, brings up the group members, and starts the rebalance
// churner.
func (w *ConsumerGroupWorker) Start(ctx context.Context) error {
	w.consumerCtx, w.consumerCancel = context.WithCancel(ctx)

	admCli, err := transport.NewClient(w.kafkaClientCfg("admin"))
	if err != nil {
		return fmt.Errorf("build kafka client: %w", err)
	}
	adm := transport.NewAdmin(admCli)
	if err := transport.CreateTopic(ctx, adm, w.topic, w.cfg.Kafka.Partitions, w.cfg.Kafka.ReplicationFactor); err != nil {
		admCli.Close()
		return err
	}
	admCli.Close()

	for i := 0; i < w.numConsumers; i++ {
		w.consumerWG.Add(1)
		go func(mi int) { defer w.consumerWG.Done(); w.consumeLoop(w.consumerCtx, mi) }(i)
	}

	// Rebalance churner: cycle one member's generation each interval so the group
	// rejoins/rebalances repeatedly, then reconcile any loss delta.
	w.consumerWG.Add(1)
	go func() { defer w.consumerWG.Done(); w.rebalanceChurner(w.consumerCtx) }()
	return nil
}

func (w *ConsumerGroupWorker) memberGen(mi int) uint64 {
	return w.consumerGen[mi].Load() + w.disconnectGeneration()
}

func (w *ConsumerGroupWorker) consumeLoop(ctx context.Context, mi int) {
	gen := w.memberGen(mi)
	var cli *kgo.Client
	closeCli := func() {
		if cli != nil {
			cli.Close()
			cli = nil
		}
	}
	defer closeCli()

	for {
		if ctx.Err() != nil {
			return
		}
		if cli == nil || w.memberGen(mi) != gen {
			gen = w.memberGen(mi)
			start := time.Now()
			cfg := w.kafkaClientCfg(fmt.Sprintf("m%d", mi))
			cfg.ConsumerGroup = w.group
			cfg.ConsumeTopics = []string{w.topic}
			c, err := transport.NewClient(cfg)
			if err != nil {
				w.recordError("connect_failure")
				w.addDowntime(time.Since(start))
				if !sleepCtx(ctx, time.Second) {
					return
				}
				continue
			}
			if cli != nil {
				w.recordReconnection()
			}
			closeCli()
			cli = c
		}
		w.signalReady()

		fetches := cli.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			if ctx.Err() != nil {
				return
			}
			w.recordError("fetch_failure")
			closeCli()
			continue
		}
		fetches.EachRecord(func(r *kgo.Record) { w.handleRecord(r) })
	}
}

func (w *ConsumerGroupWorker) handleRecord(r *kgo.Record) {
	w.recordReceived(len(r.Value))
	producerID, seq, crcHex, sentAt, ok := extractMeta(r.Headers)
	if !ok {
		return
	}
	if crcHex != "" && !payload.VerifyCRC(r.Value, crcHex) {
		w.recordCorrupted()
	}
	if !sentAt.IsZero() {
		w.recordLatency(time.Since(sentAt))
	}
	w.recordTracked(producerID, seq)
}

// rebalanceChurner cycles one member's churn generation each interval (forcing a
// rejoin/rebalance) and folds any confirmed-loss delta observed across the churn
// into the group-rebalance-loss counter.
func (w *ConsumerGroupWorker) rebalanceChurner(ctx context.Context) {
	ticker := time.NewTicker(rebalanceInterval)
	defer ticker.Stop()
	var next int
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Force a fresh gap-detection pass so any loss to date is confirmed, then
			// churn one member and reconcile.
			w.trk.DetectGaps()
			w.reconcileGroupLoss()
			w.consumerGen[next].Add(1)
			next = (next + 1) % w.numConsumers
		}
	}
}

// reconcileGroupLoss folds any newly-confirmed tracker loss (beyond what has
// already been attributed) into the group-rebalance-loss counter.
func (w *ConsumerGroupWorker) reconcileGroupLoss() {
	lost := w.trk.TotalLost()
	prev := w.attributed.Load()
	if lost > prev {
		w.recordGroupLossRebalance(lost - prev)
		w.attributed.Store(lost)
	}
}

// StartProducers launches the keyed Produce loop.
func (w *ConsumerGroupWorker) StartProducers() {
	w.producerCtx, w.producerCancel = context.WithCancel(context.Background())
	n := w.workerCfg.ProducersPerChannel
	if n < 1 {
		n = 1
	}
	for i := 0; i < n; i++ {
		w.producerWG.Add(1)
		go func(pi int) { defer w.producerWG.Done(); w.produceLoop(w.producerCtx, pi) }(i)
	}
}

func (w *ConsumerGroupWorker) produceLoop(ctx context.Context, pi int) {
	producerID := fmt.Sprintf("%s-p%d", w.channelName, pi)
	var keySeq uint64
	for {
		if ctx.Err() != nil {
			return
		}
		if w.prodCli == nil {
			c, err := transport.NewClient(w.kafkaClientCfg("prod"))
			if err != nil {
				w.recordError("connect_failure")
				if !sleepCtx(ctx, time.Second) {
					return
				}
				continue
			}
			w.prodCli = c
		}
		if err := w.waitForRate(ctx); err != nil {
			return
		}

		seq := w.seq.Add(1)
		keySeq++
		key := []byte(fmt.Sprintf("k%d", keySeq%uint64(w.cfg.Kafka.Partitions*4+1)))
		body, crcHex := payload.Build(w.selectMessageSize())
		rec := &kgo.Record{Topic: w.topic, Key: key, Value: body, Headers: stampHeaders(producerID, seq, crcHex)}

		start := time.Now()
		if err := w.prodCli.ProduceSync(ctx, rec).FirstErr(); err != nil {
			if ctx.Err() != nil {
				return
			}
			w.recordError("produce_failure")
			w.prodCli.Close()
			w.prodCli = nil
			continue
		}
		metricObserveSend(w.name, time.Since(start))
		w.recordSent(len(body))
	}
}

// StopProducers stops the produce loop and closes the producer client.
func (w *ConsumerGroupWorker) StopProducers() {
	w.BaseWorker.StopProducers()
	if w.prodCli != nil {
		w.prodCli.Close()
		w.prodCli = nil
	}
	// Final reconciliation so any loss confirmed at drain is attributed.
	w.trk.DetectGaps()
	w.reconcileGroupLoss()
}
