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

// ProduceRoundTripWorker (worker 1, spec §7.3) drives idempotent Produce → Fetch
// fidelity at sustained rate against a single topic, verifying ZERO loss + ZERO
// duplication under the idempotent producer. Invariant: max_loss_pct 0.0,
// max_duplication_pct 0.0. This is the canonical worker — every other worker
// follows this Start / consumeLoop / produceLoop shape.
type ProduceRoundTripWorker struct {
	*BaseWorker
	topic   string
	group   string
	seq     atomic.Uint64
	prodCli *kgo.Client
}

// NewProduceRoundTripWorker creates a produce_round_trip worker.
func NewProduceRoundTripWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	topic := transport.TopicName(config.WorkerProduceRoundTrip, idx)
	channel := transport.MappedChannel(topic) // kafka.<topic>, for logging
	w := &ProduceRoundTripWorker{
		BaseWorker: NewBaseWorker(config.WorkerProduceRoundTrip, channel, idx, cfg, logger),
		topic:      topic,
	}
	w.group = w.groupID("")
	return w
}

// Start creates the topic (kadm) and brings up the consumer(s).
func (w *ProduceRoundTripWorker) Start(ctx context.Context) error {
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

	n := w.workerCfg.ConsumersPerChannel
	if n < 1 {
		n = 1
	}
	for i := 0; i < n; i++ {
		tag := fmt.Sprintf("c%d", i)
		w.consumerWG.Add(1)
		go func(tag string) { defer w.consumerWG.Done(); w.consumeLoop(w.consumerCtx, tag) }(tag)
	}
	return nil
}

// consumeLoop long-polls the topic and records each record's fidelity. A group
// consumer gives at-least-once; the idempotent producer + tracker dedup catch the
// zero-dup gate.
func (w *ProduceRoundTripWorker) consumeLoop(ctx context.Context, tag string) {
	gen := w.disconnectGeneration()
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
		if cli == nil || w.disconnectGeneration() != gen {
			gen = w.disconnectGeneration()
			start := time.Now()
			cfg := w.kafkaClientCfg("cons-" + tag)
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

func (w *ProduceRoundTripWorker) handleRecord(r *kgo.Record) {
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

// StartProducers launches the idempotent Produce loop (measurement window).
func (w *ProduceRoundTripWorker) StartProducers() {
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

func (w *ProduceRoundTripWorker) produceLoop(ctx context.Context, pi int) {
	producerID := fmt.Sprintf("%s-p%d", w.channelName, pi)
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
		body, crcHex := payload.Build(w.selectMessageSize())
		rec := &kgo.Record{Topic: w.topic, Value: body, Headers: stampHeaders(producerID, seq, crcHex)}

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
func (w *ProduceRoundTripWorker) StopProducers() {
	w.BaseWorker.StopProducers()
	if w.prodCli != nil {
		w.prodCli.Close()
		w.prodCli = nil
	}
}
