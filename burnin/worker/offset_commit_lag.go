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

// lagSampleInterval is how often offset_commit_lag samples the reported group lag
// and compares it to the tracker's ground truth.
const lagSampleInterval = 3 * time.Second

// OffsetCommitLagWorker (worker 4, spec §7.3) runs a group consumer with MANUAL
// commit (kgo.DisableAutoCommit + CommitRecords) and, after each commit,
// compares the connector-reported group lag (kadm HWM − committed) against the
// tracker's ground truth (produced-acked − committed). offset == STAN Sequence,
// so the reported lag is exact; the running MAX accuracy error must stay within
// max_lag_accuracy_error_msgs (1, absorbing only in-flight poll skew). It also
// asserts resume-from-committed after a consumer restart (the standard loss gate
// then catches any gap).
type OffsetCommitLagWorker struct {
	*BaseWorker
	topic   string
	group   string
	seq     atomic.Uint64
	prodCli *kgo.Client

	restarted atomic.Bool
}

// NewOffsetCommitLagWorker creates an offset_commit_lag worker.
func NewOffsetCommitLagWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	topic := transport.TopicName(config.WorkerOffsetCommitLag, idx)
	channel := transport.MappedChannel(topic)
	w := &OffsetCommitLagWorker{
		BaseWorker: NewBaseWorker(config.WorkerOffsetCommitLag, channel, idx, cfg, logger),
		topic:      topic,
	}
	w.group = w.groupID("")
	return w
}

// Start creates the topic, brings up the manual-commit consumer, and starts the
// lag sampler.
func (w *OffsetCommitLagWorker) Start(ctx context.Context) error {
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

	w.consumerWG.Add(1)
	go func() { defer w.consumerWG.Done(); w.consumeLoop(w.consumerCtx) }()

	w.consumerWG.Add(1)
	go func() { defer w.consumerWG.Done(); w.lagSampler(w.consumerCtx) }()
	return nil
}

func (w *OffsetCommitLagWorker) consumeLoop(ctx context.Context) {
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
			cfg := w.kafkaClientCfg("cons")
			cfg.ConsumerGroup = w.group
			cfg.ConsumeTopics = []string{w.topic}
			cfg.DisableAutoCommit = true // manual CommitRecords
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

		var recs []*kgo.Record
		fetches.EachRecord(func(r *kgo.Record) {
			w.handleRecord(r)
			recs = append(recs, r)
		})
		if len(recs) > 0 {
			if err := cli.CommitRecords(ctx, recs...); err != nil {
				if ctx.Err() != nil {
					return
				}
				w.recordError("commit_failure")
				continue
			}
			for range recs {
				w.recordDeleted() // one commit per record (offset advance)
			}
		}
	}
}

func (w *OffsetCommitLagWorker) handleRecord(r *kgo.Record) {
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

// lagSampler periodically reads the connector-reported group lag and folds the
// accuracy error (|reported − true|) into the running MAX. It also triggers a
// one-shot consumer restart to exercise resume-from-committed.
func (w *OffsetCommitLagWorker) lagSampler(ctx context.Context) {
	admCli, err := transport.NewClient(w.kafkaClientCfg("lag-admin"))
	if err != nil {
		w.recordError("admin_failure")
		return
	}
	adm := transport.NewAdmin(admCli)
	defer admCli.Close()

	ticker := time.NewTicker(lagSampleInterval)
	defer ticker.Stop()
	var samples int
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			glag, err := transport.GroupLag(ctx, adm, w.group)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}
			var reported int64
			for _, parts := range glag {
				for _, ml := range parts {
					if ml.Lag > 0 {
						reported += ml.Lag
					}
				}
			}
			// Ground truth: produced-acked minus committed (clamped ≥ 0). offset ==
			// STAN Sequence, so this should match the reported lag within poll skew.
			sent := w.sent.Load()
			committed := w.deleted.Load()
			var trueLag int64
			if sent > committed {
				trueLag = int64(sent - committed)
			}
			errMsgs := reported - trueLag
			if errMsgs < 0 {
				errMsgs = -errMsgs
			}
			w.setLagAccuracyErr(uint64(errMsgs))

			// After a few clean samples, exercise resume-from-committed exactly once.
			samples++
			if samples == 3 && w.restarted.CompareAndSwap(false, true) {
				w.DisconnectConsumers() // force the group consumer to rebuild + resume
			}
		}
	}
}

// StartProducers launches the Produce loop.
func (w *OffsetCommitLagWorker) StartProducers() {
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

func (w *OffsetCommitLagWorker) produceLoop(ctx context.Context, pi int) {
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
func (w *OffsetCommitLagWorker) StopProducers() {
	w.BaseWorker.StopProducers()
	if w.prodCli != nil {
		w.prodCli.Close()
		w.prodCli = nil
	}
}
