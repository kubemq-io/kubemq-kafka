package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kubemq-io/kubemq-kafka/burnin/config"
	"github.com/kubemq-io/kubemq-kafka/burnin/payload"
	"github.com/kubemq-io/kubemq-kafka/burnin/transport"
)

// txnRecordsPerCycle is how many records each transaction produces.
const txnRecordsPerCycle = 4

// txnOutcomeHeader marks the intended commit/abort disposition of a produced
// record so the read_committed consumer can assert an aborted record was NEVER
// delivered.
const txnOutcomeHeader = "TxnOutcome"

// TransactionsEOSWorker (worker 6, spec §7.3) drives the full EOS flow:
// InitProducerId → (implicit AddPartitionsToTxn) → BeginTransaction → txn Produce
// → EndTransaction(commit|abort), alternating commit and abort each cycle. A
// separate read_committed group consumer reads back. Committed records must arrive
// EXACTLY ONCE; aborted records must NEVER be delivered under read_committed
// (any aborted delivery => eosViolation). Under EOS a duplicate of a committed
// record is the KIP-890 V1 same-epoch residual — RECORDED via kip890Residual and
// EXCLUDED from eosViolations and every gate (spec §2.5). Gate: max_eos_violations
// 0.
type TransactionsEOSWorker struct {
	*BaseWorker
	topic  string
	group  string
	txnID  string
	txnCli *kgo.Client

	committedSeq atomic.Uint64
	abortSeq     atomic.Uint64
	cycle        atomic.Uint64

	mu   sync.Mutex
	seen map[string]struct{} // committed (producerID|seq) already delivered
}

// NewTransactionsEOSWorker creates a transactions_eos worker.
func NewTransactionsEOSWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	topic := transport.TopicName(config.WorkerTransactionsEOS, idx)
	channel := transport.MappedChannel(topic)
	w := &TransactionsEOSWorker{
		BaseWorker: NewBaseWorker(config.WorkerTransactionsEOS, channel, idx, cfg, logger),
		topic:      topic,
		seen:       make(map[string]struct{}),
	}
	w.group = w.groupID("eos")
	w.txnID = cfg.Broker.ClientIDPrefix + "-" + channel + "-txn"
	return w
}

// txnClientCfg returns a transactional-producer client config. Idempotence +
// acks=all are mandatory for EOS, so acks is forced to "all".
func (w *TransactionsEOSWorker) txnClientCfg() transport.KafkaClientConfig {
	cfg := w.kafkaClientCfg("txn")
	cfg.Acks = "all"
	cfg.Idempotent = true
	cfg.TransactionalID = w.txnID
	cfg.TxnTimeoutMS = w.cfg.Kafka.TxnTimeoutMS
	return cfg
}

// Start creates the topic and brings up the read_committed consumer.
func (w *TransactionsEOSWorker) Start(ctx context.Context) error {
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
	return nil
}

func (w *TransactionsEOSWorker) consumeLoop(ctx context.Context) {
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
			cfg.IsolationLevel = "read_committed" // aborted records must be filtered
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

func (w *TransactionsEOSWorker) handleRecord(r *kgo.Record) {
	w.recordReceived(len(r.Value))
	producerID, seq, crcHex, sentAt, ok := extractMeta(r.Headers)
	outcome := headerValue(r.Headers, txnOutcomeHeader)

	// A read_committed consumer must NEVER see an aborted record: any such delivery
	// is an EOS violation.
	if outcome == "abort" {
		w.recordEOSViolation()
		return
	}
	if !ok {
		return
	}
	if crcHex != "" && !payload.VerifyCRC(r.Value, crcHex) {
		w.recordCorrupted()
	}
	if !sentAt.IsZero() {
		w.recordLatency(time.Since(sentAt))
	}

	// Exactly-once check for committed records. A first delivery advances the loss
	// tracker; a duplicate under EOS is the KIP-890 V1 same-epoch residual —
	// RECORDED but EXCLUDED from the dup counter and from eosViolations (spec §2.5).
	key := producerID + "|" + fmt.Sprint(seq)
	w.mu.Lock()
	if _, dup := w.seen[key]; dup {
		w.mu.Unlock()
		w.recordKIP890Residual()
		return
	}
	w.seen[key] = struct{}{}
	w.mu.Unlock()
	w.trk.Record(producerID, seq) // loss detection only (first-seen => never dup)
}

// StartProducers launches the transactional produce loop.
func (w *TransactionsEOSWorker) StartProducers() {
	w.producerCtx, w.producerCancel = context.WithCancel(context.Background())
	w.producerWG.Add(1)
	go func() { defer w.producerWG.Done(); w.produceLoop(w.producerCtx) }()
}

func (w *TransactionsEOSWorker) produceLoop(ctx context.Context) {
	committedPID := w.channelName + "-txn"
	abortPID := w.channelName + "-abort"
	for {
		if ctx.Err() != nil {
			return
		}
		if w.txnCli == nil {
			c, err := transport.NewClient(w.txnClientCfg())
			if err != nil {
				w.recordError("txn_failure")
				if !sleepCtx(ctx, time.Second) {
					return
				}
				continue
			}
			w.txnCli = c
		}

		commit := w.cycle.Add(1)%2 == 0

		if err := w.txnCli.BeginTransaction(); err != nil {
			if ctx.Err() != nil {
				return
			}
			w.recordError("txn_failure")
			w.txnCli.Close()
			w.txnCli = nil
			continue
		}

		bodies := make([]int, 0, txnRecordsPerCycle)
		produceErr := false
		for k := 0; k < txnRecordsPerCycle; k++ {
			if err := w.waitForRate(ctx); err != nil {
				produceErr = true
				break
			}
			var seq uint64
			var pid, outcome string
			if commit {
				seq = w.committedSeq.Add(1)
				pid = committedPID
				outcome = "commit"
			} else {
				seq = w.abortSeq.Add(1)
				pid = abortPID
				outcome = "abort"
			}
			body, crcHex := payload.Build(w.selectMessageSize())
			headers := append(stampHeaders(pid, seq, crcHex), kgo.RecordHeader{Key: txnOutcomeHeader, Value: []byte(outcome)})
			rec := &kgo.Record{Topic: w.topic, Value: body, Headers: headers}
			if err := w.txnCli.ProduceSync(ctx, rec).FirstErr(); err != nil {
				produceErr = true
				break
			}
			bodies = append(bodies, len(body))
		}

		// Decide the end disposition. A produce error forces an abort.
		end := kgo.TryCommit
		if !commit || produceErr {
			end = kgo.TryAbort
		}
		if err := w.txnCli.EndTransaction(ctx, end); err != nil {
			if ctx.Err() != nil {
				return
			}
			w.recordError("txn_failure")
			w.txnCli.Close()
			w.txnCli = nil
			continue
		}

		// Only records in a SUCCESSFULLY committed transaction count as sent (the
		// exactly-once delivery denominator). Aborted records are never delivered.
		if end == kgo.TryCommit && !produceErr {
			for _, n := range bodies {
				metricObserveSend(w.name, 0)
				w.recordSent(n)
			}
		}
		if ctx.Err() != nil {
			return
		}
	}
}

// StopProducers stops the produce loop and closes the transactional client.
func (w *TransactionsEOSWorker) StopProducers() {
	w.BaseWorker.StopProducers()
	if w.txnCli != nil {
		w.txnCli.Close()
		w.txnCli = nil
	}
}

// headerValue returns the string value of the named record header, or "".
func headerValue(headers []kgo.RecordHeader, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
