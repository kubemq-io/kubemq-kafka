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

// KeyedOrderingWorker (worker 2, spec §7.3) produces keyed records (franz-go's
// default murmur2 partitioner — the server's own client, gotcha #4) and consumes
// PER PARTITION, asserting each partition's offsets are strictly monotonic. A
// record whose offset is ≤ the last seen offset for its (topic,partition) is an
// offset-order violation. Invariant: max_offset_order_violations 0.
type KeyedOrderingWorker struct {
	*BaseWorker
	topic   string
	group   string
	seq     atomic.Uint64
	prodCli *kgo.Client

	mu         sync.Mutex
	lastOffset map[int32]int64 // per-partition last delivered offset
}

// NewKeyedOrderingWorker creates a keyed_ordering worker.
func NewKeyedOrderingWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	topic := transport.TopicName(config.WorkerKeyedOrdering, idx)
	channel := transport.MappedChannel(topic)
	w := &KeyedOrderingWorker{
		BaseWorker: NewBaseWorker(config.WorkerKeyedOrdering, channel, idx, cfg, logger),
		topic:      topic,
		lastOffset: make(map[int32]int64),
	}
	w.group = w.groupID("")
	return w
}

// Start creates the topic and brings up the per-partition consumer.
func (w *KeyedOrderingWorker) Start(ctx context.Context) error {
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

func (w *KeyedOrderingWorker) consumeLoop(ctx context.Context, tag string) {
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
		// Consume per partition so per-partition offset monotonicity can be checked.
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			partition := p.Partition
			p.EachRecord(func(r *kgo.Record) { w.handleRecord(partition, r) })
		})
	}
}

func (w *KeyedOrderingWorker) handleRecord(partition int32, r *kgo.Record) {
	w.recordReceived(len(r.Value))

	// Per-partition monotonic-offset check.
	w.mu.Lock()
	last, seen := w.lastOffset[partition]
	if seen && r.Offset <= last {
		w.recordOffsetOrderViolation()
	}
	if !seen || r.Offset > last {
		w.lastOffset[partition] = r.Offset
	}
	w.mu.Unlock()

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

// StartProducers launches the keyed Produce loop.
func (w *KeyedOrderingWorker) StartProducers() {
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

func (w *KeyedOrderingWorker) produceLoop(ctx context.Context, pi int) {
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
		// Cycle a small set of keys so records spread across partitions via murmur2;
		// every key still maps deterministically to one partition, so per-partition
		// order is well defined.
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
func (w *KeyedOrderingWorker) StopProducers() {
	w.BaseWorker.StopProducers()
	if w.prodCli != nil {
		w.prodCli.Close()
		w.prodCli = nil
	}
}
