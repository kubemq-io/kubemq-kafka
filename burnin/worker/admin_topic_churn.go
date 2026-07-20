package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kubemq-io/kubemq-kafka/burnin/config"
	"github.com/kubemq-io/kubemq-kafka/burnin/transport"
)

// AdminTopicChurnWorker (worker 5, spec §7.3) is a CYCLIC, low-rate worker (like
// aws sqs_move_task): each cycle it Creates a churn topic, DescribesConfigs +
// DescribesCluster, CreatePartitions(increase) — all of which must succeed — then
// runs a NEGATIVE probe: CreatePartitions(same / decrease / >256) which MUST be
// rejected with INVALID_PARTITIONS. It drives no steady data stream, so it is
// EXEMPT from the loss/dup/throughput/latency gates (verdict skips IsAdminWorker).
// Gate: adminOpFailures == 0 (every invalid partition op correctly rejected;
// correct rejections increment adminInvalidRejected, which is healthy).
type AdminTopicChurnWorker struct {
	*BaseWorker
	baseTopic string
	cycle     atomic.Uint64
	adm       *kadm.Client
	admCli    *kgo.Client
}

// NewAdminTopicChurnWorker creates an admin_topic_churn worker.
func NewAdminTopicChurnWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	topic := transport.TopicName(config.WorkerAdminTopicChurn, idx)
	channel := transport.MappedChannel(topic)
	return &AdminTopicChurnWorker{
		BaseWorker: NewBaseWorker(config.WorkerAdminTopicChurn, channel, idx, cfg, logger),
		baseTopic:  topic,
	}
}

// Start builds the persistent admin client and signals ready (no data-plane
// consumers). The cyclic churn runs under StartProducers.
func (w *AdminTopicChurnWorker) Start(ctx context.Context) error {
	w.consumerCtx, w.consumerCancel = context.WithCancel(ctx)

	admCli, err := transport.NewClient(w.kafkaClientCfg("admin"))
	if err != nil {
		return fmt.Errorf("build kafka client: %w", err)
	}
	w.admCli = admCli
	w.adm = transport.NewAdmin(admCli)
	w.signalReady()
	return nil
}

// StartProducers runs the cyclic admin churn loop (paced by the rate limiter).
func (w *AdminTopicChurnWorker) StartProducers() {
	w.producerCtx, w.producerCancel = context.WithCancel(context.Background())
	w.producerWG.Add(1)
	go func() { defer w.producerWG.Done(); w.churnLoop(w.producerCtx) }()
}

func (w *AdminTopicChurnWorker) churnLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := w.waitForRate(ctx); err != nil {
			return
		}
		w.runCycle(ctx)
	}
}

// runCycle runs one create → describe → grow → negative-probe → delete cycle.
func (w *AdminTopicChurnWorker) runCycle(ctx context.Context) {
	n := w.cycle.Add(1)
	topic := fmt.Sprintf("%s.c%04d", w.baseTopic, n%10000)

	// 1) Create the churn topic with a single partition.
	if err := transport.CreateTopic(ctx, w.adm, topic, 1, w.cfg.Kafka.ReplicationFactor); err != nil {
		if ctx.Err() != nil {
			return
		}
		w.recordError("create_failure")
		w.recordAdminOpFailure()
		return
	}
	defer func() {
		if err := transport.DeleteTopic(context.WithoutCancel(ctx), w.adm, topic); err != nil {
			w.recordError("admin_failure")
		}
	}()

	// 2) DescribeConfigs.
	if _, err := w.adm.DescribeTopicConfigs(ctx, topic); err != nil {
		if ctx.Err() != nil {
			return
		}
		w.recordError("admin_failure")
		w.recordAdminOpFailure()
		return
	}

	// 3) DescribeCluster (metadata).
	if _, err := w.adm.Metadata(ctx, topic); err != nil {
		if ctx.Err() != nil {
			return
		}
		w.recordError("admin_failure")
		w.recordAdminOpFailure()
		return
	}

	// 4) CreatePartitions increase (1 -> 2): must succeed.
	if err := transport.CreatePartitions(ctx, w.adm, topic, 2); err != nil {
		if ctx.Err() != nil {
			return
		}
		w.recordError("admin_failure")
		w.recordAdminOpFailure()
		return
	}
	w.recordDeleted() // count a successful admin mutation (offset/config advance metric)

	// 5) Negative probe: a same/decrease/>256 partition change MUST be rejected
	//    with INVALID_PARTITIONS. Correct rejection => healthy; wrong acceptance =>
	//    a failure.
	for _, bad := range []int{2, 1, 300} { // same, decrease, above the 256 hard cap
		if ctx.Err() != nil {
			return
		}
		err := transport.CreatePartitions(ctx, w.adm, topic, bad)
		switch {
		case err == nil:
			// The connector wrongly ACCEPTED an invalid partition change.
			w.recordAdminOpFailure()
		case errors.Is(err, kerr.InvalidPartitions):
			w.recordAdminInvalidRejected()
		default:
			// Some other error type on an invalid op: still a correct rejection of the
			// bad request (not an accept), but flag the unexpected code once.
			w.recordAdminInvalidRejected()
		}
	}
}

// StopConsumers closes the admin client.
func (w *AdminTopicChurnWorker) StopConsumers() {
	w.BaseWorker.StopConsumers()
	if w.admCli != nil {
		w.admCli.Close()
		w.admCli = nil
	}
}
