// Package engine drives the burn-in run lifecycle (warmup → measure → drain →
// verdict), grouping the per-channel workers of each of the six Kafka worker
// types. Mirrors kubemq-aws/burnin/engine recast for Kafka workers.
package engine

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/kubemq-io/kubemq-kafka/burnin/config"
	"github.com/kubemq-io/kubemq-kafka/burnin/worker"
)

// WorkerGroup holds all channel instances of one worker type.
type WorkerGroup struct {
	name    string
	workers []worker.Worker
}

// NewWorkerGroup builds the channel instances for a worker type.
func NewWorkerGroup(name string, cfg *config.Config, logger *slog.Logger) *WorkerGroup {
	numChannels := cfg.GetWorkerChannels(name)
	workers := make([]worker.Worker, 0, numChannels)
	for i := 1; i <= numChannels; i++ {
		switch name {
		case config.WorkerProduceRoundTrip:
			workers = append(workers, worker.NewProduceRoundTripWorker(cfg, i, logger))
		case config.WorkerKeyedOrdering:
			workers = append(workers, worker.NewKeyedOrderingWorker(cfg, i, logger))
		case config.WorkerConsumerGroup:
			workers = append(workers, worker.NewConsumerGroupWorker(cfg, i, logger))
		case config.WorkerOffsetCommitLag:
			workers = append(workers, worker.NewOffsetCommitLagWorker(cfg, i, logger))
		case config.WorkerAdminTopicChurn:
			workers = append(workers, worker.NewAdminTopicChurnWorker(cfg, i, logger))
		case config.WorkerTransactionsEOS:
			workers = append(workers, worker.NewTransactionsEOSWorker(cfg, i, logger))
		}
	}
	return &WorkerGroup{name: name, workers: workers}
}

// StartConsumers provisions topics and starts the consumer side of every worker.
func (g *WorkerGroup) StartConsumers(ctx context.Context) error {
	for _, w := range g.workers {
		if err := w.Start(ctx); err != nil {
			return fmt.Errorf("start consumer for %s/%s: %w", g.name, w.ChannelName(), err)
		}
	}
	return nil
}

// WaitForConsumerReady blocks until every worker signals ready or times out.
func (g *WorkerGroup) WaitForConsumerReady(timeout time.Duration) error {
	for _, w := range g.workers {
		select {
		case <-w.ConsumerReady():
		case <-time.After(timeout):
			return fmt.Errorf("consumer ready timeout for %s/%s", g.name, w.ChannelName())
		}
	}
	return nil
}

// StartProducers starts the producer side of every worker.
func (g *WorkerGroup) StartProducers() {
	for _, w := range g.workers {
		w.StartProducers()
	}
}

// StopProducers stops the producer side of every worker.
func (g *WorkerGroup) StopProducers() {
	for _, w := range g.workers {
		w.StopProducers()
	}
}

// StopConsumers stops the consumer side of every worker.
func (g *WorkerGroup) StopConsumers() {
	for _, w := range g.workers {
		w.StopConsumers()
	}
}

// DisconnectConsumers force-recreates consumer clients for every worker.
func (g *WorkerGroup) DisconnectConsumers() {
	for _, w := range g.workers {
		w.DisconnectConsumers()
	}
}

// Workers returns the worker slice.
func (g *WorkerGroup) Workers() []worker.Worker { return g.workers }

// Name returns the worker type name.
func (g *WorkerGroup) Name() string { return g.name }
