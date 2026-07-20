// Command commit-and-lag is master-table variant 7: offset commit + resume + lag
// on the KubeMQ Kafka connector.
//
//	CreateTopic -> produce a batch
//	  -> consumer #1 (manual commit): read HALF the batch, commit those offsets, close
//	  -> consumer #2 (same group): resume — must START at the committed offset and
//	     read ONLY the second half (no re-read of the committed records)
//	  -> lag = end-offset - committed-offset, computed client-side from
//	     ListEndOffsets and FetchOffsets (the connector also exposes it as the
//	     kubemq_kafka_consumer_group_lag metric)
//
// OffsetCommit (key 8) / OffsetFetch (key 9) are driven by CommitRecords and the
// group's committed-offset store. Auto-commit is disabled so the commit point is
// explicit and deterministic.
//
// Run:
//
//	export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
//	go run ./consumer-groups/commit-and-lag
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kubemq-io/kubemq-kafka-examples/go/internal/kafkaclient"
)

const batch = 10

func main() {
	kafkaclient.Banner("consumer-groups/commit-and-lag")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	topic := kafkaclient.Topic("cg", "commit")
	group := kafkaclient.Topic("cg", "cgrp")

	adm, admCl, err := kafkaclient.Admin()
	if err != nil {
		log.Fatalf("admin client: %v", err)
	}
	defer admCl.Close()
	if _, err := adm.CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		log.Fatalf("CreateTopic %s: %v", topic, err)
	}
	fmt.Printf("CreateTopic: %s (partitions=1) group=%s\n", topic, group)

	// Produce a batch of `batch` records on a single partition.
	prod, err := kafkaclient.New(kgo.DefaultProduceTopic(topic), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		log.Fatalf("producer: %v", err)
	}
	for i := 0; i < batch; i++ {
		if err := prod.ProduceSync(ctx, &kgo.Record{Value: []byte(fmt.Sprintf("m-%d", i))}).FirstErr(); err != nil {
			prod.Close()
			log.Fatalf("Produce %d: %v", i, err)
		}
	}
	prod.Close()
	fmt.Printf("Produce: %d records\n", batch)

	// Consumer #1: read exactly the first half, commit, then close.
	c1, err := kafkaclient.New(
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(1*time.Second),
	)
	if err != nil {
		log.Fatalf("consumer #1: %v", err)
	}
	half := batch / 2
	firstHalf := take(ctx, c1, half)
	if len(firstHalf) != half {
		c1.Close()
		log.Fatalf("FAIL: consumer #1 read %d records, want %d", len(firstHalf), half)
	}
	if err := c1.CommitRecords(ctx, firstHalf...); err != nil {
		c1.Close()
		log.Fatalf("CommitRecords: %v", err)
	}
	lastCommitted := firstHalf[len(firstHalf)-1].Offset
	fmt.Printf("Consumer #1: read + committed first %d records (through offset %d)\n", half, lastCommitted)
	c1.Close()

	// Lag right after committing half: end - (committed+1) == batch - half.
	committedNext, endOffset := committedAndEnd(ctx, adm, group, topic)
	lag := endOffset - committedNext
	fmt.Printf("Lag: end=%d committed=%d lag=%d\n", endOffset, committedNext, lag)
	if lag != int64(batch-half) {
		log.Fatalf("FAIL: lag %d, want %d", lag, batch-half)
	}

	// Consumer #2 (same group): resumes at the committed offset and reads ONLY the
	// second half — none of the committed records are re-read.
	c2, err := kafkaclient.New(
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // reset is ignored: committed offset wins
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(1*time.Second),
	)
	if err != nil {
		log.Fatalf("consumer #2: %v", err)
	}
	defer c2.Close()
	secondHalf := take(ctx, c2, batch-half)
	if len(secondHalf) != batch-half {
		log.Fatalf("FAIL: consumer #2 read %d records, want %d", len(secondHalf), batch-half)
	}
	if secondHalf[0].Offset != lastCommitted+1 {
		log.Fatalf("FAIL: consumer #2 resumed at offset %d, want %d (committed+1)",
			secondHalf[0].Offset, lastCommitted+1)
	}
	fmt.Printf("Consumer #2: resumed at offset %d, read the remaining %d records (no re-read)\n",
		secondHalf[0].Offset, len(secondHalf))

	if _, err := adm.DeleteTopics(ctx, topic); err != nil {
		log.Printf("warning: DeleteTopics: %v", err)
	}
	fmt.Println("DeleteTopic: ok")
	fmt.Println("PASS: commit + resume-from-committed + lag verified")
}

// take polls until exactly n records are collected.
func take(ctx context.Context, cl *kgo.Client, n int) []*kgo.Record {
	var out []*kgo.Record
	deadline := time.Now().Add(20 * time.Second)
	for len(out) < n && time.Now().Before(deadline) {
		fs := cl.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			log.Fatalf("PollFetches: %v", errs[0].Err)
		}
		for it := fs.RecordIter(); !it.Done() && len(out) < n; {
			out = append(out, it.Next())
		}
	}
	return out
}

// committedAndEnd returns the group's next-to-consume committed offset and the log
// end offset (HWM) for the single-partition topic.
func committedAndEnd(ctx context.Context, adm *kadm.Client, group, topic string) (committed, end int64) {
	fetched, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		log.Fatalf("FetchOffsets: %v", err)
	}
	co, ok := fetched.Lookup(topic, 0)
	if !ok || co.Err != nil {
		log.Fatalf("FAIL: no committed offset for group %s (ok=%v err=%v)", group, ok, co.Err)
	}

	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		log.Fatalf("ListEndOffsets: %v", err)
	}
	eo, ok := ends.Lookup(topic, 0)
	if !ok || eo.Err != nil {
		log.Fatalf("FAIL: no end offset for %s (ok=%v err=%v)", topic, ok, eo.Err)
	}
	return co.At, eo.Offset
}
