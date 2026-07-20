// Command from-beginning-latest is master-table variant 4: the two
// auto.offset.reset positions against the KubeMQ Kafka connector.
//
//	CreateTopic -> produce a SEED batch (pre-existing records)
//	  -> consumer A: ConsumeResetOffset(AtStart())  == auto.offset.reset=earliest
//	     reads ALL pre-existing records
//	  -> consumer B: ConsumeResetOffset(AtEnd())     == auto.offset.reset=latest
//	     reads ONLY records produced AFTER it subscribed
//
// franz-go's Fetch is a long-poll; kgo.FetchMaxWait bounds how long the broker
// holds an empty fetch open. AtStart()/AtEnd() are the reset positions used ONLY
// when the group/partition has no committed offset — exactly the semantics of the
// Kafka auto.offset.reset config.
//
// Run:
//
//	export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
//	go run ./consume/from-beginning-latest
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kubemq-io/kubemq-kafka-examples/go/internal/kafkaclient"
)

func main() {
	kafkaclient.Banner("consume/from-beginning-latest")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := kafkaclient.Topic("consume", "reset")

	adm, admCl, err := kafkaclient.Admin()
	if err != nil {
		log.Fatalf("admin client: %v", err)
	}
	defer admCl.Close()
	if _, err := adm.CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		log.Fatalf("CreateTopic %s: %v", topic, err)
	}
	fmt.Printf("CreateTopic: %s (partitions=1)\n", topic)

	// Seed batch: records that already exist before either consumer subscribes.
	const seedCount = 3
	produce(ctx, topic, seedRecords(seedCount)...)
	fmt.Printf("Seed: produced %d pre-existing records\n", seedCount)

	// auto.offset.reset=earliest: AtStart() reads all pre-existing records.
	early, err := kafkaclient.New(
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(2*time.Second),
	)
	if err != nil {
		log.Fatalf("earliest consumer: %v", err)
	}
	earlyRecs := drain(ctx, early, seedCount, 15*time.Second)
	early.Close()
	if len(earlyRecs) != seedCount {
		log.Fatalf("FAIL: earliest read %d records, want %d", len(earlyRecs), seedCount)
	}
	fmt.Printf("earliest (AtStart): read %d pre-existing records\n", len(earlyRecs))

	// auto.offset.reset=latest: AtEnd() ignores everything already in the log.
	late, err := kafkaclient.New(
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.FetchMaxWait(2*time.Second),
	)
	if err != nil {
		log.Fatalf("latest consumer: %v", err)
	}
	defer late.Close()
	// Force the latest consumer to establish its AtEnd position before producing
	// the post-subscribe record. franz-go's PollFetches blocks until records arrive
	// OR the context is cancelled (it does NOT return empty when FetchMaxWait elapses),
	// and there is nothing past the log end yet — so this warm-up poll MUST be bounded
	// by its own short context or it would hang. The bounded poll still drives the
	// assignment to the log end, which is all we need here.
	warmCtx, warmCancel := context.WithTimeout(ctx, 3*time.Second)
	late.PollFetches(warmCtx)
	warmCancel()

	post := []byte("post-subscribe record")
	produce(ctx, topic, &kgo.Record{Value: post})
	fmt.Println("Produce: 1 record AFTER the latest consumer subscribed")

	lateRecs := drain(ctx, late, 1, 15*time.Second)
	if len(lateRecs) != 1 {
		log.Fatalf("FAIL: latest read %d records, want exactly 1 (only the post-subscribe record)", len(lateRecs))
	}
	if string(lateRecs[0].Value) != string(post) {
		log.Fatalf("FAIL: latest read %q, want %q", lateRecs[0].Value, post)
	}
	fmt.Printf("latest (AtEnd): read only the post-subscribe record %q\n", lateRecs[0].Value)

	if _, err := adm.DeleteTopics(ctx, topic); err != nil {
		log.Printf("warning: DeleteTopics: %v", err)
	}
	fmt.Println("DeleteTopic: ok")
	fmt.Println("PASS: auto.offset.reset earliest/latest verified")
}

func seedRecords(n int) []*kgo.Record {
	out := make([]*kgo.Record, n)
	for i := 0; i < n; i++ {
		out[i] = &kgo.Record{Value: []byte(fmt.Sprintf("seed-%d", i))}
	}
	return out
}

func produce(ctx context.Context, topic string, recs ...*kgo.Record) {
	prod, err := kafkaclient.New(
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		log.Fatalf("producer: %v", err)
	}
	defer prod.Close()
	if err := prod.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		log.Fatalf("Produce: %v", err)
	}
}

// drain polls until it has want records or the timeout elapses. Each poll is
// bounded by its own short context: franz-go's PollFetches blocks until records
// arrive OR the context is cancelled (an empty fetch at the log end never returns
// on its own), so a per-poll deadline is how we notice "no more records" and let
// the outer timeout loop make progress instead of hanging.
func drain(ctx context.Context, cl *kgo.Client, want int, timeout time.Duration) []*kgo.Record {
	var out []*kgo.Record
	deadline := time.Now().Add(timeout)
	for len(out) < want && time.Now().Before(deadline) {
		pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		fs := cl.PollFetches(pollCtx)
		cancel()
		if errs := fs.Errors(); len(errs) > 0 {
			for _, e := range errs {
				// A per-poll deadline just means nothing arrived this round.
				if errors.Is(e.Err, context.DeadlineExceeded) || errors.Is(e.Err, context.Canceled) {
					continue
				}
				log.Fatalf("PollFetches: %v", e.Err)
			}
			continue
		}
		for it := fs.RecordIter(); !it.Done(); {
			out = append(out, it.Next())
		}
	}
	return out
}
