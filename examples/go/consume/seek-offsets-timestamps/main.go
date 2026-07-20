// Command seek-offsets-timestamps is master-table variant 5: explicit seeking on
// the KubeMQ Kafka connector — both by absolute offset and by timestamp.
//
//	CreateTopic -> produce a batch of records (each with a known offset + timestamp)
//	  -> SetOffsets to offset N: the next fetch delivers the record AT N first
//	  -> ListOffsetsAfterMilli(ts): resolve the first offset with timestamp >= ts,
//	     seek there, assert the delivered record is the one produced at/after ts
//
// franz-go seeks a live consumer with SetOffsets(map[topic]map[partition]EpochOffset)
// or by (re)subscribing partitions at kgo.NewOffset().At(n). By-timestamp lookup is
// the ListOffsets request (key 2), exposed on kadm as ListOffsetsAfterMilli.
//
// Run:
//
//	export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
//	go run ./consume/seek-offsets-timestamps
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
	kafkaclient.Banner("consume/seek-offsets-timestamps")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	topic := kafkaclient.Topic("consume", "seek")

	adm, admCl, err := kafkaclient.Admin()
	if err != nil {
		log.Fatalf("admin client: %v", err)
	}
	defer admCl.Close()
	if _, err := adm.CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		log.Fatalf("CreateTopic %s: %v", topic, err)
	}
	fmt.Printf("CreateTopic: %s (partitions=1)\n", topic)

	// Produce 6 records; capture the timestamp boundary in the middle so the
	// by-timestamp seek has a meaningful cut point.
	prod, err := kafkaclient.New(
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		log.Fatalf("producer: %v", err)
	}
	const total = 6
	var boundaryMilli int64
	for i := 0; i < total; i++ {
		if i == total/2 {
			time.Sleep(50 * time.Millisecond)
			boundaryMilli = time.Now().UnixMilli()
			time.Sleep(50 * time.Millisecond)
		}
		if err := prod.ProduceSync(ctx, &kgo.Record{
			Value: []byte(fmt.Sprintf("rec-%d", i)),
		}).FirstErr(); err != nil {
			prod.Close()
			log.Fatalf("Produce %d: %v", i, err)
		}
	}
	prod.Close()
	fmt.Printf("Produce: %d records, timestamp boundary between rec-2 and rec-3\n", total)

	// 1. Seek by absolute offset: assign partition 0 directly at offset 4 and expect
	//    rec-4 first. Seeking is done by (re)subscribing the partition at an explicit
	//    offset via ConsumePartitions + kgo.NewOffset().At(n). (SetOffsets is the OTHER
	//    franz-go seek path, but it only moves partitions a consumer is ALREADY
	//    consuming — calling it before the first PollFetches has assigned the partition
	//    is a no-op, so the consumer would fall back to its reset position at offset 0.)
	const seekOffset = 4
	seeker, err := kafkaclient.New(
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().At(seekOffset)},
		}),
		kgo.FetchMaxWait(2*time.Second),
	)
	if err != nil {
		log.Fatalf("seek consumer: %v", err)
	}
	defer seeker.Close()
	first := pollFirst(ctx, seeker)
	if first.Offset != seekOffset {
		log.Fatalf("FAIL: seek to offset %d delivered offset %d", seekOffset, first.Offset)
	}
	fmt.Printf("seek-to-offset(%d): first delivered record offset=%d value=%q\n",
		seekOffset, first.Offset, first.Value)

	// 2. Seek by timestamp: resolve the first offset with ts >= boundary via
	//    ListOffsetsAfterMilli, then read that record.
	listed, err := adm.ListOffsetsAfterMilli(ctx, boundaryMilli, topic)
	if err != nil {
		log.Fatalf("ListOffsetsAfterMilli: %v", err)
	}
	lo, ok := listed.Lookup(topic, 0)
	if !ok || lo.Err != nil {
		log.Fatalf("FAIL: no offset resolved for ts=%d (ok=%v err=%v)", boundaryMilli, ok, lo.Err)
	}
	fmt.Printf("ListOffsets(by-ts=%d): resolved offset=%d\n", boundaryMilli, lo.Offset)
	// The boundary was set AFTER rec-2 and BEFORE rec-3, so the first record at/after
	// it must be offset 3.
	if lo.Offset != 3 {
		log.Fatalf("FAIL: by-timestamp offset %d, want 3 (first record after the boundary)", lo.Offset)
	}

	tsSeeker, err := kafkaclient.New(
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().At(lo.Offset)},
		}),
		kgo.FetchMaxWait(2*time.Second),
	)
	if err != nil {
		log.Fatalf("ts seek consumer: %v", err)
	}
	defer tsSeeker.Close()
	tsFirst := pollFirst(ctx, tsSeeker)
	if tsFirst.Offset != lo.Offset || string(tsFirst.Value) != "rec-3" {
		log.Fatalf("FAIL: by-ts seek delivered offset=%d value=%q, want offset=%d value=rec-3",
			tsFirst.Offset, tsFirst.Value, lo.Offset)
	}
	fmt.Printf("by-timestamp seek: first delivered record offset=%d value=%q\n",
		tsFirst.Offset, tsFirst.Value)

	if _, err := adm.DeleteTopics(ctx, topic); err != nil {
		log.Printf("warning: DeleteTopics: %v", err)
	}
	fmt.Println("DeleteTopic: ok")
	fmt.Println("PASS: seek by offset + seek by timestamp verified")
}

// pollFirst returns the first record delivered after a seek. Each poll is bounded
// by its own short context: franz-go's PollFetches blocks until records arrive OR
// the context is cancelled (an empty long-poll never returns on its own), so we use
// a per-poll deadline as the "nothing this round" signal and retry until the outer
// budget elapses.
func pollFirst(ctx context.Context, cl *kgo.Client) *kgo.Record {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		pollCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		fs := cl.PollFetches(pollCtx)
		cancel()
		if errs := fs.Errors(); len(errs) > 0 {
			for _, e := range errs {
				// A per-poll deadline just means no records arrived this round.
				if errors.Is(e.Err, context.DeadlineExceeded) || errors.Is(e.Err, context.Canceled) {
					continue
				}
				log.Fatalf("PollFetches: %v", e.Err)
			}
			continue
		}
		for it := fs.RecordIter(); !it.Done(); {
			return it.Next()
		}
	}
	log.Fatalf("FAIL: timed out waiting for a record after seek")
	return nil
}
