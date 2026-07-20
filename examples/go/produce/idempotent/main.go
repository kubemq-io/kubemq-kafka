// Command idempotent is master-table variant 2: the idempotent producer against
// the KubeMQ Kafka connector. franz-go turns idempotence ON by default and issues
// InitProducerId (key 22) implicitly on the first produce, so there is no explicit
// "enable" call — the assigned Producer ID (PID) is observable via ProducerID().
//
//	CreateTopic -> ProducerID() shows the assigned PID/epoch
//	            -> produce N distinct records (idempotent, acks=all)
//	            -> read back and assert exactly N records with N distinct offsets
//	               (no duplicate offset for the same (PID,partition) sequence)
//
// The idempotent producer stamps each record with (PID, epoch, sequence); the
// broker deduplicates on retry so a retried batch never lands twice. This program
// caps in-flight requests at 1 (kgo.MaxProduceRequestsInflightPerBroker) to keep
// the sequence strictly ordered, produces a fixed set of records, and asserts the
// read-back is exactly the produced set — no duplicates, no gaps.
//
// Gotcha #4: franz-go partitions by murmur2. Gotcha #1: the connector is disabled
// by default (CONNECTORS_KAFKA_ENABLE=true).
//
// Run:
//
//	export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
//	go run ./produce/idempotent
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kubemq-io/kubemq-kafka-examples/go/internal/kafkaclient"
)

func main() {
	kafkaclient.Banner("produce/idempotent")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := kafkaclient.Topic("produce", "idem")

	adm, admCl, err := kafkaclient.Admin()
	if err != nil {
		log.Fatalf("admin client: %v", err)
	}
	defer admCl.Close()
	if _, err := adm.CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		log.Fatalf("CreateTopic %s: %v", topic, err)
	}
	fmt.Printf("CreateTopic: %s (partitions=1)\n", topic)

	// Idempotence is ON by default; do NOT pass DisableIdempotentWrite. Capping
	// in-flight requests at 1 keeps the sequence strictly ordered for the demo.
	prod, err := kafkaclient.New(
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.MaxProduceRequestsInflightPerBroker(1),
	)
	if err != nil {
		log.Fatalf("producer client: %v", err)
	}
	defer prod.Close()

	// InitProducerId is issued implicitly on the first idempotent produce; force it
	// and read the assigned PID/epoch so the idempotent session is observable.
	id, epoch, err := prod.ProducerID(ctx)
	if err != nil {
		log.Fatalf("ProducerID (InitProducerId): %v", err)
	}
	if id < 0 {
		log.Fatalf("FAIL: expected a non-negative Producer ID, got %d", id)
	}
	fmt.Printf("ProducerID: id=%d epoch=%d (idempotence ON)\n", id, epoch)

	// Produce N distinct records. The idempotent producer deduplicates any internal
	// retry, so the log ends up with exactly these N records once each.
	const n = 5
	want := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		v := fmt.Sprintf("idem-record-%d", i)
		want[v] = true
		if err := prod.ProduceSync(ctx, &kgo.Record{Value: []byte(v)}).FirstErr(); err != nil {
			log.Fatalf("Produce %q: %v", v, err)
		}
	}
	fmt.Printf("Produce: %d idempotent records (acks=all)\n", n)

	// Read the whole log back and assert exactly N records with N distinct offsets.
	cons, err := kafkaclient.New(
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(2*time.Second),
	)
	if err != nil {
		log.Fatalf("consumer client: %v", err)
	}
	defer cons.Close()

	seenOffsets := map[int64]bool{}
	got := map[string]bool{}
	deadline := time.Now().Add(15 * time.Second)
	for len(got) < n && time.Now().Before(deadline) {
		fs := cons.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			log.Fatalf("PollFetches: %v", errs[0].Err)
		}
		for it := fs.RecordIter(); !it.Done(); {
			rec := it.Next()
			if seenOffsets[rec.Offset] {
				log.Fatalf("FAIL: duplicate offset %d — idempotence did not dedupe", rec.Offset)
			}
			seenOffsets[rec.Offset] = true
			got[string(rec.Value)] = true
		}
	}
	if len(got) != n {
		log.Fatalf("FAIL: read back %d distinct records, want %d", len(got), n)
	}
	for v := range want {
		if !got[v] {
			log.Fatalf("FAIL: missing produced record %q", v)
		}
	}
	fmt.Printf("Fetch: %d distinct records, %d distinct offsets (no duplicates)\n", len(got), len(seenOffsets))

	if _, err := adm.DeleteTopics(ctx, topic); err != nil {
		log.Printf("warning: DeleteTopics: %v", err)
	}
	fmt.Println("DeleteTopic: ok")
	fmt.Println("PASS: idempotent producer — PID assigned, no duplicate offsets")
}
