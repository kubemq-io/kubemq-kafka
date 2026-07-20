// Command basic-acks is master-table variant 1: the Produce round-trip against
// the KubeMQ Kafka connector across all three ack levels, plus the oversized
// -> MESSAGE_TOO_LARGE rejection.
//
//	CreateTopic -> Produce(acks=all) -> Fetch/read-back -> assert byte-equal
//	            -> Produce(acks=1) and Produce(acks=0) succeed
//	            -> Produce(>MaxMessageBytes) -> MESSAGE_TOO_LARGE
//
// acks is a client-level option in franz-go (kgo.RequiredAcks), so each ack level
// uses its own short-lived client. The connector caps records at
// CONNECTORS_KAFKA_MAX_MESSAGE_BYTES (default 1 MiB, §2.7); a larger record is
// rejected with the Kafka error MESSAGE_TOO_LARGE (code 10).
//
// Gotcha #3: on a MULTI-NODE deployment acks=0 against a follower can silently
// drop — always use acks>=1 for durability. Gotcha #1: the connector is disabled
// by default (CONNECTORS_KAFKA_ENABLE=true). franz-go partitions by murmur2
// (gotcha #4).
//
// Mirrors connector test behavior in connectors/kafka/produce_test.go.
//
// Run:
//
//	export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"   # connector must be enabled
//	go run ./produce/basic-acks
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kubemq-io/kubemq-kafka-examples/go/internal/kafkaclient"
)

func main() {
	kafkaclient.Banner("produce/basic-acks")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := kafkaclient.Topic("produce", "acks")

	// 1. CreateTopic (1 partition) via kadm so read-back is deterministic.
	adm, admCl, err := kafkaclient.Admin()
	if err != nil {
		log.Fatalf("admin client: %v", err)
	}
	defer admCl.Close()
	if _, err := adm.CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		log.Fatalf("CreateTopic %s: %v", topic, err)
	}
	fmt.Printf("CreateTopic: %s (partitions=1)\n", topic)

	// 2. Produce one record with acks=all and read it back byte-for-byte.
	body := []byte("order #4242 — 3x widget, ship express")
	prod, err := kafkaclient.New(
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()), // acks=all (idempotence stays on by default)
	)
	if err != nil {
		log.Fatalf("producer client: %v", err)
	}
	res := prod.ProduceSync(ctx, &kgo.Record{Value: body})
	r, err := res.First()
	if err != nil {
		log.Fatalf("Produce acks=all: %v", err)
	}
	fmt.Printf("Produce(acks=all): partition=%d offset=%d\n", r.Partition, r.Offset)
	prod.Close()

	// 3. Read the record back from the start and assert the body round-trips.
	cons, err := kafkaclient.New(
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Fatalf("consumer client: %v", err)
	}
	defer cons.Close()
	got := pollOne(ctx, cons)
	fmt.Printf("Fetch: offset=%d value=%q\n", got.Offset, string(got.Value))
	if string(got.Value) != string(body) {
		log.Fatalf("FAIL: read-back %q != produced %q", got.Value, body)
	}

	// 4. acks=1 and acks=0 both accept a record (durability differs; gotcha #3).
	for _, lvl := range []struct {
		name string
		acks kgo.Acks
	}{
		{"acks=1", kgo.LeaderAck()},
		{"acks=0", kgo.NoAck()},
	} {
		// franz-go's idempotent producer is ON by default and REQUIRES acks=all,
		// so acks=1 / acks=0 MUST disable it or kgo.NewClient errors (mirrors §13.3).
		c, err := kafkaclient.New(
			kgo.DefaultProduceTopic(topic),
			kgo.RequiredAcks(lvl.acks),
			kgo.DisableIdempotentWrite(),
		)
		if err != nil {
			log.Fatalf("producer %s: %v", lvl.name, err)
		}
		// With acks=0 the broker sends no response, so ProduceSync returns once the
		// record is written to the wire; a transport error still surfaces.
		if err := c.ProduceSync(ctx, &kgo.Record{Value: []byte(lvl.name)}).FirstErr(); err != nil {
			c.Close()
			log.Fatalf("Produce %s: %v", lvl.name, err)
		}
		fmt.Printf("Produce(%s): ok\n", lvl.name)
		c.Close()
	}

	// 5. Oversized record -> MESSAGE_TOO_LARGE (connector MaxMessageBytes, §2.7).
	//    Raise the client batch ceiling above the connector limit so the oversized
	//    record actually REACHES the broker instead of being rejected client-side,
	//    and DISABLE compression: franz-go compresses batches by default (snappy), and
	//    an all-zero payload compresses far below the 1 MiB cap, so the broker would
	//    ACCEPT it. NoCompression keeps the on-wire size honest.
	//    1.5 MiB is above the 1 MiB connector cap but below the ~2 MiB request frame
	//    cap, so the record reaches the broker and is rejected with MESSAGE_TOO_LARGE.
	big := make([]byte, 1572864) // 1.5 MiB > 1 MiB connector cap, < 2 MiB frame cap
	bigProd, err := kafkaclient.New(
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(4*1024*1024),            // let the record leave the client
		kgo.ProducerBatchCompression(kgo.NoCompression()), // keep the on-wire size honest
	)
	if err != nil {
		log.Fatalf("oversized producer: %v", err)
	}
	err = bigProd.ProduceSync(ctx, &kgo.Record{Value: big}).FirstErr()
	bigProd.Close()
	if err == nil {
		log.Fatalf("FAIL: 1.5 MiB record was accepted; expected MESSAGE_TOO_LARGE")
	}
	if !errors.Is(err, kerr.MessageTooLarge) {
		log.Fatalf("FAIL: oversized rejected with %v, expected MESSAGE_TOO_LARGE", err)
	}
	fmt.Printf("Produce(1.5 MiB): rejected with %v (expected)\n", err)

	// Tear down so re-runs start clean.
	if _, err := adm.DeleteTopics(ctx, topic); err != nil {
		log.Printf("warning: DeleteTopics: %v", err)
	}
	fmt.Println("DeleteTopic: ok")
	fmt.Println("PASS: produce acks 0/1/all round-trip + MESSAGE_TOO_LARGE verified")
}

// pollOne blocks for a single record, failing the process on fetch error or timeout.
func pollOne(ctx context.Context, cl *kgo.Client) *kgo.Record {
	for {
		fs := cl.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			log.Fatalf("PollFetches: %v", errs[0].Err)
		}
		if fs.Empty() {
			if ctx.Err() != nil {
				log.Fatalf("FAIL: timed out waiting for the produced record")
			}
			continue
		}
		iter := fs.RecordIter()
		return iter.Next()
	}
}
