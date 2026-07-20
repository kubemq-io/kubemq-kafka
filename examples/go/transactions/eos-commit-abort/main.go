// Command eos-commit-abort is master-table variant 11: transactional produce
// (exactly-once semantics) against the KubeMQ Kafka connector — one committed and
// one aborted transaction, verified through a read_committed consumer.
//
//	InitProducerId (implicit via TransactionalID)
//	  -> BeginTransaction -> Produce(committed record) -> EndTransaction(TryCommit)
//	  -> BeginTransaction -> Produce(aborted   record) -> EndTransaction(TryAbort)
//	  -> read_committed consumer: sees the COMMITTED record, NEVER the aborted one
//
// EOS ceiling (gotcha #9, spec §2.5): the connector implements the KIP-890 V1
// transaction protocol. Exactly-once holds for the commit/abort visibility tested
// here; the residual same-epoch "zombie" edge that KIP-890 V2 closes is OUT of
// scope and is NOT a failure of this example. transactional.id must not contain
// '/' (gotcha #7) — kafkaclient.Topic() emits only [a-z0-9-].
//
// Traces to connector tests connectors/kafka/txn_rpcs_test.go / authz_txn_test.go.
//
// Run:
//
//	export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
//	go run ./transactions/eos-commit-abort
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
	kafkaclient.Banner("transactions/eos-commit-abort")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	topic := kafkaclient.Topic("txn", "eos")
	txnID := kafkaclient.Topic("txn", "id") // no '/' — safe transactional.id

	adm, admCl, err := kafkaclient.Admin()
	if err != nil {
		log.Fatalf("admin client: %v", err)
	}
	defer admCl.Close()
	if _, err := adm.CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		log.Fatalf("CreateTopic %s: %v", topic, err)
	}
	fmt.Printf("CreateTopic: %s (partitions=1) txn.id=%s\n", topic, txnID)

	prod, err := kafkaclient.New(
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.TransactionalID(txnID),
		kgo.TransactionTimeout(60*time.Second),
	)
	if err != nil {
		log.Fatalf("transactional producer: %v", err)
	}
	defer prod.Close()

	// InitProducerId is issued when the transactional producer initializes; read the
	// assigned PID so the transactional session is observable.
	id, epoch, err := prod.ProducerID(ctx)
	if err != nil {
		log.Fatalf("ProducerID (InitProducerId): %v", err)
	}
	fmt.Printf("InitProducerId: pid=%d epoch=%d\n", id, epoch)

	// Transaction #1: produce a record and COMMIT it.
	committed := []byte("committed-order-1")
	if err := prod.BeginTransaction(); err != nil {
		log.Fatalf("BeginTransaction #1: %v", err)
	}
	if err := prod.ProduceSync(ctx, &kgo.Record{Value: committed}).FirstErr(); err != nil {
		log.Fatalf("Produce (committed): %v", err)
	}
	if err := prod.EndTransaction(ctx, kgo.TryCommit); err != nil {
		log.Fatalf("EndTransaction commit: %v", err)
	}
	fmt.Printf("Txn #1: produced %q, EndTransaction(commit)\n", committed)

	// Transaction #2: produce a record and ABORT it.
	aborted := []byte("aborted-order-2")
	if err := prod.BeginTransaction(); err != nil {
		log.Fatalf("BeginTransaction #2: %v", err)
	}
	if err := prod.ProduceSync(ctx, &kgo.Record{Value: aborted}).FirstErr(); err != nil {
		log.Fatalf("Produce (aborted): %v", err)
	}
	if err := prod.EndTransaction(ctx, kgo.TryAbort); err != nil {
		log.Fatalf("EndTransaction abort: %v", err)
	}
	fmt.Printf("Txn #2: produced %q, EndTransaction(abort)\n", aborted)

	// read_committed consumer: must see ONLY the committed record.
	cons, err := kafkaclient.New(
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.FetchMaxWait(2*time.Second),
	)
	if err != nil {
		log.Fatalf("read_committed consumer: %v", err)
	}
	defer cons.Close()

	var got [][]byte
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		// Bound each poll: franz-go's PollFetches blocks until records arrive OR the
		// context is cancelled (it never returns empty on FetchMaxWait alone), so a
		// per-poll deadline is our "nothing more to read" signal. Without it, the poll
		// after the committed record is delivered would hang waiting for more records.
		pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		fs := cons.PollFetches(pollCtx)
		cancel()
		if errs := fs.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.DeadlineExceeded) || errors.Is(e.Err, context.Canceled) {
					continue
				}
				log.Fatalf("PollFetches: %v", e.Err)
			}
		}
		for it := fs.RecordIter(); !it.Done(); {
			got = append(got, it.Next().Value)
		}
		// Once we have seen the committed record and a later poll finds nothing more
		// (the aborted one is filtered out client-side), stop.
		if len(got) >= 1 && fs.Empty() {
			break
		}
	}

	if len(got) != 1 {
		log.Fatalf("FAIL: read_committed saw %d records, want exactly 1 (the committed one)", len(got))
	}
	if string(got[0]) != string(committed) {
		log.Fatalf("FAIL: read_committed saw %q, want %q", got[0], committed)
	}
	for _, v := range got {
		if string(v) == string(aborted) {
			log.Fatalf("FAIL: aborted record %q was visible under read_committed", aborted)
		}
	}
	fmt.Printf("read_committed: saw only %q (aborted %q absent)\n", got[0], aborted)

	if _, err := adm.DeleteTopics(ctx, topic); err != nil {
		log.Printf("warning: DeleteTopics: %v", err)
	}
	fmt.Println("DeleteTopic: ok")
	fmt.Println("PASS: committed visible + aborted absent under read_committed (KIP-890 V1)")
}
