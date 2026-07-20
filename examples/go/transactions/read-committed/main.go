// Command read-committed is master-table variant 12: the read_committed isolation
// level and the Last Stable Offset (LSO) on the KubeMQ Kafka connector.
//
//	produce one COMMITTED txn record
//	  -> open a txn and produce an in-flight record WITHOUT ending it (txn OPEN)
//	  -> while the txn is open: ListEndOffsets(read_committed) == LSO < HWM
//	     (the open txn holds the LSO back)
//	  -> ABORT the open txn
//	  -> read_committed consumer NEVER delivers the aborted record; LSO catches up
//
// read_committed filtering is CLIENT-SIDE: the broker returns the records plus the
// AbortedTransactions list, and the franz-go consumer drops aborted records locally
// (gotcha #12 — there is no server-side per-record filter). LSO is the ListOffsets
// read_committed high end: it stops at the first still-open transaction.
//
// EOS ceiling: KIP-890 V1 (gotcha #9) — same as variant 11.
//
// Run:
//
//	export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
//	go run ./transactions/read-committed
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/kubemq-io/kubemq-kafka-examples/go/internal/kafkaclient"
)

func main() {
	kafkaclient.Banner("transactions/read-committed")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	topic := kafkaclient.Topic("txn", "rc")
	txnID := kafkaclient.Topic("txn", "rcid")

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

	// One committed record so the log has a stable, visible baseline.
	committed := []byte("committed-rc-1")
	if err := prod.BeginTransaction(); err != nil {
		log.Fatalf("BeginTransaction (commit): %v", err)
	}
	if err := prod.ProduceSync(ctx, &kgo.Record{Value: committed}).FirstErr(); err != nil {
		log.Fatalf("Produce (committed): %v", err)
	}
	if err := prod.EndTransaction(ctx, kgo.TryCommit); err != nil {
		log.Fatalf("EndTransaction commit: %v", err)
	}
	fmt.Printf("Txn #1: produced %q, committed\n", committed)

	// Open a second transaction and produce WITHOUT ending it — the txn stays open.
	aborted := []byte("aborted-rc-2")
	if err := prod.BeginTransaction(); err != nil {
		log.Fatalf("BeginTransaction (open): %v", err)
	}
	if err := prod.ProduceSync(ctx, &kgo.Record{Value: aborted}).FirstErr(); err != nil {
		log.Fatalf("Produce (open txn): %v", err)
	}
	fmt.Printf("Txn #2: produced %q, LEFT OPEN\n", aborted)

	// While the txn is open, LSO (read_committed end) < HWM (read_uncommitted end).
	lso := lsoOffset(ctx, admCl, topic)
	hwm := hwmOffset(ctx, adm, topic)
	fmt.Printf("While txn open: LSO(read_committed)=%d HWM(read_uncommitted)=%d\n", lso, hwm)
	if lso >= hwm {
		log.Fatalf("FAIL: expected LSO(%d) < HWM(%d) while a transaction is open", lso, hwm)
	}

	// Abort the open transaction.
	if err := prod.EndTransaction(ctx, kgo.TryAbort); err != nil {
		log.Fatalf("EndTransaction abort: %v", err)
	}
	fmt.Println("Txn #2: aborted")

	// After abort, LSO catches up to HWM.
	lsoAfter := lsoOffset(ctx, admCl, topic)
	hwmAfter := hwmOffset(ctx, adm, topic)
	fmt.Printf("After abort: LSO=%d HWM=%d\n", lsoAfter, hwmAfter)
	if lsoAfter != hwmAfter {
		log.Fatalf("FAIL: after abort expected LSO(%d) == HWM(%d)", lsoAfter, hwmAfter)
	}

	// read_committed consumer must deliver ONLY the committed record.
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
		if len(got) >= 1 && fs.Empty() {
			break
		}
	}
	if len(got) != 1 || string(got[0]) != string(committed) {
		log.Fatalf("FAIL: read_committed delivered %d records (%q...); want only %q",
			len(got), firstOr(got), committed)
	}
	for _, v := range got {
		if string(v) == string(aborted) {
			log.Fatalf("FAIL: aborted record %q delivered under read_committed", aborted)
		}
	}
	fmt.Printf("read_committed: delivered only %q (aborted never seen)\n", got[0])

	if _, err := adm.DeleteTopics(ctx, topic); err != nil {
		log.Printf("warning: DeleteTopics: %v", err)
	}
	fmt.Println("DeleteTopic: ok")
	fmt.Println("PASS: LSO < HWM while open, aborted never delivered under read_committed")
}

// hwmOffset returns the partition-0 high-water mark (read_uncommitted end). kadm's
// ListEndOffsets issues ListOffsets at isolation read_uncommitted.
func hwmOffset(ctx context.Context, adm *kadm.Client, topic string) int64 {
	listed, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		log.Fatalf("ListEndOffsets: %v", err)
	}
	lo, _ := listed.Lookup(topic, 0)
	return lo.Offset
}

// lsoOffset returns the partition-0 Last Stable Offset (read_committed end). kadm
// exposes no read_committed ListOffsets, so we issue the raw ListOffsets request
// (key 2) with IsolationLevel=1 (read_committed) and Timestamp=-1 (latest).
func lsoOffset(ctx context.Context, cl *kgo.Client, topic string) int64 {
	req := kmsg.NewListOffsetsRequest()
	req.ReplicaID = -1
	req.IsolationLevel = 1 // read_committed -> the LSO

	reqTopic := kmsg.NewListOffsetsRequestTopic()
	reqTopic.Topic = topic
	reqPart := kmsg.NewListOffsetsRequestTopicPartition()
	reqPart.Partition = 0
	reqPart.CurrentLeaderEpoch = -1
	reqPart.Timestamp = -1 // latest stable offset
	reqTopic.Partitions = append(reqTopic.Partitions, reqPart)
	req.Topics = append(req.Topics, reqTopic)

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		log.Fatalf("ListOffsets(read_committed): %v", err)
	}
	if len(resp.Topics) == 0 || len(resp.Topics[0].Partitions) == 0 {
		log.Fatalf("FAIL: ListOffsets(read_committed) returned no partitions")
	}
	p := resp.Topics[0].Partitions[0]
	if p.ErrorCode != 0 {
		log.Fatalf("ListOffsets(read_committed) partition error code %d", p.ErrorCode)
	}
	return p.Offset
}

func firstOr(bs [][]byte) string {
	if len(bs) == 0 {
		return ""
	}
	return string(bs[0])
}
