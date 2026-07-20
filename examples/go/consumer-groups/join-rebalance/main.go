// Command join-rebalance is master-table variant 6: consumer-group membership and
// rebalancing on the KubeMQ Kafka connector, with no records lost across a member
// leaving.
//
//	CreateTopic(partitions=4) -> produce a full batch
//	  -> start member A and member B in the SAME group (Join/Sync/Heartbeat)
//	  -> both consume; partitions are split across the two members
//	  -> member A leaves (Leave -> rebalance); member B ends up owning ALL partitions
//	  -> assert EVERY produced record was delivered exactly across the group (no loss)
//
// franz-go drives the group protocol (JoinGroup 11 / SyncGroup 14 / Heartbeat 12 /
// LeaveGroup 13) automatically; kgo.ConsumerGroup(g) enrolls a client and
// kgo.CooperativeStickyBalancer selects incremental cooperative rebalancing.
//
// Run:
//
//	export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
//	go run ./consumer-groups/join-rebalance
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kubemq-io/kubemq-kafka-examples/go/internal/kafkaclient"
)

const (
	partitions = 4
	totalRecs  = 40
)

func main() {
	kafkaclient.Banner("consumer-groups/join-rebalance")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	topic := kafkaclient.Topic("cg", "rebal")
	group := kafkaclient.Topic("cg", "grp") // unique group id per run

	adm, admCl, err := kafkaclient.Admin()
	if err != nil {
		log.Fatalf("admin client: %v", err)
	}
	defer admCl.Close()
	if _, err := adm.CreateTopic(ctx, partitions, 1, nil, topic); err != nil {
		log.Fatalf("CreateTopic %s: %v", topic, err)
	}
	fmt.Printf("CreateTopic: %s (partitions=%d) group=%s\n", topic, partitions, group)

	// Produce a batch spread across partitions via distinct keys.
	prod, err := kafkaclient.New(kgo.DefaultProduceTopic(topic), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		log.Fatalf("producer: %v", err)
	}
	want := map[string]bool{}
	for i := 0; i < totalRecs; i++ {
		v := fmt.Sprintf("msg-%d", i)
		want[v] = true
		if err := prod.ProduceSync(ctx, &kgo.Record{
			Key:   []byte(fmt.Sprintf("k-%d", i)),
			Value: []byte(v),
		}).FirstErr(); err != nil {
			prod.Close()
			log.Fatalf("Produce %d: %v", i, err)
		}
	}
	prod.Close()
	fmt.Printf("Produce: %d records across %d partitions\n", totalRecs, partitions)

	var mu sync.Mutex
	got := map[string]int{} // value -> delivery count (across the whole group)

	record := func(member string, recs []*kgo.Record) {
		mu.Lock()
		defer mu.Unlock()
		for _, r := range recs {
			got[string(r.Value)]++
		}
		_ = member
	}
	total := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(got)
	}

	newMember := func(id string) *kgo.Client {
		c, err := kafkaclient.New(
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.Balancers(kgo.CooperativeStickyBalancer()),
			kgo.FetchMaxWait(1*time.Second),
			kgo.ClientID(id),
		)
		if err != nil {
			log.Fatalf("member %s: %v", id, err)
		}
		return c
	}

	// Member A and member B join the same group; partitions split across them.
	a := newMember("member-a")
	b := newMember("member-b")

	var wg sync.WaitGroup
	pump := func(member string, c *kgo.Client, stop <-chan struct{}) {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			// Bound each poll: franz-go's PollFetches blocks until records arrive OR
			// the context is cancelled (it never returns on its own for an idle
			// assignment). Once a member owns everything and the log is drained, an
			// unbounded poll would block forever and the loop would never re-check
			// `stop` — so closing stop would deadlock wg.Wait(). A short per-poll
			// deadline lets the loop come back around and observe stop promptly.
			pollCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
			fs := c.PollFetches(pollCtx)
			cancel()
			if errs := fs.Errors(); len(errs) > 0 {
				// A per-poll deadline (idle) or a transient rebalance error: keep polling.
				continue
			}
			var recs []*kgo.Record
			for it := fs.RecordIter(); !it.Done(); {
				recs = append(recs, it.Next())
			}
			if len(recs) > 0 {
				record(member, recs)
				if err := c.CommitRecords(ctx, recs...); err != nil {
					log.Printf("member %s commit: %v", member, err)
				}
			}
		}
	}

	stopA := make(chan struct{})
	stopB := make(chan struct{})
	wg.Add(2)
	go pump("a", a, stopA)
	go pump("b", b, stopB)

	// Let both members share the load for a moment.
	waitFor(func() bool { return total() >= totalRecs/2 }, 20*time.Second)
	fmt.Printf("Both members active: delivered %d/%d so far\n", total(), totalRecs)

	// Member A leaves -> rebalance; member B must pick up A's partitions.
	close(stopA)
	if err := a.LeaveGroupContext(ctx); err != nil { // explicit Leave -> prompt rebalance
		log.Printf("member A LeaveGroup: %v", err)
	}
	a.Close()
	fmt.Println("Member A left the group (LeaveGroup) -> rebalance")

	// Member B should now finish the whole set.
	if !waitFor(func() bool { return total() >= totalRecs }, 40*time.Second) {
		close(stopB)
		wg.Wait()
		b.Close()
		log.Fatalf("FAIL: after rebalance only %d/%d records delivered", total(), totalRecs)
	}
	close(stopB)
	wg.Wait()
	b.Close()

	// Assert no produced record was lost across the rebalance.
	for v := range want {
		if got[v] == 0 {
			log.Fatalf("FAIL: record %q was never delivered (lost across rebalance)", v)
		}
	}
	fmt.Printf("Delivered all %d records across the group (rebalance lost none)\n", len(want))

	if _, err := adm.DeleteTopics(ctx, topic); err != nil {
		log.Printf("warning: DeleteTopics: %v", err)
	}
	fmt.Println("DeleteTopic: ok")
	fmt.Println("PASS: join/rebalance with no record loss verified")
}

func waitFor(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return cond()
}
