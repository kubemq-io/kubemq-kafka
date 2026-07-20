// Command partitions-and-configs is master-table variant 9: the three "partial"
// admin operations on the KubeMQ Kafka connector.
//
//	CreateTopic(partitions=1)
//	  -> UpdatePartitions(set=3): increase 1 -> 3 succeeds (final count == 3)
//	  -> UpdatePartitions(set=2): DECREASE rejected with INVALID_PARTITIONS
//	  -> UpdatePartitions(set=300): >256 rejected with INVALID_PARTITIONS
//	  -> IncrementalAlterConfigs: set retention.ms on ONE key (partial update)
//	  -> DeleteRecords: truncate the low end of a partition (partial delete)
//
// Kafka scope on the connector (spec §2.3):
//   - CreatePartitions(37) is INCREASE-ONLY and capped at 256.
//   - IncrementalAlterConfigs(44) alters a SUBSET of keys, leaving the rest intact.
//   - DeleteRecords(21) truncates a partition's low end only (no arbitrary delete).
//
// UpdatePartitions(set=N) SETS the total to N (CreatePartitions(add=N) ADDS N).
// Run:
//
//	export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
//	go run ./admin/partitions-and-configs
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kubemq-io/kubemq-kafka-examples/go/internal/kafkaclient"
)

func main() {
	kafkaclient.Banner("admin/partitions-and-configs")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := kafkaclient.Topic("admin", "part")

	adm, admCl, err := kafkaclient.Admin()
	if err != nil {
		log.Fatalf("admin client: %v", err)
	}
	defer admCl.Close()
	if _, err := adm.CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		log.Fatalf("CreateTopic %s: %v", topic, err)
	}
	fmt.Printf("CreateTopic: %s (partitions=1)\n", topic)

	// 1. Increase 1 -> 3 (UpdatePartitions SETS the total).
	if _, err := adm.UpdatePartitions(ctx, 3, topic); err != nil {
		log.Fatalf("UpdatePartitions set=3: %v", err)
	}
	if got := partitionCount(ctx, adm, topic); got != 3 {
		log.Fatalf("FAIL: after increase, partition count %d, want 3", got)
	}
	fmt.Println("UpdatePartitions(set=3): 1 -> 3 (final count 3)")

	// 2. Decrease 3 -> 2 is rejected: partitions are increase-only.
	if err := expectInvalidPartitions(adm.UpdatePartitions(ctx, 2, topic)); err != nil {
		log.Fatalf("FAIL: decrease: %v", err)
	}
	fmt.Println("UpdatePartitions(set=2): rejected with INVALID_PARTITIONS (increase-only)")

	// 3. >256 is rejected: partitions are capped at 256.
	if err := expectInvalidPartitions(adm.UpdatePartitions(ctx, 300, topic)); err != nil {
		log.Fatalf("FAIL: >256: %v", err)
	}
	fmt.Println("UpdatePartitions(set=300): rejected with INVALID_PARTITIONS (>256 cap)")

	// 4. IncrementalAlterConfigs: set retention.ms only; other configs stay put.
	if _, err := adm.AlterTopicConfigs(ctx, []kadm.AlterConfig{
		{Op: kadm.SetConfig, Name: "retention.ms", Value: kadm.StringPtr("7200000")},
	}, topic); err != nil {
		log.Fatalf("IncrementalAlterConfigs: %v", err)
	}
	if got := readConfig(ctx, adm, topic, "retention.ms"); got != "7200000" {
		log.Fatalf("FAIL: retention.ms=%q after alter, want 7200000", got)
	}
	fmt.Println("IncrementalAlterConfigs: retention.ms=7200000 (partial, subset)")

	// 5. DeleteRecords: truncate the low end of partition 0. Produce a handful,
	//    then delete everything below offset 3 and assert the new log-start is 3.
	produceTo(ctx, topic, 6)
	os := kadm.Offsets{}
	os.AddOffset(topic, 0, 3, -1) // delete records with offset < 3
	if _, err := adm.DeleteRecords(ctx, os); err != nil {
		log.Fatalf("DeleteRecords: %v", err)
	}
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		log.Fatalf("ListStartOffsets: %v", err)
	}
	so, ok := starts.Lookup(topic, 0)
	if !ok || so.Err != nil {
		log.Fatalf("FAIL: no start offset after DeleteRecords (ok=%v err=%v)", ok, so.Err)
	}
	if so.Offset != 3 {
		log.Fatalf("FAIL: log-start %d after DeleteRecords, want 3", so.Offset)
	}
	fmt.Printf("DeleteRecords(<3): partition 0 log-start now %d (low-end truncation)\n", so.Offset)

	if _, err := adm.DeleteTopics(ctx, topic); err != nil {
		log.Printf("warning: DeleteTopics: %v", err)
	}
	fmt.Println("DeleteTopic: ok")
	fmt.Println("PASS: increase-only partitions + partial config + DeleteRecords verified")
}

func partitionCount(ctx context.Context, adm *kadm.Client, topic string) int {
	// Metadata can lag a moment after an increase; poll briefly for the new count.
	deadline := time.Now().Add(10 * time.Second)
	var n int
	for time.Now().Before(deadline) {
		topics, err := adm.ListTopics(ctx)
		if err != nil {
			log.Fatalf("ListTopics: %v", err)
		}
		n = len(topics[topic].Partitions)
		if n == 3 {
			return n
		}
		time.Sleep(200 * time.Millisecond)
	}
	return n
}

func readConfig(ctx context.Context, adm *kadm.Client, topic, key string) string {
	rcs, err := adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		log.Fatalf("DescribeTopicConfigs: %v", err)
	}
	rc, err := rcs.On(topic, nil)
	if err != nil {
		log.Fatalf("DescribeTopicConfigs.On: %v", err)
	}
	for _, c := range rc.Configs {
		if c.Key == key && c.Value != nil {
			return *c.Value
		}
	}
	return ""
}

func produceTo(ctx context.Context, topic string, n int) {
	prod, err := kafkaclient.New(
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Pin to partition 0 so DeleteRecords targets a known partition.
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		log.Fatalf("producer: %v", err)
	}
	defer prod.Close()
	for i := 0; i < n; i++ {
		if err := prod.ProduceSync(ctx, &kgo.Record{
			Partition: 0,
			Value:     []byte(fmt.Sprintf("d-%d", i)),
		}).FirstErr(); err != nil {
			log.Fatalf("Produce %d: %v", i, err)
		}
	}
}

// expectInvalidPartitions checks a CreatePartitions response for the expected
// INVALID_PARTITIONS rejection — either as a top-level error or per-topic.
func expectInvalidPartitions(resp kadm.CreatePartitionsResponses, topErr error) error {
	if topErr != nil {
		if errors.Is(topErr, kerr.InvalidPartitions) {
			return nil
		}
		return fmt.Errorf("got %v, want INVALID_PARTITIONS", topErr)
	}
	for _, r := range resp {
		if errors.Is(r.Err, kerr.InvalidPartitions) {
			return nil
		}
		if r.Err != nil {
			return fmt.Errorf("got %v, want INVALID_PARTITIONS", r.Err)
		}
	}
	return errors.New("no error returned; expected INVALID_PARTITIONS")
}
