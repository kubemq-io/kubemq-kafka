// Command list-and-retention is master-table variant 10: the ListOffsets request
// (earliest / latest / by-timestamp) plus retention config on the KubeMQ Kafka
// connector.
//
//	CreateTopic with retention.ms + retention.bytes in the config map
//	  -> DescribeTopicConfigs: assert both retention configs are ACCEPTED + readable
//	  -> produce a batch
//	  -> ListStartOffsets  (earliest) == log-start (0 for a fresh topic)
//	  -> ListEndOffsets    (latest)   == high-water mark == produced count
//	  -> ListOffsetsAfterMilli(ts)    == first offset with timestamp >= ts
//
// retention.ms/retention.bytes map onto the connector channel's MaxAge/MaxBytes
// (spec §2.2). Wall-clock retention is too slow to observe in a short run, so this
// program asserts the mapping is ACCEPTED and the earliest/latest/by-ts offsets are
// correct, rather than waiting for live expiry (see README).
//
// Run:
//
//	export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
//	go run ./offsets/list-and-retention
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

func main() {
	kafkaclient.Banner("offsets/list-and-retention")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := kafkaclient.Topic("offsets", "ret")

	adm, admCl, err := kafkaclient.Admin()
	if err != nil {
		log.Fatalf("admin client: %v", err)
	}
	defer admCl.Close()

	// Create with retention.ms + retention.bytes.
	if _, err := adm.CreateTopic(ctx, 1, 1, map[string]*string{
		"retention.ms":    kadm.StringPtr("600000"),    // 10 minutes
		"retention.bytes": kadm.StringPtr("104857600"), // 100 MiB
	}, topic); err != nil {
		log.Fatalf("CreateTopic %s: %v", topic, err)
	}
	fmt.Printf("CreateTopic: %s (retention.ms=600000 retention.bytes=104857600)\n", topic)

	// Both retention configs must be accepted and readable.
	rcs, err := adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		log.Fatalf("DescribeTopicConfigs: %v", err)
	}
	rc, err := rcs.On(topic, nil)
	if err != nil {
		log.Fatalf("DescribeTopicConfigs.On: %v", err)
	}
	cfg := map[string]string{}
	for _, c := range rc.Configs {
		if c.Value != nil {
			cfg[c.Key] = *c.Value
		}
	}
	if cfg["retention.ms"] != "600000" || cfg["retention.bytes"] != "104857600" {
		log.Fatalf("FAIL: retention not accepted: ms=%q bytes=%q", cfg["retention.ms"], cfg["retention.bytes"])
	}
	fmt.Printf("DescribeTopicConfigs: retention.ms=%s retention.bytes=%s (accepted)\n",
		cfg["retention.ms"], cfg["retention.bytes"])

	// Produce a batch, recording the timestamp boundary in the middle.
	prod, err := kafkaclient.New(kgo.DefaultProduceTopic(topic), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		log.Fatalf("producer: %v", err)
	}
	const total = 5
	var boundaryMilli int64
	for i := 0; i < total; i++ {
		if i == 2 {
			time.Sleep(50 * time.Millisecond)
			boundaryMilli = time.Now().UnixMilli()
			time.Sleep(50 * time.Millisecond)
		}
		if err := prod.ProduceSync(ctx, &kgo.Record{Value: []byte(fmt.Sprintf("r-%d", i))}).FirstErr(); err != nil {
			prod.Close()
			log.Fatalf("Produce %d: %v", i, err)
		}
	}
	prod.Close()
	fmt.Printf("Produce: %d records\n", total)

	// earliest == log-start.
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		log.Fatalf("ListStartOffsets: %v", err)
	}
	so, _ := starts.Lookup(topic, 0)
	if so.Offset != 0 {
		log.Fatalf("FAIL: earliest %d, want 0", so.Offset)
	}
	fmt.Printf("ListStartOffsets (earliest): %d\n", so.Offset)

	// latest == high-water mark == total.
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		log.Fatalf("ListEndOffsets: %v", err)
	}
	eo, _ := ends.Lookup(topic, 0)
	if eo.Offset != total {
		log.Fatalf("FAIL: latest %d, want %d", eo.Offset, total)
	}
	fmt.Printf("ListEndOffsets (latest): %d\n", eo.Offset)

	// by-timestamp: the first offset with ts >= boundary is offset 2 (r-2).
	afters, err := adm.ListOffsetsAfterMilli(ctx, boundaryMilli, topic)
	if err != nil {
		log.Fatalf("ListOffsetsAfterMilli: %v", err)
	}
	ao, _ := afters.Lookup(topic, 0)
	if ao.Offset != 2 {
		log.Fatalf("FAIL: by-timestamp offset %d, want 2", ao.Offset)
	}
	fmt.Printf("ListOffsetsAfterMilli (by-ts=%d): %d\n", boundaryMilli, ao.Offset)

	if _, err := adm.DeleteTopics(ctx, topic); err != nil {
		log.Printf("warning: DeleteTopics: %v", err)
	}
	fmt.Println("DeleteTopic: ok")
	fmt.Println("PASS: earliest/latest/by-timestamp offsets + retention config verified")
}
