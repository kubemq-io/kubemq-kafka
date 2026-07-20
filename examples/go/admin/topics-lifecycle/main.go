// Command topics-lifecycle is master-table variant 8: the full admin lifecycle of
// a topic on the KubeMQ Kafka connector.
//
//	CreateTopic -> ListTopics (topic present) -> DescribeTopicConfigs
//	  -> BrokerMetadata (DescribeCluster) -> DeleteTopics -> ListTopics (absent)
//	  -> CreateTopic with a '~' in the name -> INVALID_TOPIC_EXCEPTION (gotcha #6)
//
// '~' is the connector's channel/partition separator, so it is a reserved topic
// character; a create with '~' is rejected with INVALID_TOPIC_EXCEPTION (code 17).
// Covers CreateTopics(19), DeleteTopics(20), DescribeConfigs(32), Metadata(3),
// DescribeCluster(60).
//
// Run:
//
//	export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
//	go run ./admin/topics-lifecycle
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/kubemq-io/kubemq-kafka-examples/go/internal/kafkaclient"
)

func main() {
	kafkaclient.Banner("admin/topics-lifecycle")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := kafkaclient.Topic("admin", "life")

	adm, admCl, err := kafkaclient.Admin()
	if err != nil {
		log.Fatalf("admin client: %v", err)
	}
	defer admCl.Close()

	// 1. Create.
	if _, err := adm.CreateTopic(ctx, 2, 1, map[string]*string{
		"retention.ms": strPtr("3600000"),
	}, topic); err != nil {
		log.Fatalf("CreateTopic %s: %v", topic, err)
	}
	fmt.Printf("CreateTopic: %s (partitions=2)\n", topic)

	// 2. List: the topic must be present. Metadata propagation lags a moment after
	//    CreateTopic (a fresh ListTopics right away can miss it), so retry the fresh
	//    metadata request with a short backoff until it shows up — symmetric with the
	//    waitGone absence check after delete below.
	nparts, ok := waitPresent(ctx, adm, topic, 10*time.Second)
	if !ok {
		log.Fatalf("FAIL: %s not present after create", topic)
	}
	fmt.Printf("ListTopics: %s present (%d partitions)\n", topic, nparts)

	// 3. Describe configs: retention.ms must be readable.
	rcs, err := adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		log.Fatalf("DescribeTopicConfigs: %v", err)
	}
	rc, err := rcs.On(topic, nil)
	if err != nil {
		log.Fatalf("DescribeTopicConfigs.On: %v", err)
	}
	var retention string
	for _, c := range rc.Configs {
		if c.Key == "retention.ms" && c.Value != nil {
			retention = *c.Value
		}
	}
	fmt.Printf("DescribeTopicConfigs: retention.ms=%s\n", retention)

	// 4. DescribeCluster via BrokerMetadata: at least one broker is advertised.
	meta, err := adm.BrokerMetadata(ctx)
	if err != nil {
		log.Fatalf("BrokerMetadata: %v", err)
	}
	if len(meta.Brokers) == 0 {
		log.Fatalf("FAIL: DescribeCluster returned no brokers")
	}
	fmt.Printf("DescribeCluster: clusterID=%q brokers=%d\n", meta.Cluster, len(meta.Brokers))

	// 5. Delete, then confirm it is gone.
	if _, err := adm.DeleteTopics(ctx, topic); err != nil {
		log.Fatalf("DeleteTopics: %v", err)
	}
	// Metadata can lag a moment after delete; poll briefly.
	if !waitGone(ctx, adm, topic, 10*time.Second) {
		log.Fatalf("FAIL: %s still present after delete", topic)
	}
	fmt.Printf("DeleteTopics: %s removed\n", topic)

	// 6. Reserved '~' character -> INVALID_TOPIC_EXCEPTION (gotcha #6).
	bad := "kafka-ex-admin-bad~name"
	_, err = adm.CreateTopic(ctx, 1, 1, nil, bad)
	if err == nil {
		// Some paths return the per-topic error inside the response rather than a
		// top-level error; re-check by listing.
		list, _ := adm.ListTopics(ctx)
		if list.Has(bad) {
			log.Fatalf("FAIL: topic with '~' was created; expected INVALID_TOPIC_EXCEPTION")
		}
		log.Fatalf("FAIL: create with '~' returned no error; expected INVALID_TOPIC_EXCEPTION")
	}
	if !errors.Is(err, kerr.InvalidTopicException) {
		log.Fatalf("FAIL: '~' rejected with %v, want INVALID_TOPIC_EXCEPTION", err)
	}
	fmt.Printf("CreateTopic(%q): rejected with %v (expected)\n", bad, err)

	fmt.Println("PASS: topic lifecycle + reserved-char rejection verified")
}

// waitPresent retries a fresh ListTopics (each call issues a Metadata request)
// until the topic appears or the timeout elapses, absorbing the brief metadata
// propagation lag after CreateTopic. Returns the partition count on success.
func waitPresent(ctx context.Context, adm *kadm.Client, topic string, timeout time.Duration) (int, bool) {
	deadline := time.Now().Add(timeout)
	for {
		topics, err := adm.ListTopics(ctx)
		if err != nil {
			log.Fatalf("ListTopics: %v", err)
		}
		if topics.Has(topic) {
			return len(topics[topic].Partitions), true
		}
		if !time.Now().Before(deadline) {
			return 0, false
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func waitGone(ctx context.Context, adm *kadm.Client, topic string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		topics, err := adm.ListTopics(ctx)
		if err != nil {
			log.Fatalf("ListTopics: %v", err)
		}
		if !topics.Has(topic) {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func strPtr(s string) *string { return &s }
