// Command cleanup best-effort deletes the Kafka topics this burn-in agent
// created (names prefixed with BURNIN_RESOURCE_PREFIX, default "burnin") on the
// shared stateful connector. It only touches topics matching the prefix so it
// cannot disturb the other language agents.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/kubemq-io/kubemq-kafka/burnin/transport"
)

func main() {
	prefix := os.Getenv("BURNIN_RESOURCE_PREFIX")
	if prefix == "" {
		prefix = "burnin"
	}
	bootstrap := transport.BootstrapAddress(os.Getenv("KUBEMQ_BROKER_ADDRESS"))

	cl, err := transport.NewClient(transport.KafkaClientConfig{
		Bootstrap: bootstrap,
		ClientID:  "burnin-kafka-cleanup",
		Acks:      "all",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "build kafka client: %v\n", err)
		os.Exit(1)
	}
	defer cl.Close()
	adm := transport.NewAdmin(cl)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topics, err := adm.ListTopics(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "list topics: %v\n", err)
		os.Exit(1)
	}

	deleted, failed := 0, 0
	for _, name := range topics.Names() {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		if err := transport.DeleteTopic(ctx, adm, name); err != nil {
			fmt.Fprintf(os.Stderr, "delete topic %s: %v\n", name, err)
			failed++
			continue
		}
		deleted++
	}

	fmt.Printf("cleanup prefix=%q bootstrap=%s topics_deleted=%d topics_failed=%d\n",
		prefix, bootstrap, deleted, failed)
}
