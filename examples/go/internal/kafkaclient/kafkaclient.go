// Package kafkaclient is the single shared setup helper reused by every Go example
// in this repo. It builds franz-go (kgo) producer/consumer clients and kadm admin
// clients pointed at the KubeMQ Kafka connector's bootstrap endpoint.
//
// Connection model (see ../../SHARED-CONVENTIONS.md §1):
//
//   - Every example reads KUBEMQ_KAFKA_BOOTSTRAP (default localhost:9092) and uses
//     it verbatim as the Kafka bootstrap.servers value (a host:port list, NOT a URL).
//   - The connector is DISABLED by default: the operator must start the broker with
//     CONNECTORS_KAFKA_ENABLE=true (repo gotcha #1) and, for anything but a same-host
//     client, set CONNECTORS_KAFKA_ADVERTISED_HOST (gotcha #2, the M-23 hang).
//   - No SASL/TLS by default; the security/sasl-plain-scram variant adds credentials.
//   - franz-go defaults to the murmur2 partitioner (Java/kafkajs-compatible) — see
//     gotcha #4: librdkafka-based clients default to CRC32 and pick a DIFFERENT
//     partition for the same key.
package kafkaclient

import (
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	// DefaultBootstrap is the connector's default plain-TCP Kafka listener
	// (CONNECTORS_KAFKA_PORT=9092). TLS lives on :9093 (doc-only, §4.7).
	DefaultBootstrap = "localhost:9092"
)

// Bootstrap returns the connector bootstrap endpoint: KUBEMQ_KAFKA_BOOTSTRAP if
// set, else DefaultBootstrap. The value is a host:port (or comma-separated list),
// NOT a URL — Kafka takes a bootstrap.servers list, hence the _BOOTSTRAP name.
func Bootstrap() string {
	if v := os.Getenv("KUBEMQ_KAFKA_BOOTSTRAP"); v != "" {
		return v
	}
	return DefaultBootstrap
}

// seeds splits the bootstrap value into the comma-separated broker list kgo wants.
func seeds() []string {
	out := []string{}
	for _, s := range splitComma(Bootstrap()) {
		if s != "" {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		out = []string{DefaultBootstrap}
	}
	return out
}

func splitComma(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == ',' {
			out = append(out, trimSpace(cur))
			cur = ""
			continue
		}
		cur += string(r)
	}
	return append(out, trimSpace(cur))
}

func trimSpace(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}

// New builds a kgo.Client seeded at the connector bootstrap endpoint. Extra
// per-example options (RequiredAcks, ConsumeTopics, ConsumerGroup, compression,
// TransactionalID, SASL, isolation level, ...) are appended and WIN over the
// defaults here. The caller owns Close().
func New(opts ...kgo.Opt) (*kgo.Client, error) {
	base := []kgo.Opt{
		kgo.SeedBrokers(seeds()...),
		kgo.ClientID("kubemq-kafka-examples-go"),
		// Fail fast instead of the M-23 connect-then-hang when the connector is
		// down or AdvertisedHost is unset (gotcha #2).
		kgo.DialTimeout(5 * time.Second),
	}
	base = append(base, opts...)
	cl, err := kgo.NewClient(base...)
	if err != nil {
		return nil, fmt.Errorf("create kgo client: %w", err)
	}
	return cl, nil
}

// Admin builds a kadm.Client (topics / partitions / list-offsets / configs) over
// its own underlying kgo.Client. Both are returned so the caller can drive admin
// calls on the kadm.Client and own cleanup by closing the kgo.Client
// (admCl.Close()), which also tears down the wrapping kadm.Client.
func Admin(opts ...kgo.Opt) (*kadm.Client, *kgo.Client, error) {
	cl, err := New(opts...)
	if err != nil {
		return nil, nil, err
	}
	return kadm.NewClient(cl), cl, nil
}

// Topic returns a Kafka-charset-safe, per-run-unique example topic name of the
// form kafka-ex-<family>-<short>-<8hex> (§4.2). The random suffix lets concurrent
// runs across the 7 languages share one connector without colliding, and avoids
// the reserved '~' and '/' characters (gotcha #6/#7).
func Topic(family, short string) string {
	return fmt.Sprintf("kafka-ex-%s-%s-%s", family, short, uuid.NewString()[:8])
}

// Banner prints the one-line connection banner every example shows on startup so
// the operator can confirm where it is connecting and with which partitioner.
func Banner(example string) {
	fmt.Printf("[kubemq-kafka] %s | bootstrap=%s partitioner=murmur2(franz-go)\n",
		example, Bootstrap())
}
