// Package transport wraps the franz-go (kgo + kadm) Kafka client — the server's
// own conformance client — for the burn-in harness. resources.go owns the naming
// grammar: burn-in topic names and the Events-Store channel the connector maps
// them to.
package transport

import (
	"fmt"
	"os"
)

// BrokerEnv is the burn-in Kafka bootstrap env var (NOT KUBEMQ_KAFKA_BOOTSTRAP,
// which is the examples var; spec §7.5).
const BrokerEnv = "KUBEMQ_BROKER_ADDRESS"

// DefaultBroker is the default host:port for the Kafka wire listener (NOT 4566).
const DefaultBroker = "localhost:9092"

// ResourcePrefixEnv overrides the resource-name prefix so concurrent burn-in
// agents (one per language) sharing the SAME stateful connector do not collide
// on fixed topic names. Default "burnin"; the Go agent sets it to "burnin_go".
const ResourcePrefixEnv = "BURNIN_RESOURCE_PREFIX"

// ChannelPrefix is the connector's Events-Store channel prefix for a Kafka topic
// (spec §2.2): topic T maps to Events-Store channel kafka.T.
const ChannelPrefix = "kafka."

// BootstrapAddress resolves the franz-go SeedBrokers value: the supplied
// address, then KUBEMQ_BROKER_ADDRESS, then DefaultBroker. It is host:port, NOT
// a URL.
func BootstrapAddress(address string) string {
	if address != "" {
		return address
	}
	if v := os.Getenv(BrokerEnv); v != "" {
		return v
	}
	return DefaultBroker
}

func resourcePrefix() string {
	if v := os.Getenv(ResourcePrefixEnv); v != "" {
		return v
	}
	return "burnin"
}

// TopicName builds "burnin.<worker>.<idx:04d>" (dot-separated; Kafka + KubeMQ
// safe — never `~` or `/`, which the connector rejects). The connector maps this
// topic to the Events-Store channel kafka.<topic>.
func TopicName(worker string, idx int) string {
	return fmt.Sprintf("%s.%s.%04d", resourcePrefix(), worker, idx)
}

// MappedChannel returns the Events-Store channel the connector maps a topic to:
// kafka.<topic>. Workers log it alongside the Kafka topic name.
func MappedChannel(topic string) string {
	return ChannelPrefix + topic
}
