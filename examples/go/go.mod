module github.com/kubemq-io/kubemq-kafka-examples/go

go 1.25.0

require (
	github.com/google/uuid v1.6.0 // unique per-run topic/txn ids
	github.com/twmb/franz-go v1.21.4 // kgo producer/consumer/txn transport
	github.com/twmb/franz-go/pkg/kadm v1.18.0 // admin: topics, partitions, list-offsets
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // low-level request types (admin variant 9)
)

require (
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	golang.org/x/crypto v0.51.0 // indirect
)
