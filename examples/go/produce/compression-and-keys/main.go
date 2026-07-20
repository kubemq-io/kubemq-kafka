// Command compression-and-keys is master-table variant 3: every producer
// compression codec (none/gzip/snappy/lz4/zstd) round-trips through the KubeMQ
// Kafka connector, and keyed records partition DETERMINISTICALLY.
//
//	CreateTopic(partitions=3)
//	  -> for each codec: produce a keyed record, read it back, assert value + key
//	  -> produce the SAME key many times, assert every copy lands on ONE partition
//	  -> assert that partition equals the murmur2 expectation (franz-go default)
//
// Gotcha #4 (THE cross-client partitioning gotcha): franz-go, Java kafka-clients,
// and kafkajs v2+ default to the MURMUR2 partitioner; the librdkafka-based clients
// (python confluent-kafka, C# Confluent.Kafka, ruby rdkafka, rust rdkafka) default
// to CRC32 and therefore pick a DIFFERENT partition for the same key. A key routed
// here will NOT necessarily match a librdkafka client's partition — see
// docs/concepts/cross-client-partitioning.md. Compression is transparent to the
// connector: it stores the RecordBatch and returns it verbatim on Fetch.
//
// Run:
//
//	export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
//	go run ./produce/compression-and-keys
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kubemq-io/kubemq-kafka-examples/go/internal/kafkaclient"
)

const numPartitions = 3

func main() {
	kafkaclient.Banner("produce/compression-and-keys")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := kafkaclient.Topic("produce", "comp")

	adm, admCl, err := kafkaclient.Admin()
	if err != nil {
		log.Fatalf("admin client: %v", err)
	}
	defer admCl.Close()
	if _, err := adm.CreateTopic(ctx, numPartitions, 1, nil, topic); err != nil {
		log.Fatalf("CreateTopic %s: %v", topic, err)
	}
	fmt.Printf("CreateTopic: %s (partitions=%d)\n", topic, numPartitions)

	// 1. Every codec round-trips a keyed record byte-for-byte.
	codecs := []struct {
		name  string
		codec kgo.CompressionCodec
	}{
		{"none", kgo.NoCompression()},
		{"gzip", kgo.GzipCompression()},
		{"snappy", kgo.SnappyCompression()},
		{"lz4", kgo.Lz4Compression()},
		{"zstd", kgo.ZstdCompression()},
	}
	for _, c := range codecs {
		prod, err := kafkaclient.New(
			kgo.DefaultProduceTopic(topic),
			kgo.RequiredAcks(kgo.AllISRAcks()),
			kgo.ProducerBatchCompression(c.codec),
		)
		if err != nil {
			log.Fatalf("producer %s: %v", c.name, err)
		}
		key := []byte("codec-" + c.name)
		val := []byte("payload compressed with " + c.name)
		r, err := prod.ProduceSync(ctx, &kgo.Record{Key: key, Value: val}).First()
		if err != nil {
			prod.Close()
			log.Fatalf("Produce %s: %v", c.name, err)
		}
		prod.Close()

		got := readOffset(ctx, topic, r.Partition, r.Offset)
		if string(got.Value) != string(val) || string(got.Key) != string(key) {
			log.Fatalf("FAIL: %s read-back key=%q val=%q want key=%q val=%q",
				c.name, got.Key, got.Value, key, val)
		}
		fmt.Printf("Produce(%s): partition=%d offset=%d round-trip ok\n", c.name, r.Partition, r.Offset)
	}

	// 2. A fixed key is sticky: every copy lands on the SAME partition, and that
	//    partition matches franz-go's murmur2 computation for the key.
	fixedKey := []byte("customer-42")
	wantPart := murmur2Partition(fixedKey, numPartitions)
	prod, err := kafkaclient.New(
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		log.Fatalf("keyed producer: %v", err)
	}
	defer prod.Close()
	for i := 0; i < 10; i++ {
		r, err := prod.ProduceSync(ctx, &kgo.Record{
			Key:   fixedKey,
			Value: []byte(fmt.Sprintf("keyed-%d", i)),
		}).First()
		if err != nil {
			log.Fatalf("keyed Produce %d: %v", i, err)
		}
		if r.Partition != wantPart {
			log.Fatalf("FAIL: key %q landed on partition %d, murmur2 expects %d (gotcha #4)",
				fixedKey, r.Partition, wantPart)
		}
	}
	fmt.Printf("Keyed: key %q -> partition %d for all 10 copies (murmur2 expected %d)\n",
		fixedKey, wantPart, wantPart)

	if _, err := adm.DeleteTopics(ctx, topic); err != nil {
		log.Printf("warning: DeleteTopics: %v", err)
	}
	fmt.Println("DeleteTopic: ok")
	fmt.Println("PASS: all codecs round-trip + murmur2 keyed partitioning verified")
}

// readOffset consumes exactly the record at (partition, offset) and returns it.
func readOffset(ctx context.Context, topic string, partition int32, offset int64) *kgo.Record {
	cons, err := kafkaclient.New(
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {partition: kgo.NewOffset().At(offset)},
		}),
		kgo.FetchMaxWait(2*time.Second),
	)
	if err != nil {
		log.Fatalf("read consumer: %v", err)
	}
	defer cons.Close()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		fs := cons.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			log.Fatalf("PollFetches: %v", errs[0].Err)
		}
		for it := fs.RecordIter(); !it.Done(); {
			return it.Next()
		}
	}
	log.Fatalf("FAIL: timed out reading offset %d on partition %d", offset, partition)
	return nil
}

// murmur2Partition reproduces the Kafka murmur2 partitioner franz-go uses by
// default, so the example can assert the exact target partition for a key. This is
// the SAME algorithm as Java kafka-clients / kafkajs v2+, and DIFFERENT from the
// CRC32 default of the librdkafka clients (gotcha #4).
func murmur2Partition(key []byte, partitions int32) int32 {
	h := murmur2(key)
	// Kafka masks off the sign bit, then mods by the partition count.
	return (h & 0x7fffffff) % partitions
}

// murmur2 is the 32-bit MurmurHash2 with the exact seed/constants Kafka uses.
func murmur2(data []byte) int32 {
	const (
		seed = uint32(0x9747b28c)
		m    = uint32(0x5bd1e995)
		r    = 24
	)
	length := len(data)
	h := seed ^ uint32(length)
	nblocks := length / 4
	for i := 0; i < nblocks; i++ {
		k := uint32(data[i*4]) |
			uint32(data[i*4+1])<<8 |
			uint32(data[i*4+2])<<16 |
			uint32(data[i*4+3])<<24
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}
	tail := nblocks * 4
	switch length & 3 {
	case 3:
		h ^= uint32(data[tail+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[tail+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[tail])
		h *= m
	}
	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return int32(h)
}
