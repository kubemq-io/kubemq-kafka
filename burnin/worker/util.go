package worker

import (
	"context"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kubemq-io/kubemq-kafka/burnin/metrics"
	"github.com/kubemq-io/kubemq-kafka/burnin/payload"
)

// sleepCtx sleeps for d unless ctx is cancelled first. Returns false if the
// context was cancelled (caller should stop).
func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// metricObserveSend records ProduceSync / native send round-trip duration.
func metricObserveSend(workerName string, d time.Duration) {
	metrics.ObserveSendDuration(workerName, d)
}

// stampHeaders builds the instrumentation Kafka record headers carried on every
// burn-in produce (spec §7): WorkerId, Sequence, ContentHash, TimestampNs. Header
// values survive the connector's record round-trip unchanged (the connector
// passes the record value + headers through). Mirrors the aws SQS attribute stamp.
func stampHeaders(producerID string, seq uint64, crcHex string) []kgo.RecordHeader {
	return []kgo.RecordHeader{
		{Key: payload.AttrWorkerID, Value: []byte(producerID)},
		{Key: payload.AttrSequence, Value: []byte(strconv.FormatUint(seq, 10))},
		{Key: payload.AttrContentHash, Value: []byte(crcHex)},
		{Key: payload.AttrTimestampNS, Value: []byte(strconv.FormatInt(time.Now().UnixNano(), 10))},
	}
}

// extractMeta pulls (producerID, seq, crcHex, sentAt) from Kafka record headers.
// Missing/garbled fields yield zero values with ok=false. Mirrors the aws
// extractMeta(m.MessageAttributes).
func extractMeta(headers []kgo.RecordHeader) (producerID string, seq uint64, crcHex string, sentAt time.Time, ok bool) {
	if len(headers) == 0 {
		return "", 0, "", time.Time{}, false
	}
	for _, h := range headers {
		switch h.Key {
		case payload.AttrWorkerID:
			producerID = string(h.Value)
		case payload.AttrContentHash:
			crcHex = string(h.Value)
		case payload.AttrSequence:
			if n, err := strconv.ParseUint(string(h.Value), 10, 64); err == nil {
				seq = n
			}
		case payload.AttrTimestampNS:
			if ns, err := strconv.ParseInt(string(h.Value), 10, 64); err == nil {
				sentAt = time.Unix(0, ns)
			}
		}
	}
	ok = producerID != ""
	return producerID, seq, crcHex, sentAt, ok
}
