package transport

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// CreateTopic creates a topic with the given partition count / replication
// factor. It is idempotent: an already-existing topic (TOPIC_ALREADY_EXISTS) is
// treated as success (mirror aws CreateQueue idempotency).
func CreateTopic(ctx context.Context, adm *kadm.Client, topic string, partitions, rf int) error {
	_, err := adm.CreateTopic(ctx, int32(partitions), int16(rf), nil, topic)
	if err != nil {
		if errors.Is(err, kerr.TopicAlreadyExists) {
			return nil
		}
		return fmt.Errorf("create topic %s: %w", topic, err)
	}
	return nil
}

// DeleteTopic deletes a topic (best-effort; a missing topic is not an error).
func DeleteTopic(ctx context.Context, adm *kadm.Client, topic string) error {
	resp, err := adm.DeleteTopics(ctx, topic)
	if err != nil {
		return fmt.Errorf("delete topic %s: %w", topic, err)
	}
	if r, ok := resp[topic]; ok && r.Err != nil && !errors.Is(r.Err, kerr.UnknownTopicOrPartition) {
		return fmt.Errorf("delete topic %s: %w", topic, r.Err)
	}
	return nil
}

// CreatePartitions SETS the total partition count of a topic to newTotal
// (kadm.UpdatePartitions). Increase-only: a same/decrease/>256 request returns
// INVALID_PARTITIONS, which the admin_topic_churn worker uses as its negative
// probe. The returned error is the per-topic error, so callers can assert it.
func CreatePartitions(ctx context.Context, adm *kadm.Client, topic string, newTotal int) error {
	resp, err := adm.UpdatePartitions(ctx, newTotal, topic)
	if err != nil {
		return fmt.Errorf("update partitions %s -> %d: %w", topic, newTotal, err)
	}
	if r, ok := resp[topic]; ok && r.Err != nil {
		return r.Err
	}
	return resp.Error()
}

// EndOffsets returns the high-watermark (log-end) offsets per partition for a
// topic.
func EndOffsets(ctx context.Context, adm *kadm.Client, topic string) (kadm.ListedOffsets, error) {
	return adm.ListEndOffsets(ctx, topic)
}

// StartOffsets returns the log-start offsets per partition for a topic.
func StartOffsets(ctx context.Context, adm *kadm.Client, topic string) (kadm.ListedOffsets, error) {
	return adm.ListStartOffsets(ctx, topic)
}

// CommittedOffsets returns the committed offsets for a consumer group.
func CommittedOffsets(ctx context.Context, adm *kadm.Client, group string) (kadm.OffsetResponses, error) {
	return adm.FetchOffsets(ctx, group)
}

// GroupLag returns the per-(topic,partition) lag (HWM - committed) for a group.
// offset == STAN Sequence, so the lag is exact and can be compared against the
// tracker's true (HWM - committed) by the offset_commit_lag worker.
func GroupLag(ctx context.Context, adm *kadm.Client, group string) (kadm.GroupLag, error) {
	lags, err := adm.Lag(ctx, group)
	if err != nil {
		return nil, fmt.Errorf("describe group lag %s: %w", group, err)
	}
	dl, ok := lags[group]
	if !ok {
		return kadm.GroupLag{}, nil
	}
	if err := dl.Error(); err != nil {
		return nil, fmt.Errorf("group lag %s: %w", group, err)
	}
	return dl.Lag, nil
}
