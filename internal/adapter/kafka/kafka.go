package kafka

import (
	"context"
	"data-aggregation-service/internal/domain"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

type RawPackageConsumer struct {
	kafkaClient *kgo.Client
	log         *slog.Logger
}

func NewKafkaAdapter(ctx context.Context, brokerAddress []string, topicName, groupID string, log *slog.Logger) (*RawPackageConsumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddress...),
		kgo.ConsumeTopics(topicName),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AutoCommitMarks(),
	)
	if err != nil {
		return nil, fmt.Errorf("could not create kafka client: %w", err)
	}
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("could not ping kafka client: %w", err)
	}
	return &RawPackageConsumer{kafkaClient: client, log: log}, nil
}

func (c *RawPackageConsumer) Run(ctx context.Context, out chan<- domain.RawPackage) error {
	defer close(out)

	for {
		fetches := c.kafkaClient.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			if errors.Is(err, context.Canceled) {
				return
			}
			c.log.Error("kafka fetch", "topic", topic, "partition", partition, "err", err)
		})

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()

			raw, err := decode(record.Value)
			if err != nil {
				c.log.Error("decode kafka record", "offset", record.Offset, "key", string(record.Key), "err", err)
				c.kafkaClient.MarkCommitRecords(record)
				continue
			}

			select {
			case <-ctx.Done():
				return nil
			case out <- raw:
				c.kafkaClient.MarkCommitRecords(record)
			}
		}
	}
}

func (c *RawPackageConsumer) Close() {
	c.kafkaClient.Close()
}

func decode(value []byte) (domain.RawPackage, error) {
	var msg rawPackageMessage
	if err := json.Unmarshal(value, &msg); err != nil {
		return domain.RawPackage{}, fmt.Errorf("unmarshal: %w", err)
	}
	return msg.toDomain(), nil
}
