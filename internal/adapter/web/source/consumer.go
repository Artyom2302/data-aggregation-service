package source

import (
	"context"
	"data-aggregation-service/internal/domain"
	"log/slog"
	"time"
)

type Aggregator interface {
	Aggregate(ctx context.Context, raw domain.RawPackage) error
}

type Submitter interface {
	Submit(ctx context.Context, fn func(ctx context.Context) error) error
}

type Consumer struct {
	packages    <-chan domain.RawPackage
	aggregator  Aggregator
	pool        Submitter
	taskTimeout time.Duration
	log         *slog.Logger
}

func NewConsumer(packages <-chan domain.RawPackage, aggregator Aggregator, pool Submitter, taskTimeout time.Duration, log *slog.Logger) *Consumer {
	return &Consumer{
		packages:    packages,
		aggregator:  aggregator,
		pool:        pool,
		taskTimeout: taskTimeout,
		log:         log,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case raw, ok := <-c.packages:
			if !ok {
				return nil
			}
			if err := c.submit(ctx, raw); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				c.log.Error("submit package", "id", raw.ID, "err", err)
			}
		}
	}
}

func (c *Consumer) submit(ctx context.Context, raw domain.RawPackage) error {
	return c.pool.Submit(context.WithoutCancel(ctx), func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, c.taskTimeout)
		defer cancel()
		return c.aggregator.Aggregate(ctx, raw)
	})
}
