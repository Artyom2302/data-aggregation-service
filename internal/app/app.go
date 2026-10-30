package app

import (
	"context"
	pb "data-aggregation-service/gen/dataaggregation/v1"
	"data-aggregation-service/internal/adapter/kafka"
	"data-aggregation-service/internal/adapter/storage/postgres"
	"data-aggregation-service/internal/adapter/web/grpcapi"
	"data-aggregation-service/internal/adapter/web/rest"
	"data-aggregation-service/internal/adapter/web/source"
	"data-aggregation-service/internal/config"
	"data-aggregation-service/internal/domain"
	"data-aggregation-service/internal/platform/worker"
	"data-aggregation-service/internal/usecase/aggregate"
	"data-aggregation-service/internal/usecase/query"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	initTimeout = 10 * time.Second
	taskTimeout = 5 * time.Second
)

func Run(ctx context.Context, cfg *config.Config, log *slog.Logger) error {
	if err := postgres.Migrate(cfg.Database.Dsn, cfg.Database.MigrationsPath); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	initCtx, cancel := context.WithTimeout(ctx, initTimeout)
	repo, err := postgres.New(initCtx, cfg.Database.Dsn)
	cancel()
	if err != nil {
		return err
	}
	defer repo.Close()

	aggregateUC := aggregate.New(repo)
	queryUC := query.New(repo)
	handler := rest.NewHandler(log, queryUC)

	httpSrv := &http.Server{
		Addr:              cfg.HTTP.Addr,
		Handler:           http.TimeoutHandler(handler.Routes(), cfg.HTTP.HandlerTimeout, "request timeout"),
		ReadHeaderTimeout: cfg.HTTP.ReadHeaderTimeout,
		ReadTimeout:       cfg.HTTP.ReadTimeout,
		WriteTimeout:      cfg.HTTP.WriteTimeout,
		IdleTimeout:       cfg.HTTP.IdleTimeout,
	}
	grpcSrv := grpc.NewServer()
	pb.RegisterDataAggregationServiceServer(grpcSrv, grpcapi.NewServer(log, queryUC))
	reflection.Register(grpcSrv)
	lis, err := net.Listen("tcp", cfg.GRPC.Addr)
	if err != nil {
		return fmt.Errorf("failed to grpc listen: %w", err)
	}

	wp := worker.NewPool(cfg.WorkerPool.Size, cfg.WorkerPool.TasksBuf, cfg.WorkerPool.ErrorsBuf)
	go func() {
		for err := range wp.Errors() {
			log.Error("worker task failed", "err", err)
		}
	}()

	initCtx, cancel = context.WithTimeout(ctx, initTimeout)
	kafkaConsumer, err := kafka.NewKafkaAdapter(initCtx, cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.GroupID, log)
	cancel()
	if err != nil {
		return err
	}
	defer kafkaConsumer.Close()

	packages := make(chan domain.RawPackage)
	consumer := source.NewConsumer(packages, aggregateUC, wp, taskTimeout, log)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		log.Info("kafka consumer started", "topic", cfg.Kafka.Topic, "group", cfg.Kafka.GroupID)
		return kafkaConsumer.Run(gctx, packages)
	})
	g.Go(func() error {
		return consumer.Run(gctx)
	})
	g.Go(func() error {
		log.Info("http server started", "addr", httpSrv.Addr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("rest http server error: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		log.Info("grpc server started", "addr", cfg.GRPC.Addr)
		if err := grpcSrv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return fmt.Errorf("grpc server start error: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		<-gctx.Done()
		log.Info("shutting down")

		shCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()

		var grpcErr, httpErr, wpErr error
		if err := shutdownGRPC(shCtx, grpcSrv); err != nil {
			grpcErr = fmt.Errorf("grpc server shutdown: %w", err)
		}
		if err := httpSrv.Shutdown(shCtx); err != nil {
			httpErr = fmt.Errorf("http server shutdown: %w", err)
		}
		if err := wp.Shutdown(shCtx); err != nil {
			wpErr = fmt.Errorf("worker pool shutdown: %w", err)
		}
		return errors.Join(grpcErr, httpErr, wpErr)
	})

	return g.Wait()
}

func shutdownGRPC(ctx context.Context, srv *grpc.Server) error {
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		srv.Stop()
		<-done
		return ctx.Err()
	}
}
