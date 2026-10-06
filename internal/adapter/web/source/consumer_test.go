package source

import (
	"bytes"
	"context"
	"data-aggregation-service/internal/domain"
	"data-aggregation-service/internal/platform/worker"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
)

const testTaskTimeout = 5 * time.Second

type fakeAggregator struct {
	mu   sync.Mutex
	raws []domain.RawPackage
	ctxs []context.Context
	fn   func(ctx context.Context, raw domain.RawPackage) error
}

func (f *fakeAggregator) Aggregate(ctx context.Context, raw domain.RawPackage) error {
	f.mu.Lock()
	f.raws = append(f.raws, raw)
	f.ctxs = append(f.ctxs, ctx)
	f.mu.Unlock()
	if f.fn != nil {
		return f.fn(ctx, raw)
	}
	return nil
}

func (f *fakeAggregator) got() []domain.RawPackage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]domain.RawPackage(nil), f.raws...)
}

type syncSubmitter struct {
	mu        sync.Mutex
	ctxs      []context.Context
	taskErrs  []error
	submitErr func(call int) error
}

func (s *syncSubmitter) Submit(ctx context.Context, fn func(ctx context.Context) error) error {
	s.mu.Lock()
	call := len(s.ctxs)
	s.ctxs = append(s.ctxs, ctx)
	s.mu.Unlock()

	if s.submitErr != nil {
		if err := s.submitErr(call); err != nil {
			return err
		}
	}
	err := fn(ctx)
	s.mu.Lock()
	s.taskErrs = append(s.taskErrs, err)
	s.mu.Unlock()
	return nil
}

func newRaw(numbers ...int) domain.RawPackage {
	return domain.RawPackage{
		ID:            uuid.New(),
		CreatedAt:     time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC),
		RandomNumbers: numbers,
	}
}

func newLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, nil)), &buf
}

func runAsync(ctx context.Context, c *Consumer) <-chan error {
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	return done
}

func TestConsumer_Run_ProcessesAllPackagesUntilChannelClosed(t *testing.T) {
	raws := []domain.RawPackage{newRaw(1), newRaw(2, 3), newRaw(4, 5, 6)}
	packages := make(chan domain.RawPackage, len(raws))
	for _, r := range raws {
		packages <- r
	}
	close(packages)
	agg := &fakeAggregator{}
	log, logs := newLogger()

	err := NewConsumer(packages, agg, &syncSubmitter{}, testTaskTimeout, log).Run(t.Context())

	if err != nil {
		t.Fatalf("run: %v", err)
	}
	got := agg.got()
	if len(got) != len(raws) {
		t.Fatalf("aggregator: want %d calls, got %d", len(raws), len(got))
	}
	for i := range raws {
		if got[i].ID != raws[i].ID {
			t.Errorf("package %d: want id %s, got %s", i, raws[i].ID, got[i].ID)
		}
	}
	if logs.Len() != 0 {
		t.Errorf("nothing must be logged on success, got %q", logs)
	}
}

func TestConsumer_Run_StopsOnContextCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		packages := make(chan domain.RawPackage) // никогда не закрывается
		agg := &fakeAggregator{}
		log, _ := newLogger()
		ctx, cancel := context.WithCancel(context.Background())

		done := runAsync(ctx, NewConsumer(packages, agg, &syncSubmitter{}, testTaskTimeout, log))

		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("run must block while ctx is alive, returned %v", err)
		default:
		}

		cancel()
		if err := <-done; err != nil {
			t.Errorf("run after cancel: want nil, got %v", err)
		}
		if n := len(agg.got()); n != 0 {
			t.Errorf("aggregator must not be called, called %d times", n)
		}
	})
}

func TestConsumer_Run_SubmitErrorIsLoggedAndConsumingContinues(t *testing.T) {
	failed, ok := newRaw(1), newRaw(2)
	packages := make(chan domain.RawPackage, 2)
	packages <- failed
	packages <- ok
	close(packages)
	agg := &fakeAggregator{}
	sub := &syncSubmitter{submitErr: func(call int) error {
		if call == 0 {
			return errors.New("queue is full")
		}
		return nil
	}}
	log, logs := newLogger()

	err := NewConsumer(packages, agg, sub, testTaskTimeout, log).Run(t.Context())

	if err != nil {
		t.Fatalf("run: submit error must not stop consumer, got %v", err)
	}
	got := agg.got()
	if len(got) != 1 || got[0].ID != ok.ID {
		t.Errorf("aggregator: want only package %s, got %v", ok.ID, got)
	}
	out := logs.String()
	for _, want := range []string{"submit package", failed.ID.String(), "queue is full"} {
		if !strings.Contains(out, want) {
			t.Errorf("log must contain %q, got %q", want, out)
		}
	}
}

func TestConsumer_Run_SubmitErrorAfterCancel_ReturnsSilently(t *testing.T) {
	packages := make(chan domain.RawPackage, 2)
	packages <- newRaw(1)
	packages <- newRaw(2)
	ctx, cancel := context.WithCancel(t.Context())
	sub := &syncSubmitter{submitErr: func(int) error {
		cancel()
		return errors.New("pool is closed")
	}}
	agg := &fakeAggregator{}
	log, logs := newLogger()

	err := NewConsumer(packages, agg, sub, testTaskTimeout, log).Run(ctx)

	if err != nil {
		t.Fatalf("run: want nil, got %v", err)
	}
	if len(sub.ctxs) != 1 {
		t.Errorf("consumer must stop after first failed submit, submitted %d times", len(sub.ctxs))
	}
	if logs.Len() != 0 {
		t.Errorf("submit error during shutdown must not be logged, got %q", logs)
	}
}

func TestConsumer_Run_TaskErrorDoesNotStopConsumer(t *testing.T) {
	packages := make(chan domain.RawPackage, 2)
	packages <- newRaw()
	packages <- newRaw(1)
	close(packages)
	aggErr := errors.New("db is down")
	agg := &fakeAggregator{fn: func(context.Context, domain.RawPackage) error { return aggErr }}
	sub := &syncSubmitter{}
	log, _ := newLogger()

	err := NewConsumer(packages, agg, sub, testTaskTimeout, log).Run(t.Context())

	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(agg.got()) != 2 {
		t.Errorf("aggregator: want 2 calls, got %d", len(agg.got()))
	}

	for i, e := range sub.taskErrs {
		if !errors.Is(e, aggErr) {
			t.Errorf("task %d: want aggregator error, got %v", i, e)
		}
	}
}

func TestConsumer_SubmitContext_DetachedFromRunCancel(t *testing.T) {
	type ctxKey struct{}
	packages := make(chan domain.RawPackage, 1)
	packages <- newRaw(1)
	close(packages)
	ctx, cancel := context.WithCancel(context.WithValue(t.Context(), ctxKey{}, "marker"))
	sub := &syncSubmitter{}
	log, _ := newLogger()

	if err := NewConsumer(packages, &fakeAggregator{}, sub, testTaskTimeout, log).Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	cancel()

	if len(sub.ctxs) != 1 {
		t.Fatalf("submitter: want 1 call, got %d", len(sub.ctxs))
	}
	got := sub.ctxs[0]
	if got.Err() != nil {
		t.Errorf("submit ctx must not be canceled with run ctx, got %v", got.Err())
	}
	if got.Value(ctxKey{}) != "marker" {
		t.Error("submit ctx must keep values of run ctx")
	}
}

func TestConsumer_Task_HasTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		packages := make(chan domain.RawPackage, 1)
		packages <- newRaw(1)
		close(packages)
		var elapsed time.Duration
		agg := &fakeAggregator{fn: func(ctx context.Context, _ domain.RawPackage) error {
			start := time.Now()
			<-ctx.Done()
			elapsed = time.Since(start)
			return ctx.Err()
		}}
		sub := &syncSubmitter{}
		log, _ := newLogger()

		if err := NewConsumer(packages, agg, sub, testTaskTimeout, log).Run(context.Background()); err != nil {
			t.Fatalf("run: %v", err)
		}

		if elapsed != testTaskTimeout {
			t.Errorf("task must be canceled after %v, took %v", testTaskTimeout, elapsed)
		}
		if len(sub.taskErrs) != 1 || !errors.Is(sub.taskErrs[0], context.DeadlineExceeded) {
			t.Errorf("want DeadlineExceeded from task, got %v", sub.taskErrs)
		}
	})
}

func TestConsumer_WithWorkerPool(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const n = 10
		packages := make(chan domain.RawPackage, n)
		for i := range n {
			packages <- newRaw(i + 1)
		}
		close(packages)
		agg := &fakeAggregator{}
		pool := worker.NewPool(3, 2, n)
		log, _ := newLogger()

		if err := NewConsumer(packages, agg, pool, testTaskTimeout, log).Run(context.Background()); err != nil {
			t.Fatalf("run: %v", err)
		}
		if err := pool.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown: %v", err)
		}

		if got := len(agg.got()); got != n {
			t.Errorf("aggregator: want %d calls, got %d", n, got)
		}
		select {
		case err := <-pool.Errors():
			t.Errorf("unexpected pool error: %v", err)
		default:
		}
	})
}
