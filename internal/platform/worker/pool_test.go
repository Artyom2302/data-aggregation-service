package worker

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func waitFor[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for %s", what)
		var zero T
		return zero
	}
}

func blockingTask(started chan<- struct{}, release <-chan struct{}) func(context.Context) error {
	return func(context.Context) error {
		started <- struct{}{}
		<-release
		return nil
	}
}

func TestPool_Submit_Validation(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name     string
		ctx      context.Context
		fn       func(context.Context) error
		shutdown bool
		wantErr  error
		wantText string
	}{
		{name: "nil fn", ctx: context.Background(), fn: nil, wantText: "fn cannot be nil"},
		{name: "canceled ctx", ctx: canceled, fn: func(context.Context) error { return nil }, wantErr: context.Canceled},
		{name: "after shutdown", ctx: context.Background(), fn: func(context.Context) error { return nil }, shutdown: true, wantText: "pool is closed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPool(1, 1, 1)
			if tt.shutdown {
				if err := p.Shutdown(t.Context()); err != nil {
					t.Fatalf("shutdown: %v", err)
				}
			} else {
				defer p.Shutdown(context.Background())
			}

			err := p.Submit(tt.ctx, tt.fn)

			if err == nil {
				t.Fatal("want error, got nil")
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("want %v, got %v", tt.wantErr, err)
			}
			if tt.wantText != "" && !strings.Contains(err.Error(), tt.wantText) {
				t.Errorf("want error containing %q, got %q", tt.wantText, err)
			}
		})
	}
}

func TestPool_Submit_RunsTask(t *testing.T) {
	p := NewPool(2, 1, 1)
	defer p.Shutdown(context.Background())

	type ctxKey struct{}
	ctx := context.WithValue(t.Context(), ctxKey{}, "marker")
	gotValue := make(chan any, 1)

	err := p.Submit(ctx, func(ctx context.Context) error {
		gotValue <- ctx.Value(ctxKey{})
		return nil
	})

	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	if v := waitFor(t, gotValue, "task to run"); v != "marker" {
		t.Errorf("task must receive the submit context, got value %v", v)
	}
}

func TestPool_Submit_FullQueue_RespectsContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewPool(1, 1, 1)
		started, release := make(chan struct{}, 1), make(chan struct{})

		_ = p.Submit(context.Background(), blockingTask(started, release))
		<-started
		_ = p.Submit(context.Background(), blockingTask(started, release))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		start := time.Now()

		err := p.Submit(ctx, func(context.Context) error { return nil })

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("want DeadlineExceeded, got %v", err)
		}
		if elapsed := time.Since(start); elapsed != time.Second {
			t.Errorf("submit must block until ctx deadline: want 1s, got %v", elapsed)
		}

		close(release)
		_ = p.Shutdown(context.Background())
	})
}

func TestPool_Submit_FullQueue_UnblocksWhenFreed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewPool(1, 1, 1)
		started, release := make(chan struct{}, 3), make(chan struct{})

		_ = p.Submit(context.Background(), blockingTask(started, release))
		<-started
		_ = p.Submit(context.Background(), blockingTask(started, release))

		submitted := make(chan error, 1)
		go func() {
			submitted <- p.Submit(context.Background(), blockingTask(started, release))
		}()

		synctest.Wait()
		select {
		case err := <-submitted:
			t.Fatalf("submit must block while queue is full, returned %v", err)
		default:
		}

		close(release) // задачи завершаются, очередь освобождается
		if err := <-submitted; err != nil {
			t.Errorf("submit after queue freed: %v", err)
		}
		_ = p.Shutdown(context.Background())
	})
}

func TestPool_Submit_ContextCanceledWhileQueued(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewPool(1, 1, 1)
		started, release := make(chan struct{}, 1), make(chan struct{})

		_ = p.Submit(context.Background(), blockingTask(started, release))
		<-started

		ctx, cancel := context.WithCancel(context.Background())
		var called atomic.Bool
		if err := p.Submit(ctx, func(context.Context) error { called.Store(true); return nil }); err != nil {
			t.Fatalf("submit: %v", err)
		}
		cancel()
		close(release)

		err := <-p.Errors()
		if !strings.Contains(err.Error(), "ctx was canceled before task") {
			t.Errorf("want canceled-before-task error, got %v", err)
		}
		_ = p.Shutdown(context.Background())
		if called.Load() {
			t.Error("fn must not be called when ctx is canceled before start")
		}
	})
}

// shutdownAsync запускает Shutdown в отдельной горутине: сам он блокирующий,
// а тесту нужно успеть что-то сделать (отпустить задачи, сдвинуть время), пока он ждёт.
func shutdownAsync(p *Pool, ctx context.Context) <-chan error {
	done := make(chan error, 1)
	go func() { done <- p.Shutdown(ctx) }()
	return done
}

func TestPool_Shutdown_Idle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewPool(3, 1, 1)

		if err := p.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown of idle pool: %v", err)
		}
		// Если хоть один воркер не завершился, synctest упадёт с deadlock — это проверка на утечку.
	})
}

func TestPool_Shutdown_CalledTwice(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewPool(1, 1, 1)

		if err := p.Shutdown(context.Background()); err != nil {
			t.Fatalf("first shutdown: %v", err)
		}
		err := p.Shutdown(context.Background())

		if err == nil || !strings.Contains(err.Error(), "shutdown already called") {
			t.Errorf("second shutdown: want 'already called' error, got %v", err)
		}
	})
}

func TestPool_Shutdown_WaitsForRunningTask(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewPool(1, 1, 1)
		started, release := make(chan struct{}, 1), make(chan struct{})
		_ = p.Submit(context.Background(), blockingTask(started, release))
		<-started

		done := shutdownAsync(p, context.Background())

		synctest.Wait() // все горутины заблокированы: Shutdown точно ждёт, а не просто не успел вернуться
		select {
		case err := <-done:
			t.Fatalf("shutdown must wait for running task, returned %v", err)
		default:
		}

		close(release)
		if err := <-done; err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
}

func TestPool_Shutdown_DrainsQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const queued = 5
		p := NewPool(1, queued, 1)
		started, release := make(chan struct{}, 1), make(chan struct{})
		_ = p.Submit(context.Background(), blockingTask(started, release))
		<-started

		var ran atomic.Int64
		for range queued {
			if err := p.Submit(context.Background(), func(context.Context) error { ran.Add(1); return nil }); err != nil {
				t.Fatalf("submit: %v", err)
			}
		}

		done := shutdownAsync(p, context.Background())
		synctest.Wait() // Shutdown вызван, пока задачи ещё в очереди
		close(release)

		if err := <-done; err != nil {
			t.Fatalf("shutdown: %v", err)
		}
		if got := ran.Load(); got != queued {
			t.Errorf("all queued tasks must run before shutdown returns: want %d, got %d", queued, got)
		}
	})
}

func TestPool_Shutdown_Timeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewPool(1, 1, 1)
		started, release := make(chan struct{}, 1), make(chan struct{})
		_ = p.Submit(context.Background(), blockingTask(started, release))
		<-started

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		done := shutdownAsync(p, ctx)

		time.Sleep(2 * time.Second) // ненастоящее время: проходит мгновенно, дедлайн Shutdown истёк
		close(release)

		err := <-done
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("want DeadlineExceeded, got %v", err)
		}
		// Сколько задач из очереди выполнится после таймаута, здесь не проверяем:
		// воркер выбирает между done, tasks и stop случайно, такой тест был бы нестабильным.
	})
}

// Shutdown должен вернуться не позже своего таймаута, даже если задача игнорирует ctx.
func TestPool_Shutdown_RespectsOwnTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewPool(1, 1, 1)
		_ = p.Submit(context.Background(), func(context.Context) error {
			time.Sleep(time.Minute)
			return nil
		})
		synctest.Wait()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		start := time.Now()

		err := p.Shutdown(ctx)

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("want DeadlineExceeded, got %v", err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("shutdown must return by its deadline: timeout 1s, took %v", elapsed)
		}

		// Задача продолжает работать после Shutdown — так и задумано. Даём ей доработать,
		// иначе synctest посчитает её утечкой; воркер затем увидит закрытый done и выйдет.
		time.Sleep(time.Minute)
	})
}

func TestPool_TaskError_ReportedToErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewPool(1, 2, 2)
		taskErr := errors.New("boom")

		_ = p.Submit(context.Background(), func(context.Context) error { return nil })
		_ = p.Submit(context.Background(), func(context.Context) error { return taskErr })
		_ = p.Shutdown(context.Background())

		select {
		case err := <-p.Errors():
			if !errors.Is(err, taskErr) {
				t.Errorf("want task error, got %v", err)
			}
		default:
			t.Fatal("task error must be sent to Errors()")
		}
		// Успешная задача ничего не отправляет в канал ошибок.
		select {
		case err := <-p.Errors():
			t.Errorf("unexpected extra error: %v", err)
		default:
		}
	})
}

func TestPool_Panic_RecoveredAndWorkerSurvives(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewPool(1, 2, 2) // один воркер: вторая задача выполнится, только если он пережил панику
		var ran atomic.Bool

		_ = p.Submit(context.Background(), func(context.Context) error { panic("oops") })
		_ = p.Submit(context.Background(), func(context.Context) error { ran.Store(true); return nil })
		_ = p.Shutdown(context.Background())

		err := <-p.Errors()
		var pe *PanicError
		if !errors.As(err, &pe) {
			t.Fatalf("want *PanicError, got %T: %v", err, err)
		}
		if pe.Recovered != "oops" {
			t.Errorf("recovered: want %q, got %v", "oops", pe.Recovered)
		}
		if len(pe.Stack) == 0 {
			t.Error("panic error must contain stack trace")
		}
		if !strings.Contains(pe.Error(), "oops") {
			t.Errorf("error text must contain panic value, got %q", pe.Error())
		}
		if !ran.Load() {
			t.Error("worker must keep running tasks after a panic")
		}
	})
}

func TestPool_DroppedErrors_WhenErrorsBufferFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const tasks, errBuf = 5, 2
		p := NewPool(1, tasks, errBuf)
		fail := func(context.Context) error { return errors.New("fail") }

		for range tasks {
			if err := p.Submit(context.Background(), fail); err != nil {
				t.Fatalf("submit: %v", err)
			}
		}
		_ = p.Shutdown(context.Background())

		if got := len(p.Errors()); got != errBuf {
			t.Errorf("errors buffer: want %d, got %d", errBuf, got)
		}
		if got := p.DroppedErrors(); got != tasks-errBuf {
			t.Errorf("dropped errors: want %d, got %d", tasks-errBuf, got)
		}
	})
}

func TestPool_RunsTasksConcurrently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const size = 3
		p := NewPool(size, size, 1)
		started, release := make(chan struct{}, size), make(chan struct{})

		for range size {
			_ = p.Submit(context.Background(), blockingTask(started, release))
		}

		synctest.Wait()
		// Все задачи блокируются до release, значит стартовать одновременно они могут только в разных воркерах.
		if got := len(started); got != size {
			t.Errorf("want %d tasks running in parallel, got %d", size, got)
		}

		close(release)
		_ = p.Shutdown(context.Background())
	})
}

func TestPool_Submit_BlockedUnblocksOnShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewPool(1, 1, 1)
		started, release := make(chan struct{}, 2), make(chan struct{})
		_ = p.Submit(context.Background(), blockingTask(started, release))
		<-started
		_ = p.Submit(context.Background(), blockingTask(started, release)) // очередь заполнена

		submitted := make(chan error, 1)
		go func() {
			submitted <- p.Submit(context.Background(), func(context.Context) error { return nil })
		}()
		synctest.Wait() // Submit заблокирован на полной очереди

		done := shutdownAsync(p, context.Background())

		err := <-submitted
		if err == nil || !strings.Contains(err.Error(), "pool is closed") {
			t.Errorf("blocked submit must fail on shutdown with 'pool is closed', got %v", err)
		}

		close(release)
		if err := <-done; err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
}
