package worker

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

type Pool struct {
	tasks chan Task
	errs  chan error
	stop  chan struct{}
	done  chan struct{}

	wg            sync.WaitGroup
	once          sync.Once
	droppedErrors atomic.Uint64
}

type Task struct {
	ctx context.Context
	fn  func(ctx context.Context) error
}

type PanicError struct {
	Recovered any
	Stack     []byte
}

func (p *Pool) worker() {
	defer p.wg.Done()

	for {
		select {
		case <-p.done:
			return
		case task := <-p.tasks:
			p.runTask(task)

		case <-p.stop:
			p.drainAndExit()
			return
		}
	}
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("panic: %v", e.Recovered)
}

func (p *Pool) Errors() <-chan error {
	return p.errs
}
func (p *Pool) DroppedErrors() uint64 {
	return p.droppedErrors.Load()
}

func (p *Pool) runTask(t Task) {
	defer func() {
		if err := recover(); err != nil {
			p.sendErr(&PanicError{Recovered: err, Stack: debug.Stack()})
		}
	}()

	if err := t.ctx.Err(); err != nil {
		p.sendErr(fmt.Errorf("workerpool: ctx was canceled before task: %v", err))
		return
	}

	if err := t.fn(t.ctx); err != nil {
		p.sendErr(err)
	}
}

func (p *Pool) drainAndExit() {
	for {
		select {
		case <-p.done:
			return
		case task := <-p.tasks:
			p.runTask(task)
		default:
			return
		}
	}
}

func (p *Pool) sendErr(err error) {
	select {
	case p.errs <- err:
	default:
		p.droppedErrors.Add(1)
	}
}

func NewPool(size, tasksBuf, errorsBuf int) *Pool {
	p := &Pool{
		tasks: make(chan Task, tasksBuf),
		errs:  make(chan error, errorsBuf),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}

	p.wg.Add(size)
	for range size {
		go p.worker()
	}

	return p
}

func (p *Pool) Submit(ctx context.Context, fn func(ctx context.Context) error) error {
	if fn == nil {
		return fmt.Errorf("fn cannot be nil")
	}

	select {
	case <-p.stop:
		return fmt.Errorf("pool is closed")
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	t := Task{ctx: ctx, fn: fn}
	select {
	case p.tasks <- t:
		return nil
	case <-p.stop:
		return fmt.Errorf("pool is closed")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Pool) Shutdown(ctx context.Context) error {
	called := false
	p.once.Do(func() {
		called = true
		close(p.stop)
	})
	if !called {
		return fmt.Errorf("shutdown already called")
	}

	drained := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(drained)
	}()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		// Воркеры не возьмут новых задач, но уже запущенные доработают сами:
		// ждать их нельзя, иначе зависшая задача держит Shutdown дольше его таймаута.
		close(p.done)
		return fmt.Errorf("shutdown timed out: %w", ctx.Err())
	}
}
