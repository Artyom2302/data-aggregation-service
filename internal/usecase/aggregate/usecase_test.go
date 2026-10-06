package aggregate

import (
	"context"
	"data-aggregation-service/internal/domain"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// fakeSaver запоминает сохранённые пакеты и возвращает заранее заданную ошибку.
type fakeSaver struct {
	saved []domain.DataPackage
	ctx   context.Context
	err   error
}

func (f *fakeSaver) SaveDataPackage(ctx context.Context, pkg domain.DataPackage) error {
	f.ctx = ctx
	f.saved = append(f.saved, pkg)
	return f.err
}

type ctxKey struct{}

func newRaw(numbers ...int) domain.RawPackage {
	return domain.RawPackage{
		ID:            uuid.New(),
		CreatedAt:     time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC),
		RandomNumbers: numbers,
	}
}

func TestUseCase_Aggregate(t *testing.T) {
	saver := &fakeSaver{}
	raw := newRaw(3, 9, 1)
	ctx := context.WithValue(t.Context(), ctxKey{}, "marker")

	if err := New(saver).Aggregate(ctx, raw); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(saver.saved) != 1 {
		t.Fatalf("saver: want 1 call, got %d", len(saver.saved))
	}
	got := saver.saved[0]
	if got.ID != raw.ID {
		t.Errorf("id: want %s, got %s", raw.ID, got.ID)
	}
	if !got.CreatedAt.Equal(raw.CreatedAt) {
		t.Errorf("created_at: want %v, got %v", raw.CreatedAt, got.CreatedAt)
	}
	if got.Payload != 9 {
		t.Errorf("payload: want 9, got %d", got.Payload)
	}
	// Контекст должен дойти до saver: в нём таймаут задачи из consumer.
	if saver.ctx.Value(ctxKey{}) != "marker" {
		t.Error("saver must receive the caller's context")
	}
}

func TestUseCase_Aggregate_EmptyPackage(t *testing.T) {
	saver := &fakeSaver{}
	raw := newRaw()

	err := New(saver).Aggregate(t.Context(), raw)

	if !errors.Is(err, domain.ErrEmptyPackage) {
		t.Fatalf("want ErrEmptyPackage, got %v", err)
	}
	if !strings.Contains(err.Error(), raw.ID.String()) {
		t.Errorf("error must contain package id %s, got %q", raw.ID, err)
	}
	if len(saver.saved) != 0 {
		t.Errorf("saver must not be called for empty package, called %d times", len(saver.saved))
	}
}

func TestUseCase_SaverError(t *testing.T) {
	saveErr := errors.New("db is down")
	saver := &fakeSaver{err: saveErr}

	err := New(saver).Aggregate(t.Context(), newRaw(1, 2, 3))

	if !errors.Is(err, saveErr) {
		t.Fatalf("want saver error, got %v", err)
	}
	if len(saver.saved) != 1 {
		t.Errorf("saver: want 1 call, got %d", len(saver.saved))
	}
}
