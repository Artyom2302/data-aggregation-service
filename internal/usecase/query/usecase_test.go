package query

import (
	"context"
	"data-aggregation-service/internal/domain"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
)

// fakeReader запоминает аргументы вызова и возвращает заранее заданный результат.
type fakeReader struct {
	calls    int
	gotIDs   []uuid.UUID
	gotFrom  time.Time
	gotTo    time.Time
	packages []domain.DataPackage
	err      error
}

func (f *fakeReader) GetDataPackagesByIdsAndTimeRange(_ context.Context, ids []uuid.UUID, from, to time.Time) ([]domain.DataPackage, error) {
	f.calls++
	f.gotIDs, f.gotFrom, f.gotTo = ids, from, to
	return f.packages, f.err
}

func newIDs(n int) []uuid.UUID {
	ids := make([]uuid.UUID, n)
	for i := range ids {
		ids[i] = uuid.New()
	}
	return ids
}

var (
	from = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to   = time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)
)

func TestUseCase_GetPackages_Validation(t *testing.T) {
	tests := []struct {
		name     string
		ids      []uuid.UUID
		from, to time.Time
	}{
		{"ids nil", nil, from, to},
		{"ids empty", []uuid.UUID{}, from, to},
		{"too many ids", newIDs(MaxIDs + 1), from, to},
		{"from after to", newIDs(1), to, from},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := &fakeReader{}

			got, err := New(reader).GetPackages(t.Context(), tt.ids, tt.from, tt.to)

			if !errors.Is(err, domain.ErrInvalidArgument) {
				t.Fatalf("want ErrInvalidArgument, got %v", err)
			}
			if got != nil {
				t.Errorf("want nil result, got %v", got)
			}
			if reader.calls != 0 {
				t.Errorf("reader must not be called on invalid input, called %d times", reader.calls)
			}
		})
	}
}

func TestUseCase_GetPackages(t *testing.T) {
	tests := []struct {
		name     string
		ids      []uuid.UUID
		from, to time.Time
		packages []domain.DataPackage
	}{
		{
			name:     "found",
			ids:      newIDs(2),
			from:     from,
			to:       to,
			packages: []domain.DataPackage{{ID: uuid.New(), CreatedAt: from, Payload: 7}},
		},
		{
			name: "nothing found",
			ids:  newIDs(1),
			from: from,
			to:   to,
		},
		{
			name: "exactly MaxIDs",
			ids:  newIDs(MaxIDs),
			from: from,
			to:   to,
		},
		{
			name: "from equals to",
			ids:  newIDs(1),
			from: from,
			to:   from,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := &fakeReader{packages: tt.packages}

			got, err := New(reader).GetPackages(t.Context(), tt.ids, tt.from, tt.to)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !slices.Equal(got, tt.packages) {
				t.Errorf("result: want %v, got %v", tt.packages, got)
			}
			if reader.calls != 1 {
				t.Fatalf("reader: want 1 call, got %d", reader.calls)
			}
			if !slices.Equal(reader.gotIDs, tt.ids) {
				t.Errorf("reader ids: want %v, got %v", tt.ids, reader.gotIDs)
			}
			if !reader.gotFrom.Equal(tt.from) || !reader.gotTo.Equal(tt.to) {
				t.Errorf("reader range: want [%v, %v], got [%v, %v]", tt.from, tt.to, reader.gotFrom, reader.gotTo)
			}
		})
	}
}

func TestUseCase_GetPackages_ReaderFailed(t *testing.T) {
	readerErr := errors.New("db is down")
	reader := &fakeReader{err: readerErr}

	_, err := New(reader).GetPackages(t.Context(), newIDs(1), from, to)

	if !errors.Is(err, readerErr) {
		t.Fatalf("want reader error, got %v", err)
	}
	if errors.Is(err, domain.ErrInvalidArgument) {
		t.Error("reader error must not be reported as ErrInvalidArgument")
	}
}
