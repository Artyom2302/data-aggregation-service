package postgres

import (
	"context"
	"data-aggregation-service/internal/domain"
	"math"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

const migrationsPath = "../../../../migrations"

func newTestRepository(t *testing.T) *Repository {
	t.Helper()

	dsn := os.Getenv("DSN_URL")
	if dsn == "" {
		t.Skip("DSN_URL is not set: skipping Postgres integration test")
	}
	if err := Migrate(dsn, migrationsPath); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	repo, err := New(t.Context(), dsn)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(repo.Close)

	return repo
}

// cleanup удаляет только строки теста: база общая с локально запущенным сервисом.
func cleanup(t *testing.T, repo *Repository, ids ...uuid.UUID) {
	t.Helper()

	t.Cleanup(func() {
		if _, err := repo.pgx.Exec(context.Background(), `delete from data_package where id = any($1)`, ids); err != nil {
			t.Errorf("clean data_package: %v", err)
		}
	})
}

func TestGetReturnsSavedPackagesInRange(t *testing.T) {
	repo := newTestRepository(t)

	base := time.Now().Truncate(time.Microsecond)
	saved := make([]domain.DataPackage, 3)
	ids := make([]uuid.UUID, len(saved))
	for i := range saved {
		saved[i] = domain.DataPackage{
			ID:        uuid.New(),
			CreatedAt: base.Add(time.Duration(i) * time.Second),
			// Проверяем, что payload влезает в bigint.
			Payload: math.MaxInt64 - i,
		}
		ids[i] = saved[i].ID
	}
	cleanup(t, repo, ids...)

	// Сохраняем в обратном порядке, чтобы проверить сортировку по created_at.
	for i := len(saved) - 1; i >= 0; i-- {
		if err := repo.SaveDataPackage(t.Context(), saved[i]); err != nil {
			t.Fatalf("SaveDataPackage(%d) error = %v", i, err)
		}
	}

	// Последний пакет не попадает в диапазон, чужой id не находится.
	got, err := repo.GetDataPackagesByIdsAndTimeRange(t.Context(), append(ids, uuid.New()), base, base.Add(time.Second))
	if err != nil {
		t.Fatalf("GetDataPackagesByIdsAndTimeRange() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("GetDataPackagesByIdsAndTimeRange() returned %d packages, want 2", len(got))
	}

	for i, want := range saved[:2] {
		if got[i].ID != want.ID {
			t.Errorf("got[%d].ID = %s, want %s", i, got[i].ID, want.ID)
		}
		if !got[i].CreatedAt.Equal(want.CreatedAt) {
			t.Errorf("got[%d].CreatedAt = %v, want %v", i, got[i].CreatedAt, want.CreatedAt)
		}
		if got[i].Payload != want.Payload {
			t.Errorf("got[%d].Payload = %d, want %d", i, got[i].Payload, want.Payload)
		}
	}
}

func TestSaveDuplicateKeepsFirstPackage(t *testing.T) {
	repo := newTestRepository(t)

	first := domain.DataPackage{
		ID:        uuid.New(),
		CreatedAt: time.Now().Truncate(time.Microsecond),
		Payload:   1,
	}
	cleanup(t, repo, first.ID)

	if err := repo.SaveDataPackage(t.Context(), first); err != nil {
		t.Fatalf("SaveDataPackage(first) error = %v", err)
	}
	duplicate := first
	duplicate.Payload = 2
	if err := repo.SaveDataPackage(t.Context(), duplicate); err != nil {
		t.Fatalf("SaveDataPackage(duplicate) error = %v, want nil", err)
	}

	got, err := repo.GetDataPackagesByIdsAndTimeRange(t.Context(), []uuid.UUID{first.ID}, first.CreatedAt, first.CreatedAt)
	if err != nil {
		t.Fatalf("GetDataPackagesByIdsAndTimeRange() error = %v", err)
	}
	if len(got) != 1 || got[0].Payload != first.Payload {
		t.Fatalf("GetDataPackagesByIdsAndTimeRange() = %v, want only payload %d", got, first.Payload)
	}
}
