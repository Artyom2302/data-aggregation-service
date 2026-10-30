package postgres

import (
	"context"
	"data-aggregation-service/internal/domain"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pgx *pgxpool.Pool
}

// row — представление строки таблицы; наружу отдаём только domain.DataPackage.
type row struct {
	ID        uuid.UUID `db:"id"`
	CreatedAt time.Time `db:"created_at"`
	Payload   int       `db:"payload"`
}

func New(ctx context.Context, dsn string) (*Repository, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("could not connect to postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("could not ping postgres: %w", err)
	}

	return &Repository{pool}, nil
}

func (r *Repository) Close() {
	r.pgx.Close()
}

func (r *Repository) SaveDataPackage(ctx context.Context, data domain.DataPackage) error {
	_, err := r.pgx.Exec(ctx,

		`INSERT INTO data_package (id, created_at, payload) VALUES ($1, $2, $3)
		ON CONFLICT (id) DO NOTHING`,
		data.ID, data.CreatedAt, data.Payload,
	)
	if err != nil {
		return fmt.Errorf("could not save data_package: %w", err)
	}

	return nil
}

func (r *Repository) GetDataPackagesByIdsAndTimeRange(ctx context.Context, ids []uuid.UUID, from, to time.Time) ([]domain.DataPackage, error) {
	rows, err := r.pgx.Query(ctx,
		`SELECT id, created_at, payload FROM data_package
         WHERE id = ANY($1) AND created_at BETWEEN $2 AND $3
         ORDER BY created_at`,
		ids, from, to,
	)
	if err != nil {
		return nil, fmt.Errorf("could not get data_package: %w", err)
	}

	data, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.DataPackage, error) {
		v, err := pgx.RowToStructByName[row](r)
		if err != nil {
			return domain.DataPackage{}, err
		}
		return domain.DataPackage{ID: v.ID, CreatedAt: v.CreatedAt, Payload: v.Payload}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("could not get data_package: %w", err)
	}

	return data, nil
}
