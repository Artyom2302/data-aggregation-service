package postgres

import (
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

func Migrate(dsn, migrationsPath string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("sql.Open(): %w", err)
	}
	defer db.Close()

	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("goose.SetDialect(): %w", err)
	}
	if err := goose.Up(db, migrationsPath); err != nil {
		return fmt.Errorf("goose.Up(): %w", err)
	}

	return nil
}
