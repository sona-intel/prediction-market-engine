// Package pgstore keeps what pm-collect records from the extra venues in Postgres
package pgstore

import (
	"context"
	"fmt"
	"io/fs"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"pm/internal/pgstore/sql"
)

const maxConns = 4

// Numeric columns are bigint micro-USDC and integer ppm, so no digit is lost
type Store struct {
	Pool *pgxpool.Pool
}

// Connect leaves schema empty in production, where the role's search_path decides, and tests pass their own
func Connect(ctx context.Context, dsn, schema string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres config: %w", err)
	}
	cfg.MaxConns = maxConns
	if schema != "" {
		cfg.ConnConfig.RuntimeParams["search_path"] = pgx.Identifier{schema}.Sanitize()
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres pool: %w", err)
	}
	return &Store{Pool: pool}, nil
}

func (s *Store) Close() { s.Pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.Pool.Ping(ctx) }

// Migrate runs the pending files in one transaction behind an advisory lock, and every file is safe to run twice
func (s *Store) Migrate(ctx context.Context) error {
	files, err := fs.Glob(sql.FS, "*.sql")
	if err != nil {
		return err
	}
	slices.Sort(files)
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `select pg_advisory_xact_lock(hashtext('pm_migrate:' || current_schema()))`); err != nil {
			return fmt.Errorf("lock migrations: %w", err)
		}
		const create = `create table if not exists schema_migrations (version text primary key, applied_at timestamptz not null default now())`
		if _, err := tx.Exec(ctx, create); err != nil {
			return fmt.Errorf("create schema_migrations: %w", err)
		}
		rows, err := tx.Query(ctx, `select version from schema_migrations`)
		if err != nil {
			return fmt.Errorf("read schema_migrations: %w", err)
		}
		applied, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return fmt.Errorf("read schema_migrations: %w", err)
		}
		for _, name := range files {
			if slices.Contains(applied, name) {
				continue
			}
			body, err := sql.FS.ReadFile(name)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, string(body)); err != nil {
				return fmt.Errorf("migration %s: %w", name, err)
			}
			if _, err := tx.Exec(ctx, `insert into schema_migrations (version) values ($1)`, name); err != nil {
				return fmt.Errorf("record migration %s: %w", name, err)
			}
		}
		return nil
	})
}
