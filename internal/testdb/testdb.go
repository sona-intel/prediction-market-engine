package testdb

import (
	"context"
	"database/sql"
	"path/filepath"

	"pm/internal/dataset"
	"pm/internal/golden"
	"pm/internal/publish"
	"pm/internal/stage"
)

const maxPairs = 1 << 30

func Build(ctx context.Context, dir string) (dbPath string, err error) {
	return BuildFrom(ctx, golden.Path("dataset"), dir)
}

func BuildFrom(ctx context.Context, root, dir string) (dbPath string, err error) {
	work, err := Work(ctx, root, dir)
	if err != nil {
		return "", err
	}
	defer work.Close()
	if err := publish.Run(ctx, work, dir, nil); err != nil {
		return "", err
	}
	return filepath.Join(dir, "pm.duckdb"), nil
}

func Work(ctx context.Context, root, dir string) (*sql.DB, error) {
	db, err := dataset.Open(ctx, dataset.Config{Path: filepath.Join(dir, "work.duckdb"), ExtDir: filepath.Join(dir, "ext")})
	if err != nil {
		return nil, err
	}
	src, err := dataset.NewSource(ctx, db, root)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	o := stage.Options{Work: db, Src: src, StageDir: filepath.Join(dir, "stage")}
	steps := []func(context.Context, stage.Options) error{
		stage.Refs, stage.Run, stage.Pass1,
		func(ctx context.Context, o stage.Options) error { return stage.Solve(ctx, o, maxPairs) },
		stage.Pass2,
	}
	for _, step := range steps {
		if err := step(ctx, o); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return db, nil
}
