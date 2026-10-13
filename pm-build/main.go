package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"pm/internal/dataset"
	"pm/internal/golden"
)

var errUsage = errors.New("usage: pm-build probe | build [--stage S] [--keep-work] | verify [path] | fixtures | ext <dir>")

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("pm-build stopped", "error", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	switch {
	case len(args) == 1 && args[0] == "probe":
		return probe(ctx)
	case len(args) == 1 && args[0] == "fixtures":
		return fixtures(ctx)
	case len(args) >= 1 && args[0] == "build":
		return runBuild(ctx, args[1:])
	case len(args) >= 1 && len(args) <= 2 && args[0] == "verify":
		return verify(ctx, args[1:])
	case len(args) == 2 && args[0] == "ext":
		return installExt(ctx, args[1])
	}
	return errUsage
}

func dataDir() string {
	return cmp.Or(os.Getenv("PM_DATA_DIR"), "/data")
}

// spills go to the container disk when asked, the bind mount is twice as slow for the sorts
func tempDir(data string) string {
	return cmp.Or(os.Getenv("PM_TEMP_DIR"), filepath.Join(data, "tmp"))
}

func config() dataset.Config {
	return dataset.Config{
		Root:        os.Getenv("PM_DATASET_ROOT"),
		ExtDir:      os.Getenv("PM_DUCKDB_EXT_DIR"),
		TempDir:     tempDir(dataDir()),
		MemoryLimit: "700MB",
		MaxTemp:     "8GB",
		Token:       os.Getenv("HF_TOKEN"),
		Threads:     2,
	}
}

func probe(ctx context.Context) error {
	p, err := dataset.Probe(ctx, config())
	if err != nil {
		return err
	}
	fmt.Printf("root=%s rows=%d secs=%.2f duckdb=%s\n", p.Root, p.Rows, p.Elapsed.Seconds(), p.Version)
	return nil
}

func fixtures(ctx context.Context) error {
	cfg := config()
	cfg.Root = cmp.Or(cfg.Root, dataset.DefaultRoot)
	db, err := dataset.Open(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	src, err := dataset.NewSource(ctx, db, cfg.Root)
	if err != nil {
		return err
	}
	return golden.Extract(ctx, src, dataset.NewPacer(paceRate(cfg.Token)), golden.Path("dataset"))
}

func paceRate(token string) float64 {
	if token != "" {
		return 13
	}
	return 8
}

func installExt(ctx context.Context, dir string) error {
	db, err := dataset.Open(ctx, dataset.Config{Root: dataset.RootHF, ExtDir: dir})
	if err != nil {
		return err
	}
	return db.Close()
}
