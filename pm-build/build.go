package main

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"syscall"
	"time"

	"pm/internal/dataset"
	"pm/internal/publish"
	"pm/internal/stage"
)

const (
	minFree      = 35 << 30
	lastLogIndex = 1916
	// the edge table takes about 10 bytes a pair on disk, so a billion stay near 10 GB
	defaultMaxPairs = 1_000_000_000
)

var (
	errLowDisk = errors.New("not enough free disk")
	errStage   = errors.New("unknown stage")
	freeBytes  = diskFree
	stages     = []string{"refs", "stage", "pass1", "solve", "pass2", "publish"}
)

type settings struct {
	root, dataDir, token string
	maxPairs             int
	stage                string
	keepWork             bool
}

func (s settings) config() dataset.Config {
	cfg := config()
	cfg.Root, cfg.Token = s.root, s.token
	cfg.TempDir = tempDir(s.dataDir)
	cfg.Path = filepath.Join(s.dataDir, "work.duckdb")
	return cfg
}

func (s settings) expect() *publish.Totals {
	if dataset.IsRemote(s.root) {
		return &publish.FullTotals
	}
	return nil
}

/* Commands */

func parseBuild(args []string) (only string, keepWork bool, err error) {
	fs := flag.NewFlagSet("build", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&only, "stage", "", "")
	fs.BoolVar(&keepWork, "keep-work", false, "")
	if err := fs.Parse(args); err != nil || fs.NArg() > 0 {
		return "", false, errUsage
	}
	return only, keepWork, nil
}

func runBuild(ctx context.Context, args []string) error {
	only, keepWork, err := parseBuild(args)
	if err != nil {
		return err
	}
	maxPairs := defaultMaxPairs
	if v := os.Getenv("PM_WASH_MAX_PAIRS"); v != "" {
		if maxPairs, err = strconv.Atoi(v); err != nil {
			return fmt.Errorf("PM_WASH_MAX_PAIRS: %w", err)
		}
	}
	return build(ctx, settings{
		root:     cmp.Or(os.Getenv("PM_DATASET_ROOT"), dataset.DefaultRoot),
		dataDir:  dataDir(),
		token:    os.Getenv("HF_TOKEN"),
		maxPairs: maxPairs,
		stage:    only,
		keepWork: keepWork,
	})
}

func verify(ctx context.Context, args []string) error {
	path := filepath.Join(dataDir(), "pm.duckdb")
	if len(args) == 1 {
		path = args[0]
	}
	if err := publish.Verify(ctx, path, &publish.FullTotals); err != nil {
		return err
	}
	meta, err := publish.ReadMeta(ctx, path)
	if err != nil {
		return err
	}
	fmt.Println("verified", path)
	for _, key := range []string{"alg1_80", "alg1_90", "alg1_99", "alg2_share", "wash_rounds", "duplicate_rows", "unmapped_rows", "unmapped_fee_rows"} {
		fmt.Printf("%s=%s\n", key, meta[key])
	}
	return nil
}

/* Build */

func build(ctx context.Context, s settings) error {
	if s.stage != "" && !slices.Contains(stages, s.stage) {
		return fmt.Errorf("%w %q, want one of %v", errStage, s.stage, stages)
	}
	if err := os.MkdirAll(s.dataDir, 0o750); err != nil {
		return err
	}
	if err := preflight(s); err != nil {
		return err
	}
	db, err := dataset.Open(ctx, s.config())
	if err != nil {
		return err
	}
	err = errors.Join(execute(ctx, db, s), db.Close())
	if err != nil || s.stage != "" {
		return err
	}
	if err := publish.Verify(ctx, filepath.Join(s.dataDir, "pm.duckdb"), s.expect()); err != nil {
		return err
	}
	if s.keepWork {
		return nil
	}
	return cleanup(s.dataDir)
}

func execute(ctx context.Context, db *sql.DB, s settings) error {
	src, err := dataset.NewSource(ctx, db, s.root)
	if err != nil {
		return err
	}
	identity := map[string]string{"root": s.root, "revision": dataset.Revision, "rules_version": publish.RulesVersion}
	if err := stage.CheckIdentity(ctx, db, identity); err != nil {
		return err
	}
	o := stage.Options{Work: db, Src: src, StageDir: filepath.Join(s.dataDir, "stage")}
	if dataset.IsRemote(s.root) {
		o.Pace = dataset.NewPacer(paceRate(s.token))
		if err := checkSource(ctx, src, dataset.FullRows, publish.CutoffBlock, lastLogIndex); err != nil {
			return err
		}
		if err := pace(ctx, db, o.Pace); err != nil {
			return err
		}
	}
	for _, name := range stages {
		if s.stage != "" && s.stage != name {
			continue
		}
		start := time.Now()
		if err := runStage(ctx, name, o, s); err != nil {
			return fmt.Errorf("build %s: %w", name, err)
		}
		slog.Info("build stage done", "stage", name, "secs", int(time.Since(start).Seconds()))
	}
	return nil
}

func runStage(ctx context.Context, name string, o stage.Options, s settings) error {
	switch name {
	case "refs":
		if err := stage.Refs(ctx, o); err != nil {
			return err
		}
		return pace(ctx, o.Work, o.Pace)
	case "stage":
		return stage.Run(ctx, o)
	case "pass1":
		return stage.Pass1(ctx, o)
	case "solve":
		return stage.Solve(ctx, o, s.maxPairs)
	case "pass2":
		return stage.Pass2(ctx, o)
	}
	return publish.Run(ctx, o.Work, s.dataDir, s.expect())
}

func pace(ctx context.Context, db *sql.DB, p *dataset.Pacer) error {
	if p == nil {
		return nil
	}
	gets, _, err := dataset.Traffic(ctx, db)
	if err != nil {
		return err
	}
	return p.Done(ctx, gets)
}

/* Checks */

func preflight(s settings) error {
	if !dataset.IsRemote(s.root) {
		return nil
	}
	free, err := freeBytes(s.dataDir)
	if err != nil {
		return fmt.Errorf("free space of %s: %w", s.dataDir, err)
	}
	if free < minFree {
		return fmt.Errorf("%w, %s has %.1f GB free and a full build needs %d GB", errLowDisk, s.dataDir, float64(free)/(1<<30), minFree>>30)
	}
	return nil
}

func diskFree(dir string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return uint64(st.Bavail) * uint64(st.Bsize), nil
}

func checkSource(ctx context.Context, src *dataset.Source, want map[string]int64, block, logIndex uint32) error {
	if err := dataset.Check(ctx, src, want); err != nil {
		return fmt.Errorf("source files: %w", err)
	}
	gotBlock, gotLog, err := dataset.LastOrderFilled(ctx, src)
	if err != nil {
		return fmt.Errorf("last order_filled row: %w", err)
	}
	if gotBlock != block || gotLog != logIndex {
		return fmt.Errorf("last order_filled row is block %d log index %d, want block %d log index %d", gotBlock, gotLog, block, logIndex)
	}
	return nil
}

// a failed build never gets here, so it keeps its work files and can resume
func cleanup(dir string) error {
	for _, name := range []string{"work.duckdb", "work.duckdb.wal", "stage", "tmp"} {
		if err := os.RemoveAll(filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	if info, err := os.Stat(filepath.Join(dir, "pm.duckdb")); err == nil {
		slog.Info("build finished", "published_bytes", info.Size())
	}
	return nil
}
