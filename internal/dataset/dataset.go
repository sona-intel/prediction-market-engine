package dataset

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

const (
	Revision  = "7eeb860dea5b79d5c74f3182b70bd08c85c8f833"
	RootHF    = "hf://datasets/moose-code/polymarket-onchain-v1@" + Revision
	RootHTTPS = "https://huggingface.co/datasets/moose-code/polymarket-onchain-v1/resolve/" + Revision
)

var DefaultRoot = RootHF

const probeFile = "/orders_matched/year=2026/month=04.parquet"

var errSecret = errors.New("create Hugging Face secret failed")

type Config struct {
	Root, Path, ExtDir, TempDir, MemoryLimit, MaxTemp, Token string
	Threads                                                  int
}

type Probed struct {
	Root    string
	Rows    int64
	Elapsed time.Duration
	Version string
}

func Open(ctx context.Context, cfg Config) (*sql.DB, error) {
	ext := cfg.ExtDir
	if ext == "" {
		cache, err := os.UserCacheDir()
		if err != nil {
			return nil, err
		}
		ext = filepath.Join(cache, "pm-duckdb-ext")
	}
	remote := IsRemote(cfg.Root)
	stmts := []string{
		"SET autoinstall_known_extensions = false",
		"SET autoload_known_extensions = false",
		"SET preserve_insertion_order = false",
		"SET extension_directory = " + quote(ext),
	}
	if cfg.Threads > 0 {
		stmts = append(stmts, "SET threads = "+strconv.Itoa(cfg.Threads))
	}
	for _, kv := range [][2]string{
		{"memory_limit", cfg.MemoryLimit},
		{"temp_directory", cfg.TempDir},
		{"max_temp_directory_size", cfg.MaxTemp},
	} {
		if kv[1] != "" {
			stmts = append(stmts, "SET "+kv[0]+" = "+quote(kv[1]))
		}
	}
	var secret string
	if remote {
		stmts = append(stmts, "INSTALL httpfs", "LOAD httpfs",
			"SET GLOBAL http_retries = 6", "SET GLOBAL http_retry_wait_ms = 500", "SET GLOBAL http_retry_backoff = 2",
			"CALL enable_logging('HTTP')")
		if cfg.Token != "" {
			secret = "CREATE TEMPORARY SECRET IF NOT EXISTS hf (TYPE HUGGINGFACE, TOKEN " + quote(cfg.Token) + ")"
		}
	}
	connector, err := duckdb.NewConnector(cfg.Path, nil)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	defer conn.Close()
	for _, stmt := range stmts {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("duckdb init: %w", err)
		}
	}
	if secret != "" {
		if _, err := conn.ExecContext(ctx, secret); err != nil {
			_ = db.Close()
			return nil, errSecret
		}
	}
	return db, nil
}

func Probe(ctx context.Context, cfg Config) (Probed, error) {
	first := cmp.Or(cfg.Root, RootHF)
	roots := []string{first}
	if IsRemote(first) && first != RootHTTPS {
		roots = append(roots, RootHTTPS)
	}
	var errs []error
	for _, root := range roots {
		p, err := probeRoot(ctx, cfg, root)
		if err == nil {
			return p, nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", root, err))
	}
	return Probed{}, errors.Join(errs...)
}

func probeRoot(ctx context.Context, cfg Config, root string) (Probed, error) {
	cfg.Root = root
	db, err := Open(ctx, cfg)
	if err != nil {
		return Probed{}, err
	}
	defer db.Close()
	p := Probed{Root: root}
	start := time.Now()
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM read_parquet(?)", root+probeFile).Scan(&p.Rows); err != nil {
		return Probed{}, err
	}
	p.Elapsed = time.Since(start)
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&p.Version); err != nil {
		return Probed{}, err
	}
	return p, nil
}

func IsRemote(root string) bool {
	return strings.HasPrefix(root, "hf://") || strings.HasPrefix(root, "https://")
}

func quote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
