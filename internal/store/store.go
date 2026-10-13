package store

import (
	"cmp"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

var (
	ErrNotBuilt = errors.New("database not built")
	ErrNotFound = errors.New("not found")
)

type Options struct {
	MemoryLimit, TempDir string
	Threads              int
	Every                time.Duration
}

type Store struct {
	path     string
	o        Options
	cur      atomic.Pointer[handle]
	mu       sync.Mutex
	closed   bool
	lastErr  string
	retiring sync.WaitGroup
}

type fileID struct {
	ino   uint64
	mtime int64
	size  int64
}

type handle struct {
	db   *sql.DB
	id   fileID
	meta meta
	// mu holds off the close of db while a request still reads it
	mu     sync.RWMutex
	closed bool
}

/* Lifecycle */

func New(path string, o Options) *Store {
	o.Every = cmp.Or(o.Every, 10*time.Second)
	s := &Store{path: path, o: o}
	s.refresh()
	return s
}

func (s *Store) Run(ctx context.Context) {
	tick := time.NewTicker(s.o.Every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			s.refresh()
		}
	}
}

func (s *Store) Ready() bool { return s.cur.Load() != nil }

func (s *Store) Close() error {
	s.mu.Lock()
	s.closed = true
	old := s.cur.Swap(nil)
	s.mu.Unlock()
	var err error
	if old != nil {
		err = old.retire()
	}
	s.retiring.Wait()
	return err
}

func (s *Store) refresh() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	info, err := os.Stat(s.path)
	if err != nil {
		return
	}
	id := identify(info)
	if cur := s.cur.Load(); cur != nil && cur.id == id {
		return
	}
	h, err := s.open(id)
	if err != nil {
		if msg := err.Error(); msg != s.lastErr {
			s.lastErr = msg
			slog.Warn("database file is not usable", "path", s.path, "error", err)
		}
		return
	}
	s.lastErr = ""
	if old := s.cur.Swap(h); old != nil {
		s.retiring.Go(func() { _ = old.retire() })
	}
}

func identify(info os.FileInfo) fileID {
	id := fileID{mtime: info.ModTime().UnixNano(), size: info.Size()}
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		id.ino = st.Ino
	}
	return id
}

// open attaches the file to a fresh in-memory instance, because duckdb-go reuses the open instance of a path and would keep serving a replaced file
func (s *Store) open(id fileID) (*handle, error) {
	target, err := filepath.Abs(s.path)
	if err != nil {
		return nil, err
	}
	attach := "ATTACH IF NOT EXISTS " + quote(target) + " AS pm (READ_ONLY)"
	connector, err := duckdb.NewConnector(":memory:?"+s.settings(), func(e driver.ExecerContext) error {
		if _, err := e.ExecContext(context.Background(), attach, nil); err != nil {
			return err
		}
		_, err := e.ExecContext(context.Background(), "USE pm", nil)
		return err
	})
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	m, err := loadMeta(ctx, db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &handle{db: db, id: id, meta: m}, nil
}

func (s *Store) settings() string {
	q := url.Values{}
	if s.o.MemoryLimit != "" {
		q.Set("memory_limit", s.o.MemoryLimit)
	}
	if s.o.Threads > 0 {
		q.Set("threads", strconv.Itoa(s.o.Threads))
	}
	if s.o.TempDir != "" {
		q.Set("temp_directory", s.o.TempDir)
	}
	return q.Encode()
}

func (h *handle) retire() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	return h.db.Close()
}

/* Reading */

func (s *Store) read(fn func(h *handle) error) error {
	for range 2 {
		h := s.cur.Load()
		if h == nil {
			return ErrNotBuilt
		}
		h.mu.RLock()
		if h.closed {
			h.mu.RUnlock()
			continue
		}
		err := fn(h)
		h.mu.RUnlock()
		return err
	}
	return ErrNotBuilt
}

/* Meta */

type meta struct {
	builtAt, rulesVersion                    string
	fills, wallets, rated, markets, resolved int64
	coverage                                 Coverage
}

func loadMeta(ctx context.Context, db *sql.DB) (meta, error) {
	rows, err := db.QueryContext(ctx, "SELECT key, value FROM meta")
	if err != nil {
		return meta{}, err
	}
	defer rows.Close()
	kv := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return meta{}, err
		}
		kv[k] = v
	}
	if err := rows.Err(); err != nil {
		return meta{}, err
	}
	var m meta
	for _, n := range []struct {
		key string
		to  *int64
	}{
		{"total_fills", &m.fills},
		{"total_wallets", &m.wallets},
		{"total_rated", &m.rated},
		{"total_markets", &m.markets},
		{"total_resolved_markets", &m.resolved},
		{"cutoff_block", &m.coverage.Block},
		{"cutoff_ts", &m.coverage.Time},
	} {
		if *n.to, err = strconv.ParseInt(kv[n.key], 10, 64); err != nil {
			return meta{}, fmt.Errorf("meta %s: %w", n.key, err)
		}
	}
	for _, s := range []struct {
		key string
		to  *string
	}{
		{"built_at", &m.builtAt},
		{"rules_version", &m.rulesVersion},
		{"note", &m.coverage.Note},
		{"attribution", &m.coverage.Attribution},
		{"license_url", &m.coverage.LicenseURL},
		{"source_url", &m.coverage.SourceURL},
		{"revision", &m.coverage.Revision},
		{"modified", &m.coverage.Modified},
	} {
		if *s.to = kv[s.key]; *s.to == "" {
			return meta{}, fmt.Errorf("meta %s is missing", s.key)
		}
	}
	return m, nil
}

type Coverage struct {
	Note, Attribution, LicenseURL, SourceURL, Revision, Modified string
	Block, Time                                                  int64
}

type Status struct {
	BuiltAt, RulesVersion                                  string
	Fills, Wallets, RatedWallets, Markets, ResolvedMarkets int64
	Coverage                                               Coverage
}

func (s *Store) Status(context.Context) (Status, error) {
	var out Status
	err := s.read(func(h *handle) error {
		m := h.meta
		out = Status{
			BuiltAt: m.builtAt, RulesVersion: m.rulesVersion, Fills: m.fills, Wallets: m.wallets,
			RatedWallets: m.rated, Markets: m.markets, ResolvedMarkets: m.resolved, Coverage: m.coverage,
		}
		return nil
	})
	return out, err
}

func quote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
