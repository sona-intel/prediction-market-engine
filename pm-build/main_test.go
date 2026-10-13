package main

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/duckdb/duckdb-go/v2"

	"pm/internal/dataset"
	"pm/internal/dstest"
	"pm/internal/publish"
)

const (
	alice = "0xAbCdEf0123456789aBcDeF0123456789AbCdEf01"
	bob   = "0x0123456789ABCDEF0123456789abcdef01234567"
)

func tinyWorld(t *testing.T) *dstest.World {
	t.Helper()
	t.Setenv("PM_DUCKDB_EXT_DIR", t.TempDir())
	w := dstest.NewWorld(t)
	tok := w.Toks[0][0]
	w.Rows("order_filled", "01",
		dstest.Buy("137_100_1", 1700000100, alice, bob, tok, 100),
		dstest.Sell("137_100_2", 1700000110, bob, dstest.CTFExchange, tok, 100),
		dstest.Buy("137_101_1", 1700000200, bob, alice, tok, 100),
		dstest.Sell("137_101_2", 1700000210, alice, dstest.CTFExchange, tok, 100),
	)
	w.Rows("redemption", "01", []any{"137_150_1", alice, w.IDs[0], "1700000300"})
	return w
}

func local(w *dstest.World, dir string) settings {
	return settings{root: w.Root, dataDir: dir, maxPairs: 1 << 20}
}

func entries(t *testing.T, dir string) []string {
	t.Helper()
	list, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range list {
		names = append(names, e.Name())
	}
	return names
}

func withFree(t *testing.T, bytes uint64) {
	t.Helper()
	was := freeBytes
	freeBytes = func(string) (uint64, error) { return bytes, nil }
	t.Cleanup(func() { freeBytes = was })
}

/* Preflight */

func TestPreflightRefusesLowDisk(t *testing.T) {
	withFree(t, 34<<30)
	dir := t.TempDir()
	err := build(t.Context(), settings{root: dataset.DefaultRoot, dataDir: dir, maxPairs: 1})
	if !errors.Is(err, errLowDisk) {
		t.Fatalf("build returned %v, want errLowDisk", err)
	}
	if got := entries(t, dir); len(got) != 0 {
		t.Errorf("the data folder holds %v after a refused run, want nothing", got)
	}
	withFree(t, 35<<30)
	if err := preflight(settings{root: dataset.DefaultRoot, dataDir: dir}); err != nil {
		t.Errorf("35 GB free was refused: %v", err)
	}

	withFree(t, 1<<30)
	w := tinyWorld(t)
	s := local(w, t.TempDir())
	s.stage = "refs"
	if err := build(t.Context(), s); err != nil {
		t.Fatalf("a local root with 1 GB free: %v", err)
	}
}

/* Source check */

func TestBuildRefusesShortFileList(t *testing.T) {
	w := tinyWorld(t)
	w.Rows("order_filled", "02", dstest.Buy("137_200_7", 1700000300, alice, bob, w.Toks[0][0], 5))
	db, err := dataset.Open(t.Context(), dataset.Config{ExtDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	src, err := dataset.NewSource(t.Context(), db, w.Root)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{"order_filled": 5}
	if err := checkSource(t.Context(), src, want, 200, 7); err != nil {
		t.Fatalf("the complete set failed: %v", err)
	}
	err = checkSource(t.Context(), src, want, 201, 7)
	if err == nil || !strings.Contains(err.Error(), "last order_filled row") {
		t.Errorf("a wrong last block gave %v, want it to name the last order_filled row", err)
	}
	if err := os.Remove(filepath.Join(w.Root, "order_filled", "year=2025", "month=02.parquet")); err != nil {
		t.Fatal(err)
	}
	short, err := dataset.NewSource(t.Context(), db, w.Root)
	if err != nil {
		t.Fatal(err)
	}
	err = checkSource(t.Context(), short, want, 200, 7)
	if err == nil || !strings.Contains(err.Error(), "order_filled") {
		t.Errorf("a short file list gave %v, want it to name order_filled", err)
	}
}

/* Stages */

func workState(t *testing.T, dir string) (items []string, tables int) {
	t.Helper()
	connector, err := duckdb.NewConnector(filepath.Join(dir, "work.duckdb"), nil)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	rows, err := db.QueryContext(t.Context(), "SELECT stage || '/' || item || '/' || status FROM build_state ORDER BY 1")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var item string
		if err := rows.Scan(&item); err != nil {
			t.Fatal(err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), "SELECT CAST(count(*) AS BIGINT) FROM information_schema.tables WHERE table_name IN ('wallet_acc', 'wash_score')").Scan(&tables); err != nil {
		t.Fatal(err)
	}
	return items, tables
}

func TestBuildStageFlag(t *testing.T) {
	w := tinyWorld(t)
	dir := t.TempDir()
	s := local(w, dir)
	s.stage = "refs"
	for range 2 {
		if err := build(t.Context(), s); err != nil {
			t.Fatal(err)
		}
	}
	items, later := workState(t, dir)
	if want := []string{"refs/all/done"}; !slices.Equal(items, want) || later != 0 {
		t.Errorf("build_state holds %v and %d later tables, want %v and none", items, later, want)
	}
	if got := entries(t, dir); !slices.Contains(got, "work.duckdb") {
		t.Errorf("a single stage removed the work file, the folder holds %v", got)
	}
	s.stage = "nonsense"
	if err := build(t.Context(), s); err == nil || !strings.Contains(err.Error(), "nonsense") {
		t.Errorf("an unknown stage gave %v, want an error naming it", err)
	}
}

func TestBuildResumesAfterSingleStage(t *testing.T) {
	w := tinyWorld(t)
	dir := t.TempDir()
	s := local(w, dir)
	s.stage = "refs"
	if err := build(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	s.stage = ""
	if err := build(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	if got := entries(t, dir); !slices.Equal(got, []string{"pm.duckdb"}) {
		t.Fatalf("the data folder holds %v after the resumed build, want only pm.duckdb", got)
	}
}

func resolvedTimes(t *testing.T, dir string) int64 {
	t.Helper()
	connector, err := duckdb.NewConnector(filepath.Join(dir, "pm.duckdb")+"?access_mode=READ_ONLY", nil)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	var n int64
	if err := db.QueryRowContext(t.Context(), "SELECT CAST(count(resolved_ts) AS BIGINT) FROM market").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestBuildResumeKeepsResolvedTimes(t *testing.T) {
	w := tinyWorld(t)
	straight := t.TempDir()
	if err := build(t.Context(), local(w, straight)); err != nil {
		t.Fatal(err)
	}
	want := resolvedTimes(t, straight)
	if want != 1 {
		t.Fatalf("the straight build has %d markets with a resolution time, want 1", want)
	}

	resumed := t.TempDir()
	s := local(w, resumed)
	for _, name := range []string{"refs", "stage", "pass1", "solve", "pass2"} {
		s.stage = name
		if err := build(t.Context(), s); err != nil {
			t.Fatalf("stage %s: %v", name, err)
		}
	}
	s.stage = ""
	if err := build(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	if got := resolvedTimes(t, resumed); got != want {
		t.Errorf("the build resumed after pass2 has %d markets with a resolution time, want %d", got, want)
	}
}

func TestParseBuildArgs(t *testing.T) {
	for _, tc := range []struct {
		args     []string
		stage    string
		keepWork bool
		ok       bool
	}{
		{nil, "", false, true},
		{[]string{"--stage", "pass1"}, "pass1", false, true},
		{[]string{"--keep-work"}, "", true, true},
		{[]string{"--stage=refs", "--keep-work"}, "refs", true, true},
		{[]string{"--bogus"}, "", false, false},
		{[]string{"extra"}, "", false, false},
	} {
		stage, keep, err := parseBuild(tc.args)
		if (err == nil) != tc.ok || stage != tc.stage || keep != tc.keepWork {
			t.Errorf("parseBuild(%v) = %q, %v, %v", tc.args, stage, keep, err)
		}
	}
}

func TestConfigFitsContainer(t *testing.T) {
	cfg := config()
	if cfg.MemoryLimit != "700MB" || cfg.Threads != 2 || cfg.MaxTemp != "8GB" {
		t.Errorf("builder limits are %s, %d threads, %s spill, want 700MB, 2 threads, 8GB", cfg.MemoryLimit, cfg.Threads, cfg.MaxTemp)
	}
}

func TestSpillFolder(t *testing.T) {
	t.Setenv("PM_DATA_DIR", "/data")
	t.Setenv("PM_TEMP_DIR", "")
	if got := (settings{dataDir: "/data"}).config().TempDir; got != "/data/tmp" {
		t.Errorf("spill folder is %s, want /data/tmp", got)
	}
	t.Setenv("PM_TEMP_DIR", "/tmp/pm")
	if got := (settings{dataDir: "/data"}).config().TempDir; got != "/tmp/pm" {
		t.Errorf("spill folder is %s, want /tmp/pm", got)
	}
}

/* Data folder */

func TestBuildLeavesOnlyPublishedFile(t *testing.T) {
	w := tinyWorld(t)
	dir := t.TempDir()
	if err := build(t.Context(), local(w, dir)); err != nil {
		t.Fatal(err)
	}
	if got := entries(t, dir); !slices.Equal(got, []string{"pm.duckdb"}) {
		t.Fatalf("the data folder holds %v after a full build, want only pm.duckdb", got)
	}
	if err := publish.Verify(t.Context(), filepath.Join(dir, "pm.duckdb"), nil); err != nil {
		t.Fatalf("the published file does not verify: %v", err)
	}
}

func TestBuildKeepWorkFlag(t *testing.T) {
	w := tinyWorld(t)
	dir := t.TempDir()
	s := local(w, dir)
	s.keepWork = true
	if err := build(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	got := entries(t, dir)
	for _, name := range []string{"pm.duckdb", "work.duckdb"} {
		if !slices.Contains(got, name) {
			t.Errorf("--keep-work left %v, missing %s", got, name)
		}
	}
}

func TestBuildKeepsWorkWhenItFails(t *testing.T) {
	w := tinyWorld(t)
	if err := os.Remove(filepath.Join(w.Root, "position.parquet")); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	err := build(t.Context(), local(w, dir))
	if err == nil || !strings.Contains(err.Error(), "position") {
		t.Fatalf("build returned %v, want an error naming the missing table", err)
	}
	got := entries(t, dir)
	if !slices.Contains(got, "work.duckdb") || slices.Contains(got, "pm.duckdb") {
		t.Errorf("a failed build left %v, want the work file kept and no pm.duckdb", got)
	}
}
