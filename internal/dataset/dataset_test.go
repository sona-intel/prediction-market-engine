package dataset_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"pm/internal/dataset"
)

func TestOpenAppliesConfig(t *testing.T) {
	ext, tmp := t.TempDir(), t.TempDir()
	path := filepath.Join(t.TempDir(), "work.duckdb")
	db, err := dataset.Open(t.Context(), dataset.Config{Path: path, Threads: 2, MemoryLimit: "1GB", ExtDir: ext, TempDir: tmp, MaxTemp: "2GB"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	want := map[string]string{
		"threads":                   "2",
		"extension_directory":       ext,
		"temp_directory":            tmp,
		"preserve_insertion_order":  "false",
		"autoload_known_extensions": "false",
	}
	for name, w := range want {
		var got string
		if err := db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT CAST(current_setting('%s') AS VARCHAR)", name)).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != w {
			t.Errorf("%s = %q, want %q", name, got, w)
		}
	}
	if _, err := db.ExecContext(t.Context(), "CREATE TABLE t AS SELECT 1 AS x"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}

func TestOpenInMemory(t *testing.T) {
	db, err := dataset.Open(t.Context(), dataset.Config{ExtDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var n int
	if err := db.QueryRowContext(t.Context(), "SELECT 41 + 1").Scan(&n); err != nil || n != 42 {
		t.Fatalf("n %d err %v", n, err)
	}
}

func TestOpenRejectsBadSetting(t *testing.T) {
	_, err := dataset.Open(t.Context(), dataset.Config{ExtDir: t.TempDir(), MemoryLimit: "lots"})
	if err == nil {
		t.Fatal("want an error for an invalid memory_limit")
	}
}

func TestOpenSecondConnectionAfterSpill(t *testing.T) {
	db, err := dataset.Open(t.Context(), dataset.Config{ExtDir: t.TempDir(), TempDir: t.TempDir(), MemoryLimit: "100MB", MaxTemp: "2GB", Threads: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	first, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	var n int64
	if err := first.QueryRowContext(t.Context(), "SELECT count(*) FROM (SELECT md5(i::VARCHAR) AS h FROM range(20000000) t(i) ORDER BY h)").Scan(&n); err != nil {
		t.Fatal(err)
	}
	second, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	var threads int
	if err := second.QueryRowContext(t.Context(), "SELECT CAST(current_setting('threads') AS INTEGER)").Scan(&threads); err != nil {
		t.Fatal(err)
	}
	if threads != 2 {
		t.Errorf("threads on the second connection = %d, want 2", threads)
	}
}
