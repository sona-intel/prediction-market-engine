package dataset_test

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"pm/internal/dataset"
	"pm/internal/dstest"
)

const (
	march = "order_filled/year=2025/month=03.parquet"
	april = "order_filled/year=2025/month=04.parquet"
)

func openMemory(t *testing.T) *sql.DB {
	t.Helper()
	db, err := dataset.Open(t.Context(), dataset.Config{ExtDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newSource(t *testing.T, set *dstest.Set) *dataset.Source {
	t.Helper()
	src, err := dataset.NewSource(t.Context(), openMemory(t), set.Root)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func miniSet(t *testing.T) *dstest.Set {
	t.Helper()
	set := dstest.New(t)
	set.Write(march, []string{"id", "maker"}, []any{"137_10_1", "0xa"}, []any{"137_10_2", "0xb"})
	set.Write(april, []string{"id", "maker"}, []any{"137_20_1", "0xc"})
	set.Write("condition.parquet", []string{"id", "payoutNumerators"}, []any{"0x01", []string{"1", "0"}})
	return set
}

func TestListLocal(t *testing.T) {
	set := miniSet(t)
	for _, name := range []string{"README.md", "SNAPSHOT.json"} {
		if err := os.WriteFile(filepath.Join(set.Root, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	got, err := dataset.List(t.Context(), set.Root)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{
		"order_filled": {filepath.Join(set.Root, march), filepath.Join(set.Root, april)},
		"condition":    {filepath.Join(set.Root, "condition.parquet")},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestListHFParsesTree(t *testing.T) {
	const tree = `[
		{"type":"directory","oid":"a","size":0,"path":"order_filled"},
		{"type":"file","oid":"b","size":1,"path":"README.md"},
		{"type":"file","oid":"c","size":1,"path":"SNAPSHOT.json"},
		{"type":"file","oid":"d","size":9,"path":"order_filled/year=2025/month=04.parquet"},
		{"type":"file","oid":"e","size":9,"path":"order_filled/year=2025/month=03.parquet"}
	]`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(tree))
	}))
	t.Cleanup(srv.Close)
	const root = "hf://datasets/o/n@rev"
	got, err := dataset.ListRemote(t.Context(), srv.URL, root)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{"order_filled": {root + "/" + march, root + "/" + april}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestListHFRefusesBadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "slow down", http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)
	_, err := dataset.ListRemote(t.Context(), srv.URL, "hf://datasets/o/n@rev")
	if err == nil || !strings.Contains(err.Error(), "429") {
		t.Fatalf("want an error naming 429, got %v", err)
	}
}

func TestFromSelectsColumns(t *testing.T) {
	src := newSource(t, miniSet(t))
	rows, err := src.DB.QueryContext(t.Context(), "SELECT id, maker FROM "+src.From(src.Files("order_filled"), "id", "maker")+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var id, maker string
		if err := rows.Scan(&id, &maker); err != nil {
			t.Fatal(err)
		}
		got = append(got, id+" "+maker)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []string{"137_10_1 0xa", "137_10_2 0xb", "137_20_1 0xc"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestFromQuotesPaths(t *testing.T) {
	set := dstest.New(t)
	set.Write("order_filled/it's.parquet", []string{"id"}, []any{"0x01"})
	src := newSource(t, set)
	files := src.Files("order_filled")
	if len(files) != 1 {
		t.Fatalf("files %v", files)
	}
	var n int
	if err := src.DB.QueryRowContext(t.Context(), "SELECT count(*) FROM "+src.From(files, "id")).Scan(&n); err != nil || n != 1 {
		t.Fatalf("n %d err %v", n, err)
	}
}

func TestRowsFooter(t *testing.T) {
	src := newSource(t, miniSet(t))
	n, err := src.Rows(t.Context(), src.Files("order_filled")[0])
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("rows %d, want 2", n)
	}
}

func TestCheckAcceptsMatch(t *testing.T) {
	src := newSource(t, miniSet(t))
	if err := dataset.Check(t.Context(), src, map[string]int64{"order_filled": 3, "condition": 1}); err != nil {
		t.Fatal(err)
	}
}

func TestCheckReportsMismatch(t *testing.T) {
	src := newSource(t, miniSet(t))
	err := dataset.Check(t.Context(), src, map[string]int64{"order_filled": 4})
	if err == nil {
		t.Fatal("want an error")
	}
	for _, s := range []string{"order_filled", "3", "4"} {
		if !strings.Contains(err.Error(), s) {
			t.Errorf("error %q lacks %q", err, s)
		}
	}
}

func TestCheckReportsMissingTable(t *testing.T) {
	src := newSource(t, miniSet(t))
	err := dataset.Check(t.Context(), src, map[string]int64{"redemption": 7})
	if err == nil || !strings.Contains(err.Error(), "redemption") {
		t.Fatalf("want an error naming redemption, got %v", err)
	}
}

func TestLastOrderFilled(t *testing.T) {
	set := dstest.New(t)
	set.Write(march, []string{"id"}, []any{"137_200000_5"})
	set.Write("order_filled/year=2026/month=04.parquet", []string{"id"}, []any{"137_100000_1"}, []any{"137_99999_9"})
	block, logIndex, err := dataset.LastOrderFilled(t.Context(), newSource(t, set))
	if err != nil {
		t.Fatal(err)
	}
	if block != 100000 || logIndex != 1 {
		t.Fatalf("got (%d, %d), want (100000, 1)", block, logIndex)
	}
}

func TestLastOrderFilledNeedsFiles(t *testing.T) {
	if _, _, err := dataset.LastOrderFilled(t.Context(), newSource(t, dstest.New(t))); err == nil {
		t.Fatal("want an error without order_filled files")
	}
}

func TestCountGets(t *testing.T) {
	db := openMemory(t)
	ctx := t.Context()
	if _, err := db.ExecContext(ctx, "CALL enable_logging('HTTP')"); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"GET", "HEAD", "GET"} {
		if _, err := db.ExecContext(ctx, "SELECT write_log('{''request'': {''type'': "+kind+", ''url'': ''x''}}', log_type := 'HTTP')"); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []int{2, 0} {
		got, err := dataset.CountGets(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("gets %d, want %d", got, want)
		}
	}
}

func TestTraffic(t *testing.T) {
	db := openMemory(t)
	ctx := t.Context()
	if _, err := db.ExecContext(ctx, "CALL enable_logging('HTTP')"); err != nil {
		t.Fatal(err)
	}
	for _, e := range []struct{ kind, size string }{{"GET", "1000"}, {"HEAD", "5000"}, {"GET", "234"}} {
		msg := "{''request'': {''type'': " + e.kind + ", ''url'': ''x''}, ''response'': {''headers'': {content-length=" + e.size + ", accept-ranges=bytes}}}"
		if _, err := db.ExecContext(ctx, "SELECT write_log('"+msg+"', log_type := 'HTTP')"); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []struct {
		gets  int
		bytes int64
	}{{2, 1234}, {0, 0}} {
		gets, bytes, err := dataset.Traffic(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		if gets != want.gets || bytes != want.bytes {
			t.Fatalf("traffic %d requests and %d bytes, want %d and %d", gets, bytes, want.gets, want.bytes)
		}
	}
}
