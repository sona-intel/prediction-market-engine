package dstest

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/duckdb/duckdb-go/v2"
)

type Set struct {
	Root string
	db   *sql.DB
	t    testing.TB
}

func New(t testing.TB) *Set {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	return &Set{Root: t.TempDir(), db: db, t: t}
}

func (s *Set) Write(rel string, cols []string, rows ...[]any) {
	s.t.Helper()
	path := filepath.Join(s.Root, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		s.t.Fatal(err)
	}
	lists := make([]bool, len(cols))
	for _, row := range rows {
		for i, v := range row {
			if _, ok := v.([]string); ok {
				lists[i] = true
			}
		}
	}
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = `"` + strings.ReplaceAll(c, `"`, `""`) + `"`
	}
	query := empty(names)
	if len(rows) > 0 {
		tuples := make([]string, len(rows))
		for r, row := range rows {
			cells := make([]string, len(row))
			for i, v := range row {
				cells[i] = literal(v, lists[i])
			}
			tuples[r] = "(" + strings.Join(cells, ", ") + ")"
		}
		query = "SELECT * FROM (VALUES " + strings.Join(tuples, ", ") + ") t(" + strings.Join(names, ", ") + ")"
	}
	stmt := "COPY (" + query + ") TO " + quote(path) + " (FORMAT parquet)"
	if _, err := s.db.ExecContext(s.t.Context(), stmt); err != nil {
		s.t.Fatal(err)
	}
}

func empty(names []string) string {
	cells := make([]string, len(names))
	for i, n := range names {
		cells[i] = "CAST(NULL AS VARCHAR) AS " + n
	}
	return "SELECT " + strings.Join(cells, ", ") + " WHERE false"
}

func literal(v any, list bool) string {
	switch x := v.(type) {
	case nil:
		if list {
			return "CAST(NULL AS VARCHAR[])"
		}
		return "CAST(NULL AS VARCHAR)"
	case []string:
		items := make([]string, len(x))
		for i, s := range x {
			items[i] = quote(s)
		}
		return "CAST([" + strings.Join(items, ", ") + "] AS VARCHAR[])"
	case string:
		return quote(x)
	default:
		return quote(fmt.Sprint(x))
	}
}

func quote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
