//go:build live

package refs_test

import (
	"path/filepath"
	"testing"

	"pm/internal/dataset"
	"pm/internal/refs"
)

func TestBuildLive(t *testing.T) {
	cfg := dataset.Config{
		Root: dataset.RootHF, Path: filepath.Join(t.TempDir(), "work.duckdb"), Threads: 3, MemoryLimit: "2500MB",
		ExtDir: t.TempDir(), TempDir: t.TempDir(),
	}
	db, err := dataset.Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	src, err := dataset.NewSource(t.Context(), db, cfg.Root)
	if err != nil {
		t.Fatal(err)
	}
	if err := refs.Build(t.Context(), src, db); err != nil {
		t.Fatal(err)
	}
	gets, err := dataset.CountGets(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, query string
		want        int
	}{
		{"markets", "SELECT count(*) FROM market", 1_117_152},
		{"resolved markets", "SELECT count(*) FROM market WHERE resolved", 992_485},
		{"tokens", "SELECT count(*) FROM tok", 2_234_304},
		{"tokens per market", "SELECT count(*) FROM (SELECT market_id FROM tok GROUP BY 1 HAVING count(*) <> 2 OR min(outcome) <> 0 OR max(outcome) <> 1)", 0},
		{"neg-risk markets without a question row", "SELECT count(*) FROM market m WHERE neg_risk AND NOT EXISTS (SELECT 1 FROM nr_question q WHERE q.market_id = m.market_id)", 0},
		{"markets of the wrong bucket", "SELECT count(*) FROM market m JOIN ek ON ek.code = m.ek WHERE ek.bucket <> m.bucket OR m.bucket >= 64", 0},
	} {
		if got := count(t, db, tc.query); got != tc.want {
			t.Errorf("%s: got %d, want %d", tc.name, got, tc.want)
		}
	}
	for _, q := range []string{
		"SELECT count(*) FROM market WHERE neg_risk",
		"SELECT count(*) FROM ek",
		"SELECT count(*) FROM market WHERE kind = 1",
		"SELECT count(*) FROM market WHERE kind = 2",
		"SELECT count(*) FROM market WHERE end_date IS NOT NULL",
		"SELECT count(*) FROM wallet_reg",
		"SELECT count(*) FROM amm",
	} {
		t.Logf("%s = %d", q, count(t, db, q))
	}
	t.Logf("refs built in %d GETs", gets)
}
