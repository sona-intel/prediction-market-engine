package stage_test

import (
	"path/filepath"
	"testing"

	"pm/internal/dataset"
	"pm/internal/dstest"
	"pm/internal/stage"
)

func TestRefsRecordsAndSkips(t *testing.T) {
	root := dstest.NewWorld(t).Root
	db, err := dataset.Open(t.Context(), dataset.Config{Path: filepath.Join(t.TempDir(), "work.duckdb"), ExtDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	src, err := dataset.NewSource(t.Context(), db, root)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, o: stage.Options{Work: db, Src: src, StageDir: filepath.Join(t.TempDir(), "stage")}}
	if err := stage.Refs(t.Context(), e.o); err != nil {
		t.Fatal(err)
	}
	if got := e.count("SELECT CAST(count(*) AS BIGINT) FROM build_state WHERE stage = 'refs' AND item = 'all' AND status = 'done'"); got != 1 {
		t.Fatalf("build_state holds %d finished refs rows, want 1", got)
	}
	if got := e.count("SELECT CAST(count(*) AS BIGINT) FROM market"); got != 3 {
		t.Fatalf("market holds %d rows, want 3", got)
	}
	if _, err := db.ExecContext(t.Context(), "DELETE FROM market WHERE market_id = 0"); err != nil {
		t.Fatal(err)
	}
	if err := stage.Refs(t.Context(), e.o); err != nil {
		t.Fatal(err)
	}
	if got := e.count("SELECT CAST(count(*) AS BIGINT) FROM market"); got != 2 {
		t.Errorf("a second Refs rebuilt market, it holds %d rows, want 2", got)
	}
	if got := e.count("SELECT CAST(count(*) AS BIGINT) FROM build_state"); got != 1 {
		t.Errorf("build_state holds %d rows, want 1", got)
	}
}
