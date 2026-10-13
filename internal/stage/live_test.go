//go:build live

package stage_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"pm/internal/dataset"
	"pm/internal/refs"
	"pm/internal/stage"
)

func TestStageLive(t *testing.T) {
	cfg := dataset.Config{
		Root: dataset.RootHF, Path: filepath.Join(t.TempDir(), "work.duckdb"), Threads: 3, MemoryLimit: "2500MB",
		ExtDir: t.TempDir(), TempDir: t.TempDir(),
	}
	db, err := dataset.Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	hf, err := dataset.NewSource(t.Context(), db, cfg.Root)
	if err != nil {
		t.Fatal(err)
	}
	if err := refs.Build(t.Context(), hf, db); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	for _, s := range []struct{ table, month, cols, limit string }{
		{"order_filled", "year=2025/month=03", "id, timestamp, maker, taker, makerAssetId, takerAssetId, makerAmountFilled, takerAmountFilled", ""},
		{"redemption", "year=2025/month=03", "id, redeemer, condition, timestamp", ""},
		{"split", "year=2025/month=03", "id, timestamp, stakeholder, condition, amount", ""},
		{"merge", "year=2025/month=03", "id, timestamp, stakeholder, condition, amount", ""},
		{"neg_risk_conversion", "year=2025/month=05", "id, timestamp, stakeholder, negRiskMarketId, amount, indexSet, questionCount", ""},
		{"fee_refunded", "year=2026/month=01", "id, refundee, tokenId, feeCharged", " LIMIT 3000000"},
	} {
		rel := s.table + "/" + s.month + ".parquet"
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, rel)), 0o750); err != nil {
			t.Fatal(err)
		}
		stmt := "COPY (SELECT " + s.cols + " FROM read_parquet('" + dataset.RootHF + "/" + rel + "', hive_partitioning=false)" + s.limit +
			") TO '" + filepath.Join(root, rel) + "' (FORMAT parquet)"
		if _, err := db.ExecContext(t.Context(), stmt); err != nil {
			t.Fatal(err)
		}
	}
	local, err := dataset.NewSource(t.Context(), db, root)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, o: stage.Options{Work: db, Src: local, StageDir: filepath.Join(t.TempDir(), "stage")}}
	start := time.Now()
	e.run()
	t.Logf("staged in %s", time.Since(start).Round(time.Second))

	if got := e.rows("fill"); got != 8_072_153 {
		t.Errorf("fills %d, want 8072153", got)
	}
	if got := e.count("SELECT CAST(count(*) AS BIGINT) FROM " + e.staged("fill") + " WHERE taker IS NULL"); got != 3_449_405 {
		t.Errorf("summary fills %d, want 3449405", got)
	}
	var sampled, staged int64
	feeFile := filepath.Join(root, "fee_refunded", "year=2026", "month=01.parquet")
	if err := db.QueryRowContext(t.Context(), "SELECT CAST(sum(CAST(feeCharged AS HUGEINT)) AS BIGINT) FROM read_parquet('"+feeFile+"')").Scan(&sampled); err != nil {
		t.Fatal(err)
	}
	staged = e.count("SELECT CAST(sum(fee) AS BIGINT) FROM " + e.staged("fee"))
	if sampled != staged {
		t.Errorf("fee sum %d, want %d", staged, sampled)
	}
	if got := e.audit("unmapped_rows"); got != 0 {
		t.Errorf("unmapped_rows %d, want 0", got)
	}
	for _, q := range []string{
		"SELECT count(*) FROM " + e.staged("ctf"),
		"SELECT count(*) FROM " + e.staged("ctf") + " WHERE kind = 4",
		"SELECT count(*) FROM " + e.staged("fee"),
		"SELECT count(*) FROM " + e.staged("fee") + " WHERE market_id IS NULL",
		"SELECT count(*) FROM wallet_dict",
		"SELECT count(*) FROM first_redeem",
	} {
		t.Logf("%s = %d", q, e.count("SELECT CAST(("+q+") AS BIGINT)"))
	}
	t.Logf("folders %d", len(e.folders()))
	for _, kind := range []string{"fill", "ctf", "fee"} {
		var size int64
		files, err := filepath.Glob(filepath.Join(e.o.StageDir, "b=*", kind+"-*.parquet"))
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range files {
			info, err := os.Stat(f)
			if err != nil {
				t.Fatal(err)
			}
			size += info.Size()
		}
		t.Logf("%s: %d files, %.1f MB, %.2f bytes per row", kind, len(files), float64(size)/1e6, float64(size)/float64(e.rows(kind)))
	}
}
