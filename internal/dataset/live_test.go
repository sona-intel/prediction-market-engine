//go:build live

package dataset_test

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"pm/internal/dataset"
)

const (
	goldenWallet    = "0x9b068d4b41bd5e7e7A48323F0B3Fa15764f0A9C1"
	ctfExchange     = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
	negRiskExchange = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
)

func liveConfig(t *testing.T) dataset.Config {
	t.Helper()
	return dataset.Config{Threads: 3, MemoryLimit: "1500MB", ExtDir: t.TempDir(), TempDir: t.TempDir()}
}

func TestProbe(t *testing.T) {
	p, err := dataset.Probe(t.Context(), dataset.Config{Threads: 3, MemoryLimit: "1500MB", ExtDir: t.TempDir(), TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if p.Rows != 71_865_077 {
		t.Fatalf("rows %d via %s", p.Rows, p.Root)
	}
	t.Logf("root %s, %s, DuckDB %s", p.Root, p.Elapsed, p.Version)
}

func TestGoldenSample(t *testing.T) {
	cfg := liveConfig(t)
	p, err := dataset.Probe(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Root = p.Root
	db, err := dataset.Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	var rows, buys, summary, usdc int64
	err = db.QueryRowContext(t.Context(), fmt.Sprintf(`
		SELECT count(*), count(*) FILTER (WHERE makerAssetId = '0'),
		       count(*) FILTER (WHERE taker IN ('%s', '%s')),
		       CAST(sum(CAST(CASE WHEN makerAssetId = '0' THEN makerAmountFilled ELSE takerAmountFilled END AS HUGEINT)) AS BIGINT)
		FROM read_parquet('%s/order_filled/year=2025/month=11.parquet') WHERE maker = '%s'`,
		ctfExchange, negRiskExchange, p.Root, goldenWallet)).Scan(&rows, &buys, &summary, &usdc)
	if err != nil {
		t.Fatal(err)
	}
	if rows != 5 || buys != 4 || summary != 5 || usdc != 186815597 {
		t.Fatalf("rows %d buys %d summary %d usdc %d", rows, buys, summary, usdc)
	}

	for _, table := range []string{"user_position", "position", "condition"} {
		view(t, db, table, p.Root)
	}
	var settled int64
	err = db.QueryRowContext(t.Context(), fmt.Sprintf(`
		SELECT CAST(sum(CAST(u.realizedPnl AS HUGEINT)) + sum(CASE WHEN CAST(u.amount AS HUGEINT) > 0 AND CAST(c.payoutDenominator AS HUGEINT) > 0
		  THEN CAST(u.amount AS HUGEINT) * (CAST(c.payoutNumerators[CAST(p.outcomeIndex AS INT) + 1] AS HUGEINT) * 1000000 // CAST(c.payoutDenominator AS HUGEINT)
		       - CAST(u.avgPrice AS HUGEINT)) // 1000000 ELSE 0 END) AS BIGINT)
		FROM user_position u JOIN position p ON p.id = u.tokenId JOIN condition c ON c.id = p.condition WHERE u."user" = '%s'`,
		goldenWallet)).Scan(&settled)
	if err != nil {
		t.Fatal(err)
	}
	if d := settled - (-113752331); d < -200 || d > 200 {
		t.Fatalf("settled pnl %d, want -113752331 within 200", settled)
	}
}

func TestSeam(t *testing.T) {
	cfg := liveConfig(t)
	cfg.Root = dataset.RootHF
	db, err := dataset.Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	src, err := dataset.NewSource(t.Context(), db, cfg.Root)
	if err != nil {
		t.Fatal(err)
	}
	files := src.Files("order_filled")
	newest := files[len(files)-1]
	if !strings.HasSuffix(newest, "/order_filled/year=2026/month=04.parquet") {
		t.Fatalf("newest order_filled file %s", newest)
	}
	rows, err := src.Rows(t.Context(), newest)
	if err != nil {
		t.Fatal(err)
	}
	if rows != 198363260 {
		t.Fatalf("rows %d", rows)
	}
	block, logIndex, err := dataset.LastOrderFilled(t.Context(), src)
	if err != nil {
		t.Fatal(err)
	}
	if block != 85948288 || logIndex != 1916 {
		t.Fatalf("last (%d, %d)", block, logIndex)
	}
	gets, err := dataset.CountGets(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	if gets == 0 {
		t.Fatal("no GET requests counted over hf://")
	}
	t.Logf("seam (%d, %d) in %d GETs", block, logIndex, gets)
}

func TestFullRows(t *testing.T) {
	cfg := liveConfig(t)
	cfg.Root = dataset.RootHF
	db, err := dataset.Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	src, err := dataset.NewSource(t.Context(), db, cfg.Root)
	if err != nil {
		t.Fatal(err)
	}
	if err := dataset.Check(t.Context(), src, dataset.FullRows); err != nil {
		t.Fatal(err)
	}
	gets, err := dataset.CountGets(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("footers of %d tables in %d GETs", len(dataset.FullRows), gets)
}

func view(t *testing.T, db *sql.DB, table, root string) {
	t.Helper()
	q := fmt.Sprintf(`CREATE VIEW "%s" AS SELECT * FROM read_parquet('%s/%s.parquet')`, table, root, table)
	if _, err := db.ExecContext(t.Context(), q); err != nil {
		t.Fatal(err)
	}
}
