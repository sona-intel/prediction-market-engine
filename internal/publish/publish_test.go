package publish_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/duckdb/duckdb-go/v2"

	"pm/internal/dstest"
	"pm/internal/golden"
	"pm/internal/publish"
	"pm/internal/skill"
	"pm/internal/testdb"
)

const (
	alice = "0xAbCdEf0123456789aBcDeF0123456789AbCdEf01"
	bob   = "0x0123456789ABCDEF0123456789abcdef01234567"
	carol = "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
	dave  = "0x00000000000000000000000000000000000d4ae0"
	erin  = "0x00000000000000000000000000000000000e4140"
	goldW = "0x467ecfc0c6265683e90899C41881c0431bDEE2ed"
)

const (
	specNote        = "Polymarket history through 2026-04-24 07:43:40 UTC (Polygon block 85,948,288). Order-book trades start on 2022-11-21. Later trades are not included. Trades on the old automated market maker are not part of profit and loss."
	specAttribution = "Polymarket history: Envio, CC-BY-4.0"
	specLicense     = "https://creativecommons.org/licenses/by/4.0/"
	specSource      = "https://huggingface.co/datasets/moose-code/polymarket-onchain-v1"
	specRevision    = "7eeb860dea5b79d5c74f3182b70bd08c85c8f833"
	specModified    = "Modified by Sona: source rows were aggregated into per-wallet and per-market figures. Not affiliated with or endorsed by Polymarket or Envio."
)

var tables = []string{"meta", "market", "wallet", "wallet_market", "wallet_calibration", "calibration", "wash_counterparty", "wash_trade"}

/* Shared builds */

type built struct {
	once sync.Once
	dir  string
	path string
	err  error
}

var goldenBuild, washedBuild built

func TestMain(m *testing.M) {
	code := m.Run()
	for _, b := range []*built{&goldenBuild, &washedBuild} {
		if b.dir != "" {
			_ = os.RemoveAll(b.dir)
		}
	}
	os.Exit(code)
}

func (b *built) get(t *testing.T, build func(context.Context, string) (string, error)) string {
	t.Helper()
	b.once.Do(func() {
		b.dir, b.err = os.MkdirTemp("", "pm-publish-")
		if b.err == nil {
			b.path, b.err = build(context.Background(), b.dir)
		}
	})
	if b.err != nil {
		t.Fatal(b.err)
	}
	return b.path
}

func goldenDB(t *testing.T) *sql.DB {
	t.Helper()
	return openRO(t, goldenBuild.get(t, testdb.Build))
}

func washedDB(t *testing.T) *sql.DB {
	t.Helper()
	path := washedBuild.get(t, func(ctx context.Context, dir string) (string, error) {
		w := washWorld(t)
		return testdb.BuildFrom(ctx, w.Root, dir)
	})
	return openRO(t, path)
}

func openRO(t testing.TB, path string) *sql.DB {
	t.Helper()
	connector, err := duckdb.NewConnector(path+"?access_mode=READ_ONLY", nil)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func trade(block int, ts int, buyer, seller, tok string, shares int) [][]any {
	id := func(log int) string { return fmt.Sprintf("137_%d_%d", block, log) }
	return [][]any{
		dstest.Buy(id(1), ts, buyer, seller, tok, shares),
		dstest.Sell(id(2), ts, seller, dstest.CTFExchange, tok, shares),
	}
}

func washWorld(t testing.TB) *dstest.World {
	t.Helper()
	w := dstest.NewWorld(t)
	var rows [][]any
	block := 100
	for _, p := range []struct {
		buyer, seller, tok string
		sizes              []int
	}{
		{alice, bob, w.Toks[0][0], []int{100, 300, 100}},
		{carol, dave, w.Toks[1][0], []int{50, 200}},
	} {
		for i, size := range p.sizes {
			rows = append(rows, trade(block, 1700000000+1000*block+20*i, p.buyer, p.seller, p.tok, size)...)
			rows = append(rows, trade(block+1, 1700000000+1000*block+20*i+10, p.seller, p.buyer, p.tok, size)...)
			block += 2
		}
	}
	// an open trade in a third market keeps alice and carol below 1, which gives their closed pairs a threshold
	rows = append(rows, trade(300, 1700000000+300000, alice, carol, w.Toks[2][0], 150)...)
	w.Rows("order_filled", "01", rows...)
	w.Rows("split", "01", []any{"137_150_1", "1700000500", erin, w.IDs[0], "5000000"})
	return w
}

/* Query helpers */

func one[T any](t testing.TB, db *sql.DB, query string, args ...any) T {
	t.Helper()
	var v T
	if err := db.QueryRowContext(t.Context(), query, args...).Scan(&v); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return v
}

func metaOf(t testing.TB, db *sql.DB) map[string]string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "SELECT key, value FROM meta")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	meta := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			t.Fatal(err)
		}
		meta[k] = v
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return meta
}

func assertOrdered(t *testing.T, db *sql.DB, table string, cols ...string) {
	t.Helper()
	if n := one[int64](t, db, "SELECT CAST(count(*) AS BIGINT) FROM "+table); n < 2 {
		t.Fatalf("%s has %d rows, too few to show an order", table, n)
	}
	rows, err := db.QueryContext(t.Context(), "SELECT "+strings.Join(cols, ", ")+" FROM "+table+" ORDER BY rowid")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var prev []uint64
	for rows.Next() {
		cur := make([]uint64, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cur {
			ptrs[i] = &cur[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		if prev != nil && slices.Compare(prev, cur) > 0 {
			t.Fatalf("%s is not ordered by %v, %v comes after %v", table, cols, cur, prev)
		}
		prev = cur
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func lowerAddr(addr string) string { return strings.ToLower(addr[2:]) }

/* Tables */

func TestPublishTables(t *testing.T) {
	t.Run("golden", func(t *testing.T) {
		db := goldenDB(t)
		for _, table := range tables {
			if n := one[int64](t, db, "SELECT CAST(count(*) AS BIGINT) FROM information_schema.tables WHERE table_name = ?", table); n != 1 {
				t.Errorf("table %s is missing", table)
			}
		}
		makers := one[int64](t, db, "SELECT CAST(count(DISTINCT lower(maker)) AS BIGINT) FROM read_parquet(?, hive_partitioning=false)",
			golden.Path("dataset", "order_filled", "*", "*.parquet"))
		if got := one[int64](t, db, "SELECT CAST(count(*) AS BIGINT) FROM wallet"); got != makers || got == 0 {
			t.Errorf("wallet has %d rows, want one per distinct maker (%d)", got, makers)
		}
		type row struct{ fills, markets, won uint32 }
		var got row
		err := db.QueryRowContext(t.Context(), "SELECT fills, markets, won_markets FROM wallet WHERE address = unhex(?)", lowerAddr(goldW)).Scan(&got.fills, &got.markets, &got.won)
		if err != nil || got != (row{290, 11, 4}) {
			t.Errorf("golden wallet is %+v (%v), want 290 fills, 11 markets, 4 won", got, err)
		}
		for _, w := range goldenWallets(t) {
			var fills uint32
			var volume, fees uint64
			var gross, biggest int64
			err := db.QueryRowContext(t.Context(), "SELECT fills, volume_usdc, gross_pnl, fees, biggest_win FROM wallet WHERE address = unhex(?)", lowerAddr(w.Address)).
				Scan(&fills, &volume, &gross, &fees, &biggest)
			if err != nil {
				t.Fatalf("%s: %v", w.Address, err)
			}
			if fills != w.Fills || int64(volume) != w.Volume || biggest != w.BiggestWin {
				t.Errorf("%s has %d fills, volume %d, biggest win %d, want %d, %d, %d", w.Address, fills, volume, biggest, w.Fills, w.Volume, w.BiggestWin)
			}
			if d := gross - int64(fees) - w.Realized; d > w.Tolerance || -d > w.Tolerance {
				t.Errorf("%s net %d, want %d within %d", w.Address, gross-int64(fees), w.Realized, w.Tolerance)
			}
		}
		assertOrdered(t, db, "wallet_market", "wallet_id", "market_id")
		assertOrdered(t, db, "wallet", "wallet_id")
		assertOrdered(t, db, "market", "market_id")
		assertOrdered(t, db, "wallet_calibration", "wallet_id", "bucket")
	})
	t.Run("meta", func(t *testing.T) {
		meta := metaOf(t, goldenDB(t))
		for key, want := range map[string]string{
			"note": specNote, "attribution": specAttribution, "license_url": specLicense, "source_url": specSource,
			"revision": specRevision, "modified": specModified, "cutoff_block": "85948288", "cutoff_ts": "1777016620",
			"rules_version": "pm-rules-1",
		} {
			if meta[key] != want {
				t.Errorf("meta %s is %q, want %q", key, meta[key], want)
			}
		}
		for _, key := range []string{
			"built_at", "total_wallets", "total_rated", "unmapped_rows", "duplicate_rows", "no_price_12h",
			"wash_rounds", "alg1_80", "alg1_90", "alg1_99", "alg2_share", "accuracy_12h",
		} {
			if meta[key] == "" {
				t.Errorf("meta %s is missing", key)
			}
		}
	})
	t.Run("washed", func(t *testing.T) {
		db := washedDB(t)
		if n := one[int64](t, db, "SELECT CAST(count(*) AS BIGINT) FROM wallet WHERE address = unhex(?)", lowerAddr(erin)); n != 0 {
			t.Errorf("a wallet that only splits has %d profile rows, want 0", n)
		}
		if n := one[int64](t, db, "SELECT CAST(count(*) AS BIGINT) FROM wallet"); n != 4 {
			t.Errorf("wallet has %d rows, want alice, bob, carol and dave", n)
		}
		if n := one[int64](t, db, "SELECT CAST(count(*) AS BIGINT) FROM wallet_market WHERE wallet_id NOT IN (SELECT wallet_id FROM wallet)"); n != 0 {
			t.Errorf("wallet_market has %d rows of wallets with no profile", n)
		}
		if n := one[int64](t, db, "SELECT CAST(count(*) AS BIGINT) FROM wash_trade"); n != 22 {
			t.Fatalf("wash_trade has %d rows, want 22 (11 trades seen from both wallets)", n)
		}
		assertOrdered(t, db, "wash_trade", "wallet_id", "rank")
		assertOrdered(t, db, "wash_counterparty", "wallet_id", "rank")
		assertOrdered(t, db, "wallet_market", "wallet_id", "market_id")
		evidence, err := db.QueryContext(t.Context(), `SELECT rank, shares FROM wash_trade
			WHERE wallet_id = (SELECT wallet_id FROM wallet WHERE address = unhex(?)) ORDER BY rank`, lowerAddr(alice))
		if err != nil {
			t.Fatal(err)
		}
		defer evidence.Close()
		var ranks, shares []uint64
		for evidence.Next() {
			var rank, n uint64
			if err := evidence.Scan(&rank, &n); err != nil {
				t.Fatal(err)
			}
			ranks, shares = append(ranks, rank), append(shares, n)
		}
		if want := []uint64{1, 2, 3, 4, 5, 6, 7}; !slices.Equal(ranks, want) {
			t.Errorf("alice evidence ranks %v, want %v", ranks, want)
		}
		if want := []uint64{300e6, 300e6, 150e6, 100e6, 100e6, 100e6, 100e6}; !slices.Equal(shares, want) {
			t.Errorf("alice evidence shares %v, want %v", shares, want)
		}
		if n := one[int64](t, db, "SELECT CAST(count(*) AS BIGINT) FROM wallet WHERE wash_shares > 0"); n != 4 {
			t.Errorf("%d wallets are flagged, want 4", n)
		}
	})
}

func goldenWallets(t *testing.T) []golden.Wallet {
	t.Helper()
	wallets, err := golden.LoadWallets("wallets_14")
	if err != nil {
		t.Fatal(err)
	}
	return wallets
}

/* Refusals */

func TestPublishRefusesIncompleteStage(t *testing.T) {
	dir := t.TempDir()
	work, err := testdb.Work(t.Context(), washWorld(t).Root, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = work.Close() })
	const file = "order_filled/year=2025/month=01.parquet"
	if _, err := work.ExecContext(t.Context(), "INSERT OR REPLACE INTO build_state VALUES ('stage', ?, 'failed', 0, 0, now())", file); err != nil {
		t.Fatal(err)
	}
	err = publish.Run(t.Context(), work, dir, nil)
	if !errors.Is(err, publish.ErrIncomplete) || !strings.Contains(err.Error(), file) {
		t.Fatalf("Run returned %v, want ErrIncomplete naming %s", err, file)
	}
	if _, err := os.Stat(filepath.Join(dir, "pm.duckdb")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("pm.duckdb exists after a refused run (%v)", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "pm.duckdb.new")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("pm.duckdb.new exists after a refused run (%v)", err)
	}
}

func TestPublishRefusesWithoutPass1Finish(t *testing.T) {
	dir := t.TempDir()
	work, err := testdb.Work(t.Context(), washWorld(t).Root, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = work.Close() })
	if _, err := work.ExecContext(t.Context(), "DELETE FROM build_state WHERE stage = 'pass1' AND item = 'finish'"); err != nil {
		t.Fatal(err)
	}
	err = publish.Run(t.Context(), work, dir, nil)
	if !errors.Is(err, publish.ErrIncomplete) || !strings.Contains(err.Error(), "pass1") {
		t.Fatalf("Run returned %v, want ErrIncomplete naming pass1", err)
	}
}

func TestPublishRefusesUnfinishedStage(t *testing.T) {
	dir := t.TempDir()
	work, err := testdb.Work(t.Context(), washWorld(t).Root, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = work.Close() })
	if _, err := work.ExecContext(t.Context(), "DELETE FROM build_state WHERE stage = 'pass2' AND item = '07'"); err != nil {
		t.Fatal(err)
	}
	err = publish.Run(t.Context(), work, dir, nil)
	if !errors.Is(err, publish.ErrIncomplete) || !strings.Contains(err.Error(), "pass2") {
		t.Fatalf("Run returned %v, want ErrIncomplete naming pass2", err)
	}
}

/* Counterparties */

func TestPublishSumsCounterpartiesAcrossBuckets(t *testing.T) {
	dir := t.TempDir()
	work, err := testdb.Work(t.Context(), washWorld(t).Root, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = work.Close() })
	rows := [][4]uint64{
		{10, 1, 2, 500},
		{10, 1, 3, 700},
		{11, 1, 1, 1100},
		{12, 1, 1, 1000},
		{13, 1, 1, 900},
		{14, 1, 1, 800},
		{15, 1, 1, 100},
		{16, 1, 1, 50},
	}
	if _, err := work.ExecContext(t.Context(), "DELETE FROM wash_cp"); err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if _, err := work.ExecContext(t.Context(), "INSERT INTO wash_cp VALUES (0, ?, ?, ?, ?)", r[0], r[1], r[2], r[3]); err != nil {
			t.Fatal(err)
		}
	}
	if err := publish.Run(t.Context(), work, dir, nil); err != nil {
		t.Fatal(err)
	}
	db := openRO(t, filepath.Join(dir, "pm.duckdb"))
	rank, err := db.QueryContext(t.Context(), "SELECT rank, counterparty_id, markets, trades, shares FROM wash_counterparty WHERE wallet_id = 0 ORDER BY rank")
	if err != nil {
		t.Fatal(err)
	}
	defer rank.Close()
	type got struct{ rank, cp, markets, trades, shares uint64 }
	var all []got
	for rank.Next() {
		var g got
		if err := rank.Scan(&g.rank, &g.cp, &g.markets, &g.trades, &g.shares); err != nil {
			t.Fatal(err)
		}
		all = append(all, g)
	}
	want := []got{{1, 10, 2, 5, 1200}, {2, 11, 1, 1, 1100}, {3, 12, 1, 1, 1000}, {4, 13, 1, 1, 900}, {5, 14, 1, 1, 800}}
	if !slices.Equal(all, want) {
		t.Fatalf("wash_counterparty is %v, want %v", all, want)
	}
}

/* Atomic rename */

func TestPublishIsAtomic(t *testing.T) {
	dir := t.TempDir()
	work, err := testdb.Work(t.Context(), washWorld(t).Root, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = work.Close() })
	path := filepath.Join(dir, "pm.duckdb")
	if err := publish.Run(t.Context(), work, dir, nil); err != nil {
		t.Fatal(err)
	}
	reader := openRO(t, path)
	old := one[string](t, reader, "SELECT value FROM meta WHERE key = 'built_at'")
	if err := publish.Run(t.Context(), work, dir, nil); err != nil {
		t.Fatal(err)
	}
	if got := one[string](t, reader, "SELECT value FROM meta WHERE key = 'built_at'"); got != old {
		t.Errorf("the reader sees built_at %s after the rename, want its own file's %s", got, old)
	}
	if n := one[int64](t, reader, "SELECT CAST(count(*) AS BIGINT) FROM wallet"); n != 4 {
		t.Errorf("the reader sees %d wallets, want 4", n)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	fresh := openRO(t, path)
	if got := one[string](t, fresh, "SELECT value FROM meta WHERE key = 'built_at'"); got == old {
		t.Errorf("a fresh open sees built_at %s, want the new file's", got)
	}
	left, err := filepath.Glob(filepath.Join(dir, "pm.duckdb.*"))
	if err != nil || len(left) != 0 {
		t.Errorf("files left beside pm.duckdb: %v (%v)", left, err)
	}
}

/* Verify */

func TestVerifyFullTotalsMismatch(t *testing.T) {
	path := goldenBuild.get(t, testdb.Build)
	err := publish.Verify(t.Context(), path, &publish.FullTotals)
	if err == nil {
		t.Fatal("Verify passed the small database against the full totals")
	}
	for _, name := range []string{"fills", "taker rows", "volume_usdc", "markets", "resolved markets", "with a resolution time"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("the error does not list %q\n%v", name, err)
		}
	}
	for _, name := range []string{"wallets", "last block"} {
		if strings.Contains(err.Error(), name) {
			t.Errorf("the error lists %q, which matches\n%v", name, err)
		}
	}
	if err := publish.Verify(t.Context(), path, nil); err != nil {
		t.Fatalf("Verify without totals: %v", err)
	}
}

func TestVerifyReadOnly(t *testing.T) {
	src, err := os.Open(goldenBuild.get(t, testdb.Build))
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	copyPath := filepath.Join(t.TempDir(), "pm.duckdb")
	dst, err := os.Create(copyPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(dst, src); err != nil {
		t.Fatal(err)
	}
	if err := dst.Close(); err != nil {
		t.Fatal(err)
	}
	holder := openRO(t, copyPath)
	if n := one[int64](t, holder, "SELECT CAST(count(*) AS BIGINT) FROM wallet"); n == 0 {
		t.Fatal("the copy is empty")
	}
	if err := publish.Verify(t.Context(), copyPath, nil); err != nil {
		t.Fatalf("Verify while a reader holds the file: %v", err)
	}
}

func TestVerifyCatchesBrokenFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pm.duckdb")
	if err := os.WriteFile(path, []byte("not a database"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := publish.Verify(t.Context(), path, nil); err == nil {
		t.Fatal("Verify passed a file that is not a database")
	}
	if err := publish.Verify(t.Context(), filepath.Join(t.TempDir(), "missing.duckdb"), nil); err == nil {
		t.Fatal("Verify passed a missing file")
	}
}

/* Calibration and accuracy */

func priced(t *testing.T) *sql.DB {
	t.Helper()
	dir := t.TempDir()
	work, err := testdb.Work(t.Context(), dstest.NewWorld(t).Root, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = work.Close() })
	for id, m := range []struct {
		price                 int
		payout0, payout1, den string
	}{
		{990000, "1000000000000000000", "0", "1000000000000000000"},
		{510000, "0", "1", "1"},
		{40000, "0", "1", "1"},
	} {
		stmts := []string{
			fmt.Sprintf("UPDATE market SET resolved = true, payout0 = %s, payout1 = %s, payout_den = %s WHERE market_id = %d", m.payout0, m.payout1, m.den, id),
			fmt.Sprintf("UPDATE market_acc SET p0_12h = %d WHERE market_id = %d", m.price, id),
		}
		for _, stmt := range stmts {
			if _, err := work.ExecContext(t.Context(), stmt); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := work.ExecContext(t.Context(), "UPDATE market_acc SET p0_4h = NULL, p0_24h = NULL, p0_168h = NULL"); err != nil {
		t.Fatal(err)
	}
	if err := publish.Run(t.Context(), work, dir, nil); err != nil {
		t.Fatal(err)
	}
	return openRO(t, filepath.Join(dir, "pm.duckdb"))
}

func TestCalibrationRows(t *testing.T) {
	db := priced(t)
	rows, err := db.QueryContext(t.Context(), "SELECT bucket, markets FROM calibration WHERE horizon_h = 12 AND kind = 0 ORDER BY bucket DESC")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var buckets [][2]uint64
	for rows.Next() {
		var b, n uint64
		if err := rows.Scan(&b, &n); err != nil {
			t.Fatal(err)
		}
		buckets = append(buckets, [2]uint64{b, n})
	}
	if want := [][2]uint64{{19, 1}, {10, 1}, {0, 1}}; !slices.Equal(buckets, want) {
		t.Fatalf("buckets %v, want %v", buckets, want)
	}
	brier := one[float64](t, db, "SELECT sum(sum_sq) / sum(markets) FROM calibration WHERE horizon_h = 12 AND kind = 0")
	if math.Abs(brier-0.0872667) > 1e-6 {
		t.Errorf("brier %v, want 0.0872667", brier)
	}
	if n := one[int64](t, db, "SELECT CAST(count(*) AS BIGINT) FROM calibration WHERE horizon_h <> 12"); n != 0 {
		t.Errorf("%d rows for horizons without a price", n)
	}
}

func TestCalibrationBucketsMatchSkill(t *testing.T) {
	dir := t.TempDir()
	work, err := testdb.Work(t.Context(), dstest.NewWorld(t).Root, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = work.Close() })
	prices := []int64{0, 49999, 50000, 499999, 500000, 999999, 1000000, 1200000}
	if _, err := work.ExecContext(t.Context(), "DELETE FROM market_acc"); err != nil {
		t.Fatal(err)
	}
	if _, err := work.ExecContext(t.Context(), "DELETE FROM market WHERE market_id <> 0"); err != nil {
		t.Fatal(err)
	}
	for i, p := range prices {
		stmt := fmt.Sprintf(`INSERT INTO market SELECT %d, unhex('%064x'), question, slug, end_date, 0, false, ek, bucket, true, 0, 1, 1
			FROM market WHERE market_id = 0`, i+10, i)
		if _, err := work.ExecContext(t.Context(), stmt); err != nil {
			t.Fatal(err)
		}
		if _, err := work.ExecContext(t.Context(), "INSERT INTO market_acc VALUES (?, 1, 1, 1, NULL, ?, NULL, NULL)", i+10, p); err != nil {
			t.Fatal(err)
		}
	}
	if err := publish.Run(t.Context(), work, dir, nil); err != nil {
		t.Fatal(err)
	}
	db := openRO(t, filepath.Join(dir, "pm.duckdb"))
	want := map[uint64]uint64{}
	for _, p := range prices {
		want[uint64(skill.Bucket20(p))]++
	}
	rows, err := db.QueryContext(t.Context(), "SELECT bucket, markets FROM calibration WHERE horizon_h = 12 AND kind = 0")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[uint64]uint64{}
	for rows.Next() {
		var b, n uint64
		if err := rows.Scan(&b, &n); err != nil {
			t.Fatal(err)
		}
		got[b] = n
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("buckets %v, want %v", got, want)
	}
}

func TestAccuracyMeta(t *testing.T) {
	meta := metaOf(t, priced(t))
	got, err := strconv.ParseFloat(meta["accuracy_12h"], 64)
	if err != nil || math.Abs(got-0.6666667) > 1e-6 {
		t.Errorf("accuracy_12h is %q (%v), want 0.6666667", meta["accuracy_12h"], err)
	}
	for _, key := range []string{"accuracy_4h", "accuracy_24h", "accuracy_168h"} {
		if v, ok := meta[key]; ok {
			t.Errorf("meta %s is %q, want no key for a horizon with no price", key, v)
		}
	}
}

/* Rating */

func TestPublishRatedWallets(t *testing.T) {
	db := goldenDB(t)
	rows, err := db.QueryContext(t.Context(), "SELECT rated_markets, rated_events, rated, brier, edge, edge_se FROM wallet")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var rated int
	for rows.Next() {
		var markets, events uint32
		var isRated bool
		var brier, edge, se sql.Null[float64]
		if err := rows.Scan(&markets, &events, &isRated, &brier, &edge, &se); err != nil {
			t.Fatal(err)
		}
		if isRated != skill.Rated(markets, events) {
			t.Fatalf("wallet with %d rated markets in %d events has rated %v", markets, events, isRated)
		}
		if isRated != (brier.Valid && edge.Valid && se.Valid) {
			t.Fatalf("rated %v with brier %v, edge %v, edge_se %v", isRated, brier.Valid, edge.Valid, se.Valid)
		}
		if isRated {
			rated++
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if total := one[string](t, db, "SELECT value FROM meta WHERE key = 'total_rated'"); total != strconv.Itoa(rated) {
		t.Errorf("total_rated is %s, want %d", total, rated)
	}
	if rated > 0 {
		if n := one[int64](t, db, "SELECT CAST(count(*) AS BIGINT) FROM wallet_calibration"); n == 0 {
			t.Error("rated wallets have no calibration rows")
		}
	}
	if n := one[int64](t, db, "SELECT CAST(count(*) AS BIGINT) FROM wallet_calibration WHERE wallet_id NOT IN (SELECT wallet_id FROM wallet WHERE rated)"); n != 0 {
		t.Errorf("%d calibration rows belong to unrated wallets", n)
	}
	if n := one[int64](t, db, "SELECT CAST(count(*) AS BIGINT) FROM wallet WHERE biggest_win > 0 AND biggest_win <= 1000000"); n != 0 {
		t.Errorf("%d wallets have a biggest win of $1.00 or less above 0", n)
	}
}

func TestPublishRatesFromTheFloor(t *testing.T) {
	dir := t.TempDir()
	work, err := testdb.Work(t.Context(), washWorld(t).Root, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = work.Close() })
	id := func(addr string) uint32 {
		return one[uint32](t, work, "SELECT wallet_id FROM wallet_dict WHERE address = unhex(?)", lowerAddr(addr))
	}
	acc := skill.Acc{N: 40, SumT: 8_000_000, SumT2: 3_600_000_000_000}
	for _, w := range []struct {
		addr           string
		markets, event int
	}{{alice, 40, 5}, {bob, 29, 5}, {carol, 40, 2}} {
		stmt := fmt.Sprintf("UPDATE wallet_acc SET rated_markets = %d, rated_events = %d, sum_t = %d, sum_t2 = %d WHERE wallet_id = ?",
			w.markets, w.event, acc.SumT, acc.SumT2)
		if _, err := work.ExecContext(t.Context(), stmt, id(w.addr)); err != nil {
			t.Fatal(err)
		}
		if _, err := work.ExecContext(t.Context(), "INSERT INTO wallet_cal VALUES (?, 4, 40, 36000000, 40000000)", id(w.addr)); err != nil {
			t.Fatal(err)
		}
	}
	if err := publish.Run(t.Context(), work, dir, nil); err != nil {
		t.Fatal(err)
	}
	db := openRO(t, filepath.Join(dir, "pm.duckdb"))
	se, ok := acc.EdgeSE()
	if !ok {
		t.Fatal("no standard error for 40 markets")
	}
	var rated bool
	var brier, edge, gotSE float64
	err = db.QueryRowContext(t.Context(), "SELECT rated, brier, edge, edge_se FROM wallet WHERE address = unhex(?)", lowerAddr(alice)).Scan(&rated, &brier, &edge, &gotSE)
	if err != nil || !rated || brier != acc.Brier() || edge != acc.Edge() || gotSE != se {
		t.Errorf("alice is rated %v with brier %v, edge %v, se %v (%v), want true, %v, %v, %v", rated, brier, edge, gotSE, err, acc.Brier(), acc.Edge(), se)
	}
	for _, addr := range []string{bob, carol} {
		var isRated bool
		var b sql.Null[float64]
		if err := db.QueryRowContext(t.Context(), "SELECT rated, brier FROM wallet WHERE address = unhex(?)", lowerAddr(addr)).Scan(&isRated, &b); err != nil || isRated || b.Valid {
			t.Errorf("%s is rated %v with brier %v (%v), want unrated with no brier", addr, isRated, b, err)
		}
	}
	if got := one[string](t, db, "SELECT value FROM meta WHERE key = 'total_rated'"); got != "1" {
		t.Errorf("total_rated is %s, want 1", got)
	}
	if n := one[int64](t, db, "SELECT CAST(count(*) AS BIGINT) FROM wallet_calibration"); n != 1 {
		t.Errorf("wallet_calibration has %d rows, want only alice's", n)
	}
}
