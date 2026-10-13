package store_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"pm/internal/dataset"
	"pm/internal/golden"
	"pm/internal/skill"
	"pm/internal/store"
	"pm/internal/testdb"
)

const (
	goldWallet   = "0x467ecfc0c6265683e90899C41881c0431bDEE2ed"
	ratedWallet  = "0x6326fC8A7361717B3926287f2998c91ee4fAADE2"
	cutoffWallet = "0xAaDce4CcC5a50Ed1CCDC7a0c88dc6adf9B93B7BA"
)

/* Fixtures */

type built struct {
	once      sync.Once
	dir, path string
	err       error
}

var goldenBuild, pairBuild built

func TestMain(m *testing.M) {
	code := m.Run()
	for _, b := range []*built{&goldenBuild, &pairBuild} {
		if b.dir != "" {
			_ = os.RemoveAll(b.dir)
		}
	}
	os.Exit(code)
}

func (b *built) get(t *testing.T, build func(string) (string, error)) string {
	t.Helper()
	b.once.Do(func() {
		b.dir, b.err = os.MkdirTemp("", "pm-store-")
		if b.err == nil {
			b.path, b.err = build(b.dir)
		}
	})
	if b.err != nil {
		t.Fatal(b.err)
	}
	return b.path
}

func goldenPath(t *testing.T) string {
	t.Helper()
	return goldenBuild.get(t, func(dir string) (string, error) { return testdb.Build(context.Background(), dir) })
}

func pairPath(t *testing.T) string {
	t.Helper()
	return pairBuild.get(t, func(dir string) (string, error) { return testdb.Pair(t, dir) })
}

func copyFile(t testing.TB, src, dst string) {
	t.Helper()
	in, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, in); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
}

func openStore(t *testing.T, path string, every time.Duration) *store.Store {
	t.Helper()
	s := store.New(path, store.Options{Every: every})
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func goldenStore(t *testing.T) *store.Store {
	t.Helper()
	return openStore(t, goldenPath(t), time.Hour)
}

func readOnly(t testing.TB, path string) *sql.DB {
	t.Helper()
	connector, err := duckdb.NewConnector(path+"?access_mode=READ_ONLY", nil)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func one[T any](t testing.TB, db *sql.DB, query string, args ...any) T {
	t.Helper()
	var v T
	if err := db.QueryRowContext(t.Context(), query, args...).Scan(&v); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return v
}

func addr(t testing.TB, s string) [20]byte {
	t.Helper()
	raw, err := hex.DecodeString(strings.ToLower(s[2:]))
	if err != nil || len(raw) != 20 {
		t.Fatalf("bad address %q", s)
	}
	return [20]byte(raw)
}

func waitFor(t *testing.T, within time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %v", within)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

/* Not built */

func assertNotBuilt(t *testing.T, s *store.Store) {
	t.Helper()
	ctx := t.Context()
	if s.Ready() {
		t.Error("Ready is true")
	}
	if _, err := s.Status(ctx); !errors.Is(err, store.ErrNotBuilt) {
		t.Errorf("Status returned %v, want ErrNotBuilt", err)
	}
	if _, err := s.Trader(ctx, addr(t, goldWallet)); !errors.Is(err, store.ErrNotBuilt) {
		t.Errorf("Trader returned %v, want ErrNotBuilt", err)
	}
	if _, err := s.Board(ctx, "pnl", 5, 0); !errors.Is(err, store.ErrNotBuilt) {
		t.Errorf("Board returned %v, want ErrNotBuilt", err)
	}
	if _, err := s.Calibration(ctx, 12, "other"); !errors.Is(err, store.ErrNotBuilt) {
		t.Errorf("Calibration returned %v, want ErrNotBuilt", err)
	}
}

func TestStoreNotBuilt(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "pm.duckdb"), time.Hour)
	assertNotBuilt(t, s)
}

func TestStoreCorruptFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pm.duckdb")
	if err := os.WriteFile(path, bytes.Repeat([]byte("not a database "), 4000), 0o600); err != nil {
		t.Fatal(err)
	}
	s := openStore(t, path, 50*time.Millisecond)
	assertNotBuilt(t, s)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go s.Run(ctx)
	time.Sleep(200 * time.Millisecond)
	assertNotBuilt(t, s)

	good := filepath.Join(dir, "good.duckdb")
	copyFile(t, goldenPath(t), good)
	if err := os.Rename(good, path); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, s.Ready)
	if _, err := s.Status(t.Context()); err != nil {
		t.Errorf("Status after the valid file arrived returned %v", err)
	}
}

func TestStoreKeepsServingOnBadReplace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pm.duckdb")
	copyFile(t, goldenPath(t), path)
	s := openStore(t, path, 50*time.Millisecond)
	if !s.Ready() {
		t.Fatal("a valid file did not open")
	}
	before, err := s.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go s.Run(ctx)
	bad := filepath.Join(dir, "bad.duckdb")
	if err := os.WriteFile(bad, bytes.Repeat([]byte("half written "), 5000), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(bad, path); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	after, err := s.Status(t.Context())
	if err != nil || after.BuiltAt != before.BuiltAt {
		t.Errorf("Status after a bad replace is %q (%v), want the old %q", after.BuiltAt, err, before.BuiltAt)
	}
}

func TestStoreReopensOnReplace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pm.duckdb")
	next := filepath.Join(dir, "next.duckdb")
	copyFile(t, goldenPath(t), path)
	copyFile(t, pairPath(t), next)
	peek := filepath.Join(dir, "peek.duckdb")
	copyFile(t, pairPath(t), peek)
	want, err := openStore(t, peek, time.Hour).Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	s := openStore(t, path, 50*time.Millisecond)
	old, err := s.Status(t.Context())
	if err != nil || old.BuiltAt == want.BuiltAt {
		t.Fatalf("old built_at %q (%v) should differ from %q", old.BuiltAt, err, want.BuiltAt)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go s.Run(ctx)

	var served, bad atomic.Int64
	var mu sync.Mutex
	var other []error
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, err1 := s.Status(ctx)
				_, err2 := s.Board(ctx, "volume", 3, 0)
				for _, err := range []error{err1, err2} {
					switch {
					case err == nil:
						served.Add(1)
					case errors.Is(err, store.ErrNotBuilt):
						bad.Add(1)
					default:
						mu.Lock()
						other = append(other, err)
						mu.Unlock()
					}
				}
			}
		})
	}
	time.Sleep(100 * time.Millisecond)
	if err := os.Rename(next, path); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		got, err := s.Status(ctx)
		return err == nil && got.BuiltAt == want.BuiltAt
	})
	time.Sleep(100 * time.Millisecond)
	close(stop)
	readers.Wait()
	if len(other) > 0 {
		t.Errorf("%d readers saw an error other than ErrNotBuilt during the swap, first %v", len(other), other[0])
	}
	if served.Load() == 0 {
		t.Error("no reader was served")
	}
	page, err := s.Board(t.Context(), "volume", 5, 0)
	if err != nil || page.Total != 3 {
		t.Errorf("board total after the swap is %d (%v), want the 3 wallets of the new file", page.Total, err)
	}
}

/* Status */

func TestStatus(t *testing.T) {
	path := goldenPath(t)
	db := readOnly(t, path)
	got, err := openStore(t, path, time.Hour).Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if want := one[int64](t, db, "SELECT CAST(count(*) AS BIGINT) FROM wallet"); got.Wallets != want {
		t.Errorf("Wallets %d, want %d", got.Wallets, want)
	}
	if want := one[int64](t, db, "SELECT CAST(count(*) AS BIGINT) FROM wallet WHERE rated"); got.RatedWallets != want {
		t.Errorf("RatedWallets %d, want %d", got.RatedWallets, want)
	}
	if want := one[int64](t, db, "SELECT CAST(sum(fills) AS BIGINT) FROM wallet"); got.Fills != want {
		t.Errorf("Fills %d, want %d", got.Fills, want)
	}
	if want := one[int64](t, db, "SELECT CAST(count(*) AS BIGINT) FROM market"); got.Markets != want {
		t.Errorf("Markets %d, want %d", got.Markets, want)
	}
	if want := one[int64](t, db, "SELECT CAST(count(*) AS BIGINT) FROM market WHERE resolved"); got.ResolvedMarkets != want {
		t.Errorf("ResolvedMarkets %d, want %d", got.ResolvedMarkets, want)
	}
	if got.RulesVersion != "pm-rules-1" || got.BuiltAt == "" {
		t.Errorf("rules %q, built_at %q", got.RulesVersion, got.BuiltAt)
	}
	c := got.Coverage
	if c.Block != 85948288 || c.Time != 1777016620 || c.Revision != dataset.Revision || c.LicenseURL == "" || c.Note == "" || c.Attribution == "" || c.Modified == "" || c.SourceURL == "" {
		t.Errorf("coverage %+v is incomplete", c)
	}
}

/* Trader */

func TestTraderGolden(t *testing.T) {
	s := goldenStore(t)
	got, err := s.Trader(t.Context(), addr(t, goldWallet))
	if err != nil {
		t.Fatal(err)
	}
	if got.Fills != 290 || got.VolumeUSDC != 716582888996 || got.Markets != 11 || got.ResolvedMarkets != 11 || got.WonMarkets != 4 || got.BiggestWin != 95527217758 {
		t.Errorf("golden wallet is %+v, want 290 fills, volume 716582888996, 11 markets, 11 resolved, 4 won, biggest win 95527217758", got)
	}
	if d := got.GrossPnL - got.Fees - -16628825094; d > 200 || -d > 200 {
		t.Errorf("net %d, want -16628825094 within 200", got.GrossPnL-got.Fees)
	}
	if got.Wallet != addr(t, goldWallet) || got.OpenUnresolved != 0 || got.Rated {
		t.Errorf("wallet %x, open %d, rated %v", got.Wallet, got.OpenUnresolved, got.Rated)
	}
	if len(got.Calibration) != 0 || got.Brier != nil || got.Edge != nil || got.EdgeSE != nil {
		t.Errorf("an unrated wallet carries skill figures: %+v", got)
	}
	if got.Coverage.Block != 85948288 {
		t.Errorf("coverage block %d", got.Coverage.Block)
	}
	if len(got.Counterparties) != 0 && got.WashShares == 0 {
		t.Error("an unflagged wallet carries counterparties")
	}
}

func TestTraderAllGoldenWallets(t *testing.T) {
	s := goldenStore(t)
	wallets, err := golden.LoadWallets("wallets_14")
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range wallets {
		got, err := s.Trader(t.Context(), addr(t, w.Address))
		if err != nil {
			t.Fatalf("%s: %v", w.Address, err)
		}
		if got.Fills != int64(w.Fills) || got.VolumeUSDC != w.Volume || got.BiggestWin != w.BiggestWin {
			t.Errorf("%s has %d fills, volume %d, biggest win %d, want %d, %d, %d", w.Address, got.Fills, got.VolumeUSDC, got.BiggestWin, w.Fills, w.Volume, w.BiggestWin)
		}
		if d := got.GrossPnL - got.Fees - w.Realized; d > w.Tolerance || -d > w.Tolerance {
			t.Errorf("%s net %d, want %d within %d", w.Address, got.GrossPnL-got.Fees, w.Realized, w.Tolerance)
		}
	}
}

func TestTraderRated(t *testing.T) {
	s := goldenStore(t)
	got, err := s.Trader(t.Context(), addr(t, ratedWallet))
	if err != nil {
		t.Fatal(err)
	}
	if !got.Rated || got.Brier == nil || got.Edge == nil || got.EdgeSE == nil {
		t.Fatalf("rated wallet has rated %v, brier %v, edge %v, edge_se %v", got.Rated, got.Brier, got.Edge, got.EdgeSE)
	}
	if got.RatedMarkets < skill.MinMarkets || got.RatedEvents < skill.MinEvents {
		t.Errorf("rated with %d markets in %d events, under the floor", got.RatedMarkets, got.RatedEvents)
	}
	var n int64
	for i, b := range got.Calibration {
		if b.Bucket < 0 || b.Bucket > 4 || (i > 0 && b.Bucket <= got.Calibration[i-1].Bucket) {
			t.Errorf("calibration buckets are out of order at %d: %+v", i, got.Calibration)
		}
		n += b.N
	}
	if n != got.RatedMarkets {
		t.Errorf("calibration holds %d markets, want the %d rated markets", n, got.RatedMarkets)
	}
	if got.Markets != 56 || got.ResolvedMarkets < 30 {
		t.Errorf("wallet has %d markets and %d resolved, want 56 and at least 30", got.Markets, got.ResolvedMarkets)
	}
}

func TestTraderOpenPositions(t *testing.T) {
	got, err := goldenStore(t).Trader(t.Context(), addr(t, cutoffWallet))
	if err != nil {
		t.Fatal(err)
	}
	if got.OpenUnresolved != 26 {
		t.Errorf("open unresolved %d, want 26", got.OpenUnresolved)
	}
}

func TestTraderNotFound(t *testing.T) {
	_, err := goldenStore(t).Trader(t.Context(), addr(t, "0x1234567890123456789012345678901234567890"))
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Trader returned %v, want ErrNotFound", err)
	}
}

func TestTraderLoadsNoIndex(t *testing.T) {
	s := goldenStore(t)
	if _, err := s.Trader(t.Context(), addr(t, goldWallet)); err != nil {
		t.Fatal(err)
	}
	got, err := s.Query(t.Context(), "SELECT CAST(coalesce(sum(memory_usage_bytes), 0) AS VARCHAR) FROM duckdb_memory() WHERE tag = 'ART_INDEX'")
	if err != nil || got != "0" {
		t.Errorf("ART_INDEX memory is %q (%v), want 0", got, err)
	}
}

func TestTraderWashEvidence(t *testing.T) {
	s := openStore(t, pairPath(t), time.Hour)
	a, err := s.Trader(t.Context(), addr(t, testdb.Alice))
	if err != nil {
		t.Fatal(err)
	}
	if a.WashScore == nil || *a.WashScore <= 0.8 || *a.WashScore >= 1 || a.PairedShares != 240_000_000 || a.WashShares != 200_000_000 {
		t.Fatalf("alice has score %v, paired %d, flagged %d, want a score in (0.8, 1), 240000000, 200000000", a.WashScore, a.PairedShares, a.WashShares)
	}
	if len(a.Counterparties) != 1 {
		t.Fatalf("alice has %d counterparties, want 1", len(a.Counterparties))
	}
	cp := a.Counterparties[0]
	if cp.Wallet != addr(t, testdb.Bob) || cp.Trades != 2 || cp.Markets != 1 || cp.Shares != 200_000_000 || cp.Score <= 0.9 || cp.Score >= 1 {
		t.Errorf("counterparty %+v, want bob with 2 trades in 1 market and 200000000 shares, score in (0.9, 1)", cp)
	}
	if len(a.Trades) != 2 {
		t.Fatalf("alice has %d evidence trades, want 2", len(a.Trades))
	}
	first, second := a.Trades[0], a.Trades[1]
	if first.Block != 100 || first.LogIndex != 1 || second.Block != 101 || second.LogIndex != 1 {
		t.Errorf("trades are at %d/%d and %d/%d, want 100/1 then 101/1", first.Block, first.LogIndex, second.Block, second.LogIndex)
	}
	if !first.Buy || second.Buy || first.Outcome != 0 || first.Shares != 100_000_000 || first.USDC != 50_000_000 || first.TS != 1700000100 {
		t.Errorf("first trade is %+v", first)
	}
	if first.Counterparty != addr(t, testdb.Bob) || hex.EncodeToString(first.Condition[:]) != strings.Repeat("1", 64) {
		t.Errorf("first trade counterparty %x, condition %x", first.Counterparty, first.Condition)
	}
	if first.Question == nil || *first.Question != "Will A happen?" {
		t.Errorf("question %v, want %q", first.Question, "Will A happen?")
	}
}

func TestTraderNoWashPairs(t *testing.T) {
	db := readOnly(t, goldenPath(t))
	var raw []byte
	if err := db.QueryRowContext(t.Context(), "SELECT address FROM wallet WHERE wash_score IS NULL LIMIT 1").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	got, err := goldenStore(t).Trader(t.Context(), [20]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if got.WashScore != nil || got.PairedShares != 0 || got.WashShares != 0 || len(got.Counterparties) != 0 || len(got.Trades) != 0 {
		t.Errorf("a wallet with no paired rows carries wash figures: %+v", got)
	}
}

/* Board */

func TestBoardOrder(t *testing.T) {
	s := goldenStore(t)
	total := one[int64](t, readOnly(t, goldenPath(t)), "SELECT CAST(count(*) AS BIGINT) FROM wallet")
	for _, board := range []string{"pnl", "volume"} {
		page, err := s.Board(t.Context(), board, int(total), 0)
		if err != nil {
			t.Fatal(err)
		}
		if page.Total != total || int64(len(page.Rows)) != total || page.Board != board || page.Limit != int(total) || page.Offset != 0 {
			t.Fatalf("%s page has total %d, %d rows, board %q", board, page.Total, len(page.Rows), page.Board)
		}
		key := func(x store.Row) int64 {
			if board == "pnl" {
				return x.Realized
			}
			return x.VolumeUSDC
		}
		var ties int
		for i, r := range page.Rows {
			if r.Rank != i+1 {
				t.Fatalf("%s row %d has rank %d", board, i, r.Rank)
			}
			if i == 0 {
				continue
			}
			p := page.Rows[i-1]
			switch {
			case key(p) < key(r), key(p) == key(r) && p.VolumeUSDC < r.VolumeUSDC:
				t.Fatalf("%s rows %d and %d are out of order: %+v then %+v", board, i-1, i, p, r)
			case key(p) == key(r) && p.VolumeUSDC == r.VolumeUSDC:
				ties++
				// wallet ids follow address order
				if bytes.Compare(p.Wallet[:], r.Wallet[:]) >= 0 {
					t.Fatalf("%s rows %d and %d tie and are not in wallet order", board, i-1, i)
				}
			}
		}
		t.Logf("%s board of %d rows has %d full ties", board, total, ties)
	}
}

func TestBoardOffsetAndLimit(t *testing.T) {
	s := goldenStore(t)
	all, err := s.Board(t.Context(), "pnl", 20, 0)
	if err != nil {
		t.Fatal(err)
	}
	part, err := s.Board(t.Context(), "pnl", 5, 7)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range part.Rows {
		if r.Rank != 8+i || r.Wallet != all.Rows[7+i].Wallet {
			t.Errorf("row %d is rank %d wallet %x, want rank %d wallet %x", i, r.Rank, r.Wallet, 8+i, all.Rows[7+i].Wallet)
		}
	}
	mid, err := s.Board(t.Context(), "pnl", 10, 5000)
	if err != nil || len(mid.Rows) != 10 || mid.Rows[0].Rank != 5001 {
		t.Errorf("offset 5000 gives %d rows (%v), want 10 starting at rank 5001", len(mid.Rows), err)
	}
	// the golden set has fewer than 10000 wallets, so the largest offset is past the end
	past, err := s.Board(t.Context(), "pnl", 10, 10000)
	if err != nil || len(past.Rows) != 0 || past.Rows == nil || past.Total != all.Total {
		t.Errorf("offset 10000 gives %d rows, total %d (%v), want an empty page and total %d", len(past.Rows), past.Total, err, all.Total)
	}
}

func TestBoardSkill(t *testing.T) {
	s := goldenStore(t)
	page, err := s.Board(t.Context(), "skill", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 3 || len(page.Rows) != 3 {
		t.Fatalf("skill board has total %d and %d rows, want the 3 rated wallets", page.Total, len(page.Rows))
	}
	for i, r := range page.Rows {
		if !r.Rated || r.RatedMarkets < skill.MinMarkets || r.RatedEvents < skill.MinEvents {
			t.Errorf("row %d is not a rated wallet: %+v", i, r)
		}
		if want := r.Edge - skill.Z*r.EdgeSE; math.Abs(r.EdgeLB-want) > 1e-12 {
			t.Errorf("row %d edge_lb %v, want %v", i, r.EdgeLB, want)
		}
		if i > 0 && page.Rows[i-1].EdgeLB < r.EdgeLB {
			t.Errorf("rows %d and %d are not in edge_lb order", i-1, i)
		}
	}
	empty := openStore(t, pairPath(t), time.Hour)
	none, err := empty.Board(t.Context(), "skill", 10, 0)
	if err != nil || none.Total != 0 || len(none.Rows) != 0 {
		t.Errorf("skill board without rated wallets has total %d, %d rows (%v), want none", none.Total, len(none.Rows), err)
	}
}

func TestBoardShares(t *testing.T) {
	page, err := openStore(t, pairPath(t), time.Hour).Board(t.Context(), "volume", 5, 0)
	if err != nil || len(page.Rows) != 3 {
		t.Fatalf("pair board has %d rows (%v), want 3", len(page.Rows), err)
	}
	for _, r := range page.Rows {
		switch r.Wallet {
		case addr(t, testdb.Carol):
			if r.PairedShares != 40_000_000 || r.WashShares != 0 {
				t.Errorf("carol has paired %d and flagged %d, want 40000000 and none", r.PairedShares, r.WashShares)
			}
		case addr(t, testdb.Alice):
			if r.PairedShares != 240_000_000 || r.WashShares != 200_000_000 {
				t.Errorf("alice has paired %d and flagged %d, want 240000000 and 200000000", r.PairedShares, r.WashShares)
			}
		default:
			if r.PairedShares != 200_000_000 || r.WashShares != 200_000_000 {
				t.Errorf("row %x has paired %d and flagged %d, want 200000000 each", r.Wallet, r.PairedShares, r.WashShares)
			}
		}
	}
}

func TestBoardUnknown(t *testing.T) {
	if _, err := goldenStore(t).Board(t.Context(), "foo", 5, 0); err == nil || errors.Is(err, store.ErrNotBuilt) {
		t.Errorf("an unknown board returned %v, want an error other than ErrNotBuilt", err)
	}
}

/* Calibration */

func TestCalibration(t *testing.T) {
	s := goldenStore(t)
	db := readOnly(t, goldenPath(t))
	for _, h := range store.HorizonHours {
		for kind, code := range map[string][2]int{"other": {0, 0}, "updown": {1, 1}, "unknown": {2, 2}, "all": {0, 2}} {
			got, err := s.Calibration(t.Context(), h, kind)
			if err != nil {
				t.Fatalf("%dh %s: %v", h, kind, err)
			}
			if got.HorizonHours != h || got.Kind != kind || len(got.Buckets) != 20 {
				t.Fatalf("%dh %s answered %dh %q with %d buckets", h, kind, got.HorizonHours, got.Kind, len(got.Buckets))
			}
			var markets int64
			for _, b := range got.Buckets {
				markets += b.Markets
			}
			col := "p0_" + strconv.Itoa(h) + "h"
			wantMarkets := one[int64](t, db, "SELECT CAST(count(*) AS BIGINT) FROM market WHERE resolved AND "+col+" IS NOT NULL AND kind BETWEEN ? AND ?", code[0], code[1])
			wantNoPrice := one[int64](t, db, "SELECT CAST(count(*) AS BIGINT) FROM market WHERE resolved AND "+col+" IS NULL AND kind BETWEEN ? AND ?", code[0], code[1])
			if markets != wantMarkets || got.Markets != wantMarkets || got.NoPrice != wantNoPrice {
				t.Errorf("%dh %s has %d (%d) markets and %d without a price, want %d and %d", h, kind, markets, got.Markets, got.NoPrice, wantMarkets, wantNoPrice)
			}
			if wantMarkets > 0 {
				sq := one[float64](t, db, "SELECT sum(sum_sq) FROM calibration WHERE horizon_h = ? AND kind BETWEEN ? AND ?", h, code[0], code[1])
				if math.Abs(got.SumSq/float64(got.Markets)-sq/float64(wantMarkets)) > 1e-12 {
					t.Errorf("%dh %s brier numerator %v over %d markets, want %v", h, kind, got.SumSq, got.Markets, sq)
				}
			}
		}
	}
}

func TestCalibrationUnknownHorizon(t *testing.T) {
	s := goldenStore(t)
	for _, tc := range []struct {
		h    int
		kind string
	}{{2, "other"}, {12, "x"}} {
		if _, err := s.Calibration(t.Context(), tc.h, tc.kind); err == nil || errors.Is(err, store.ErrNotBuilt) {
			t.Errorf("Calibration(%d, %q) returned %v, want an error other than ErrNotBuilt", tc.h, tc.kind, err)
		}
	}
}

func TestStoreSettings(t *testing.T) {
	temp := t.TempDir()
	s := store.New(goldenPath(t), store.Options{MemoryLimit: "160MB", Threads: 2, TempDir: temp, Every: time.Hour})
	t.Cleanup(func() { _ = s.Close() })
	for query, want := range map[string]string{
		"SELECT current_setting('threads')":        "2",
		"SELECT current_setting('temp_directory')": temp,
		"SELECT current_setting('memory_limit')":   "152.5 MiB",
	} {
		got, err := s.Query(t.Context(), query)
		if err != nil || got != want {
			t.Errorf("%s is %q (%v), want %q", query, got, err, want)
		}
	}
}

func TestStoreIsReadOnly(t *testing.T) {
	s := goldenStore(t)
	for _, stmt := range []string{"INSERT INTO meta VALUES ('x', 'y')", "DELETE FROM wallet", "CREATE TABLE extra (a INTEGER)"} {
		if _, err := s.Query(t.Context(), stmt); err == nil || !strings.Contains(err.Error(), "read-only") {
			t.Errorf("%s returned %v, want a read-only error", stmt, err)
		}
	}
}
