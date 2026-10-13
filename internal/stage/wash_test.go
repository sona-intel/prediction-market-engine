package stage_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"pm/internal/dataset"
	"pm/internal/golden"
	"pm/internal/refs"
	"pm/internal/stage"
	"pm/internal/wash"
)

const (
	million  = 1_000_000
	maxPairs = 350_000_000
)

var washTables = []string{"wash_score", "wash_wallet", "wash_market", "wash_cp", "wash_trade", "wash_alg1"}

func solveStep(ctx context.Context, o stage.Options) error { return stage.Solve(ctx, o, maxPairs) }

func goldenWash(t *testing.T) *env {
	t.Helper()
	return goldenWashed.get(t, stage.Pass1, solveStep, stage.Pass2)
}

func buyFrom(id string, ts int, maker, taker, tok string, shares int) []any {
	return fill(id, strconv.Itoa(ts), maker, taker, "0", tok, strconv.Itoa(shares*million/2), strconv.Itoa(shares*million))
}

func sellTo(id string, ts int, maker, taker, tok string, shares int) []any {
	return fill(id, strconv.Itoa(ts), maker, taker, tok, "0", strconv.Itoa(shares*million), strconv.Itoa(shares*million/2))
}

func (e *env) solve() {
	e.t.Helper()
	if err := stage.Solve(e.t.Context(), e.o, maxPairs); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) pass2() {
	e.t.Helper()
	if err := stage.Pass2(e.t.Context(), e.o); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) through() {
	e.t.Helper()
	e.run()
	e.pass1()
	e.solve()
	e.pass2()
}

func (e *env) auditValue(key string) float64 {
	e.t.Helper()
	var v float64
	if err := e.o.Work.QueryRowContext(e.t.Context(), "SELECT value FROM audit WHERE key = ?", key).Scan(&v); err != nil {
		e.t.Fatalf("audit %s: %v", key, err)
	}
	return v
}

func (e *env) score(addr string) (x, x0 float64) {
	e.t.Helper()
	err := e.o.Work.QueryRowContext(e.t.Context(), "SELECT x, x0 FROM wash_score WHERE wallet_id = ?", e.walletID(addr)).Scan(&x, &x0)
	if err != nil {
		e.t.Fatalf("wash_score of %s: %v", addr, err)
	}
	return x, x0
}

type marketWash struct {
	theta           sql.Null[uint32]
	paired, flagged uint64
}

func (e *env) washMarket(condition string) marketWash {
	e.t.Helper()
	var m marketWash
	err := e.o.Work.QueryRowContext(e.t.Context(), "SELECT theta, paired_shares, wash_shares FROM wash_market WHERE market_id = ?",
		e.market(condition)).Scan(&m.theta, &m.paired, &m.flagged)
	if err != nil {
		e.t.Fatalf("wash_market of %s: %v", condition, err)
	}
	return m
}

func (e *env) washShares(addr string) uint64 {
	e.t.Helper()
	var n uint64
	err := e.o.Work.QueryRowContext(e.t.Context(), "SELECT CAST(coalesce(sum(wash_shares), 0) AS UBIGINT) FROM wash_wallet WHERE wallet_id = ?",
		e.walletID(addr)).Scan(&n)
	if err != nil {
		e.t.Fatal(err)
	}
	return n
}

type evidence struct {
	cp, block, log, market uint32
	buy                    bool
	outcome                uint8
	shares, usdc           uint64
	ts                     uint32
}

func (e *env) evidence(addr string) []evidence {
	e.t.Helper()
	rows, err := e.o.Work.QueryContext(e.t.Context(), `SELECT cp_id, block, log_index, market_id, is_buy, outcome, shares, usdc, ts
		FROM wash_trade WHERE wallet_id = ? ORDER BY shares DESC, block, log_index`, e.walletID(addr))
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var got []evidence
	for rows.Next() {
		var v evidence
		if err := rows.Scan(&v.cp, &v.block, &v.log, &v.market, &v.buy, &v.outcome, &v.shares, &v.usdc, &v.ts); err != nil {
			e.t.Fatal(err)
		}
		got = append(got, v)
	}
	if err := rows.Err(); err != nil {
		e.t.Fatal(err)
	}
	return got
}

func (e *env) stagedFiles() []string {
	e.t.Helper()
	files, err := filepath.Glob(filepath.Join(e.o.StageDir, "b=*", "*.parquet"))
	if err != nil {
		e.t.Fatal(err)
	}
	return files
}

func isolatedPair(t *testing.T) (*world, *env) {
	t.Helper()
	w := newWorld(t)
	tok := w.toks[condA][0]
	w.write("order_filled", "01",
		buyFrom("137_100_1", 1700000100, alice, bob, tok, 100),
		sellTo("137_100_2", 1700000200, alice, bob, tok, 100),
	)
	return w, prepare(t, w.set.Root)
}

func TestSolveGuard(t *testing.T) {
	e := goldenPass1(t)
	err := stage.Solve(t.Context(), e.o, 1)
	if !errors.Is(err, stage.ErrGraphTooLarge) {
		t.Fatalf("Solve with a limit of 1 pair returned %v, want ErrGraphTooLarge", err)
	}
}

func TestSolveNeedsPass1(t *testing.T) {
	w := newWorld(t)
	w.write("order_filled", "01", buyFrom("137_100_1", 1700000100, alice, bob, w.toks[condA][0], 100))
	e := prepare(t, w.set.Root)
	e.run()
	if err := stage.Solve(t.Context(), e.o, maxPairs); !errors.Is(err, stage.ErrIncomplete) {
		t.Fatalf("Solve before Pass1 returned %v, want ErrIncomplete", err)
	}
}

func TestSolveNeedsPass1Finish(t *testing.T) {
	e := goldenPass1(t)
	if _, err := e.o.Work.ExecContext(t.Context(), "DELETE FROM build_state WHERE stage = 'pass1' AND item = 'finish'"); err != nil {
		t.Fatal(err)
	}
	if err := stage.Solve(t.Context(), e.o, maxPairs); !errors.Is(err, stage.ErrIncomplete) {
		t.Fatalf("Solve without the pass1 finish returned %v, want ErrIncomplete", err)
	}
}

func TestPass2NeedsSolve(t *testing.T) {
	w := newWorld(t)
	w.write("order_filled", "01", buyFrom("137_100_1", 1700000100, alice, bob, w.toks[condA][0], 100))
	e := prepare(t, w.set.Root)
	e.run()
	e.pass1()
	if err := stage.Pass2(t.Context(), e.o); !errors.Is(err, stage.ErrIncomplete) {
		t.Fatalf("Pass2 before Solve returned %v, want ErrIncomplete", err)
	}
	if len(e.stagedFiles()) == 0 {
		t.Fatal("a refused Pass2 removed the staged rows")
	}
}

func threeWallets(t *testing.T) (*world, *env) {
	t.Helper()
	w := newWorld(t)
	w.write("order_filled", "01",
		buyFrom("137_100_1", 1700000100, alice, bob, w.toks[condA][0], 1000),
		sellTo("137_100_2", 1700000200, alice, bob, w.toks[condA][0], 1000),
		buyFrom("137_100_3", 1700000300, alice, carol, w.toks[condB][0], 400),
	)
	return w, prepare(t, w.set.Root)
}

func TestSolveScores(t *testing.T) {
	_, e := threeWallets(t)
	e.run()
	e.pass1()
	e.solve()
	// x_alice = (5/6 + 5/6 x_bob + 1/6 x_carol) / 2, x_bob = (1 + x_alice) / 2, x_carol = x_alice / 2
	for _, tc := range []struct {
		name, addr string
		x, x0      float64
	}{
		{"alice", alice, 5.0 / 6, 5.0 / 6},
		{"bob", bob, 11.0 / 12, 1},
		{"carol", carol, 5.0 / 12, 0},
	} {
		x, x0 := e.score(tc.addr)
		if math.Abs(x-tc.x) > 1e-3 || math.Abs(x0-tc.x0) > 1e-9 {
			t.Errorf("%s has x %.5f and x0 %.5f, want %.5f and %.5f", tc.name, x, x0, tc.x, tc.x0)
		}
	}
	if rounds := e.auditValue("wash_rounds"); rounds < 2 || rounds > 200 {
		t.Errorf("wash_rounds %v, want a count between 2 and 200", rounds)
	}
}

func (e *env) referenceScores() (x []float64, rounds int) {
	e.t.Helper()
	ctx := e.t.Context()
	var n int
	if err := e.o.Work.QueryRowContext(ctx, "SELECT CAST(count(*) AS BIGINT) FROM wallet_dict").Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	x0 := make([]float64, n)
	rows, err := e.o.Work.QueryContext(ctx, "SELECT wallet_id, x0_num, paired_shares FROM wallet_acc WHERE paired_shares > 0")
	if err != nil {
		e.t.Fatal(err)
	}
	for rows.Next() {
		var id uint32
		var closed, paired uint64
		if err := rows.Scan(&id, &closed, &paired); err != nil {
			e.t.Fatal(err)
		}
		x0[id] = float64(closed) / float64(paired)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		e.t.Fatal(err)
	}
	pairs, err := e.o.Work.QueryContext(ctx, `SELECT a, b, CAST(sum(shares) AS UBIGINT) FROM (
			SELECT CASE d.dir WHEN 0 THEN wm.wallet_id ELSE wt.wallet_id END AS a, CASE d.dir WHEN 0 THEN wt.wallet_id ELSE wm.wallet_id END AS b, f.shares
			FROM `+e.staged("fill")+` f
			JOIN wallet_dict wm ON wm.code = f.maker
			JOIN wallet_dict wt ON wt.code = f.taker
			CROSS JOIN (VALUES (0), (1)) d(dir)
			WHERE f.shares > 0 AND f.maker <> f.taker)
		GROUP BY a, b ORDER BY a, b`)
	if err != nil {
		e.t.Fatal(err)
	}
	g := wash.NewGraph(n)
	var cur uint32
	var cols []uint32
	var shares []uint64
	flush := func() {
		if len(cols) > 0 {
			g.AddRow(cur, cols, shares)
			cols, shares = cols[:0], shares[:0]
		}
	}
	for pairs.Next() {
		var a, b uint32
		var v uint64
		if err := pairs.Scan(&a, &b, &v); err != nil {
			e.t.Fatal(err)
		}
		if a != cur {
			flush()
			cur = a
		}
		cols, shares = append(cols, b), append(shares, v)
	}
	if err := errors.Join(pairs.Err(), pairs.Close()); err != nil {
		e.t.Fatal(err)
	}
	flush()
	return wash.Solve(g, x0)
}

func TestSolveMatchesGraphSolver(t *testing.T) {
	e := prepare(t, golden.Path("dataset"))
	e.run()
	e.pass1()
	e.solve()
	want, rounds := e.referenceScores()
	if got := e.auditValue("wash_rounds"); math.Abs(got-float64(rounds)) > 1 {
		t.Errorf("wash_rounds %v, the in-memory solver took %d", got, rounds)
	}
	rows, err := e.o.Work.QueryContext(t.Context(), "SELECT wallet_id, x FROM wash_score")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var n int
	for rows.Next() {
		var id uint32
		var x float64
		if err := rows.Scan(&id, &x); err != nil {
			t.Fatal(err)
		}
		if n++; math.Abs(x-want[id]) > 1e-4 {
			t.Fatalf("wallet %d has x %.7f, the in-memory solver gives %.7f", id, x, want[id])
		}
	}
	if err := rows.Err(); err != nil || n == 0 {
		t.Fatalf("%d scores (%v)", n, err)
	}
}

func TestPass2IsolatedPairHasNoCandidateAndNoFlags(t *testing.T) {
	w, e := isolatedPair(t)
	e.through()
	for _, addr := range []string{alice, bob} {
		x, x0 := e.score(addr)
		if x != 1 || x0 != 1 {
			t.Errorf("%s has x %v and x0 %v, want 1 and 1", addr, x, x0)
		}
		if got, want := e.washShares(addr), e.acc(addr).paired; got != 0 || want != 200*million {
			t.Errorf("%s has wash_shares %d against paired_shares %d, want 0 and 200000000", addr, got, want)
		}
	}
	m := e.washMarket(w.ids[condA])
	if m.theta.Valid || m.paired != 200*million || m.flagged != 0 {
		t.Errorf("wash_market %+v, want NULL theta, 200000000 paired and none flagged", m)
	}
	if got := e.auditValue("alg2_share"); got != 0 {
		t.Errorf("alg2_share %v, want 0", got)
	}
	if got := e.auditValue("alg1_99"); got != 1 {
		t.Errorf("alg1_99 %v, want 1", got)
	}
	if got := len(e.evidence(alice)); got != 0 {
		t.Errorf("alice has %d evidence trades, want 0", got)
	}
}

func TestPass2OpenPositionNotFlagged(t *testing.T) {
	w := newWorld(t)
	w.write("order_filled", "01", buyFrom("137_100_1", 1700000100, alice, bob, w.toks[condA][0], 100))
	e := prepare(t, w.set.Root)
	e.through()
	for _, addr := range []string{alice, bob} {
		if x, x0 := e.score(addr); x != 0 || x0 != 0 {
			t.Errorf("%s has x %v and x0 %v, want 0 and 0", addr, x, x0)
		}
	}
	if m := e.washMarket(w.ids[condA]); m.theta.Valid || m.paired != 100*million || m.flagged != 0 {
		t.Errorf("wash_market %+v, want NULL theta, 100000000 paired and none flagged", m)
	}
	for _, table := range []string{"wash_trade", "wash_cp", "wash_wallet"} {
		if n := e.count("SELECT CAST(count(*) AS BIGINT) FROM " + table); n != 0 {
			t.Errorf("%s has %d rows, want 0", table, n)
		}
	}
	if got := e.auditValue("alg2_share"); got != 0 {
		t.Errorf("alg2_share %v, want 0", got)
	}
}

func TestPass2FlagsBelowOne(t *testing.T) {
	w, e := threeWallets(t)
	e.through()
	first, second := e.washMarket(w.ids[condA]), e.washMarket(w.ids[condB])
	// the closed pair is the only candidate and its theta is the lower of the two scores, 5/6
	if !first.theta.Valid || math.Abs(float64(first.theta.V)-5.0/6*million) > 1000 || first.flagged != 2000*million || first.paired != 2000*million {
		t.Errorf("first market %+v, want theta near 833333 with 2000000000 flagged of 2000000000", first)
	}
	if second.theta.Valid || second.flagged != 0 || second.paired != 400*million {
		t.Errorf("second market %+v, want NULL theta, none flagged of 400000000", second)
	}
	for addr, want := range map[string]uint64{alice: 2000 * million, bob: 2000 * million, carol: 0} {
		if got := e.washShares(addr); got != want {
			t.Errorf("wash_shares of %s %d, want %d", addr, got, want)
		}
	}
	if got := len(e.evidence(carol)); got != 0 {
		t.Errorf("carol has %d evidence trades, want 0", got)
	}
	for _, tc := range []struct {
		key  string
		want float64
	}{{"alg1_80", 2000.0 / 2400}, {"alg1_90", 0}, {"alg1_99", 0}, {"alg2_share", 2000.0 / 2400}} {
		if got := e.auditValue(tc.key); math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("%s %v, want %v", tc.key, got, tc.want)
		}
	}
}

func TestPass2EvidenceTop20(t *testing.T) {
	w := newWorld(t)
	tok := w.toks[condA][0]
	var rows [][]any
	var total int
	blockOf := map[int]int{}
	var fifties []int
	distinct := 100
	for i := range 25 {
		size, block := distinct, 100+i
		if i%2 == 1 && len(fifties) < 11 {
			size = 50
			fifties = append(fifties, block)
		} else {
			blockOf[size] = block
			distinct++
		}
		total += size
		rows = append(rows, buyFrom(fmt.Sprintf("137_%d_1", block), 1700000000+10*i, alice, bob, tok, size))
	}
	rows = append(rows, sellTo("137_300_1", 1700000500, alice, bob, tok, total))
	rows = append(rows, buyFrom("137_400_1", 1700000600, alice, carol, w.toks[condB][0], 1000))
	w.write("order_filled", "01", rows...)
	e := prepare(t, w.set.Root)
	e.through()

	want := []int{300}
	for size := 113; size >= 100; size-- {
		want = append(want, blockOf[size])
	}
	want = append(want, fifties[:5]...)
	a, b := e.evidence(alice), e.evidence(bob)
	if len(a) != 20 || len(b) != 20 {
		t.Fatalf("alice has %d evidence trades and bob %d, want 20 each", len(a), len(b))
	}
	for i, v := range a {
		if int(v.block) != want[i] {
			t.Errorf("alice evidence %d is block %d, want %d", i, v.block, want[i])
		}
		if v.buy == (v.block == 300) {
			t.Errorf("alice evidence at block %d has is_buy %v", v.block, v.buy)
		}
		if p := b[i]; p.block != v.block || p.log != v.log || p.buy == v.buy || p.shares != v.shares || p.usdc != v.usdc || p.ts != v.ts {
			t.Errorf("bob evidence %d is %+v against alice's %+v, want the same trade on the other side", i, p, v)
		}
	}
	if a[0].cp != e.walletID(bob) || b[0].cp != e.walletID(alice) {
		t.Errorf("counterparties are %d and %d, want bob's and alice's ids", a[0].cp, b[0].cp)
	}
	if a[0].shares != uint64(total*million) || a[0].market != e.market(w.ids[condA]) || a[0].outcome != 0 {
		t.Errorf("first evidence trade %+v, want %d shares in the first market on outcome 0", a[0], total*million)
	}
	type counterparty struct {
		cp, markets, trades uint32
		shares              uint64
	}
	var got []counterparty
	rowsCP, err := e.o.Work.QueryContext(t.Context(), "SELECT cp_id, markets, trades, shares FROM wash_cp WHERE wallet_id = ?", e.walletID(alice))
	if err != nil {
		t.Fatal(err)
	}
	defer rowsCP.Close()
	for rowsCP.Next() {
		var c counterparty
		if err := rowsCP.Scan(&c.cp, &c.markets, &c.trades, &c.shares); err != nil {
			t.Fatal(err)
		}
		got = append(got, c)
	}
	if want := (counterparty{e.walletID(bob), 1, 26, uint64(2 * total * million)}); len(got) != 1 || got[0] != want {
		t.Errorf("wash_cp of alice is %+v, want the one row %+v", got, want)
	}
}

func TestPass2DeletesStagedBucket(t *testing.T) {
	_, e := isolatedPair(t)
	e.run()
	e.pass1()
	e.solve()
	if len(e.stagedFiles()) == 0 {
		t.Fatal("nothing was staged")
	}
	e.pass2()
	if files := e.stagedFiles(); len(files) != 0 {
		t.Fatalf("staged files left after Pass2: %v", files)
	}
	if dirs := e.folders(); len(dirs) != 0 {
		t.Fatalf("bucket folders left after Pass2: %v", dirs)
	}
	if got := e.count("SELECT CAST(count(*) AS BIGINT) FROM build_state WHERE stage = 'pass2' AND status = 'done'"); got != refs.Buckets {
		t.Fatalf("%d done buckets, want %d", got, refs.Buckets)
	}
}

func TestPass2Invariants(t *testing.T) {
	e := goldenWash(t)
	var marketPaired, marketFlagged, walletPaired, walletFlagged uint64
	err := e.o.Work.QueryRowContext(t.Context(), `SELECT CAST(coalesce(sum(paired_shares), 0) AS UBIGINT), CAST(coalesce(sum(wash_shares), 0) AS UBIGINT) FROM wash_market`).
		Scan(&marketPaired, &marketFlagged)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.o.Work.QueryRowContext(t.Context(), "SELECT CAST(coalesce(sum(paired_shares), 0) AS UBIGINT) FROM wallet_acc").Scan(&walletPaired); err != nil {
		t.Fatal(err)
	}
	if err := e.o.Work.QueryRowContext(t.Context(), "SELECT CAST(coalesce(sum(wash_shares), 0) AS UBIGINT) FROM wash_wallet").Scan(&walletFlagged); err != nil {
		t.Fatal(err)
	}
	t.Logf("golden set pairs %d paired shares of which %d flagged, in %d wallets", marketPaired, marketFlagged, e.count("SELECT CAST(count(*) AS BIGINT) FROM wash_wallet"))
	if walletPaired != 2*marketPaired || walletFlagged != 2*marketFlagged {
		t.Errorf("wallets hold %d paired and %d flagged shares, markets %d and %d, want twice the market figures", walletPaired, walletFlagged, marketPaired, marketFlagged)
	}
	for name, query := range map[string]string{
		"a wallet flagged beyond its paired shares": "SELECT CAST(count(*) AS BIGINT) FROM wash_wallet w JOIN wallet_acc a USING (wallet_id) WHERE w.wash_shares > a.paired_shares",
		"a wallet with two wash_wallet rows":        "SELECT CAST(count(*) - count(DISTINCT wallet_id) AS BIGINT) FROM wash_wallet",
		"a theta that disagrees with the flags":     "SELECT CAST(count(*) AS BIGINT) FROM wash_market WHERE (theta IS NULL) <> (wash_shares = 0)",
		"a score for a wallet with no paired rows":  "SELECT CAST(count(*) AS BIGINT) FROM wash_score s LEFT JOIN wallet_acc a USING (wallet_id) WHERE coalesce(a.paired_shares, 0) = 0",
		"a paired wallet with no score":             "SELECT CAST(count(*) AS BIGINT) FROM wallet_acc a LEFT JOIN wash_score s USING (wallet_id) WHERE a.paired_shares > 0 AND s.wallet_id IS NULL",
		"a wallet whose counterparties disagree with its flagged shares": `SELECT CAST(count(*) AS BIGINT) FROM wash_wallet w
			LEFT JOIN (SELECT wallet_id, sum(shares) AS s FROM wash_cp GROUP BY wallet_id) c USING (wallet_id) WHERE c.s IS DISTINCT FROM w.wash_shares`,
	} {
		if n := e.count(query); n != 0 {
			t.Errorf("%d rows with %s", n, name)
		}
	}
}

func TestPass2PagedMarketsGiveTheSameResult(t *testing.T) {
	want := goldenWash(t).fingerprint(washTables...)
	t.Cleanup(stage.PageRows(50))
	e := prepare(t, golden.Path("dataset"))
	e.through()
	if got := e.fingerprint(washTables...); got != want {
		t.Fatalf("a build in pages of 50 rows differs from one in whole buckets\n%s\n%s", got, want)
	}
}

func TestPass2RerunChangesNothing(t *testing.T) {
	e := goldenWash(t)
	before := e.fingerprint(washTables...)
	rounds := e.auditValue("wash_rounds")
	e.solve()
	e.pass2()
	if after := e.fingerprint(washTables...); after != before {
		t.Fatalf("a second run changed the tables\n%s\n%s", before, after)
	}
	if got := e.auditValue("wash_rounds"); got != rounds {
		t.Fatalf("wash_rounds changed from %v to %v", rounds, got)
	}
}

func TestPass2ResumeAfterFailure(t *testing.T) {
	reference := goldenWash(t)
	want := reference.fingerprint(washTables...)
	e := prepare(t, golden.Path("dataset"))
	e.run()
	e.pass1()
	e.solve()
	files, err := filepath.Glob(filepath.Join(e.o.StageDir, "b=*", "fill-*.parquet"))
	if err != nil || len(files) < 2 {
		t.Fatalf("staged fill files %v: %v", files, err)
	}
	last := slices.Max(files)
	original, err := os.ReadFile(last)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(last, []byte("not parquet"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := stage.Pass2(t.Context(), e.o); err == nil {
		t.Fatal("Pass2 accepted a broken staged file")
	}
	done := e.count("SELECT CAST(count(*) AS BIGINT) FROM build_state WHERE stage = 'pass2' AND status = 'done'")
	if done == 0 || done >= refs.Buckets {
		t.Fatalf("%d buckets done after the failure, want some but not all", done)
	}
	if err := os.WriteFile(last, original, 0o600); err != nil {
		t.Fatal(err)
	}
	e.pass2()
	if got := e.fingerprint(washTables...); got != want {
		t.Fatalf("resumed Pass2 differs from an uninterrupted one\n%s\n%s", got, want)
	}
	for _, key := range []string{"alg1_80", "alg1_90", "alg1_99", "alg2_share"} {
		if got, ref := e.auditValue(key), reference.auditValue(key); got != ref {
			t.Errorf("%s is %v after the resume and %v uninterrupted", key, got, ref)
		}
	}
}

/* March 2025 */

func marchEnv(t *testing.T, path string) *env {
	t.Helper()
	ctx := t.Context()
	db, err := dataset.Open(ctx, dataset.Config{Path: filepath.Join(t.TempDir(), "work.duckdb"), ExtDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := stage.Setup(ctx, db); err != nil {
		t.Fatal(err)
	}
	src := "read_parquet('" + path + "')"
	tmp := filepath.Join(t.TempDir(), "partitioned")
	stageDir := filepath.Join(t.TempDir(), "stage")
	for _, stmt := range []string{
		`INSERT INTO wallet_dict SELECT address, n, n FROM (
			SELECT address, CAST(row_number() OVER (ORDER BY address) - 1 AS UINTEGER) AS n FROM (
				SELECT DISTINCT unhex(substr(a, 3)) AS address FROM (SELECT maker AS a FROM ` + src + ` UNION ALL SELECT taker FROM ` + src + `)))`,
		`CREATE TABLE market AS SELECT id AS market_id, unhex(substr(cond, 3)) AS condition, CAST(NULL AS VARCHAR) AS question,
				CAST(NULL AS VARCHAR) AS slug, CAST(NULL AS DATE) AS end_date, CAST(0 AS UTINYINT) AS kind, false AS neg_risk, id AS ek,
				CAST(id % 64 AS UTINYINT) AS bucket, false AS resolved, CAST(NULL AS UBIGINT) AS payout0, CAST(NULL AS UBIGINT) AS payout1,
				CAST(NULL AS UBIGINT) AS payout_den
			FROM (SELECT cond, CAST(row_number() OVER (ORDER BY cond) - 1 AS UINTEGER) AS id FROM (SELECT DISTINCT cond FROM ` + src + `))`,
		"CREATE TABLE ek AS SELECT market_id AS code, condition AS key, bucket, CAST(1 AS UINTEGER) AS question_count FROM market",
		"CREATE TABLE nr_question (ek UINTEGER, q UTINYINT, market_id UINTEGER)",
		`CREATE TABLE tok AS SELECT CAST(2 * market_id + o AS UINTEGER) AS code, hex(condition) || '_' || o AS token, market_id,
				CAST(o AS UTINYINT) AS outcome
			FROM market, (VALUES (0), (1)) t(o)`,
		"CREATE TABLE first_redeem (market_id UINTEGER, ts UINTEGER)",
		`COPY (SELECT m.bucket AS bucket, CAST(s.b AS UINTEGER) AS block, CAST(s.l AS UINTEGER) AS log_index, CAST(s.ts AS UINTEGER) AS ts,
				wm.code AS maker, wt.code AS taker, k.code AS tok, s.is_buy AS buy, CAST(s.shares AS UBIGINT) AS shares, CAST(s.usdc AS UBIGINT) AS usdc
			FROM ` + src + ` s
			JOIN market m ON m.condition = unhex(substr(s.cond, 3))
			JOIN tok k ON k.market_id = m.market_id AND k.outcome = s.oidx
			JOIN wallet_dict wm ON wm.address = unhex(substr(s.maker, 3))
			JOIN wallet_dict wt ON wt.address = unhex(substr(s.taker, 3)))
			TO '` + tmp + `' (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (bucket), FILENAME_PATTERN 'fill-march_{i}')`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt[:min(len(stmt), 60)], err)
		}
	}
	dirs, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(stageDir, 0o750); err != nil {
		t.Fatal(err)
	}
	for _, d := range dirs {
		n, err := strconv.Atoi(strings.TrimPrefix(d.Name(), "bucket="))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(filepath.Join(tmp, d.Name()), filepath.Join(stageDir, fmt.Sprintf("b=%02d", n))); err != nil {
			t.Fatal(err)
		}
	}
	return &env{t: t, o: stage.Options{Work: db, StageDir: stageDir}}
}

func TestWashMarch2025(t *testing.T) {
	path := golden.Path("..", "large", "p19_2025_03.parquet")
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	e := marchEnv(t, path)
	e.pass1()
	e.solve()
	e.pass2()
	rounds := e.auditValue("wash_rounds")
	t.Logf("rounds %v, algorithm 2 share %.4f", rounds, e.auditValue("alg2_share"))
	if rounds < 10 || rounds > 12 {
		t.Errorf("solver took %v rounds, want 10 to 12", rounds)
	}
	for _, tc := range []struct {
		key  string
		want float64
	}{{"alg1_80", 0.379}, {"alg1_90", 0.326}, {"alg1_99", 0.243}} {
		got := e.auditValue(tc.key)
		t.Logf("%s %.4f", tc.key, got)
		if math.Abs(got-tc.want) > 0.001 {
			t.Errorf("%s is %.4f, want %.3f within 0.001", tc.key, got, tc.want)
		}
	}
}

func TestCheckIdentityRefusesAnotherBuild(t *testing.T) {
	e := goldenPass1(t)
	same := map[string]string{"root": "a", "revision": "r1", "rules_version": "v1"}
	if err := stage.CheckIdentity(t.Context(), e.o.Work, same); err != nil {
		t.Fatal(err)
	}
	if err := stage.CheckIdentity(t.Context(), e.o.Work, same); err != nil {
		t.Fatalf("a resume with the same identity returned %v", err)
	}
	other := map[string]string{"root": "a", "revision": "r2", "rules_version": "v2"}
	err := stage.CheckIdentity(t.Context(), e.o.Work, other)
	if !errors.Is(err, stage.ErrWorkMismatch) || !strings.Contains(err.Error(), "revision") || !strings.Contains(err.Error(), "rules_version") {
		t.Fatalf("a resume with another revision and rules returned %v, want ErrWorkMismatch naming both", err)
	}
}
