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
	"sync"
	"testing"

	"pm/internal/dataset"
	"pm/internal/dstest"
	"pm/internal/golden"
	"pm/internal/pnl"
	"pm/internal/refs"
	"pm/internal/skill"
	"pm/internal/stage"
)

type sharedBuild struct {
	once sync.Once
	dir  string
	db   *sql.DB
	env  *env
	err  error
}

var goldenBuild, goldenWashed sharedBuild

func TestMain(m *testing.M) {
	code := m.Run()
	goldenBuild.close()
	goldenWashed.close()
	os.Exit(code)
}

func (s *sharedBuild) close() {
	if s.db != nil {
		_ = s.db.Close()
	}
	if s.dir != "" {
		_ = os.RemoveAll(s.dir)
	}
}

func (s *sharedBuild) get(t *testing.T, steps ...func(context.Context, stage.Options) error) *env {
	t.Helper()
	s.once.Do(func() { s.env, s.err = s.build(steps) })
	if s.err != nil {
		t.Fatal(s.err)
	}
	e := *s.env
	e.t = t
	return &e
}

func (s *sharedBuild) build(steps []func(context.Context, stage.Options) error) (*env, error) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "pm-golden-")
	if err != nil {
		return nil, err
	}
	s.dir = dir
	db, err := dataset.Open(ctx, dataset.Config{Path: filepath.Join(dir, "work.duckdb"), ExtDir: filepath.Join(dir, "ext")})
	if err != nil {
		return nil, err
	}
	s.db = db
	src, err := dataset.NewSource(ctx, db, golden.Path("dataset"))
	if err != nil {
		return nil, err
	}
	o := stage.Options{Work: db, Src: src, StageDir: filepath.Join(dir, "stage")}
	if err := refs.Build(ctx, src, db); err != nil {
		return nil, err
	}
	if err := stage.Run(ctx, o); err != nil {
		return nil, err
	}
	for _, step := range steps {
		if err := step(ctx, o); err != nil {
			return nil, err
		}
	}
	return &env{o: o}, nil
}

func goldenPass1(t *testing.T) *env {
	t.Helper()
	return goldenBuild.get(t, stage.Pass1)
}

func (e *env) pass1() {
	e.t.Helper()
	if err := stage.Pass1(e.t.Context(), e.o); err != nil {
		e.t.Fatal(err)
	}
}

type walletAcc struct {
	fills, takerRows, firstTS, lastTS, markets, resolved, won, open, rated, ratedEvents uint32
	volume, buyVolume, takerVolume, fees, sumT2, x0Num, paired                          uint64
	gross, bestNet, sumT                                                                int64
}

func (a walletAcc) net() int64 { return a.gross - int64(a.fees) }

func (e *env) walletID(addr string) uint32 {
	e.t.Helper()
	var id uint32
	err := e.o.Work.QueryRowContext(e.t.Context(), "SELECT wallet_id FROM wallet_dict WHERE address = unhex(substr(lower(?), 3))", addr).Scan(&id)
	if err != nil {
		e.t.Fatalf("wallet_id of %s: %v", addr, err)
	}
	return id
}

func (e *env) acc(addr string) walletAcc {
	e.t.Helper()
	var a walletAcc
	err := e.o.Work.QueryRowContext(e.t.Context(), `SELECT fills, volume_usdc, buy_volume_usdc, taker_rows, taker_volume_usdc, first_ts, last_ts,
			markets, gross_pnl, fees, best_net, resolved_markets, won_markets, open_unresolved, rated_markets, rated_events,
			sum_t, sum_t2, x0_num, paired_shares
		FROM wallet_acc WHERE wallet_id = ?`, e.walletID(addr)).Scan(&a.fills, &a.volume, &a.buyVolume, &a.takerRows, &a.takerVolume,
		&a.firstTS, &a.lastTS, &a.markets, &a.gross, &a.fees, &a.bestNet, &a.resolved, &a.won, &a.open, &a.rated, &a.ratedEvents,
		&a.sumT, &a.sumT2, &a.x0Num, &a.paired)
	if err != nil {
		e.t.Fatalf("wallet_acc of %s: %v", addr, err)
	}
	return a
}

func goldenWallets(t *testing.T, name string) []golden.Wallet {
	t.Helper()
	wallets, err := golden.LoadWallets(name)
	if err != nil {
		t.Fatal(err)
	}
	return wallets
}

func goldenMarkets(t *testing.T, name string) []golden.Market {
	t.Helper()
	markets, err := golden.LoadMarkets(name)
	if err != nil {
		t.Fatal(err)
	}
	return markets
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

func TestPass1GoldenWallets(t *testing.T) {
	e := goldenPass1(t)
	for _, w := range goldenWallets(t, "wallets_14") {
		a := e.acc(w.Address)
		if a.fills != w.Fills || int64(a.volume) != w.Volume {
			t.Errorf("%s has %d fills and volume %d, want %d and %d", w.Address, a.fills, a.volume, w.Fills, w.Volume)
		}
		if d := abs64(a.net() - w.Realized); d > w.Tolerance {
			t.Errorf("%s net %d, want %d within %d (off by %d)", w.Address, a.net(), w.Realized, w.Tolerance, d)
		}
		if got := (pnl.Summary{BestNet: a.bestNet}).BiggestWin(); got != w.BiggestWin {
			t.Errorf("%s biggest win %d, want %d", w.Address, got, w.BiggestWin)
		}
	}
}

func TestPass1GoldenMarketsCount(t *testing.T) {
	e := goldenPass1(t)
	wallets := goldenWallets(t, "wallets_14")
	matched := 0
	for _, w := range wallets {
		if got := e.acc(w.Address).markets; got == w.Markets {
			matched++
		} else {
			t.Logf("markets differ for %s: %d here, %d at Polymarket", w.Address, got, w.Markets)
		}
	}
	if matched < 13 {
		t.Fatalf("markets match for %d of %d wallets, want at least 13", matched, len(wallets))
	}
}

func TestPass1TakerRows(t *testing.T) {
	e := goldenPass1(t)
	a := e.acc("0x0FE9463bCfA3e3dcA586Acc12acf1D74EfB97E68")
	if a.takerRows != 2 || a.takerVolume != 60760000 {
		t.Errorf("taker rows %d and volume %d, want 2 and 60760000", a.takerRows, a.takerVolume)
	}
	for _, w := range goldenWallets(t, "wallets_14") {
		if got := e.acc(w.Address).takerRows; got != w.TakerRows {
			t.Errorf("%s has %d taker rows, want %d", w.Address, got, w.TakerRows)
		}
	}
}

func TestPass1ConversionWallets(t *testing.T) {
	e := goldenPass1(t)
	wallets := goldenWallets(t, "wallets_conversion")
	if len(wallets) != 6 {
		t.Fatalf("%d conversion wallets, want 6", len(wallets))
	}
	for _, w := range wallets {
		a := e.acc(w.Address)
		if a.fills != w.Fills || int64(a.volume) != w.Volume {
			t.Errorf("%s has %d fills and volume %d, want %d and %d", w.Address, a.fills, a.volume, w.Fills, w.Volume)
		}
		if d := abs64(a.net() - w.Realized); d > w.Tolerance {
			t.Errorf("%s net %d, want %d within %d (off by %d)", w.Address, a.net(), w.Realized, w.Tolerance, d)
		}
	}
}

func TestPass1FeeEra(t *testing.T) {
	e := goldenPass1(t)
	for _, w := range goldenWallets(t, "wallets_fee_era") {
		a := e.acc(w.Address)
		limit := w.FeesPaid*3/100 + 10000
		t.Logf("%s gross %d fees %d (fees paid %d) net %d, realized %d", w.Address, a.gross, a.fees, w.FeesPaid, a.net(), w.Realized)
		if d := abs64(a.net() - w.Realized); d > limit {
			t.Errorf("%s net %d, want %d within %d (off by %d)", w.Address, a.net(), w.Realized, limit, d)
		}
	}
}

func TestPass1CutoffWallets(t *testing.T) {
	e := goldenPass1(t)
	wallets := goldenWallets(t, "wallets_cutoff")
	if len(wallets) != 2 {
		t.Fatalf("%d cutoff wallets, want 2", len(wallets))
	}
	for _, w := range wallets {
		a := e.acc(w.Address)
		if a.open != 26 {
			t.Errorf("%s has %d open unresolved, want 26", w.Address, a.open)
		}
		if d := abs64(a.net() - w.Settled); d > w.Tolerance {
			t.Errorf("%s net %d, want settled %d within %d (off by %d)", w.Address, a.net(), w.Settled, w.Tolerance, d)
		}
	}
}

type heldToken struct {
	market uint32
	out    uint8
}

type userPosition struct{ amount, avg, realized, bought int64 }

func (e *env) userPositions(addr string) map[heldToken]userPosition {
	e.t.Helper()
	rows, err := e.o.Work.QueryContext(e.t.Context(), `SELECT t.market_id, t.outcome, u.amount, u.avgPrice, u.realizedPnl, u.totalBought
		FROM read_parquet(?) u JOIN tok t ON t.token = u.tokenId WHERE lower(u."user") = lower(?)`,
		golden.Path("dataset", "user_position.parquet"), addr)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	got := map[heldToken]userPosition{}
	for rows.Next() {
		var key heldToken
		var amount, avg, realized, bought string
		if err := rows.Scan(&key.market, &key.out, &amount, &avg, &realized, &bought); err != nil {
			e.t.Fatal(err)
		}
		var p userPosition
		for dst, src := range map[*int64]string{&p.amount: amount, &p.avg: avg, &p.realized: realized, &p.bought: bought} {
			v, err := strconv.ParseInt(src, 10, 64)
			if err != nil {
				e.t.Fatal(err)
			}
			*dst = v
		}
		got[key] = p
	}
	if err := rows.Err(); err != nil {
		e.t.Fatal(err)
	}
	return got
}

func (e *env) look() func(uint32) pnl.Market {
	e.t.Helper()
	rows, err := e.o.Work.QueryContext(e.t.Context(), "SELECT market_id, payout0, payout1, payout_den FROM market WHERE resolved")
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	markets := map[uint32]pnl.Market{}
	for rows.Next() {
		var id uint32
		var p0, p1, den uint64
		if err := rows.Scan(&id, &p0, &p1, &den); err != nil {
			e.t.Fatal(err)
		}
		markets[id] = pnl.Market{Resolved: true, Payout: [2]int64{pnl.PricePPM(p0, den), pnl.PricePPM(p1, den)}}
	}
	if err := rows.Err(); err != nil {
		e.t.Fatal(err)
	}
	return func(id uint32) pnl.Market { return markets[id] }
}

func TestPass1TokenLevel(t *testing.T) {
	e := goldenPass1(t)
	look := e.look()
	for _, w := range goldenWallets(t, "tokens_user_position") {
		events, err := stage.WalletEvents(t.Context(), e.o, e.walletID(w.Address))
		if err != nil {
			t.Fatal(err)
		}
		held := func(skipConversions bool) []pnl.Position {
			l := pnl.NewLedger(look)
			for _, ev := range events {
				if !skipConversions || ev.Kind != pnl.Convert {
					l.Apply(ev)
				}
			}
			return l.Positions()
		}
		got := held(false)
		want := e.userPositions(w.Address)
		if len(got) != int(w.Tokens) || len(want) != int(w.Tokens) {
			t.Errorf("%s holds %d tokens and the fixture has %d, want %d", w.Address, len(got), len(want), w.Tokens)
		}
		var total int64
		for _, p := range got {
			total += p.Realized
			fixture, ok := want[heldToken{p.Market, p.Outcome}]
			if !ok {
				t.Errorf("%s token %d/%d is not in the fixture", w.Address, p.Market, p.Outcome)
				continue
			}
			if have := (userPosition{p.Amount, p.Avg, p.Realized, p.Bought}); have != fixture {
				t.Errorf("%s token %d/%d is %+v, fixture %+v", w.Address, p.Market, p.Outcome, have, fixture)
			}
		}
		if d := abs64(total - w.Realized); d > w.Tolerance {
			t.Errorf("%s total realized %d, want %d within %d", w.Address, total, w.Realized, w.Tolerance)
		}
		if w.Conversions > 0 {
			var without int64
			for _, p := range held(true) {
				without += p.Realized
			}
			if without != w.WithoutConversions {
				t.Errorf("%s without conversions %d, want %d", w.Address, without, w.WithoutConversions)
			}
		}
	}
}

func TestPass1MarketVolume(t *testing.T) {
	e := goldenPass1(t)
	for _, m := range goldenMarkets(t, "markets_volume_5") {
		var shares uint64
		if err := e.o.Work.QueryRowContext(t.Context(), "SELECT volume_shares FROM market_acc WHERE market_id = ?", e.market(m.Condition)).Scan(&shares); err != nil {
			t.Fatal(err)
		}
		if got := math.Round(float64(shares)/1e6*100) / 100; math.Abs(got-m.Volume) > 1e-9 {
			t.Errorf("%s volume %.2f, want %.2f", m.Condition, got, m.Volume)
		}
	}
}

func TestPass1FirstRedemptionOffsets(t *testing.T) {
	e := prepare(t, golden.Path("dataset"))
	e.run()
	var offsets []int64
	for _, m := range goldenMarkets(t, "markets_may25_29") {
		var ts uint32
		if err := e.o.Work.QueryRowContext(t.Context(), "SELECT ts FROM first_redeem WHERE market_id = ?", e.market(m.Condition)).Scan(&ts); err != nil {
			t.Fatalf("%s: %v", m.Condition, err)
		}
		offsets = append(offsets, int64(ts)-int64(m.ClosedTime))
	}
	slices.Sort(offsets)
	if len(offsets) != 29 || offsets[len(offsets)/2] != 112 || offsets[0] != 24 || offsets[len(offsets)-1] != 3782 {
		t.Fatalf("%d offsets with median %d, min %d, max %d, want 29, 112, 24, 3782", len(offsets), offsets[len(offsets)/2], offsets[0], offsets[len(offsets)-1])
	}
}

func TestPass1MarketPrices(t *testing.T) {
	e := goldenPass1(t)
	columns := map[string]string{"4h": "p0_4h", "12h": "p0_12h", "1d": "p0_24h", "1w": "p0_168h"}
	var diffs []float64
	for _, m := range goldenMarkets(t, "prices_12") {
		if got := e.count("SELECT CAST(outcome AS BIGINT) FROM tok WHERE token = ?", m.Token0); got != 0 {
			t.Fatalf("%s: token0 has outcome %d", m.Condition, got)
		}
		for key, column := range columns {
			want := m.Prices[key]
			if want == nil {
				continue
			}
			var got sql.Null[uint32]
			if err := e.o.Work.QueryRowContext(t.Context(), "SELECT "+column+" FROM market_acc WHERE market_id = ?", e.market(m.Condition)).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if !got.Valid {
				t.Errorf("%s has no %s", m.Condition, column)
				continue
			}
			diffs = append(diffs, math.Abs(float64(got.V)/1e6-*want))
		}
	}
	slices.Sort(diffs)
	if len(diffs) == 0 {
		t.Fatal("no prices compared")
	}
	median := diffs[len(diffs)/2]
	if len(diffs)%2 == 0 {
		median = (diffs[len(diffs)/2-1] + diffs[len(diffs)/2]) / 2
	}
	t.Logf("%d prices, median difference %.6f, maximum %.6f", len(diffs), median, diffs[len(diffs)-1])
	if median > 0.001 || diffs[len(diffs)-1] > 0.02 {
		t.Fatalf("median %.6f and maximum %.6f, want at most 0.001 and 0.02", median, diffs[len(diffs)-1])
	}
}

/* Rated floor world */

const (
	releaseTS = 1700200000
	earlyTS   = releaseTS - 50000
	lateTS    = releaseTS - 3600
)

type ratedWorld struct {
	t       *testing.T
	set     *dstest.Set
	conds   [][]any
	posns   [][]any
	names   [][]any
	redeems [][]any
	fills   [][]any
	events  [][]any
	convert [][]any
	next    int
}

func newRatedWorld(t *testing.T) *ratedWorld {
	t.Helper()
	return &ratedWorld{t: t, set: dstest.New(t)}
}

func wallet(n int) string { return fmt.Sprintf("0x%040x", 0xa00+n) }

func (w *ratedWorld) condition(id, name string, resolved bool) [2]string {
	w.next++
	tok := [2]string{strconv.Itoa(5000000 + 2*w.next), strconv.Itoa(5000001 + 2*w.next)}
	numerators, den := []string{}, "0"
	if resolved {
		numerators, den = []string{"1", "0"}, "1"
	}
	w.conds = append(w.conds, []any{id, []string{tok[0], tok[1]}, numerators, den})
	w.posns = append(w.posns, []any{tok[0], id, "0"}, []any{tok[1], id, "1"})
	if name != "" {
		w.names = append(w.names, []any{tok[0], "2024-09-10T00:00:00Z", id, name, "m" + strconv.Itoa(w.next)})
	}
	if resolved {
		w.redeems = append(w.redeems, []any{"137_" + strconv.Itoa(900000+w.next) + "_1", wallet(99), id, strconv.Itoa(releaseTS)})
	}
	return tok
}

func (w *ratedWorld) plain(i int, name string, resolved bool) [2]string {
	return w.condition(fmt.Sprintf("0x%064x", 0x1000+i), name, resolved)
}

func (w *ratedWorld) trade(maker int, tok string, ts int) {
	w.next++
	id := "137_" + strconv.Itoa(w.next) + "_1"
	w.fills = append(w.fills, fill(id, strconv.Itoa(ts), wallet(maker), wallet(98), "0", tok, "600000", "1000000"))
}

func (w *ratedWorld) sell(maker int, tok string, ts int) {
	w.next++
	id := "137_" + strconv.Itoa(w.next) + "_1"
	w.fills = append(w.fills, fill(id, strconv.Itoa(ts), wallet(maker), wallet(98), tok, "0", "1000000", "700000"))
}

func negKey(n int) [32]byte { return [32]byte{0xee, byte(n)} }

func (w *ratedWorld) negEvent(n, questions int) [][2]string {
	key := negKey(n)
	w.events = append(w.events, []any{hex32(key), "0", strconv.Itoa(questions)})
	var toks [][2]string
	for q := range questions {
		cond := refs.ConditionID(refs.NegRiskAdapter, refs.QuestionID(key, uint8(q)))
		toks = append(toks, w.condition(hex32(cond), fmt.Sprintf("Will option %d of event %d win?", q, n), true))
	}
	return toks
}

func (w *ratedWorld) publish() string {
	w.t.Helper()
	w.set.Write("condition.parquet", []string{"id", "positionIds", "payoutNumerators", "payoutDenominator"}, w.conds...)
	w.set.Write("position.parquet", []string{"id", "condition", "outcomeIndex"}, w.posns...)
	w.set.Write("market_data.parquet", []string{"id", "endDate", "condition", "marketName", "marketSlug"}, w.names...)
	w.set.Write("neg_risk_event.parquet", []string{"id", "feeBps", "questionCount"}, w.events...)
	w.set.Write("wallet.parquet", []string{"id", "signer", "type"}, []any{wallet(1), wallet(2), "proxy"})
	w.set.Write("fpmm_transaction/year=2022/month=06.parquet", []string{"id", "type", "user"}, []any{"137_1_1", "Buy", wallet(3)})
	for table, cols := range tableCols {
		w.set.Write(table+"/year=2024/month=01.parquet", cols)
	}
	w.set.Write("neg_risk_conversion/year=2025/month=01.parquet", convertCols, w.convert...)
	w.set.Write("redemption/year=2025/month=01.parquet", redemptionCols, w.redeems...)
	w.set.Write("order_filled/year=2025/month=01.parquet", fillCols, w.fills...)
	return w.set.Root
}

func TestPass1RatedFloor(t *testing.T) {
	w := newRatedWorld(t)
	const r1, r2, r3, r4 = 1, 2, 3, 4
	var first string
	for i := range 30 {
		tok := w.plain(i, fmt.Sprintf("Will thing %d happen?", i), true)[0]
		if i == 0 {
			first = tok
		}
		w.trade(r1, tok, earlyTS)
		if i < 29 {
			w.trade(r2, tok, earlyTS)
		}
	}
	for n, toks := range [][][2]string{w.negEvent(1, 15), w.negEvent(2, 15)} {
		for _, tok := range toks {
			w.trade(r3, tok[0], earlyTS+n)
		}
	}
	updown := w.plain(100, "Bitcoin Up or Down - 5pm", true)[0]
	unknown := w.plain(101, "", true)[0]
	open := w.plain(102, "Will it open?", false)[0]
	late := w.plain(103, "Will it be late?", true)[0]
	for _, tok := range []string{updown, unknown, open} {
		w.trade(r4, tok, earlyTS)
	}
	w.trade(r4, late, lateTS)
	e := prepare(t, w.publish())
	e.run()
	e.pass1()
	for _, tc := range []struct {
		name          string
		id            int
		markets, evts uint32
		floor         bool
	}{{"R1", r1, 30, 30, true}, {"R2", r2, 29, 29, false}, {"R3", r3, 30, 2, false}, {"R4", r4, 0, 0, false}} {
		a := e.acc(wallet(tc.id))
		if a.rated != tc.markets || a.ratedEvents != tc.evts {
			t.Errorf("%s rated %d markets in %d events, want %d and %d", tc.name, a.rated, a.ratedEvents, tc.markets, tc.evts)
		}
		if got := skill.Rated(a.rated, a.ratedEvents); got != tc.floor {
			t.Errorf("%s clears the rated floor: %v, want %v", tc.name, got, tc.floor)
		}
		var cal uint32
		if err := e.o.Work.QueryRowContext(t.Context(), "SELECT CAST(coalesce(sum(n), 0) AS UINTEGER) FROM wallet_cal WHERE wallet_id = ?", e.walletID(wallet(tc.id))).Scan(&cal); err != nil {
			t.Fatal(err)
		}
		if cal != tc.markets {
			t.Errorf("%s calibration counts %d markets, want %d", tc.name, cal, tc.markets)
		}
	}
	if a := e.acc(wallet(r1)); a.sumT != 30*(1000000-600000) || a.sumT2 != 30*400000*400000 {
		t.Errorf("R1 sum_t %d and sum_t2 %d", a.sumT, a.sumT2)
	}
	if a := e.acc(wallet(r4)); a.markets != 4 {
		t.Errorf("R4 trades %d markets, want 4", a.markets)
	}
	type pair struct{ f, o sql.Null[uint32] }
	pairs := func(who int, token string) pair {
		var p pair
		err := e.o.Work.QueryRowContext(t.Context(), `SELECT f_ppm, o_ppm FROM wallet_market
			WHERE wallet_id = ? AND market_id = (SELECT market_id FROM tok WHERE token = ?)`, e.walletID(wallet(who)), token).Scan(&p.f, &p.o)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	if p := pairs(r1, first); p.f.V != 600000 || p.o.V != 1000000 || !p.f.Valid || !p.o.Valid {
		t.Errorf("R1 first market f=%v o=%v, want 600000 and 1000000", p.f, p.o)
	}
	for name, tok := range map[string]string{"updown": updown, "unknown": unknown, "unresolved": open, "late": late} {
		if p := pairs(r4, tok); p.f.Valid || p.o.Valid {
			t.Errorf("R4 %s market has f=%v o=%v, want NULL", name, p.f, p.o)
		}
	}
}

func TestPass1ForecastIsTheLastFillOfTheMarket(t *testing.T) {
	w := newRatedWorld(t)
	toks := w.negEvent(1, 2)
	w.trade(5, toks[0][0], earlyTS)
	w.trade(5, toks[1][0], earlyTS+1)
	w.sell(5, toks[0][0], earlyTS+2)
	e := prepare(t, w.publish())
	e.run()
	e.pass1()
	forecast := func(token string) (f, o uint32) {
		err := e.o.Work.QueryRowContext(t.Context(), `SELECT f_ppm, o_ppm FROM wallet_market
			WHERE wallet_id = ? AND market_id = (SELECT market_id FROM tok WHERE token = ?)`, e.walletID(wallet(5)), token).Scan(&f, &o)
		if err != nil {
			t.Fatal(err)
		}
		return f, o
	}
	if f, o := forecast(toks[0][0]); f != 300000 || o != 0 {
		t.Errorf("first question forecast %d with outcome %d, want 300000 and 0 (a sell at 0.7 backs the other side at 0.3)", f, o)
	}
	if f, o := forecast(toks[1][0]); f != 600000 || o != 1000000 {
		t.Errorf("second question forecast %d with outcome %d, want 600000 and 1000000", f, o)
	}
	if a := e.acc(wallet(5)); a.rated != 2 || a.ratedEvents != 1 {
		t.Errorf("rated %d markets in %d events, want 2 in 1", a.rated, a.ratedEvents)
	}
}

func TestPass1Resume(t *testing.T) {
	e := prepare(t, golden.Path("dataset"))
	e.run()
	e.pass1()
	fingerprint := func() string {
		return e.fingerprint("wallet_acc", "wallet_cal", "wallet_market", "market_acc")
	}
	before := fingerprint()
	e.pass1()
	if after := fingerprint(); after != before {
		t.Fatalf("a second run changed the tables\n%s\n%s", before, after)
	}
	if got := e.count("SELECT CAST(count(*) AS BIGINT) FROM build_state WHERE stage = 'pass1' AND status = 'done'"); got != refs.Buckets+1 {
		t.Fatalf("%d done items, want %d buckets and the finish", got, refs.Buckets)
	}
}

func (e *env) fingerprint(tables ...string) string {
	e.t.Helper()
	var parts []string
	for _, table := range tables {
		var rows int64
		var sum string
		q := fmt.Sprintf("SELECT CAST(count(*) AS BIGINT), CAST(coalesce(sum(CAST(hash(t) AS HUGEINT)), 0) AS VARCHAR) FROM %s t", table)
		if err := e.o.Work.QueryRowContext(e.t.Context(), q).Scan(&rows, &sum); err != nil {
			e.t.Fatalf("%s: %v", q, err)
		}
		parts = append(parts, fmt.Sprintf("%s %d rows hash %s", table, rows, sum))
	}
	var audit string
	if err := e.o.Work.QueryRowContext(e.t.Context(), `SELECT string_agg(key || '=' || CAST(value AS VARCHAR), ',' ORDER BY key)
		FROM audit WHERE key = 'duplicate_rows' OR key LIKE 'no_price_%'`).Scan(&audit); err != nil {
		e.t.Fatal(err)
	}
	return strings.Join(append(parts, audit), "\n")
}

func TestPass1PagedBucketsGiveTheSameResult(t *testing.T) {
	reference := goldenPass1(t)
	tables := []string{"wallet_acc", "wallet_cal", "wallet_market", "market_acc"}
	t.Cleanup(stage.PageRows(50))
	e := prepare(t, golden.Path("dataset"))
	e.run()
	e.pass1()
	if got, want := e.fingerprint(tables...), reference.fingerprint(tables...); got != want {
		t.Fatalf("a build in pages of 50 rows differs from one in whole buckets\n%s\n%s", got, want)
	}
}

func TestPass1ResumeAfterFailure(t *testing.T) {
	reference := goldenPass1(t)
	want := reference.fingerprint("wallet_acc", "wallet_cal", "wallet_market", "market_acc")
	if reference.count("SELECT CAST(count(*) AS BIGINT) FROM wallet_cal") == 0 {
		t.Fatal("the golden build rates no wallet, so the calibration round trip is not covered")
	}
	e := prepare(t, golden.Path("dataset"))
	e.run()
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
	if err := stage.Pass1(t.Context(), e.o); err == nil {
		t.Fatal("Pass1 accepted a broken staged file")
	}
	done := e.count("SELECT CAST(count(*) AS BIGINT) FROM build_state WHERE stage = 'pass1' AND status = 'done'")
	if done == 0 || done >= refs.Buckets {
		t.Fatalf("%d buckets done after the failure, want some but not all", done)
	}
	if err := os.WriteFile(last, original, 0o600); err != nil {
		t.Fatal(err)
	}
	e.pass1()
	if got := e.fingerprint("wallet_acc", "wallet_cal", "wallet_market", "market_acc"); got != want {
		t.Fatalf("resumed build differs from an uninterrupted one\n%s\n%s", got, want)
	}
}

/* Small worlds */

func TestPass1PairedRows(t *testing.T) {
	w := newWorld(t)
	tok := w.toks[condA][0]
	w.write("order_filled", "01",
		fill("137_100_1", "1700000100", alice, bob, "0", tok, "40000000", "100000000"),
		fill("137_100_2", "1700000200", bob, alice, "0", tok, "45000000", "100000000"),
		fill("137_100_3", "1700000250", alice, bob, "0", tok, "5", "0"),
		fill("137_100_4", "1700000260", alice, alice, "0", tok, "5000000", "10000000"),
		fill("137_100_5", "1700000300", carol, bob, "0", tok, "40000000", "100000000"),
		fill("137_100_6", "1700000310", bob, ctfExchange, tok, "0", "100000000", "45000000"),
	)
	e := prepare(t, w.set.Root)
	e.run()
	e.pass1()
	for _, tc := range []struct {
		name, addr    string
		x0, paired    uint64
		fills, taking uint32
	}{
		{"alice", alice, 200000000, 200000000, 3, 0},
		{"bob", bob, 300000000, 300000000, 2, 1},
		{"carol", carol, 0, 100000000, 1, 0},
	} {
		a := e.acc(tc.addr)
		if a.x0Num != tc.x0 || a.paired != tc.paired {
			t.Errorf("%s has x0_num %d and paired %d, want %d and %d", tc.name, a.x0Num, a.paired, tc.x0, tc.paired)
		}
		if a.fills != tc.fills || a.takerRows != tc.taking {
			t.Errorf("%s has %d fills and %d taker rows, want %d and %d", tc.name, a.fills, a.takerRows, tc.fills, tc.taking)
		}
	}
}

func TestPass1DuplicateRows(t *testing.T) {
	w := newWorld(t)
	tok := w.toks[condA][0]
	w.write("order_filled", "01",
		fill("137_100_1", "1700000100", alice, bob, "0", tok, "40000000", "100000000"),
		fill("137_100_1", "1700000100", alice, bob, "0", tok, "40000000", "100000000"),
		fill("137_100_2", "1700000200", alice, bob, tok, "0", "100000000", "45000000"),
	)
	e := prepare(t, w.set.Root)
	e.run()
	e.pass1()
	if got := e.audit("duplicate_rows"); got != 1 {
		t.Errorf("duplicate_rows %d, want 1", got)
	}
	a := e.acc(alice)
	if a.fills != 3 || a.volume != 125000000 {
		t.Errorf("alice has %d fills and volume %d, want the raw 3 and 125000000", a.fills, a.volume)
	}
	if a.gross != 5000000 || a.markets != 1 {
		t.Errorf("alice gross %d over %d markets, want 5000000 over 1 (the repeat applies once)", a.gross, a.markets)
	}
}

func TestPass1Fees(t *testing.T) {
	w := newWorld(t)
	a, b := w.toks[condA][0], w.toks[condB][0]
	w.write("order_filled", "01",
		fill("137_100_1", "1700000100", alice, ctfExchange, "0", a, "40000000", "100000000"),
		fill("137_100_2", "1700000200", alice, bob, "0", b, "40000000", "100000000"),
	)
	w.write("fee_refunded", "01",
		[]any{"137_300_1", alice, a, "1000000"},
		[]any{"137_300_2", alice, a, "500000"},
		[]any{"137_300_3", alice, "0", "7"},
		[]any{"137_300_4", alice, b, "30"},
		[]any{"137_300_5", bob, a, "11"},
	)
	e := prepare(t, w.set.Root)
	e.run()
	e.pass1()
	if got := e.acc(alice).fees; got != 600037 {
		t.Errorf("alice fees %d, want 600037 (a fee in shares is worth its taker buy price, 0.4)", got)
	}
	if got := e.acc(bob).fees; got != 11 {
		t.Errorf("bob fees %d, want 11 at face value although he has no event in the market", got)
	}
	onMarket := func(wallet, condition string) int64 {
		return e.count("SELECT CAST(coalesce(sum(fees), 0) AS BIGINT) FROM wallet_market WHERE wallet_id = ? AND market_id = ?", e.walletID(wallet), e.market(condition))
	}
	if got := onMarket(alice, w.ids[condA]); got != 600000 {
		t.Errorf("alice fees on A %d, want 600000", got)
	}
	if got := onMarket(alice, w.ids[condB]); got != 30 {
		t.Errorf("alice fees on B %d, want 30 at face value without a taker buy", got)
	}
	if got := e.count("SELECT CAST(count(*) AS BIGINT) FROM wallet_market WHERE wallet_id = ?", e.walletID(bob)); got != 0 {
		t.Errorf("bob has %d market rows, want 0", got)
	}
}

func TestPass1CountsFeeRowsWithAnUnknownToken(t *testing.T) {
	w := newWorld(t)
	w.write("fee_refunded", "01",
		[]any{"137_300_1", alice, w.toks[condA][0], "300"},
		[]any{"137_300_2", alice, "0", "20"},
		[]any{"137_300_3", alice, "123456789", "40"},
		[]any{"137_300_4", bob, "987654321", "50"},
	)
	e := prepare(t, w.set.Root)
	e.run()
	e.pass1()
	if got := e.audit("unmapped_fee_rows"); got != 2 {
		t.Errorf("unmapped_fee_rows %d, want 2", got)
	}
	if a := e.acc(alice); a.fees != 360 {
		t.Errorf("alice fees %d, want 360 (the current booking is kept)", a.fees)
	}
}

func TestPass1FeesWithoutFills(t *testing.T) {
	w := newWorld(t)
	w.write("fee_refunded", "01",
		[]any{"137_300_1", alice, w.toks[condA][0], "300"},
		[]any{"137_300_2", alice, "0", "20"},
	)
	e := prepare(t, w.set.Root)
	e.run()
	e.pass1()
	if a := e.acc(alice); a.fees != 320 || a.fills != 0 {
		t.Errorf("alice has fees %d and %d fills, want 320 and 0", a.fees, a.fills)
	}
}

func TestPass1NoPrice(t *testing.T) {
	w := newRatedWorld(t)
	early := w.plain(1, "Will A happen?", true)[0]
	w.trade(1, early, earlyTS)
	late := w.plain(2, "Will B happen?", true)[0]
	w.trade(1, late, lateTS)
	w.plain(3, "Will C happen?", true)
	w.plain(4, "Will D happen?", false)
	e := prepare(t, w.publish())
	e.run()
	e.pass1()
	if got := e.count("SELECT CAST(count(*) AS BIGINT) FROM market_acc"); got != 4 {
		t.Errorf("market_acc has %d rows, want 4", got)
	}
	for key, want := range map[string]int64{"no_price_4h": 2, "no_price_12h": 2, "no_price_24h": 3, "no_price_168h": 3} {
		if got := e.audit(key); got != want {
			t.Errorf("%s %d, want %d", key, got, want)
		}
	}
	prices := func(token string) (p [4]sql.Null[uint32]) {
		err := e.o.Work.QueryRowContext(t.Context(), `SELECT p0_4h, p0_12h, p0_24h, p0_168h FROM market_acc
			WHERE market_id = (SELECT market_id FROM tok WHERE token = ?)`, token).Scan(&p[0], &p[1], &p[2], &p[3])
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	for i, p := range prices(early) {
		if want := i < 2; p.Valid != want || (want && p.V != 600000) {
			t.Errorf("early market price %d is %v, want valid %v at 600000", i, p, want)
		}
	}
	for i, p := range prices(late) {
		if p.Valid {
			t.Errorf("late market price %d is %v, want NULL", i, p)
		}
	}
}

func TestPass1RejectsBadIndexSet(t *testing.T) {
	w := newWorld(t)
	w.write("neg_risk_conversion", "01",
		[]any{"137_400_1", "1700000400", alice, "0x" + negMarket, "5000000", "not-a-number", strconv.Itoa(negCount)})
	e := prepare(t, w.set.Root)
	e.run()
	if err := stage.Pass1(t.Context(), e.o); err == nil || !strings.Contains(err.Error(), "not-a-number") {
		t.Fatalf("Pass1 returned %v, want an error naming the index set", err)
	}
}

func TestPass1RejectsIncompleteEvent(t *testing.T) {
	for _, tc := range []struct {
		rowCount string
		mapped   []uint8
		want     string
	}{
		{"3", []uint8{0, 1}, "2 of 3 question conditions"},
		{"2", []uint8{0, 1}, ""},
		{"2", []uint8{0, 2}, "1 of 2 question conditions"},
	} {
		w := newRatedWorld(t)
		key := [32]byte{0xef, 1}
		w.events = append(w.events, []any{hex32(key), "0", "3"})
		for _, q := range tc.mapped {
			w.condition(hex32(refs.ConditionID(refs.NegRiskAdapter, refs.QuestionID(key, q))), "Will it happen?", true)
		}
		w.convert = append(w.convert, []any{"137_400_1", strconv.Itoa(earlyTS), wallet(1), hex32(key), "1000000", "1", tc.rowCount})
		e := prepare(t, w.publish())
		e.run()
		err := stage.Pass1(t.Context(), e.o)
		if tc.want == "" && err != nil {
			t.Errorf("row count %s with questions %v: Pass1 returned %v, want success", tc.rowCount, tc.mapped, err)
		}
		if tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
			t.Errorf("row count %s with questions %v: Pass1 returned %v, want an error about %s", tc.rowCount, tc.mapped, err, tc.want)
		}
	}
}

func TestPass1ConversionUsesItsOwnQuestionCount(t *testing.T) {
	w := newRatedWorld(t)
	toks := w.negEvent(1, 3)
	const holder = 7
	w.trade(holder, toks[0][1], earlyTS)
	w.convert = append(w.convert, []any{"137_400_1", strconv.Itoa(earlyTS + 1), wallet(holder), hex32(negKey(1)), "1000000", "1", "2"})
	e := prepare(t, w.publish())
	e.run()
	e.pass1()
	market := func(q int) uint32 {
		return uint32(e.count("SELECT CAST(market_id AS BIGINT) FROM tok WHERE token = ?", toks[q][0]))
	}
	events, err := stage.WalletEvents(t.Context(), e.o, e.walletID(wallet(holder)))
	if err != nil {
		t.Fatal(err)
	}
	l := pnl.NewLedger(e.look())
	for _, ev := range events {
		l.Apply(ev)
	}
	var yes []pnl.Position
	for _, p := range l.Positions() {
		if p.Outcome == 0 {
			yes = append(yes, p)
		}
	}
	if len(yes) != 1 || yes[0].Market != market(1) || yes[0].Amount != 1000000 || yes[0].Avg != 600000 {
		t.Errorf("YES positions %+v, want one on the second question of 1000000 shares at 600000 (a count of 2, not the final 3)", yes)
	}
	rows := e.count("SELECT CAST(count(*) AS BIGINT) FROM wallet_market WHERE wallet_id = ?", e.walletID(wallet(holder)))
	third := e.count("SELECT CAST(count(*) AS BIGINT) FROM wallet_market WHERE wallet_id = ? AND market_id = ?", e.walletID(wallet(holder)), market(2))
	if rows != 2 || third != 0 {
		t.Errorf("%d wallet_market rows with %d on the third question, want 2 and 0", rows, third)
	}
}

func TestPass1NeedsFinishedStage(t *testing.T) {
	w := newWorld(t)
	e := prepare(t, w.set.Root)
	if err := e.o.Work.QueryRowContext(t.Context(), "SELECT 1").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.o.Work.ExecContext(t.Context(), "CREATE TABLE wallet_dict (address BLOB, code UINTEGER, wallet_id UINTEGER)"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.o.Work.ExecContext(t.Context(), "INSERT INTO wallet_dict VALUES (unhex('00'), 0, NULL)"); err != nil {
		t.Fatal(err)
	}
	if err := stage.Pass1(t.Context(), e.o); !errors.Is(err, stage.ErrIncomplete) {
		t.Fatalf("Pass1 returned %v, want ErrIncomplete", err)
	}
}

func TestPass1FirstAndLastTrade(t *testing.T) {
	e := goldenPass1(t)
	for _, name := range []string{"wallets_14", "wallets_conversion", "wallets_fee_era", "wallets_cutoff"} {
		for _, w := range goldenWallets(t, name) {
			a := e.acc(w.Address)
			if a.firstTS != w.FirstTS || a.lastTS != w.LastTS {
				t.Errorf("%s %s trades from %d to %d, want %d to %d", name, w.Address, a.firstTS, a.lastTS, w.FirstTS, w.LastTS)
			}
		}
	}
}
