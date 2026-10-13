package pnl_test

import (
	"math/bits"
	"testing"

	"pm/internal/pnl"
)

const scale = 1000000

var markets = map[uint32]pnl.Market{
	1: {Resolved: true, Payout: [2]int64{1000000, 0}},
	3: {Resolved: true, Payout: [2]int64{500000, 500000}},
}

func look(id uint32) pnl.Market { return markets[id] }

func usdc(qty, price int64) uint64 {
	hi, lo := bits.Mul64(uint64(qty), uint64(price))
	q, _ := bits.Div64(hi, lo, scale)
	return q
}

type run struct {
	t *testing.T
	l *pnl.Ledger
	n uint32
}

func newRun(t *testing.T) *run {
	t.Helper()
	return &run{t: t, l: pnl.NewLedger(look)}
}

func (r *run) apply(e pnl.Event) {
	e.Block, e.Log = 100, r.n
	r.n++
	r.l.Apply(e)
}

func (r *run) buy(market uint32, outcome uint8, qty, price int64) {
	r.apply(pnl.Event{Kind: pnl.Buy, Market: market, Outcome: outcome, Qty: qty, USDC: usdc(qty, price)})
}

func (r *run) sell(market uint32, outcome uint8, qty, price int64) {
	r.apply(pnl.Event{Kind: pnl.Sell, Market: market, Outcome: outcome, Qty: qty, USDC: usdc(qty, price)})
}

func (r *run) ctf(kind pnl.Kind, market uint32, qty int64) {
	r.apply(pnl.Event{Kind: kind, Market: market, Qty: qty})
}

func (r *run) convert(qty int64, set pnl.IndexSet, questions ...uint32) {
	r.apply(pnl.Event{Kind: pnl.Convert, Qty: qty, IndexSet: set, Questions: questions})
}

func (r *run) pos(market uint32, outcome uint8) pnl.Position {
	r.t.Helper()
	for _, p := range r.l.Positions() {
		if p.Market == market && p.Outcome == outcome {
			return p
		}
	}
	return pnl.Position{}
}

func (r *run) market(res pnl.Result, id uint32) pnl.MarketPnL {
	r.t.Helper()
	for _, m := range res.Markets {
		if m.Market == id {
			return m
		}
	}
	r.t.Fatalf("market %d missing from %+v", id, res.Markets)
	return pnl.MarketPnL{}
}

func TestPricePPM(t *testing.T) {
	for _, tc := range []struct {
		usdc, shares uint64
		want         int64
	}{
		{2200000, 5000000, 440000},
		{1, 3, 333333},
		{5, 0, 0},
		{10000000000000, 20000000000000, 500000},
		{1000000000000000000, 1000000000000000000, 1000000},
		{0, 1000000000000000000, 0},
		{500000, 1000000, 500000},
	} {
		if got := pnl.PricePPM(tc.usdc, tc.shares); got != tc.want {
			t.Errorf("PricePPM(%d, %d) = %d, want %d", tc.usdc, tc.shares, got, tc.want)
		}
	}
}

func TestPricePPMSaturates(t *testing.T) {
	const max = int64(1<<63 - 1)
	if got := pnl.PricePPM(1<<63, 1); got != max {
		t.Fatalf("PricePPM(2^63, 1) = %d, want %d", got, max)
	}
	if got := pnl.PricePPM(1<<62, 1<<40); got != 4194304000000 {
		t.Fatalf("PricePPM(2^62, 2^40) = %d", got)
	}
}

func TestParseIndexSet(t *testing.T) {
	set, ok := pnl.ParseIndexSet("137301486137384042495")
	if !ok || !set.Has(66) || set.Has(63) || set.Count() != 52 {
		t.Fatalf("67 bit value: ok=%v set=%v has66=%v has63=%v count=%d", ok, set, set.Has(66), set.Has(63), set.Count())
	}
	if set, ok := pnl.ParseIndexSet("18446744073709551616"); !ok || set != (pnl.IndexSet{0, 1, 0, 0}) {
		t.Fatalf("2^64 = %v, %v", set, ok)
	}
	if set, ok := pnl.ParseIndexSet("0"); !ok || set != (pnl.IndexSet{}) || set.Count() != 0 {
		t.Fatalf("0 = %v, %v", set, ok)
	}
	top := "115792089237316195423570985008687907853269984665640564039457584007913129639935"
	if set, ok := pnl.ParseIndexSet(top); !ok || set.Count() != 256 || !set.Has(255) {
		t.Fatalf("2^256-1 = %v, %v", set, ok)
	}
	for _, s := range []string{"12x", "", "-1", "+1", " 1", "1_0", "115792089237316195423570985008687907853269984665640564039457584007913129639936"} {
		if _, ok := pnl.ParseIndexSet(s); ok {
			t.Errorf("ParseIndexSet(%q) accepted", s)
		}
	}
}

func TestIndexSetHasOutOfRange(t *testing.T) {
	set := pnl.IndexSet{1, 0, 0, 1 << 63}
	if !set.Has(0) || !set.Has(255) || set.Has(256) || set.Has(-1) {
		t.Fatalf("Has bounds wrong for %v", set)
	}
}

func TestLedgerAverageCost(t *testing.T) {
	r := newRun(t)
	r.buy(2, 0, 10e6, 400000)
	r.buy(2, 0, 10e6, 600000)
	if p := r.pos(2, 0); p.Avg != 500000 || p.Amount != 20e6 {
		t.Fatalf("after buys %+v", p)
	}
	r.sell(2, 0, 5e6, 700000)
	if p := r.pos(2, 0); p.Realized != 1000000 || p.Amount != 15e6 || p.Avg != 500000 {
		t.Fatalf("after sell %+v", p)
	}
}

func TestLedgerTruncatesTowardZero(t *testing.T) {
	r := newRun(t)
	r.buy(2, 0, 1e6, 500000)
	r.apply(pnl.Event{Kind: pnl.Sell, Market: 2, Outcome: 0, Qty: 1, USDC: 0})
	if p := r.pos(2, 0); p.Realized != 0 {
		t.Fatalf("a loss of half a unit gave Realized %d, want 0", p.Realized)
	}
	r = newRun(t)
	r.buy(2, 0, 1e6, 500000)
	r.sell(2, 0, 1e6, 499999)
	if p := r.pos(2, 0); p.Realized != -1 {
		t.Fatalf("a loss of one unit gave Realized %d, want -1", p.Realized)
	}
	r = newRun(t)
	r.buy(2, 1, 3e6, 333333)
	r.buy(2, 1, 4e6, 500000)
	if p := r.pos(2, 1); p.Avg != 428571 {
		t.Fatalf("Avg %d, want 428571", p.Avg)
	}
}

func TestLedgerSellCappedAtHeld(t *testing.T) {
	r := newRun(t)
	r.buy(2, 0, 5e6, 500000)
	r.sell(2, 0, 8e6, 800000)
	if p := r.pos(2, 0); p.Realized != 1500000 || p.Amount != 0 {
		t.Fatalf("got %+v", p)
	}
}

func TestLedgerSellWithoutPosition(t *testing.T) {
	r := newRun(t)
	r.sell(2, 0, 5e6, 800000)
	if p := r.pos(2, 0); p.Realized != 0 || p.Amount != 0 {
		t.Fatalf("got %+v", p)
	}
	if res := r.l.Close(); res.OpenUnresolved != 0 || len(res.Markets) != 1 {
		t.Fatalf("got %+v", res)
	}
}

func TestLedgerSplitMerge(t *testing.T) {
	r := newRun(t)
	r.ctf(pnl.Split, 2, 10e6)
	for o := range uint8(2) {
		if p := r.pos(2, o); p.Amount != 10e6 || p.Avg != 500000 {
			t.Fatalf("after split outcome %d %+v", o, p)
		}
	}
	r.ctf(pnl.Merge, 2, 4e6)
	for o := range uint8(2) {
		if p := r.pos(2, o); p.Amount != 6e6 || p.Realized != 0 {
			t.Fatalf("after merge outcome %d %+v", o, p)
		}
	}
	r.sell(2, 0, 6e6, 700000)
	if p := r.pos(2, 0); p.Realized != 1200000 || p.Amount != 0 {
		t.Fatalf("after sell %+v", p)
	}
}

func TestLedgerRedeem(t *testing.T) {
	r := newRun(t)
	r.buy(1, 0, 10e6, 400000)
	r.buy(1, 1, 5e6, 300000)
	r.ctf(pnl.Redeem, 1, 10e6)
	p0, p1 := r.pos(1, 0), r.pos(1, 1)
	if p0.Realized+p1.Realized != 4500000 || p0.Amount != 0 || p1.Amount != 0 {
		t.Fatalf("got %+v %+v", p0, p1)
	}
	if res := r.l.Close(); r.market(res, 1).Gross != 4500000 || res.OpenUnresolved != 0 {
		t.Fatalf("got %+v", res)
	}
}

func TestLedgerRedeemUnresolvedChangesNothing(t *testing.T) {
	r := newRun(t)
	r.buy(2, 0, 10e6, 400000)
	r.ctf(pnl.Redeem, 2, 10e6)
	if p := r.pos(2, 0); p.Amount != 10e6 || p.Realized != 0 || p.Avg != 400000 {
		t.Fatalf("got %+v", p)
	}
	if res := r.l.Close(); res.OpenUnresolved != 1 || r.market(res, 2).Gross != 0 {
		t.Fatalf("got %+v", res)
	}
}

func TestLedgerConvert(t *testing.T) {
	r := newRun(t)
	r.buy(4, 1, 10e6, 600000)
	r.buy(5, 1, 10e6, 700000)
	r.convert(10e6, pnl.IndexSet{0b011}, 4, 5, 6)
	if p := r.pos(4, 1); p.Amount != 0 {
		t.Fatalf("NO of 4 %+v", p)
	}
	if p := r.pos(5, 1); p.Amount != 0 {
		t.Fatalf("NO of 5 %+v", p)
	}
	if p := r.pos(6, 0); p.Amount != 10e6 || p.Avg != 300000 || p.Realized != 0 {
		t.Fatalf("YES of 6 %+v", p)
	}
	if p := r.pos(6, 1); p.Amount != 0 {
		t.Fatalf("NO of 6 %+v", p)
	}
}

func TestLedgerConvertYesPriceCanBeNegative(t *testing.T) {
	r := newRun(t)
	r.buy(4, 1, 10e6, 400000)
	r.buy(5, 1, 10e6, 400000)
	r.convert(10e6, pnl.IndexSet{0b011}, 4, 5, 6)
	if p := r.pos(6, 0); p.Amount != 10e6 || p.Avg != -200000 || p.Bought != 10e6 {
		t.Fatalf("YES of 6 %+v", p)
	}
	r.buy(6, 0, 10e6, 300000)
	if p := r.pos(6, 0); p.Amount != 20e6 || p.Avg != 50000 || p.Bought != 20e6 {
		t.Fatalf("YES of 6 after a buy %+v", p)
	}
	r.sell(6, 0, 20e6, 100000)
	if p := r.pos(6, 0); p.Amount != 0 || p.Realized != 1000000 {
		t.Fatalf("YES of 6 after a sell %+v", p)
	}
}

func TestLedgerNegativeAverageTruncatesTowardZero(t *testing.T) {
	r := newRun(t)
	r.buy(4, 1, 10e6, 400000)
	r.buy(5, 1, 10e6, 400000)
	r.convert(10e6, pnl.IndexSet{0b011}, 4, 5, 6)
	r.apply(pnl.Event{Kind: pnl.Buy, Market: 6, Outcome: 0, Qty: 3})
	if p := r.pos(6, 0); p.Avg != -199999 {
		t.Fatalf("YES of 6 %+v, want an average of -199999", p)
	}
}

func TestLedgerConvertAllOrNoneBuysNothing(t *testing.T) {
	for _, set := range []pnl.IndexSet{{0b111}, {}} {
		r := newRun(t)
		r.buy(4, 1, 10e6, 600000)
		r.convert(10e6, set, 4, 5, 6)
		for _, id := range []uint32{4, 5, 6} {
			if p := r.pos(id, 0); p.Amount != 0 || p.Bought != 0 {
				t.Fatalf("set %v bought YES of %d %+v", set, id, p)
			}
		}
	}
}

func TestLedgerConvertWideIndexSet(t *testing.T) {
	const questions = 84
	ids := make([]uint32, questions)
	for q := range ids {
		ids[q] = 1000 + uint32(q)
	}
	var set pnl.IndexSet
	r := newRun(t)
	for q := 60; q <= 70; q++ {
		set[q>>6] |= 1 << (q & 63)
		r.buy(ids[q], 1, 10e6, 950000)
	}
	if set != (pnl.IndexSet{0xF000000000000000, 0x7F, 0, 0}) {
		t.Fatalf("test set %v", set)
	}
	r.convert(10e6, set, ids...)
	var no, yes int
	for q, id := range ids {
		n, y := r.pos(id, 1), r.pos(id, 0)
		if set.Has(q) {
			if n.Amount != 0 || y.Amount != 0 {
				t.Fatalf("question %d NO %+v YES %+v", q, n, y)
			}
			no++
			continue
		}
		if y.Amount != 10e6 || y.Avg != 6164 {
			t.Fatalf("question %d YES %+v", q, y)
		}
		yes++
	}
	if no != 11 || yes != 73 {
		t.Fatalf("sold %d NO, bought %d YES", no, yes)
	}
}

func TestLedgerSettlesResolved(t *testing.T) {
	r := newRun(t)
	r.buy(1, 1, 10e6, 300000)
	res := r.l.Close()
	if m := r.market(res, 1); m.Gross != -3000000 || !m.Resolved || m.Volume != 3000000 {
		t.Fatalf("got %+v", m)
	}
	if res.OpenUnresolved != 0 {
		t.Fatalf("OpenUnresolved %d", res.OpenUnresolved)
	}
}

func TestLedgerSettlesFiftyFifty(t *testing.T) {
	r := newRun(t)
	r.buy(3, 0, 10e6, 600000)
	if m := r.market(r.l.Close(), 3); m.Gross != -1000000 {
		t.Fatalf("got %+v", m)
	}
}

func TestLedgerCloseSumsBothOutcomes(t *testing.T) {
	r := newRun(t)
	r.buy(1, 0, 10e6, 400000)
	r.sell(1, 1, 5e6, 100000)
	r.buy(2, 0, 1e6, 500000)
	res := r.l.Close()
	if len(res.Markets) != 2 || res.Markets[0].Market != 1 || res.Markets[1].Market != 2 {
		t.Fatalf("markets %+v", res.Markets)
	}
	if m := res.Markets[0]; m.Gross != 6000000 || m.Volume != 4500000 {
		t.Fatalf("got %+v", m)
	}
	if m := res.Markets[1]; m.Resolved || m.Gross != 0 || m.Volume != 500000 {
		t.Fatalf("got %+v", m)
	}
}

func TestLedgerLeavesUnresolvedOpen(t *testing.T) {
	r := newRun(t)
	r.buy(2, 0, 5e6, 500000)
	res := r.l.Close()
	if res.OpenUnresolved != 1 || r.market(res, 2).Gross != 0 {
		t.Fatalf("got %+v", res)
	}
	r = newRun(t)
	r.buy(2, 0, 5e6, 500000)
	r.buy(2, 1, 5e6, 500000)
	if res := r.l.Close(); res.OpenUnresolved != 2 {
		t.Fatalf("got %+v", res)
	}
}

func TestLedgerCloseTwiceChangesNothing(t *testing.T) {
	r := newRun(t)
	r.buy(1, 1, 10e6, 300000)
	r.buy(2, 0, 5e6, 500000)
	first := r.l.Close()
	second := r.l.Close()
	if first.OpenUnresolved != second.OpenUnresolved || r.market(first, 1) != r.market(second, 1) {
		t.Fatalf("first %+v second %+v", first, second)
	}
}

func TestLedgerDuplicateAppliesOnce(t *testing.T) {
	l := pnl.NewLedger(look)
	e := pnl.Event{Block: 7, Log: 3, Kind: pnl.Buy, Market: 2, Qty: 10e6, USDC: 4e6}
	l.Apply(e)
	l.Apply(e)
	l.Apply(pnl.Event{Block: 7, Log: 4, Kind: pnl.Buy, Market: 2, Qty: 10e6, USDC: 4e6})
	res := l.Close()
	if res.Duplicates != 1 || res.Markets[0].Volume != 8e6 {
		t.Fatalf("got %+v", res)
	}
	if ps := l.Positions(); len(ps) != 1 || ps[0].Amount != 20e6 {
		t.Fatalf("positions %+v", ps)
	}
}

func TestLedgerFirstEventAtOriginIsNotDuplicate(t *testing.T) {
	l := pnl.NewLedger(look)
	l.Apply(pnl.Event{Kind: pnl.Buy, Market: 2, Qty: 1e6, USDC: 1e6})
	if res := l.Close(); res.Duplicates != 0 || res.Markets[0].Volume != 1e6 {
		t.Fatalf("got %+v", res)
	}
}

func TestLedgerZeroShareFill(t *testing.T) {
	l := pnl.NewLedger(look)
	l.Apply(pnl.Event{Block: 1, Kind: pnl.Buy, Market: 2, Qty: 0, USDC: 5})
	if len(l.Positions()) != 0 {
		t.Fatalf("created %+v", l.Positions())
	}
	res := l.Close()
	if len(res.Markets) != 1 || res.Markets[0].Volume != 5 || res.OpenUnresolved != 0 {
		t.Fatalf("got %+v", res)
	}
}

func TestLedgerLargePositionExact(t *testing.T) {
	r := newRun(t)
	r.buy(2, 0, 2e13, 500000)
	r.buy(2, 0, 2e13, 700000)
	if p := r.pos(2, 0); p.Avg != 600000 || p.Amount != 4e13 {
		t.Fatalf("got %+v", p)
	}
	r.sell(2, 0, 4e13, 650000)
	if p := r.pos(2, 0); p.Realized != 2000000000000 || p.Amount != 0 {
		t.Fatalf("got %+v", p)
	}
}

func TestLedgerPositionsExposeBought(t *testing.T) {
	r := newRun(t)
	r.buy(2, 0, 5e6, 400000)
	r.ctf(pnl.Split, 2, 10e6)
	if p := r.pos(2, 0); p.Bought != 15e6 {
		t.Fatalf("outcome 0 %+v", p)
	}
	if p := r.pos(2, 1); p.Bought != 10e6 {
		t.Fatalf("outcome 1 %+v", p)
	}
}

func TestLedgerPositionsAreOrdered(t *testing.T) {
	r := newRun(t)
	r.buy(6, 1, 1e6, 500000)
	r.buy(2, 1, 1e6, 500000)
	r.buy(2, 0, 1e6, 500000)
	ps := r.l.Positions()
	if len(ps) != 3 || ps[0].Market != 2 || ps[0].Outcome != 0 || ps[1].Outcome != 1 || ps[2].Market != 6 {
		t.Fatalf("got %+v", ps)
	}
}
