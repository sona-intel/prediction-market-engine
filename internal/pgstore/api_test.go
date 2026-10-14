package pgstore_test

import (
	"errors"
	"testing"
	"time"

	"pm/internal/hip4"
	"pm/internal/levr"
	"pm/internal/pgstore"
	"pm/internal/pgstore/pgtest"
	"pm/internal/venue"
)

func TestTopRecentSkipsMarketsOutsideTheNewestSweeps(t *testing.T) {
	st := pgtest.New(t)
	ctx := t.Context()
	k := venue.Kalshi
	if err := st.UpsertMarkets(ctx, []venue.Market{
		{Venue: k, ID: "A", Title: "a", Status: "open"}, {Venue: k, ID: "B", Title: "b", Status: "open"}, {Venue: k, ID: "OLD", Title: "old", Status: "open"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.InsertSnaps(ctx, []venue.Snap{
		{Venue: k, MarketID: "A", TS: t0, YesPPM: 100000, Vol24: 5},
		{Venue: k, MarketID: "A", TS: t0.Add(10 * time.Minute), YesPPM: 200000, Vol24: 7},
		{Venue: k, MarketID: "B", TS: t0.Add(10 * time.Minute), YesPPM: 300000, Vol24: 9},
		{Venue: k, MarketID: "OLD", TS: t0.Add(-2 * time.Hour), YesPPM: 400000, Vol24: 99},
	}); err != nil {
		t.Fatal(err)
	}
	got, err := st.TopRecent(ctx, k, 10)
	if err != nil || len(got) != 2 || got[0].ID != "B" || got[1].ID != "A" || got[1].Last.YesPPM != 200000 || got[1].Last.Vol24 != 7 {
		t.Fatalf("%+v, %v", got, err)
	}
	if got, _ := st.TopRecent(ctx, k, 1); len(got) != 1 {
		t.Fatalf("limit ignored %+v", got)
	}
	if got, err := st.TopRecent(ctx, venue.PolymarketUS, 5); err != nil || len(got) != 0 {
		t.Fatalf("%+v, %v", got, err)
	}
}

func TestTapeReadsForHIP4(t *testing.T) {
	st := pgtest.New(t)
	ctx := t.Context()
	hl := venue.Hyperliquid
	if err := st.UpsertMarkets(ctx, []venue.Market{{Venue: hl, ID: "#10", Title: "x", Status: "open"}, {Venue: hl, ID: "#20", Title: "y", Status: "open"}}); err != nil {
		t.Fatal(err)
	}
	a, b := []byte("wallet-a-20-bytes---"), []byte("wallet-b-20-bytes---")
	if _, err := st.InsertTrades(ctx, []venue.Trade{
		{Venue: hl, ID: "1", MarketID: "#10", TS: t0, Side: "buy", PricePPM: 400000, Qty: 1, USD: 5, Taker: a, Maker: b},
		{Venue: hl, ID: "2", MarketID: "#10", TS: t0.Add(2 * time.Minute), Side: "sell", PricePPM: 450000, Qty: 1, USD: 6, Taker: a, Maker: b},
		{Venue: hl, ID: "3", MarketID: "#10", TS: t0.Add(7 * time.Minute), Side: "buy", PricePPM: 500000, Qty: 1, USD: 7, Taker: a, Maker: b},
		{Venue: hl, ID: "4", MarketID: "#20", TS: t0.Add(time.Minute), Side: "buy", PricePPM: 100000, Qty: 1, USD: 50, Taker: a, Maker: b},
	}); err != nil {
		t.Fatal(err)
	}
	top, err := st.TopTape(ctx, hl, t0, 10)
	if err != nil || len(top) != 2 || top[0].ID != "#20" || top[0].Last.Vol24 != 50 || top[1].Last.Vol24 != 18 || top[1].Last.YesPPM != 500000 {
		t.Fatalf("%+v, %v", top, err)
	}
	if top, _ := st.TopTape(ctx, hl, t0.Add(5*time.Minute), 10); len(top) != 1 || top[0].ID != "#10" {
		t.Fatalf("since ignored %+v", top)
	}
	series, err := st.TapeSeries(ctx, hl, "#10", t0, 5*time.Minute, 10)
	if err != nil || len(series) != 2 || !series[0].TS.Equal(t0) || series[0].YesPPM != 450000 || !series[1].TS.Equal(t0.Add(5*time.Minute)) || series[1].YesPPM != 500000 {
		t.Fatalf("%+v, %v", series, err)
	}
	if last, err := st.TapeSeries(ctx, hl, "#10", t0, 5*time.Minute, 1); err != nil || len(last) != 1 || last[0].YesPPM != 500000 {
		t.Fatalf("limit must keep the newest bucket %+v, %v", last, err)
	}
	named, err := st.LargeNamed(ctx, hl, 6, 10)
	if err != nil || len(named) != 3 || named[0].ID != "3" || named[0].Title != "x" || string(named[0].Taker) != string(a) {
		t.Fatalf("%+v, %v", named, err)
	}
}

func TestGapsReadsTheCursorCount(t *testing.T) {
	st := pgtest.New(t)
	ctx := t.Context()
	if _, ok, err := st.Gaps(ctx, venue.Kalshi); ok || err != nil {
		t.Fatalf("%v, %v", ok, err)
	}
	if err := st.SetCursor(ctx, "kalshi/gaps", "3"); err != nil {
		t.Fatal(err)
	}
	if n, ok, err := st.Gaps(ctx, venue.Kalshi); n != 3 || !ok || err != nil {
		t.Fatalf("%d, %v, %v", n, ok, err)
	}
}

func TestHLBoardsAndWallet(t *testing.T) {
	st := pgtest.New(t)
	ctx := t.Context()
	a, b, c := []byte("wallet-a-20-bytes---"), []byte("wallet-b-20-bytes---"), []byte("wallet-c-20-bytes---")
	if err := st.UpsertHLWallets(ctx, []hip4.Wallet{
		{Address: a, Fills: 3, Volume: 30, Gross: 5, Markets: 1},
		{Address: b, Fills: 2, Volume: 90, Gross: -4, Markets: 2},
		{Address: c, Fills: 9, Volume: 10, Gross: 8, Markets: 3, Rated: true, Brier: 0.2, Edge: 0.1, EdgeSE: 0.02},
	}); err != nil {
		t.Fatal(err)
	}
	order := func(board string, limit, offset int) (string, int64) {
		p, err := st.HLBoard(ctx, board, limit, offset)
		if err != nil {
			t.Fatal(err)
		}
		var s []byte
		for _, w := range p.Rows {
			s = append(s, w.Address[7])
		}
		return string(s), p.Total
	}
	if got, n := order("pnl", 10, 0); got != "cab" || n != 3 {
		t.Fatalf("pnl %s %d", got, n)
	}
	if got, _ := order("volume", 10, 0); got != "bac" {
		t.Fatalf("volume %s", got)
	}
	if got, n := order("skill", 10, 0); got != "c" || n != 1 {
		t.Fatalf("skill %s %d", got, n)
	}
	if got, _ := order("pnl", 1, 1); got != "a" {
		t.Fatalf("page %s", got)
	}
	if _, err := st.HLBoard(ctx, "nope", 1, 0); !errors.Is(err, pgstore.ErrNotFound) {
		t.Fatalf("%v", err)
	}
	w, err := st.HLWallet(ctx, c)
	if err != nil || !w.Rated || w.Edge != 0.1 || w.Fills != 9 {
		t.Fatalf("%+v, %v", w, err)
	}
	if _, err := st.HLWallet(ctx, []byte("wallet-z-20-bytes---")); !errors.Is(err, pgstore.ErrNotFound) {
		t.Fatalf("%v", err)
	}
}

func TestLevrWalletReads(t *testing.T) {
	st := pgtest.New(t)
	ctx := t.Context()
	a, b := []byte("wallet-a-20-bytes---"), []byte("wallet-b-20-bytes---")
	if _, err := st.UpsertPositions(ctx, []levr.Position{
		{ID: "p1", Wallet: a, MarketID: "m1", Opened: t0, Stake: 5_000_000, Size: 10_000_000, Side: 1, Status: levr.Open},
		{ID: "p2", Wallet: a, MarketID: "m1", Opened: t0.Add(time.Minute), Stake: 2_000_000, Size: 8_000_000, Side: 2, Status: levr.Open},
		{ID: "p3", Wallet: b, MarketID: "m2", Opened: t0, Stake: 1_000_000, Size: 50_000_000, Side: 1, Status: levr.Open},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ClosePositions(ctx, []levr.Close{levr.Liquidation("p2", t0)}); err != nil {
		t.Fatal(err)
	}
	ps, err := st.WalletPositions(ctx, a)
	if err != nil || len(ps) != 2 || ps[0].ID != "p1" || ps[1].Status != levr.Liquidated {
		t.Fatalf("%+v, %v", ps, err)
	}
	if ps, _ := st.WalletPositions(ctx, []byte("wallet-z-20-bytes---")); len(ps) != 0 {
		t.Fatalf("%+v", ps)
	}
	if err := (&levr.Wallets{Store: st}).Poll(ctx); err != nil {
		t.Fatal(err)
	}
	p, err := st.LevrBoard(ctx, "pnl", 10, 0)
	if err != nil || p.Total != 2 || len(p.Rows) != 2 || p.Rows[0].Address[7] != 'b' || p.Rows[1].Realized != -2_000_000 || p.Rows[1].Liquidated != 1 || p.Rows[1].Settled() != 1 {
		t.Fatalf("%+v, %v", p, err)
	}
	if p, _ := st.LevrBoard(ctx, "volume", 1, 0); len(p.Rows) != 1 || p.Rows[0].Address[7] != 'b' || p.Rows[0].Size != 50_000_000 {
		t.Fatalf("%+v", p)
	}
	if _, err := st.LevrBoard(ctx, "skill", 1, 0); !errors.Is(err, pgstore.ErrNotFound) {
		t.Fatalf("%v", err)
	}
}
