package pgstore_test

import (
	"testing"
	"time"

	"pm/internal/pgstore"
	"pm/internal/pgstore/pgtest"
	"pm/internal/venue"
)

var t0 = time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)

func TestMigrateTwiceKeepsRows(t *testing.T) {
	st := pgtest.New(t)
	if err := st.SetCursor(t.Context(), "k/trades", "c1"); err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if v, err := st.Cursor(t.Context(), "k/trades"); err != nil || v != "c1" {
		t.Fatalf("cursor %q, %v", v, err)
	}
	var n int
	if err := st.Pool.QueryRow(t.Context(), `select count(*) from schema_migrations`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("migrations recorded %d, %v", n, err)
	}
}

func TestMigrationFileRunsTwice(t *testing.T) {
	st := pgtest.New(t)
	body, err := pgstore.ReadMigration()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Pool.Exec(t.Context(), body); err != nil {
		t.Fatalf("second run: %v", err)
	}
}

func TestCursor(t *testing.T) {
	st := pgtest.New(t)
	ctx := t.Context()
	if v, err := st.Cursor(ctx, "none"); err != nil || v != "" {
		t.Fatalf("unset cursor %q, %v", v, err)
	}
	for _, v := range []string{"a", "b"} {
		if err := st.SetCursor(ctx, "s", v); err != nil {
			t.Fatal(err)
		}
	}
	if v, _ := st.Cursor(ctx, "s"); v != "b" {
		t.Fatalf("cursor %q", v)
	}
}

func TestVenues(t *testing.T) {
	st := pgtest.New(t)
	ctx := t.Context()
	info := venue.Catalog[venue.Kalshi]
	info.Since = t0
	if err := st.UpsertVenue(ctx, info); err != nil {
		t.Fatal(err)
	}
	info.Since = t0.Add(time.Hour)
	info.Name = "Kalshi renamed"
	if err := st.UpsertVenue(ctx, info); err != nil {
		t.Fatal(err)
	}
	vs, err := st.Venues(ctx)
	if err != nil || len(vs) != 1 {
		t.Fatalf("%v, %v", vs, err)
	}
	if vs[0].Name != "Kalshi renamed" || !vs[0].Since.Equal(t0) || !vs[0].LastOK.IsZero() {
		t.Fatalf("got %+v", vs[0])
	}
	if err := st.MarkOK(ctx, venue.Kalshi, t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	vs, _ = st.Venues(ctx)
	if !vs[0].LastOK.Equal(t0.Add(time.Minute)) {
		t.Fatalf("last ok %v", vs[0].LastOK)
	}
}

func TestMarketsAndSnaps(t *testing.T) {
	st := pgtest.New(t)
	ctx := t.Context()
	ms := []venue.Market{
		{Venue: "kalshi", ID: "A", Title: "Alpha", Kind: "binary", Close: t0.Add(24 * time.Hour), Status: "open"},
		{Venue: "kalshi", ID: "B", Title: "Beta", Status: "open"},
	}
	if err := st.UpsertMarkets(ctx, ms); err != nil {
		t.Fatal(err)
	}
	ms[0].Result = "yes"
	ms[0].Status = "settled"
	if err := st.UpsertMarkets(ctx, ms[:1]); err != nil {
		t.Fatal(err)
	}
	m, ok, err := st.Market(ctx, "kalshi", "A")
	if err != nil || !ok || m.Result != "yes" || !m.Close.Equal(t0.Add(24*time.Hour)) {
		t.Fatalf("%+v %v %v", m, ok, err)
	}
	if b, _, _ := st.Market(ctx, "kalshi", "B"); !b.Close.IsZero() {
		t.Fatalf("close %v", b.Close)
	}
	if _, ok, _ := st.Market(ctx, "kalshi", "nope"); ok {
		t.Fatal("found a market that was never stored")
	}
	if ids, _ := st.OpenMarketIDs(ctx, "kalshi"); len(ids) != 1 || ids[0] != "B" {
		t.Fatalf("open %v", ids)
	}

	snaps := []venue.Snap{
		{Venue: "kalshi", MarketID: "A", TS: t0, YesPPM: 400000, Volume: 5, Vol24: 1},
		{Venue: "kalshi", MarketID: "A", TS: t0.Add(time.Minute), YesPPM: 450000, Volume: 9, Vol24: 2},
		{Venue: "kalshi", MarketID: "B", TS: t0, YesPPM: 100000, Volume: 50, Vol24: 20},
	}
	if n, err := st.InsertSnaps(ctx, snaps); err != nil || n != 3 {
		t.Fatalf("inserted %d, %v", n, err)
	}
	if n, err := st.InsertSnaps(ctx, snaps); err != nil || n != 0 {
		t.Fatalf("replay inserted %d, %v", n, err)
	}
	top, err := st.TopMarkets(ctx, "kalshi", 10)
	if err != nil || len(top) != 2 || top[0].ID != "B" || top[1].Last.YesPPM != 450000 {
		t.Fatalf("top %+v, %v", top, err)
	}
	hist, err := st.Snaps(ctx, "kalshi", "A", t0, 10)
	if err != nil || len(hist) != 2 || hist[0].YesPPM != 400000 {
		t.Fatalf("history %+v, %v", hist, err)
	}
	if n, err := st.PruneSnaps(ctx, t0.Add(30*time.Second)); err != nil || n != 2 {
		t.Fatalf("pruned %d, %v", n, err)
	}
}

func TestTrades(t *testing.T) {
	st := pgtest.New(t)
	ctx := t.Context()
	alice, bob := []byte{0xa1}, []byte{0xb0}
	ts := []venue.Trade{
		{Venue: "hyperliquid", ID: "1", MarketID: "#10", TS: t0, Side: "buy", PricePPM: 600000, Qty: 10 * venue.Micro, USD: 6 * venue.Micro, Maker: bob, Taker: alice},
		{Venue: "hyperliquid", ID: "2", MarketID: "#10", TS: t0.Add(time.Second), Side: "sell", PricePPM: 700000, Qty: 20_000 * venue.Micro, USD: 14_000 * venue.Micro, Maker: alice, Taker: bob},
		{Venue: "kalshi", ID: "1", MarketID: "T", TS: t0, Side: "buy", PricePPM: 500000, Qty: 1, USD: 20_000 * venue.Micro},
	}
	if n, err := st.InsertTrades(ctx, ts); err != nil || n != 3 {
		t.Fatalf("inserted %d, %v", n, err)
	}
	if n, err := st.InsertTrades(ctx, ts); err != nil || n != 0 {
		t.Fatalf("replay inserted %d, %v", n, err)
	}
	big, err := st.LargeTrades(ctx, "hyperliquid", 10_000*venue.Micro, 10)
	if err != nil || len(big) != 1 || big[0].ID != "2" {
		t.Fatalf("large %+v, %v", big, err)
	}
	k, _ := st.LargeTrades(ctx, "kalshi", 0, 10)
	if len(k) != 1 || k[0].Maker != nil || k[0].Taker != nil {
		t.Fatalf("kalshi %+v", k)
	}
	w, err := st.WalletTrades(ctx, "hyperliquid", alice, 10)
	if err != nil || len(w) != 2 || w[0].ID != "2" {
		t.Fatalf("wallet %+v, %v", w, err)
	}
	var seen []string
	err = st.EachTrade(ctx, "hyperliquid", t0, func(tr venue.Trade) error {
		seen = append(seen, tr.ID)
		return nil
	})
	if err != nil || len(seen) != 2 || seen[0] != "1" {
		t.Fatalf("each %v, %v", seen, err)
	}
	if n, err := st.PruneTrades(ctx, "hyperliquid", t0.Add(time.Millisecond)); err != nil || n != 1 {
		t.Fatalf("pruned %d, %v", n, err)
	}
}

func TestBatchesPastTheChunk(t *testing.T) {
	st := pgtest.New(t)
	var ts []venue.Trade
	for i := range 2500 {
		ts = append(ts, venue.Trade{Venue: "kalshi", ID: string(rune('a'+i%26)) + time.Duration(i).String(), MarketID: "T", TS: t0, Side: "buy"})
	}
	n, err := st.InsertTrades(t.Context(), ts)
	if err != nil || n != 2500 {
		t.Fatalf("inserted %d, %v", n, err)
	}
}

func TestResultSurvivesAnOpenUpsert(t *testing.T) {
	st := pgtest.New(t)
	ctx := t.Context()
	open := venue.Market{Venue: "polymarket-us", ID: "A", Title: "Alpha", Close: t0, Status: "open"}
	if err := st.UpsertMarkets(ctx, []venue.Market{open, {Venue: "polymarket-us", ID: "B", Title: "Beta", Close: t0.Add(time.Hour), Status: "open"}}); err != nil {
		t.Fatal(err)
	}
	if err := st.SetResult(ctx, "polymarket-us", "A", "resolved", "yes"); err != nil {
		t.Fatal(err)
	}
	open.Title = "Alpha renamed"
	if err := st.UpsertMarkets(ctx, []venue.Market{open}); err != nil {
		t.Fatal(err)
	}
	m, _, _ := st.Market(ctx, "polymarket-us", "A")
	if m.Result != "yes" || m.Status != "resolved" || m.Title != "Alpha renamed" {
		t.Fatalf("%+v", m)
	}
	ids, err := st.ExpiredMarketIDs(ctx, "polymarket-us", t0.Add(2*time.Hour), 10)
	if err != nil || len(ids) != 1 || ids[0] != "B" {
		t.Fatalf("expired %v, %v", ids, err)
	}
	if err := st.MarkClosed(ctx, "polymarket-us", []string{"A", "B"}); err != nil {
		t.Fatal(err)
	}
	if a, _, _ := st.Market(ctx, "polymarket-us", "A"); a.Status != "resolved" {
		t.Fatalf("a closed over its result: %+v", a)
	}
	if b, _, _ := st.Market(ctx, "polymarket-us", "B"); b.Status != "closed" {
		t.Fatalf("b %+v", b)
	}
}

func TestLastTradeTS(t *testing.T) {
	st := pgtest.New(t)
	ctx := t.Context()
	if _, ok, err := st.LastTradeTS(ctx, "hyperliquid", "#10"); ok || err != nil {
		t.Fatalf("empty market gave %v, %v", ok, err)
	}
	ts := []venue.Trade{
		{Venue: "hyperliquid", ID: "1", MarketID: "#10", TS: t0, Side: "buy"},
		{Venue: "hyperliquid", ID: "2", MarketID: "#10", TS: t0.Add(time.Minute), Side: "buy"},
	}
	if _, err := st.InsertTrades(ctx, ts); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := st.LastTradeTS(ctx, "hyperliquid", "#10"); err != nil || !ok || !got.Equal(t0.Add(time.Minute)) {
		t.Fatalf("got %v, %v, %v", got, ok, err)
	}
}
