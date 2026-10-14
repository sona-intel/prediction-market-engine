package pgstore_test

import (
	"testing"
	"time"

	"pm/internal/hip4"
	"pm/internal/pgstore/pgtest"
	"pm/internal/venue"
)

func TestHIP4SettlementReadsAndWrites(t *testing.T) {
	st := pgtest.New(t)
	ctx := t.Context()
	hl := venue.Hyperliquid
	if err := st.UpsertMarkets(ctx, []venue.Market{
		{Venue: hl, ID: "#10", Title: "old", Kind: "sportsContestWinner", Close: t0, Status: "open"},
		{Venue: hl, ID: "#20", Title: "new", Kind: "binaryPrice", Close: t0.Add(time.Hour), Status: "open"},
		{Venue: hl, ID: "#30", Title: "live", Kind: "binaryPrice", Status: "open"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkClosed(ctx, hl, []string{"#10", "#20"}); err != nil {
		t.Fatal(err)
	}
	ids, err := st.UnsettledClosed(ctx, hl, 10)
	if err != nil || len(ids) != 2 || ids[0] != "#20" || ids[1] != "#10" {
		t.Fatalf("%v, %v", ids, err)
	}
	a, b, c := []byte("wallet-a-20-bytes---"), []byte("wallet-b-20-bytes---"), []byte("wallet-c-20-bytes---")
	if _, err := st.InsertTrades(ctx, []venue.Trade{
		{Venue: hl, ID: "1", MarketID: "#10", TS: t0, Side: "buy", PricePPM: 500000, Qty: 1, USD: 1, Taker: a, Maker: b},
		{Venue: hl, ID: "2", MarketID: "#10", TS: t0.Add(time.Minute), Side: "sell", PricePPM: 500000, Qty: 1, USD: 1, Taker: c, Maker: a},
	}); err != nil {
		t.Fatal(err)
	}
	w, err := st.LastTraders(ctx, hl, "#10", 2)
	if err != nil || len(w) != 2 || string(w[0]) != string(a) && string(w[0]) != string(c) {
		t.Fatalf("%q, %v", w, err)
	}
	if w, _ := st.LastTraders(ctx, hl, "#10", 1); len(w) != 1 {
		t.Fatalf("limit ignored %q", w)
	}
	at := t0.Add(2 * time.Hour)
	if err := st.SettleMarket(ctx, hl, "#10", "yes", at); err != nil {
		t.Fatal(err)
	}
	if ids, _ := st.UnsettledClosed(ctx, hl, 10); len(ids) != 1 || ids[0] != "#20" {
		t.Fatalf("%v", ids)
	}
	// a later sweep of the live list must not undo the settlement
	if err := st.UpsertMarkets(ctx, []venue.Market{{Venue: hl, ID: "#10", Title: "old", Kind: "sportsContestWinner", Close: t0, Status: "open"}}); err != nil {
		t.Fatal(err)
	}
	ms, err := st.MarketResults(ctx, hl)
	if err != nil || len(ms) != 3 || ms[0].ID != "#10" || ms[0].Result != "yes" || !ms[0].SettledAt.Equal(at) || ms[0].Kind != "sportsContestWinner" || !ms[1].SettledAt.IsZero() {
		t.Fatalf("%+v, %v", ms, err)
	}
}

func TestHIP4WalletJobOnPostgres(t *testing.T) {
	st := pgtest.New(t)
	ctx := t.Context()
	hl := venue.Hyperliquid
	if err := st.UpsertMarkets(ctx, []venue.Market{{Venue: hl, ID: "#110", Title: "m", Kind: "sportsContestWinner", Status: "open"}}); err != nil {
		t.Fatal(err)
	}
	a, b := []byte("wallet-a-20-bytes---"), []byte("wallet-b-20-bytes---")
	if _, err := st.InsertTrades(ctx, []venue.Trade{
		{Venue: hl, ID: "1", MarketID: "#110", TS: t0, Side: "buy", PricePPM: 400000, Qty: 10_000000, USD: 4_000000, Taker: a, Maker: b},
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.SettleMarket(ctx, hl, "#110", "yes", t0.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	job := &hip4.Wallets{Store: st}
	for range 2 {
		if err := job.Poll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := st.Pool.Query(ctx, `select address, fills, volume_micro, gross_pnl, markets, resolved, won, rated, brier from hl_wallet order by address`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type row struct {
		addr                   []byte
		fills, vol, gross      int64
		markets, resolved, won int
		rated                  bool
		brier                  *float64
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.addr, &r.fills, &r.vol, &r.gross, &r.markets, &r.resolved, &r.won, &r.rated, &r.brier); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if len(got) != 2 {
		t.Fatalf("%d wallets", len(got))
	}
	if x := got[0]; string(x.addr) != string(a) || x.gross != 6_000000 || x.won != 1 || x.resolved != 1 || x.vol != 4_000000 || x.rated || x.brier != nil {
		t.Fatalf("%+v", x)
	}
	if y := got[1]; y.gross != -6_000000 || y.won != 0 || y.fills != 1 {
		t.Fatalf("%+v", y)
	}
}
