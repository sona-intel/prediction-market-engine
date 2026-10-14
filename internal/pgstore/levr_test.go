package pgstore_test

import (
	"testing"
	"time"

	"pm/internal/levr"
	"pm/internal/pgstore/pgtest"
)

func TestLevrPositionsCloseOnceAndSurviveReplay(t *testing.T) {
	st := pgtest.New(t)
	ctx := t.Context()
	a, b := []byte("wallet-a-20-bytes---"), []byte("wallet-b-20-bytes---")
	ps := []levr.Position{
		{ID: "p1", Wallet: a, MarketID: "m1", Opened: t0, Stake: 5_000_000, Size: 10_000_000, Fee: 100_000, EntryPPM: 500000, LiqPPM: 400000, Side: 1, Status: levr.Open},
		{ID: "p2", Wallet: a, MarketID: "m1", Opened: t0.Add(time.Minute), Stake: 2_000_000, Size: 2_000_000, EntryPPM: 300000, Side: 2, Status: levr.Open},
		{ID: "p3", Wallet: b, MarketID: "m2", Opened: t0, Stake: 1_000_000, Size: 5_000_000, EntryPPM: 200000, LiqPPM: 150000, Status: levr.Open},
	}
	if n, err := st.UpsertPositions(ctx, ps); err != nil || n != 3 {
		t.Fatalf("inserted %d, %v", n, err)
	}
	pay := int64(9_000_000)
	closes := []levr.Close{{ID: "p1", Status: levr.Won, At: t0.Add(time.Hour), Payout: &pay}, {ID: "p3", Status: levr.Void, At: t0.Add(time.Hour)}}
	if n, err := st.ClosePositions(ctx, closes); err != nil || n != 2 {
		t.Fatalf("closed %d, %v", n, err)
	}
	// a replay of the opened page must not reopen anything, and a second close must change nothing
	if n, err := st.UpsertPositions(ctx, ps); err != nil || n != 0 {
		t.Fatalf("replay inserted %d, %v", n, err)
	}
	lost := levr.Liquidation("p1", t0.Add(2*time.Hour))
	if n, err := st.ClosePositions(ctx, []levr.Close{lost}); err != nil || n != 0 {
		t.Fatalf("closed a closed position %d, %v", n, err)
	}
	if n, err := st.LoseOpen(ctx, "m1", 1, t0.Add(3*time.Hour)); err != nil || n != 1 {
		t.Fatalf("lost %d, %v", n, err)
	}
	got, err := st.Positions(ctx, []string{"p1", "p2", "p3", "missing"})
	if err != nil || len(got) != 3 {
		t.Fatalf("%v, %v", got, err)
	}
	if p := got["p1"]; p.Status != levr.Won || p.Payout == nil || *p.Payout != pay || p.Fee != 100_000 || p.LiqPPM != 400000 || !p.Closed.Equal(t0.Add(time.Hour)) {
		t.Fatalf("%+v", p)
	}
	if p := got["p2"]; p.Status != levr.Lost || p.Side != 2 || p.Payout == nil || *p.Payout != 0 || p.LiqPPM != 0 || !p.Closed.Equal(t0.Add(3*time.Hour)) {
		t.Fatalf("%+v", p)
	}
	if p := got["p3"]; p.Status != levr.Void || p.Payout != nil {
		t.Fatalf("%+v", p)
	}
}

func TestLevrWalletJob(t *testing.T) {
	st := pgtest.New(t)
	ctx := t.Context()
	a, b := []byte("wallet-a-20-bytes---"), []byte("wallet-b-20-bytes---")
	if _, err := st.UpsertPositions(ctx, []levr.Position{
		{ID: "p1", Wallet: a, MarketID: "m1", Opened: t0, Stake: 5_000_000, Size: 10_000_000, Status: levr.Open},
		{ID: "p2", Wallet: a, MarketID: "m1", Opened: t0.Add(time.Minute), Stake: 2_000_000, Size: 8_000_000, Status: levr.Open},
		{ID: "p3", Wallet: b, MarketID: "m2", Opened: t0, Stake: 1_000_000, Size: 5_000_000, Status: levr.Open},
	}); err != nil {
		t.Fatal(err)
	}
	pay := int64(8_000_000)
	if _, err := st.ClosePositions(ctx, []levr.Close{{ID: "p1", Status: levr.Won, At: t0, Payout: &pay}, levr.Liquidation("p2", t0)}); err != nil {
		t.Fatal(err)
	}
	job := &levr.Wallets{Store: st}
	for range 2 {
		if err := job.Poll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	var positions, liquidations, settled, won int
	var stake, size, pnl int64
	err := st.Pool.QueryRow(ctx, `select positions, liquidations, settled, won, stake_micro, size_micro, realized_pnl from levr_wallet where address = $1`, a).
		Scan(&positions, &liquidations, &settled, &won, &stake, &size, &pnl)
	if err != nil {
		t.Fatal(err)
	}
	if positions != 2 || liquidations != 1 || settled != 2 || won != 1 || stake != 7_000_000 || size != 18_000_000 || pnl != 3_000_000-2_000_000 {
		t.Fatalf("%d %d %d %d %d %d %d", positions, liquidations, settled, won, stake, size, pnl)
	}
	var wallets int
	if err := st.Pool.QueryRow(ctx, `select count(*) from levr_wallet`).Scan(&wallets); err != nil || wallets != 2 {
		t.Fatalf("%d wallets, %v", wallets, err)
	}
}
