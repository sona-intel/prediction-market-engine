package hip4_test

import (
	"encoding/json/v2"
	"math"
	"os"
	"testing"
	"time"

	"pm/internal/venue"
	"pm/internal/venue/hip4"
)

const fixtures = "../../../testdata/venues/"

func load[T any](t testing.TB, name string) T {
	t.Helper()
	b, err := os.ReadFile(fixtures + name)
	if err != nil {
		t.Fatal(err)
	}
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestConvertRecordedTrades(t *testing.T) {
	rows := load[[]hip4.Trade](t, "hl_recenttrades.json")
	first, err := hip4.Convert(rows[0])
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != "1112983006140902" || first.MarketID != "#111210" || first.Side != "buy" || first.PricePPM != 235000 {
		t.Fatalf("%+v", first)
	}
	if string(first.Taker) == string(first.Maker) || len(first.Taker) != 20 || len(first.Maker) != 20 {
		t.Fatalf("wallets %x %x", first.Taker, first.Maker)
	}
	sell, err := hip4.Convert(rows[3])
	if err != nil || sell.Side != "sell" {
		t.Fatalf("%+v %v", sell, err)
	}
	for _, r := range rows {
		got, err := hip4.Convert(r)
		if err != nil {
			t.Fatal(err)
		}
		sz, px := mustFloat(t, r.Sz), mustFloat(t, r.Px)
		if got.Qty != int64(math.Round(sz*venue.Micro)) || math.Abs(float64(got.USD)-sz*px*venue.Micro) > 1 {
			t.Fatalf("amounts %+v from %+v", got, r)
		}
	}
}

func mustFloat(t testing.TB, s string) float64 {
	t.Helper()
	var f float64
	if err := json.Unmarshal([]byte(s), &f); err != nil {
		t.Fatal(err)
	}
	return f
}

// the two sides of an outcome are one fill seen twice, with the price mirrored and the wallets swapped
func TestSidesCarryTheSameFills(t *testing.T) {
	side0 := load[[]hip4.Trade](t, "hl_recenttrades.json")
	side1 := load[[]hip4.Trade](t, "hl_recenttrades_side1.json")
	byHash := map[string]hip4.Trade{}
	for _, r := range side1 {
		byHash[r.Hash] = r
	}
	for _, a := range side0 {
		b, ok := byHash[a.Hash]
		if !ok {
			t.Fatalf("no mirror for %s", a.Hash)
		}
		if a.Time != b.Time || a.Sz != b.Sz || a.Side == b.Side || a.Users[0] != b.Users[1] || a.Users[1] != b.Users[0] {
			t.Fatalf("not mirrored %+v %+v", a, b)
		}
		if math.Abs(mustFloat(t, a.Px)+mustFloat(t, b.Px)-1) > 1e-4 {
			t.Fatalf("prices %s %s", a.Px, b.Px)
		}
	}
}

func TestConvertSkipsUnusableRows(t *testing.T) {
	good := load[[]hip4.Trade](t, "hl_recenttrades.json")[0]
	for name, mod := range map[string]func(*hip4.Trade){
		"price above one": func(r *hip4.Trade) { r.Px = "1.5" },
		"negative price":  func(r *hip4.Trade) { r.Px = "-0.1" },
		"price not a num": func(r *hip4.Trade) { r.Px = "NaN" },
		"zero size":       func(r *hip4.Trade) { r.Sz = "0" },
		"huge size":       func(r *hip4.Trade) { r.Sz = "1e30" },
		"one user":        func(r *hip4.Trade) { r.Users = r.Users[:1] },
		"short wallet":    func(r *hip4.Trade) { r.Users = []string{"0x12", r.Users[1]} },
		"unknown side":    func(r *hip4.Trade) { r.Side = "X" },
	} {
		r := good
		r.Users = append([]string(nil), good.Users...)
		mod(&r)
		if _, err := hip4.Convert(r); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestMarketFromOutcome(t *testing.T) {
	meta := load[hip4.Meta](t, "hl_outcomemeta.json")
	got := make([]venue.Market, len(meta.Outcomes))
	for i, o := range meta.Outcomes {
		got[i] = o.Market()
	}
	if got[0].ID != "#111210" || got[0].Title != "FC Shakhtar Donetsk" || got[0].Kind != "sportsContestParticipant2" || got[0].Status != "open" || got[0].Venue != venue.Hyperliquid {
		t.Fatalf("%+v", got[0])
	}
	if want := time.Date(2026, 10, 16, 13, 35, 0, 0, time.UTC); !got[2].Close.Equal(want) || got[2].Title != "xyz:TSM 1-minute candle 500" {
		t.Fatalf("%+v", got[2])
	}
}
