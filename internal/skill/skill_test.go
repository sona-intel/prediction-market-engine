package skill_test

import (
	"database/sql"
	"math"
	"slices"
	"testing"

	"pm/internal/dataset"
	"pm/internal/golden"
	"pm/internal/pnl"
	"pm/internal/skill"
)

func near(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-8 {
		t.Errorf("%s = %.10f, want %.10f", name, got, want)
	}
}

func TestPriceAtOrdering(t *testing.T) {
	fills := []skill.Fill{
		{TS: 80000, Price: 400000},
		{TS: 85600, Block: 10, Log: 2, Price: 600000},
		{TS: 85600, Block: 10, Log: 7, Price: 650000},
		{TS: 85601, Price: 900000},
	}
	if p, ok := skill.PriceAt(fills, 100000, 4); !ok || p != 650000 {
		t.Fatalf("got %d, %v, want 650000, true", p, ok)
	}
	if p, ok := skill.PriceAt(fills[3:], 100000, 4); ok || p != 0 {
		t.Fatalf("no fill before the cutoff gave %d, %v, want 0, false", p, ok)
	}
	if p, ok := skill.PriceAt(fills, 100000, 168); ok || p != 0 {
		t.Fatalf("cutoff before the first fill gave %d, %v, want 0, false", p, ok)
	}
	if p, ok := skill.PriceAt(fills, 3600, 4); ok || p != 0 {
		t.Fatalf("horizon longer than t gave %d, %v, want 0, false", p, ok)
	}
	if _, ok := skill.PriceAt(nil, 100000, 4); ok {
		t.Fatal("no fills gave ok")
	}
}

func TestForecastStances(t *testing.T) {
	const at = 1000000
	payout := [2]int64{1000000, 0}
	cases := []struct {
		name  string
		fills []skill.Fill
		f, o  int64
		ok    bool
	}{
		{"sell outcome 0", []skill.Fill{{TS: 900000, Buy: false, Outcome: 0, Price: 700000}}, 300000, 0, true},
		{"buy outcome 1", []skill.Fill{{TS: 900000, Buy: true, Outcome: 1, Price: 300000}}, 300000, 0, true},
		{"buy outcome 0", []skill.Fill{{TS: 900000, Buy: true, Outcome: 0, Price: 400000}}, 400000, 1000000, true},
		{"sell outcome 1", []skill.Fill{{TS: 900000, Buy: false, Outcome: 1, Price: 800000}}, 200000, 1000000, true},
		{"fill at the cutoff counts", []skill.Fill{{TS: 956800, Buy: true, Outcome: 0, Price: 400000}}, 400000, 1000000, true},
		{"fill one second late does not", []skill.Fill{{TS: 956801, Buy: true, Outcome: 0, Price: 400000}}, 0, 0, false},
		{"last fill before the cutoff wins", []skill.Fill{
			{TS: 900000, Buy: true, Outcome: 0, Price: 400000},
			{TS: 956800, Block: 5, Log: 1, Buy: false, Outcome: 0, Price: 700000},
			{TS: 956801, Buy: true, Outcome: 0, Price: 100000},
		}, 300000, 0, true},
		{"same second keeps the later one", []skill.Fill{
			{TS: 956800, Block: 5, Log: 1, Buy: true, Outcome: 0, Price: 400000},
			{TS: 956800, Block: 5, Log: 2, Buy: true, Outcome: 1, Price: 250000},
		}, 250000, 0, true},
		{"no fills", nil, 0, 0, false},
	}
	for _, c := range cases {
		f, o, ok := skill.Forecast(c.fills, at, payout)
		if f != c.f || o != c.o || ok != c.ok {
			t.Errorf("%s gave %d, %d, %v, want %d, %d, %v", c.name, f, o, ok, c.f, c.o, c.ok)
		}
	}
	if _, _, ok := skill.Forecast([]skill.Fill{{TS: 0, Buy: true, Price: 400000}}, skill.Horizon-1, payout); ok {
		t.Error("t shorter than the horizon gave ok")
	}
}

func TestForecastFiftyFifty(t *testing.T) {
	fills := []skill.Fill{{TS: 900000, Buy: true, Outcome: 0, Price: 600000}}
	f, o, ok := skill.Forecast(fills, 1000000, [2]int64{500000, 500000})
	if !ok || f != 600000 || o != 500000 {
		t.Fatalf("got %d, %d, %v, want 600000, 500000, true", f, o, ok)
	}
}

func TestForecastClampsPrice(t *testing.T) {
	fills := []skill.Fill{{TS: 900000, Buy: false, Outcome: 0, Price: 1200000}}
	if f, _, _ := skill.Forecast(fills, 1000000, [2]int64{1000000, 0}); f != 0 {
		t.Fatalf("sell above 1 gave f %d, want 0", f)
	}
	fills[0].Buy = true
	if f, _, _ := skill.Forecast(fills, 1000000, [2]int64{1000000, 0}); f != 1000000 {
		t.Fatalf("buy above 1 gave f %d, want 1000000", f)
	}
}

func fourTerms() skill.Acc {
	var a skill.Acc
	a.Add(300000, 0)
	a.Add(800000, 1000000)
	a.Add(500000, 500000)
	a.Add(400000, 500000)
	return a
}

func TestAcc(t *testing.T) {
	a := fourTerms()
	near(t, "Edge", a.Edge(), 0)
	near(t, "Brier", a.Brier(), 0.035)
	se, ok := a.EdgeSE()
	if !ok {
		t.Fatal("EdgeSE not ok")
	}
	near(t, "EdgeSE", se, 0.10801234)
	near(t, "EdgeLB", a.EdgeLB(), -0.21170420)

	var one skill.Acc
	one.Add(300000, 0)
	if _, ok := one.EdgeSE(); ok {
		t.Error("EdgeSE ok with a single term")
	}
}

func TestAccEmpty(t *testing.T) {
	var a skill.Acc
	if a.Brier() != 0 || a.Edge() != 0 || a.EdgeLB() != 0 {
		t.Fatalf("empty Acc gave Brier %v, Edge %v, EdgeLB %v", a.Brier(), a.Edge(), a.EdgeLB())
	}
	if _, ok := a.EdgeSE(); ok {
		t.Fatal("EdgeSE ok on empty Acc")
	}
}

func TestEdgeSEIdenticalTermsIsZero(t *testing.T) {
	var a skill.Acc
	for range 100000 {
		a.Add(1, 1000000)
	}
	se, ok := a.EdgeSE()
	if !ok || se != 0 {
		t.Fatalf("got %v, %v, want 0, true", se, ok)
	}
}

func TestAccMerge(t *testing.T) {
	var a, b skill.Acc
	a.Add(300000, 0)
	a.Add(800000, 1000000)
	b.Add(500000, 500000)
	b.Add(400000, 500000)
	a.Merge(b)
	if want := fourTerms(); a != want {
		t.Fatalf("got %+v, want %+v", a, want)
	}
}

func TestAccCalibrationBuckets(t *testing.T) {
	var a skill.Acc
	a.Add(100000, 0)
	a.Add(950000, 1000000)
	if c := a.Cal[0]; c.N != 1 || c.SumF != 100000 || c.SumO != 0 {
		t.Errorf("Cal[0] = %+v", c)
	}
	if c := a.Cal[4]; c.N != 1 || c.SumF != 950000 || c.SumO != 1000000 {
		t.Errorf("Cal[4] = %+v", c)
	}
	if a.Cal[1].N+a.Cal[2].N+a.Cal[3].N != 0 {
		t.Errorf("middle buckets hold rows: %+v", a.Cal)
	}
}

func TestRatedFloor(t *testing.T) {
	for _, c := range []struct {
		markets, events uint32
		want            bool
	}{{30, 3, true}, {30, 2, false}, {29, 3, false}, {31, 4, true}, {0, 0, false}} {
		if got := skill.Rated(c.markets, c.events); got != c.want {
			t.Errorf("Rated(%d, %d) = %v, want %v", c.markets, c.events, got, c.want)
		}
	}
}

func TestBuckets(t *testing.T) {
	for _, c := range []struct{ in, want int64 }{{0, 0}, {199999, 0}, {200000, 1}, {999999, 4}, {1000000, 4}, {-5, 0}, {1500000, 4}} {
		if got := skill.Bucket5(c.in); int64(got) != c.want {
			t.Errorf("Bucket5(%d) = %d, want %d", c.in, got, c.want)
		}
	}
	for _, c := range []struct{ in, want int64 }{{0, 0}, {49999, 0}, {50000, 1}, {999999, 19}, {1000000, 19}, {-5, 0}, {1500000, 19}} {
		if got := skill.Bucket20(c.in); int64(got) != c.want {
			t.Errorf("Bucket20(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestWilson(t *testing.T) {
	for _, c := range []struct {
		k, n int
		want float64
	}{{4, 11, 0.1517}, {15, 30, 0.3315}} {
		if got := math.Round(skill.Wilson(c.k, c.n)*1e4) / 1e4; got != c.want {
			t.Errorf("Wilson(%d, %d) = %v, want %v", c.k, c.n, got, c.want)
		}
	}
	if got := skill.Wilson(0, 10); got != 0 {
		t.Errorf("Wilson(0, 10) = %v, want 0", got)
	}
	if got := skill.Wilson(0, 0); got != 0 {
		t.Errorf("Wilson(0, 0) = %v, want 0", got)
	}
	if got := skill.Wilson(30, 30); got <= 0.88 || got >= 1 {
		t.Errorf("Wilson(30, 30) = %v, want between 0.88 and 1", got)
	}
}

func token0Fills(t *testing.T, db *sql.DB, from, token string) []skill.Fill {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `
		SELECT CAST(timestamp AS UINTEGER) AS ts, CAST(split_part(id, '_', 2) AS UINTEGER) AS b, CAST(split_part(id, '_', 3) AS UINTEGER) AS l,
			CAST(CASE WHEN makerAssetId = '0' THEN makerAmountFilled ELSE takerAmountFilled END AS UBIGINT),
			CAST(CASE WHEN makerAssetId = '0' THEN takerAmountFilled ELSE makerAmountFilled END AS UBIGINT)
		FROM `+from+` WHERE ? IN (makerAssetId, takerAssetId) ORDER BY ts, b, l`, token)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var fills []skill.Fill
	for rows.Next() {
		var f skill.Fill
		var usdc, shares uint64
		if err := rows.Scan(&f.TS, &f.Block, &f.Log, &usdc, &shares); err != nil {
			t.Fatal(err)
		}
		f.Price = pnl.PricePPM(usdc, shares)
		fills = append(fills, f)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return fills
}

func TestPriceAtMatchesPolymarket(t *testing.T) {
	db, err := dataset.Open(t.Context(), dataset.Config{ExtDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	src, err := dataset.NewSource(t.Context(), db, golden.Path("dataset"))
	if err != nil {
		t.Fatal(err)
	}
	files := src.Files("order_filled")
	if len(files) == 0 {
		t.Fatalf("no fixture order_filled files under %s", golden.Path("dataset"))
	}
	from := src.From(files, "id", "timestamp", "makerAssetId", "takerAssetId", "makerAmountFilled", "takerAmountFilled")
	markets, err := golden.LoadMarkets("prices_12")
	if err != nil {
		t.Fatal(err)
	}
	hours := map[string]int{"4h": 4, "12h": 12, "1d": 24, "1w": 168}
	var diffs []float64
	for _, m := range markets {
		fills := token0Fills(t, db, from, m.Token0)
		for key, want := range m.Prices {
			h, known := hours[key]
			if !known {
				t.Fatalf("unknown horizon %q", key)
			}
			if want == nil {
				continue
			}
			p, ok := skill.PriceAt(fills, m.ClosedTime, h)
			if !ok {
				t.Errorf("%s %s has no fill among %d", m.Condition, key, len(fills))
				continue
			}
			diffs = append(diffs, math.Abs(float64(p)/1e6-*want))
		}
	}
	if len(diffs) != 48 {
		t.Fatalf("compared %d prices, want 48", len(diffs))
	}
	slices.Sort(diffs)
	median := (diffs[len(diffs)/2-1] + diffs[len(diffs)/2]) / 2
	largest := diffs[len(diffs)-1]
	t.Logf("median %.6f, maximum %.6f over %d prices", median, largest, len(diffs))
	if median > 0.00055 || largest > 0.0155 {
		t.Fatalf("median %.6f (limit 0.00055), maximum %.6f (limit 0.0155)", median, largest)
	}
}
