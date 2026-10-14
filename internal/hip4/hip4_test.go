package hip4_test

import (
	"context"
	"encoding/hex"
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"pm/internal/hip4"
	"pm/internal/httpx"
	"pm/internal/venue"
	vh "pm/internal/venue/hip4"
)

const fixtures = "../../testdata/venues/"

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

func recorded(t testing.TB) []venue.Trade {
	t.Helper()
	var out []venue.Trade
	for _, r := range load[[]vh.Trade](t, "hl_recenttrades.json") {
		v, err := vh.Convert(r)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	slices.SortFunc(out, func(a, b venue.Trade) int { return a.TS.Compare(b.TS) })
	return out
}

func byPrefix(t testing.TB, ws []hip4.Wallet, prefix string) hip4.Wallet {
	t.Helper()
	for _, w := range ws {
		if strings.HasPrefix(hex.EncodeToString(w.Address), prefix) {
			return w
		}
	}
	t.Fatalf("no wallet %s", prefix)
	return hip4.Wallet{}
}

func book(t testing.TB, result string, trades []venue.Trade) []hip4.Wallet {
	t.Helper()
	b, err := hip4.NewBook([]hip4.MarketInfo{{ID: "#111210", Kind: "sportsContestParticipant2", Result: result, SettledAt: time.UnixMilli(1792100000000)}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tr := range trades {
		if err := b.Add(tr); err != nil {
			t.Fatal(err)
		}
	}
	return b.Wallets()
}

// golden figures worked out by hand from the six recorded fills of one outcome
func TestBookMatchesTheHandWorkedFigures(t *testing.T) {
	cases := []struct {
		result string
		want   map[string]int64
	}{
		{"yes", map[string]int64{"257736": 198_363_000, "14c736": -9_483_000, "c36457": -214_890_000, "a97758": -5_835_600, "124059": 5_835_600, "273432": 26_010_000}},
		{"no", map[string]int64{"257736": -57_637_000, "14c736": -9_483_000, "c36457": 75_110_000, "a97758": 3_164_400, "124059": -3_164_400, "273432": -7_990_000}},
	}
	for _, c := range cases {
		ws := book(t, c.result, recorded(t))
		if len(ws) != len(c.want) {
			t.Fatalf("%d wallets", len(ws))
		}
		var sum int64
		for prefix, want := range c.want {
			w := byPrefix(t, ws, prefix)
			if !within(w.Gross, want) || w.Resolved != 1 || w.Markets != 1 {
				t.Fatalf("%s %s gross %d want %d, %+v", c.result, prefix, w.Gross, want, w)
			}
			sum += w.Gross
		}
		// a settled market moves money between traders and creates none
		if !within(sum, 0) {
			t.Fatalf("%s gross sums to %d", c.result, sum)
		}
	}
	ws := book(t, "yes", recorded(t))
	mover := byPrefix(t, ws, "14c736")
	if mover.Fills != 5 || mover.Won != 0 {
		t.Fatalf("%+v", mover)
	}
	// 145 + 145 + 145 + 34 + 111 shares at the traded prices
	if want := int64(145_000000*259/1000 + 145_000000*2176/10000 + 145_000000*259/1000 + 34_000000*235/1000 + 111_000000*235/1000); mover.Volume != want {
		t.Fatalf("volume %d want %d", mover.Volume, want)
	}
	if w := byPrefix(t, ws, "257736"); w.Won != 1 {
		t.Fatalf("%+v", w)
	}
}

func TestUnsettledMarketKeepsOnlyClosedProfit(t *testing.T) {
	ws := book(t, "", recorded(t))
	if w := byPrefix(t, ws, "14c736"); !within(w.Gross, -9_483_000) || w.Resolved != 0 {
		t.Fatalf("%+v", w)
	}
	if w := byPrefix(t, ws, "257736"); w.Gross != 0 || w.Resolved != 0 {
		t.Fatalf("an open position booked profit %+v", w)
	}
}

func TestBookRejectsOtherCoins(t *testing.T) {
	b, _ := hip4.NewBook(nil)
	tr := recorded(t)[0]
	tr.MarketID = "#111211"
	if err := b.Add(tr); err == nil {
		t.Fatal("accepted a side-1 coin")
	}
	if _, err := hip4.NewBook([]hip4.MarketInfo{{ID: "BTC"}}); err == nil {
		t.Fatal("accepted a perp as a market")
	}
}

// synthetic tapes of 31 settled outcomes, one wallet buying Yes at 0.60 a day before each settles
func tape(kind string, n int) (trades []venue.Trade, markets []hip4.MarketInfo) {
	buyer, seller := make([]byte, 20), make([]byte, 20)
	buyer[0], seller[0] = 1, 2
	settled := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	for i := range n {
		coin := "#" + strconv.Itoa(1000+10*i)
		markets = append(markets, hip4.MarketInfo{ID: coin, Kind: kind, Result: "yes", SettledAt: settled})
		trades = append(trades, venue.Trade{
			Venue: venue.Hyperliquid, ID: strconv.Itoa(i), MarketID: coin, TS: settled.Add(-24 * time.Hour).Add(time.Duration(i) * time.Second),
			Side: "buy", PricePPM: 600000, Qty: 10_000000, USD: 6_000000, Taker: buyer, Maker: seller,
		})
	}
	return trades, markets
}

func TestSkillIsRatedOnQuestionOutcomesOnly(t *testing.T) {
	trades, markets := tape("sportsContestWinner", 31)
	b, err := hip4.NewBook(markets)
	if err != nil {
		t.Fatal(err)
	}
	for _, tr := range trades {
		if err := b.Add(tr); err != nil {
			t.Fatal(err)
		}
	}
	ws := b.Wallets()
	buyer, seller := byPrefix(t, ws, "01"), byPrefix(t, ws, "02")
	// the buyer forecast 0.60 for an outcome that happened, so each miss is 0.40
	if !buyer.Rated || !near(buyer.Brier, 0.16) || !near(buyer.Edge, 0.40) || buyer.Gross != 31*4_000000 {
		t.Fatalf("%+v", buyer)
	}
	// the seller backed No at 0.40 and was wrong each time
	if !seller.Rated || !near(seller.Brier, 0.16) || !near(seller.Edge, -0.40) || seller.Gross != -31*4_000000 {
		t.Fatalf("%+v", seller)
	}

	trades, markets = tape("binaryPrice", 31)
	b, _ = hip4.NewBook(markets)
	for _, tr := range trades {
		_ = b.Add(tr)
	}
	if w := byPrefix(t, b.Wallets(), "01"); w.Rated || w.Resolved != 31 {
		t.Fatalf("a recurring price outcome was rated %+v", w)
	}

	trades, markets = tape("sportsContestWinner", 29)
	b, _ = hip4.NewBook(markets)
	for _, tr := range trades {
		_ = b.Add(tr)
	}
	if w := byPrefix(t, b.Wallets(), "01"); w.Rated {
		t.Fatalf("29 outcomes cleared a floor of 30 %+v", w)
	}
}

// within allows the ledger's whole-ppm average price, which moves a position by under a micro-USDC a share
func within(got, want int64) bool { return got-want < 500 && want-got < 500 }

func near(a, b float64) bool { return a-b < 1e-9 && b-a < 1e-9 }

func TestSettlementFromRecordedFills(t *testing.T) {
	fills := load[[]hip4.Fill](t, "hl_userfills.json")
	// the No coin settled at 1.0, so Yes paid nothing
	yes, at, ok := hip4.Settlement(fills, "#120950")
	if !ok || yes != 0 || at.UnixMilli() != 1792011607128 {
		t.Fatalf("%d %v %v", yes, at, ok)
	}
	yes, at, ok = hip4.Settlement(fills, "#120940")
	if !ok || yes != 0 || at.UnixMilli() != 1792011606995 {
		t.Fatalf("%d %v %v", yes, at, ok)
	}
	if _, _, ok := hip4.Settlement(fills, "#114670"); ok {
		t.Fatal("a plain buy was read as a settlement")
	}
	if _, _, ok := hip4.Settlement(fills, "#999990"); ok {
		t.Fatal("an unrelated coin settled")
	}
	won := []hip4.Fill{{Coin: "#50", Px: "1.0", Dir: "Settlement", Time: 5}, {Coin: "#51", Px: "0.0", Dir: "Settlement", Time: 5}}
	if yes, _, _ := hip4.Settlement(won, "#50"); yes != venue.PPM {
		t.Fatalf("%d", yes)
	}
	if yes, _, _ := hip4.Settlement(won[1:], "#50"); yes != venue.PPM {
		t.Fatalf("complement %d", yes)
	}
	for ppm, want := range map[int32]string{venue.PPM: "yes", 0: "no", 350000: "0.35"} {
		got := hip4.Result(ppm)
		y, n, ok := hip4.Payout(got)
		if got != want || !ok || y != int64(ppm) || y+n != venue.PPM {
			t.Fatalf("%d gives %q, %d %d %v", ppm, got, y, n, ok)
		}
	}
	if _, _, ok := hip4.Payout(""); ok {
		t.Fatal("an empty result is a payout")
	}
}

type settleStore struct {
	mu      sync.Mutex
	closed  []string
	last    time.Time
	traders [][]byte
	settled map[string]string
	at      map[string]time.Time
}

func (s *settleStore) UnsettledClosed(_ context.Context, _ string, limit int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, id := range s.closed {
		if _, done := s.settled[id]; !done && len(out) < limit {
			out = append(out, id)
		}
	}
	return out, nil
}

func (s *settleStore) LastTradeTS(context.Context, string, string) (time.Time, bool, error) {
	return s.last, true, nil
}

func (s *settleStore) LastTraders(_ context.Context, _, _ string, limit int) ([][]byte, error) {
	return s.traders[:min(limit, len(s.traders))], nil
}

func (s *settleStore) SettleMarket(_ context.Context, _, id, result string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settled[id], s.at[id] = result, at
	return nil
}

// info answers userFillsByTime with the recorded fills for the first wallet that holds them
type info struct {
	mu     sync.Mutex
	bodies []map[string]any
	fills  map[string][]hip4.Fill
	fail   bool
}

func (i *info) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	i.mu.Lock()
	defer i.mu.Unlock()
	var body map[string]any
	if err := json.UnmarshalRead(r.Body, &body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	i.bodies = append(i.bodies, body)
	if i.fail {
		http.Error(w, "boom", http.StatusInternalServerError)
		return
	}
	fills := i.fills[body["user"].(string)]
	if fills == nil {
		fills = []hip4.Fill{}
	}
	_ = json.MarshalWrite(w, fills)
}

func TestSettlementsAskRecentTradersAndRecordTheResult(t *testing.T) {
	w1, w2 := make([]byte, 20), make([]byte, 20)
	w1[0], w2[0] = 1, 2
	last := time.UnixMilli(1792011000000)
	st := &settleStore{closed: []string{"#120940", "#777770"}, last: last, traders: [][]byte{w1, w2}, settled: map[string]string{}, at: map[string]time.Time{}}
	api := &info{fills: map[string][]hip4.Fill{"0x" + hex.EncodeToString(w2): load[[]hip4.Fill](t, "hl_userfills.json")}}
	srv := httptest.NewServer(api)
	defer srv.Close()
	m := hip4.NewSettleMetrics(prometheus.NewRegistry())
	now := time.Date(2026, 10, 14, 22, 0, 0, 0, time.UTC)
	s := &hip4.Settlements{
		HTTP: &httpx.Client{HTTP: srv.Client(), UserAgent: "test"}, InfoURL: srv.URL, Store: st, Batch: 5, Metrics: m,
		Now: func() time.Time { return now },
	}
	if err := s.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if st.settled["#120940"] != "no" || st.at["#120940"].UnixMilli() != 1792011606995 {
		t.Fatalf("%v %v", st.settled, st.at)
	}
	if _, ok := st.settled["#777770"]; ok {
		t.Fatal("a market without a settlement fill was settled")
	}
	first := api.bodies[0]
	if first["type"] != "userFillsByTime" || first["user"] != "0x"+hex.EncodeToString(w1) || first["startTime"].(float64) != float64(last.UnixMilli()) || first["aggregateByTime"] != false {
		t.Fatalf("%v", first)
	}
	if n := testutil.ToFloat64(m.Settled()); n != 1 {
		t.Fatalf("settled %v", n)
	}
	// the unsettled market is not asked again within the hour, and is asked again after it
	calls := len(api.bodies)
	if err := s.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(api.bodies) != calls {
		t.Fatalf("asked again at once, %d calls", len(api.bodies)-calls)
	}
	now = now.Add(2 * time.Hour)
	if err := s.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(api.bodies) == calls {
		t.Fatal("never asked again")
	}
}

func TestSettlementLookupFailuresAreCounted(t *testing.T) {
	st := &settleStore{closed: []string{"#120940"}, last: time.UnixMilli(1792011000000), traders: [][]byte{make([]byte, 20)}, settled: map[string]string{}, at: map[string]time.Time{}}
	api := &info{fail: true}
	srv := httptest.NewServer(api)
	defer srv.Close()
	m := hip4.NewSettleMetrics(prometheus.NewRegistry())
	s := &hip4.Settlements{HTTP: &httpx.Client{HTTP: srv.Client(), UserAgent: "test"}, InfoURL: srv.URL, Store: st, Batch: 5, Metrics: m}
	if err := s.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n := testutil.ToFloat64(m.Failures()); n != 1 || len(st.settled) != 0 {
		t.Fatalf("failures %v, settled %v", n, st.settled)
	}
}

type walletStore struct {
	trades  []venue.Trade
	markets []hip4.MarketInfo
	out     []hip4.Wallet
}

func (w *walletStore) EachTrade(_ context.Context, _ string, _ time.Time, fn func(venue.Trade) error) error {
	for _, t := range w.trades {
		if err := fn(t); err != nil {
			return err
		}
	}
	return nil
}

func (w *walletStore) MarketResults(context.Context, string) ([]hip4.MarketInfo, error) {
	return w.markets, nil
}

func (w *walletStore) UpsertHLWallets(_ context.Context, ws []hip4.Wallet) error {
	w.out = ws
	return nil
}

func TestWalletJobWritesEveryWallet(t *testing.T) {
	ws := &walletStore{
		trades:  append(recorded(t), venue.Trade{ID: "x", MarketID: "#111211", TS: time.UnixMilli(1792009653700)}),
		markets: []hip4.MarketInfo{{ID: "#111210", Kind: "sportsContestParticipant2", Result: "yes", SettledAt: time.UnixMilli(1792100000000)}},
	}
	job := &hip4.Wallets{Store: ws}
	if job.Name() != "hyperliquid/wallets" {
		t.Fatal(job.Name())
	}
	if err := job.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(ws.out) != 6 || !within(byPrefix(t, ws.out, "257736").Gross, 198_363_000) {
		t.Fatalf("%d wallets", len(ws.out))
	}
}
