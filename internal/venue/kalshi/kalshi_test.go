package kalshi_test

import (
	"context"
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"pm/internal/httpx"
	"pm/internal/venue"
	"pm/internal/venue/kalshi"
	"pm/internal/venue/venuetest"
)

const fixtures = "../../../testdata/venues/"

var at = time.Date(2026, 10, 14, 21, 40, 0, 0, time.UTC)

type markets struct {
	Markets []kalshi.Market `json:"markets"`
}

type tape struct {
	Trades []kalshi.Trade `json:"trades"`
}

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

func TestMarketConversion(t *testing.T) {
	ms := load[markets](t, "kalshi_markets.json").Markets
	busy := ms[0].Venue()
	if busy.Venue != venue.Kalshi || busy.ID != "KXTEMPMIAH-26OCT1418-T85.99" || busy.Kind != "KXTEMPMIAH" || busy.Status != "open" || busy.Close.IsZero() {
		t.Fatalf("%+v", busy)
	}
	s, ok := ms[0].Snap(at)
	if !ok || s.YesPPM != 10000 || s.Volume != 4691_760000 || s.OI != 4619_540000 || s.Vol24 != 4691_760000 {
		t.Fatalf("%+v %v", s, ok)
	}
	// nothing has traded, so the price is the middle of the book
	book, ok := ms[2].Snap(at)
	if !ok || book.YesPPM != 495000 || book.Volume != 0 {
		t.Fatalf("%+v %v", book, ok)
	}
	if _, ok := (kalshi.Market{Ticker: "X"}).Snap(at); ok {
		t.Fatal("a market with no price made a snapshot")
	}
}

func TestTradeConversion(t *testing.T) {
	rows := load[tape](t, "kalshi_trades.json").Trades
	for _, r := range rows {
		v, ok := r.Convert()
		if !ok {
			t.Fatalf("%+v", r)
		}
		if v.Maker != nil || v.Taker != nil {
			t.Fatalf("kalshi has no trader ids %+v", v)
		}
	}
	// 3051 contracts at 0.913 paid by a yes taker
	yes, _ := rows[1].Convert()
	if yes.USD != 2785_563000 || yes.PricePPM != 913000 || yes.Qty != 3051_000000 || yes.Side != "yes" || yes.MarketID != rows[1].Ticker {
		t.Fatalf("%+v", yes)
	}
	// a no taker pays the no price, 1341.33 contracts at 0.80
	no, _ := rows[3].Convert()
	if no.USD != 1073_064000 || no.PricePPM != 200000 || no.Side != "no" {
		t.Fatalf("%+v", no)
	}
	bad := []kalshi.Trade{
		{ID: "a", Ticker: "T", Created: "2026-10-14T21:00:00Z", Count: "1", YesPrice: "1.5", NoPrice: "0", TakerSide: "yes"},
		{ID: "a", Ticker: "T", Created: "2026-10-14T21:00:00Z", Count: "0", YesPrice: "0.5", NoPrice: "0.5", TakerSide: "yes"},
		{ID: "a", Ticker: "T", Created: "2026-10-14T21:00:00Z", Count: "1", YesPrice: "0.5", NoPrice: "0.5", TakerSide: "up"},
		{ID: "a", Ticker: "T", Created: "yesterday", Count: "1", YesPrice: "0.5", NoPrice: "0.5", TakerSide: "yes"},
	}
	for _, r := range bad {
		if _, ok := r.Convert(); ok {
			t.Fatalf("accepted %+v", r)
		}
	}
}

// api serves the recorded rows and logs the queries it saw
type api struct {
	mu      sync.Mutex
	queries []url.Values
	pages   []markets
	settled markets
	prints  [][]kalshi.Trade
	limited atomic.Int32
}

func (a *api) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	q := r.URL.Query()
	a.queries = append(a.queries, q)
	if a.limited.Add(-1) >= 0 {
		http.Error(w, "slow down", http.StatusTooManyRequests)
		return
	}
	page, _ := strconv.Atoi(q.Get("cursor"))
	switch r.URL.Path {
	case "/markets":
		if q.Get("status") == "settled" {
			_ = json.MarshalWrite(w, map[string]any{"markets": a.settled.Markets, "cursor": ""})
			return
		}
		next := ""
		if page+1 < len(a.pages) {
			next = strconv.Itoa(page + 1)
		}
		_ = json.MarshalWrite(w, map[string]any{"markets": a.pages[page].Markets, "cursor": next})
	case "/markets/trades":
		next := ""
		if page+1 < len(a.prints) {
			next = strconv.Itoa(page + 1)
		}
		_ = json.MarshalWrite(w, map[string]any{"trades": a.prints[page], "cursor": next})
	default:
		http.NotFound(w, r)
	}
}

func newClient(t testing.TB, a *api) *kalshi.Client {
	t.Helper()
	srv := httptest.NewServer(a)
	t.Cleanup(srv.Close)
	sleep := func(context.Context, time.Duration) error { return nil }
	return &kalshi.Client{HTTP: &httpx.Client{HTTP: srv.Client(), UserAgent: "test", Retries: 3, Sleep: sleep}, BaseURL: srv.URL}
}

func TestCatalogueSweepsPagesAndSnapshotsTheBusiestMarkets(t *testing.T) {
	ms := load[markets](t, "kalshi_markets.json").Markets
	a := &api{pages: []markets{{ms[:2]}, {ms[2:]}}}
	st := venuetest.New()
	if err := st.UpsertMarkets(t.Context(), []venue.Market{{Venue: venue.Kalshi, ID: "GONE", Title: "left the list", Status: "open"}}); err != nil {
		t.Fatal(err)
	}
	m := kalshi.NewMetrics(prometheus.NewRegistry())
	c := &kalshi.Catalogue{Client: newClient(t, a), Store: st, Top: 1, Metrics: m, Now: func() time.Time { return at }}
	if err := c.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(a.queries) != 2 {
		t.Fatalf("%d requests", len(a.queries))
	}
	q := a.queries[0]
	if q.Get("status") != "open" || q.Get("mve_filter") != "exclude" || q.Get("limit") != "1000" || q.Get("cursor") != "" {
		t.Fatalf("%v", q)
	}
	if a.queries[1].Get("cursor") != "1" {
		t.Fatalf("second page %v", a.queries[1])
	}
	for _, x := range ms {
		if _, ok := st.Market(venue.Kalshi, x.Ticker); !ok {
			t.Fatalf("%s missing", x.Ticker)
		}
	}
	snaps := st.Snaps(venue.Kalshi)
	if len(snaps) != 1 || snaps[0].MarketID != ms[0].Ticker || !snaps[0].TS.Equal(at) {
		t.Fatalf("%+v", snaps)
	}
	if g, _ := st.Market(venue.Kalshi, "GONE"); g.Status != "closed" {
		t.Fatalf("a market that left the list stayed %q", g.Status)
	}
}

func TestCatalogueFailureClosesNothing(t *testing.T) {
	ms := load[markets](t, "kalshi_markets.json").Markets
	a := &api{pages: []markets{{ms}}}
	a.limited.Store(100)
	st := venuetest.New()
	_ = st.UpsertMarkets(t.Context(), []venue.Market{{Venue: venue.Kalshi, ID: "KEEP", Title: "still open", Status: "open"}})
	c := &kalshi.Catalogue{Client: newClient(t, a), Store: st, Top: 5, Metrics: kalshi.NewMetrics(prometheus.NewRegistry())}
	if err := c.Poll(t.Context()); err == nil {
		t.Fatal("a rate-limited sweep reported success")
	}
	if k, _ := st.Market(venue.Kalshi, "KEEP"); k.Status != "open" {
		t.Fatalf("%+v", k)
	}
}

func TestTradesKeepLargePrintsAndFollowTheCursor(t *testing.T) {
	rows := load[tape](t, "kalshi_trades.json").Trades
	// the recorded tape has a combo print, three big plain prints and one tiny one
	a := &api{prints: [][]kalshi.Trade{rows[:2], rows[2:]}}
	a.limited.Store(2)
	st := venuetest.New()
	reg := prometheus.NewRegistry()
	m := kalshi.NewMetrics(reg)
	tr := &kalshi.Trades{Client: newClient(t, a), Store: st, MinUSD: 1000, Metrics: m, Now: func() time.Time { return at }}
	if err := tr.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	got := st.Trades(venue.Kalshi)
	if len(got) != 3 {
		t.Fatalf("%d trades kept: %+v", len(got), got)
	}
	for _, g := range got {
		if g.USD < 1000_000000 || g.MarketID == rows[0].Ticker {
			t.Fatalf("kept %+v", g)
		}
	}
	// two 429 answers were retried, then two pages were read
	if len(a.queries) != 4 {
		t.Fatalf("%d requests", len(a.queries))
	}
	if want := strconv.FormatInt(at.Add(-5*time.Second).Unix(), 10); a.queries[2].Get("min_ts") != want {
		t.Fatalf("min_ts %q, want %q", a.queries[2].Get("min_ts"), want)
	}
	cur, _ := st.Cursor(t.Context(), "kalshi/trades")
	if cur == "" {
		t.Fatal("no cursor")
	}
	// a second poll replays the same page and stores nothing new
	a.limited.Store(0)
	if err := tr.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n := len(st.Trades(venue.Kalshi)); n != 3 {
		t.Fatalf("%d after replay", n)
	}
	last := a.queries[len(a.queries)-1]
	newest, _ := time.Parse(time.RFC3339Nano, cur)
	if last.Get("min_ts") == "" || last.Get("min_ts") != strconv.FormatInt(newest.Add(-5*time.Second).Unix(), 10) {
		t.Fatalf("second poll min_ts %v", last)
	}
	if n := testutil.ToFloat64(m.Trades()); n != 3 {
		t.Fatalf("stored counter %v", n)
	}
}

func TestTradesCountAGapWhenThePageCapIsHit(t *testing.T) {
	row := load[tape](t, "kalshi_trades.json").Trades[1]
	a := &api{}
	for range 40 {
		a.prints = append(a.prints, []kalshi.Trade{row})
	}
	m := kalshi.NewMetrics(prometheus.NewRegistry())
	tr := &kalshi.Trades{Client: newClient(t, a), Store: venuetest.New(), MinUSD: 10000, Metrics: m}
	if err := tr.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n := testutil.ToFloat64(m.Gaps()); n != 1 || len(a.queries) != 30 {
		t.Fatalf("gaps %v after %d requests", n, len(a.queries))
	}
}

func TestSettlementsSetResultsOnKnownMarkets(t *testing.T) {
	settled := load[markets](t, "kalshi_settled.json")
	a := &api{settled: settled}
	st := venuetest.New()
	first, second := settled.Markets[0], settled.Markets[1]
	_ = st.UpsertMarkets(t.Context(), []venue.Market{
		{Venue: venue.Kalshi, ID: first.Ticker, Title: "a", Status: "open"},
		{Venue: venue.Kalshi, ID: second.Ticker, Title: "b", Status: "open"},
	})
	s := &kalshi.Settlements{Client: newClient(t, a), Store: st, Metrics: kalshi.NewMetrics(prometheus.NewRegistry()), Now: func() time.Time { return at }}
	if err := s.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	q := a.queries[0]
	if q.Get("status") != "settled" || q.Get("mve_filter") != "exclude" || q.Get("min_settled_ts") != strconv.FormatInt(at.Add(-10*time.Minute).Unix(), 10) {
		t.Fatalf("%v", q)
	}
	for _, m := range settled.Markets {
		got, _ := st.Market(venue.Kalshi, m.Ticker)
		if got.Result != m.Result || got.Status != "settled" {
			t.Fatalf("%s %+v", m.Ticker, got)
		}
	}
	cur, _ := st.Cursor(t.Context(), "kalshi/settled")
	when, _ := time.Parse(time.RFC3339, first.SettledAt)
	if cur != strconv.FormatInt(when.Unix(), 10) {
		t.Fatalf("cursor %q", cur)
	}
	if err := s.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if a.queries[1].Get("min_settled_ts") != cur {
		t.Fatalf("second poll %v", a.queries[1])
	}
}
