package polyus_test

import (
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"pm/internal/httpx"
	"pm/internal/venue"
	"pm/internal/venue/polyus"
	"pm/internal/venue/venuetest"
)

const (
	fixtures  = "../../../testdata/venues/"
	openSlug  = "paccc-usho-midterms-2026-11-03-dem"
	otherSlug = "tec-mlb-wsleague-2026-11-01-w-al"
	doneSlug  = "aadc-fwc-esp-arg-2026-07-19-to-advance"
)

func raw(t testing.TB, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(fixtures + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// gateway answers from the recorded files and logs the requests it saw
type gateway struct {
	t       testing.TB
	mu      sync.Mutex
	queries []map[string][]string
	bbo     map[string]string
	settle  map[string]string
	pages   int
	rateLim bool
	markets []byte
}

func (g *gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	defer g.mu.Unlock()
	path := r.URL.Path
	switch {
	case path == "/markets":
		g.queries = append(g.queries, r.URL.Query())
		off, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		if off >= g.pages*500 {
			_, _ = w.Write([]byte(`{"markets":[]}`))
			return
		}
		_, _ = w.Write(g.markets)
	case len(path) > 13 && path[len(path)-4:] == "/bbo":
		slug := path[len("/markets/") : len(path)-len("/bbo")]
		f, ok := g.bbo[slug]
		if !ok || g.rateLim {
			http.Error(w, "error code: 1015", http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write(raw(g.t, f))
	default:
		slug := path[len("/markets/") : len(path)-len("/settlement")]
		f, ok := g.settle[slug]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":5,"message":"Settlement not found","details":[]}`))
			return
		}
		_, _ = w.Write(raw(g.t, f))
	}
}

func newGateway(t testing.TB) (*gateway, *polyus.Client) {
	t.Helper()
	g := &gateway{t: t, pages: 1, markets: raw(t, "polyus_markets.json"), bbo: map[string]string{openSlug: "polyus_bbo.json", doneSlug: "polyus_bbo_expired.json"}}
	srv := httptest.NewServer(g)
	t.Cleanup(srv.Close)
	return g, &polyus.Client{HTTP: &httpx.Client{HTTP: srv.Client(), UserAgent: "test"}, BaseURL: srv.URL}
}

func TestCatalogueStoresOpenMarkets(t *testing.T) {
	g, cli := newGateway(t)
	g.pages = 2
	st := venuetest.New()
	if err := (&polyus.Catalogue{Client: cli, Store: st}).Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	m, ok := st.Market(venue.PolymarketUS, openSlug)
	if !ok || m.Title != "U.S House Midterm Winner: Democratic Party" || m.Kind != "election" || m.Status != "open" || !m.Close.Equal(time.Date(2027, 2, 2, 4, 59, 0, 0, time.UTC)) {
		t.Fatalf("%+v %v", m, ok)
	}
	if len(g.queries) != 3 || g.queries[0]["active"][0] != "true" || g.queries[0]["closed"][0] != "false" || g.queries[2]["offset"][0] != "1000" {
		t.Fatalf("requests %v", g.queries)
	}
}

func TestQuotesSnapshotTheTopMarketsAndCountFailures(t *testing.T) {
	g, cli := newGateway(t)
	st := venuetest.New()
	m := polyus.NewMetrics(prometheus.NewRegistry())
	at := time.Date(2026, 10, 14, 12, 0, 0, 500, time.UTC)
	q := &polyus.Quotes{Client: cli, Store: st, Top: 2, Metrics: m, Now: func() time.Time { return at }}
	if err := q.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	snaps := st.Snaps(venue.PolymarketUS)
	if len(snaps) != 1 {
		t.Fatalf("snapshots %+v", snaps)
	}
	s := snaps[0]
	if s.MarketID != openSlug || s.YesPPM != 897500 || s.Volume != 44443051*venue.Micro || s.OI != 1398766*venue.Micro || !s.TS.Equal(at.Truncate(time.Second)) {
		t.Fatalf("%+v", s)
	}
	if got := testutil.ToFloat64(m.Failures("bbo")); got != 1 {
		t.Fatalf("failures %v", got)
	}
	if limit := g.queries[0]["limit"][0]; limit != "2" || g.queries[0]["orderBy"][0] != "volume" {
		t.Fatalf("top query %v", g.queries[0])
	}
	if _, ok := st.Market(venue.PolymarketUS, otherSlug); !ok {
		t.Fatal("a top market whose quote failed was not stored")
	}
}

func TestQuotesFailWhenEveryRequestFails(t *testing.T) {
	g, cli := newGateway(t)
	g.rateLim = true
	q := &polyus.Quotes{Client: cli, Store: venuetest.New(), Top: 2, Metrics: polyus.NewMetrics(prometheus.NewRegistry())}
	if err := q.Poll(t.Context()); err == nil {
		t.Fatal("a poll with no quote at all reported success")
	}
}

func TestSnapNeedsAPrice(t *testing.T) {
	var b polyus.BBO
	if err := json.Unmarshal(raw(t, "polyus_bbo_expired.json"), &b); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.Snap(doneSlug, time.Now()); ok {
		t.Fatal("an expired market with no quote produced a snapshot")
	}
	if err := json.Unmarshal(raw(t, "polyus_bbo.json"), &b); err != nil {
		t.Fatal(err)
	}
	b.MarketData.CurrentPx, b.MarketData.LastTradePx, b.MarketData.LongQuote = nil, nil, nil
	if s, ok := b.Snap(openSlug, time.Now()); !ok || s.YesPPM != 897500 {
		t.Fatalf("midpoint fallback gave %+v %v", s, ok)
	}
	b.MarketData.BestAsk.Value = "1.7"
	if _, ok := b.Snap(openSlug, time.Now()); ok {
		t.Fatal("a price outside zero to one was accepted")
	}
}

func TestSettlementUsesTheFlagAndKeepsTheResult(t *testing.T) {
	g, cli := newGateway(t)
	g.settle = map[string]string{doneSlug: "polyus_settlement.json"}
	st := venuetest.New()
	past := time.Now().Add(-time.Hour)
	if err := st.UpsertMarkets(t.Context(), []venue.Market{
		{Venue: venue.PolymarketUS, ID: doneSlug, Title: "Spain vs Argentina", Close: past, Status: "open"},
		{Venue: venue.PolymarketUS, ID: openSlug, Title: "Still waiting", Close: past.Add(-time.Hour), Status: "open"},
		{Venue: venue.PolymarketUS, ID: otherSlug, Title: "Future", Close: time.Now().Add(time.Hour), Status: "open"},
	}); err != nil {
		t.Fatal(err)
	}
	m := polyus.NewMetrics(prometheus.NewRegistry())
	s := &polyus.Settlements{Client: cli, Store: st, Batch: 10, Metrics: m}
	if err := s.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.Market(venue.PolymarketUS, doneSlug); got.Result != "yes" || got.Status != "resolved" {
		t.Fatalf("%+v", got)
	}
	if got, _ := st.Market(venue.PolymarketUS, openSlug); got.Result != "" {
		t.Fatalf("a market with no settlement was settled: %+v", got)
	}
	if got, _ := st.Market(venue.PolymarketUS, otherSlug); got.Result != "" {
		t.Fatalf("a market before its end date was settled: %+v", got)
	}
	// a later sweep must not wipe the result
	if err := (&polyus.Catalogue{Client: cli, Store: st}).Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	g.markets = []byte(`{"markets":[{"slug":"` + doneSlug + `","question":"Spain vs Argentina","status":"MARKET_STATUS_OPEN","endDate":"2026-07-19T22:02:28Z"}]}`)
	if err := (&polyus.Catalogue{Client: cli, Store: st}).Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.Market(venue.PolymarketUS, doneSlug); got.Result != "yes" || got.Status != "resolved" {
		t.Fatalf("sweep overwrote the result: %+v", got)
	}
	if got := testutil.ToFloat64(m.Settled()); got != 1 {
		t.Fatalf("settled counter %v", got)
	}
}

func TestClosedMarketMapsToResolved(t *testing.T) {
	var l struct {
		Markets []polyus.Market `json:"markets"`
	}
	if err := json.Unmarshal(raw(t, "polyus_markets_closed.json"), &l); err != nil {
		t.Fatal(err)
	}
	m := l.Markets[0].Venue()
	if m.ID != doneSlug || m.Status != "resolved" || m.Close.IsZero() {
		t.Fatalf("%+v", m)
	}
}
