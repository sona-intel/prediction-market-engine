package server_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"pm/internal/live"
	"pm/internal/server"
)

const (
	topPath   = "/internal/v1/pm/markets/top"
	largePath = "/internal/v1/pm/trades/large"
)

const fakeGamma = `[
 {"id":"11","slug":"big-game","title":"Big game","volume24hr":4115173.31,"volume":4199141.1,"liquidity":7916220.4,"endDate":"2026-10-13T19:00:00Z",
  "markets":[{"question":"Will A win?","groupItemTitle":"A","closed":false,"outcomes":"[\"Seahawks\", \"Broncos\"]","outcomePrices":"[\"0.535\", \"0.465\"]"}]},
 {"id":"12","slug":"open-ended","title":"Open ended","volume24hr":900.5,"liquidity":10,"markets":[]},
 {"id":"13","slug":"third","title":"Third","volume24hr":10,"liquidity":1,"endDate":"2026-12-01T00:00:00Z","markets":[]}
]`

const fakeData = `[
 {"proxyWallet":"0xbb","side":"BUY","size":70000,"price":0.19,"timestamp":1791916197,"title":"New one","outcome":"Yes","transactionHash":"0x02"},
 {"proxyWallet":"0xaa","side":"SELL","size":20000,"price":0.5,"timestamp":1791916100,"title":"Old one","outcome":"No","transactionHash":"0x01"},
 {"proxyWallet":"0xcc","side":"BUY","size":100000,"price":0.9,"timestamp":1791916000,"title":"Biggest","outcome":"Yes","transactionHash":"0x00"}
]`

type fakePolymarket struct {
	down atomic.Bool
	srv  *httptest.Server
}

func newFakePolymarket(t testing.TB) *fakePolymarket {
	t.Helper()
	f := &fakePolymarket{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if f.down.Load() {
			http.Error(w, "down", http.StatusInternalServerError)
			return
		}
		if r.URL.Path == "/events" {
			fmt.Fprint(w, fakeGamma)
			return
		}
		fmt.Fprint(w, fakeData)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakePolymarket) poller() *live.Poller {
	return live.New(live.Options{GammaURL: f.srv.URL + "/events", DataURL: f.srv.URL + "/trades", Now: func() time.Time {
		return time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	}})
}

func liveHandler(t testing.TB, stale bool) http.Handler {
	t.Helper()
	f := newFakePolymarket(t)
	p := f.poller()
	p.Poll(t.Context())
	if stale {
		f.down.Store(true)
		p.Poll(t.Context())
	}
	return server.NewLive(failing{}, p, server.NewMetrics(prometheus.NewRegistry()))
}

func liveServer(t *testing.T, stale bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(liveHandler(t, stale))
	t.Cleanup(srv.Close)
	return srv
}

func TestTopMarketsBody(t *testing.T) {
	r := get(t, liveServer(t, false), topPath+"?limit=2")
	if r.status != 200 || r.header.Get("X-Sona-Cache") != "" || r.header.Get("Sona-Block") != "" {
		t.Fatalf("%d %v", r.status, r.header)
	}
	want := `{"kind":"pm_top_markets","items":[` +
		`{"event_id":"11","slug":"big-game","title":"Big game","volume_24h_usd":4115173.31,"volume_total_usd":4199141.1,"liquidity_usd":7916220.4,"end_date":"2026-10-13T19:00:00Z","markets":[{"question":"Will A win?","label":"A","outcomes":["Seahawks","Broncos"],"outcome_prices":[0.535,0.465]}]},` +
		`{"event_id":"12","slug":"open-ended","title":"Open ended","volume_24h_usd":900.5,"volume_total_usd":null,"liquidity_usd":10,"end_date":null,"markets":[]}],` +
		`"fetched_at":"2026-10-13T12:00:00Z","stale":false,"source":"polymarket-gamma"}`
	if string(r.body) != want {
		t.Fatalf("\n got %s\nwant %s", r.body, want)
	}
}

func TestTopMarketsDefaultLimitReturnsEverythingHeld(t *testing.T) {
	r := get(t, liveServer(t, false), topPath)
	if n := len(dig(t, r.json(t), "items").([]any)); n != 3 {
		t.Fatalf("%d items", n)
	}
}

func TestLargeTradesBody(t *testing.T) {
	r := get(t, liveServer(t, false), largePath+"?limit=1")
	want := `{"kind":"pm_large_trades","items":[` +
		`{"tx":"0x02","time":"2026-10-13T18:29:57Z","wallet":"0xbb","question":"New one","side":"BUY","outcome":"Yes","size":70000,"price":0.19,"usd":13300}],` +
		`"fetched_at":"2026-10-13T12:00:00Z","stale":false,"source":"polymarket-data"}`
	if r.status != 200 || string(r.body) != want {
		t.Fatalf("%d\n got %s\nwant %s", r.status, r.body, want)
	}
}

func TestLargeTradesMinUSDFilters(t *testing.T) {
	srv := liveServer(t, false)
	for _, c := range []struct {
		query string
		txs   string
	}{
		{"", "0x02,0x01,0x00"},
		{"?min_usd=13300", "0x02,0x00"},
		{"?min_usd=13300.01", "0x00"},
		{"?min_usd=90000", "0x00"},
		{"?min_usd=90001", ""},
	} {
		items := dig(t, get(t, srv, largePath+c.query).json(t), "items").([]any)
		var got []string
		for _, it := range items {
			got = append(got, it.(map[string]any)["tx"].(string))
		}
		if strings.Join(got, ",") != c.txs {
			t.Errorf("%q gave %v, want %s", c.query, got, c.txs)
		}
	}
}

func TestStaleFeedIsMarkedInBodyAndHeader(t *testing.T) {
	srv := liveServer(t, true)
	for _, path := range []string{topPath, largePath} {
		r := get(t, srv, path)
		if r.status != 200 || dig(t, r.json(t), "stale") != true || r.header.Get("X-Sona-Cache") != "stale" {
			t.Errorf("%s: %d %v %s", path, r.status, r.header, r.body)
		}
		if got := dig(t, r.json(t), "fetched_at"); got != "2026-10-13T12:00:00Z" {
			t.Errorf("%s fetched_at %v", path, got)
		}
	}
}

func TestLiveRoutesAnswer503BeforeTheFirstPoll(t *testing.T) {
	f := newFakePolymarket(t)
	f.down.Store(true)
	p := f.poller()
	p.Poll(t.Context())
	for name, h := range map[string]http.Handler{
		"failed poll": server.NewLive(failing{}, p, server.NewMetrics(prometheus.NewRegistry())),
		"no feed":     server.New(failing{}, server.NewMetrics(prometheus.NewRegistry())),
	} {
		srv := httptest.NewServer(h)
		for _, path := range []string{topPath, largePath} {
			r := get(t, srv, path)
			expectProblem(t, r, http.StatusServiceUnavailable, "upstream_unavailable")
			if dig(t, r.json(t), "retry_after_s") != float64(10) || r.header.Get("Retry-After") != "10" {
				t.Errorf("%s %s: %v %s", name, path, r.header, r.body)
			}
		}
		srv.Close()
	}
}

func TestLiveRoutesRejectBadQueries(t *testing.T) {
	srv := liveServer(t, false)
	for _, q := range []string{
		topPath + "?limit=0", topPath + "?limit=51", topPath + "?limit=x", topPath + "?limit=",
		largePath + "?limit=0", largePath + "?limit=101", largePath + "?limit=1.5",
		largePath + "?min_usd=9999", largePath + "?min_usd=-5", largePath + "?min_usd=abc",
		largePath + "?min_usd=NaN", largePath + "?min_usd=Inf", largePath + "?min_usd=",
	} {
		expectProblem(t, get(t, srv, q), http.StatusBadRequest, "invalid_query")
	}
	for _, q := range []string{topPath + "?limit=50", largePath + "?limit=100&min_usd=10000", largePath + "?min_usd=1e5"} {
		if r := get(t, srv, q); r.status != 200 {
			t.Errorf("%s: %d %s", q, r.status, r.body)
		}
	}
}

func TestLiveRoutesHonourAnSpentDeadline(t *testing.T) {
	r := get(t, liveServer(t, false), topPath, "Sona-Deadline-Ms", "0")
	expectProblem(t, r, http.StatusGatewayTimeout, "deadline_exceeded")
}

func liveSpecCases(fresh, stale, empty http.Handler) []specCase {
	bad, unavailable := http.StatusBadRequest, http.StatusServiceUnavailable
	return []specCase{
		{name: "top markets", path: topPath, via: fresh},
		{name: "top markets page", path: topPath + "?limit=2", via: fresh},
		{name: "top markets at the cap", path: topPath + "?limit=50", via: fresh},
		{name: "stale top markets", path: topPath, via: stale},
		{name: "large trades", path: largePath, via: fresh},
		{name: "large trades with a floor", path: largePath + "?limit=100&min_usd=50000.5", via: fresh},
		{name: "large trades that none reaches", path: largePath + "?min_usd=1000000", via: fresh},
		{name: "stale large trades", path: largePath, via: stale},
		{name: "top markets limit 0", path: topPath + "?limit=0", status: bad, malformed: true, via: fresh},
		{name: "top markets limit 51", path: topPath + "?limit=51", status: bad, malformed: true, via: fresh},
		{name: "large trades limit 101", path: largePath + "?limit=101", status: bad, malformed: true, via: fresh},
		{name: "large trades floor below 10000", path: largePath + "?min_usd=9999", status: bad, via: fresh},
		{name: "large trades floor below any venue's", path: largePath + "?min_usd=999", status: bad, malformed: true, via: fresh},
		{name: "large trades floor that is not a number", path: largePath + "?min_usd=abc", status: bad, malformed: true, via: fresh},
		{name: "top markets before the first poll", path: topPath, status: unavailable, via: empty},
		{name: "large trades before the first poll", path: largePath, status: unavailable, via: empty},
		{name: "top markets past its deadline", path: topPath, header: []string{"Sona-Deadline-Ms", "0"}, status: http.StatusGatewayTimeout, via: fresh},
		{name: "large trades past its deadline", path: largePath, header: []string{"Sona-Deadline-Ms", "-1"}, status: http.StatusGatewayTimeout, via: fresh},
	}
}
