package live_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"pm/internal/live"
)

const gammaBody = `[
 {"id":"11","slug":"big-game","title":"Big game","volume24hr":4115173.31,"liquidity":7916220.4,"endDate":"2026-10-13T19:00:00Z",
  "markets":[
   {"question":"Will A win?","closed":false,"outcomePrices":"[\"0.535\", \"0.465\"]"},
   {"question":"Done already?","closed":true,"outcomePrices":"[\"1\", \"0\"]"},
   {"question":"Broken prices","closed":false,"outcomePrices":"not json"}]},
 {"id":"12","slug":"open-ended","title":"Open ended","volume24hr":900.5,"markets":[]}
]`

const tradesBody = `[
 {"proxyWallet":"0xaa","side":"SELL","size":20000,"price":0.5,"timestamp":1791916100,"title":"Old one","outcome":"No","transactionHash":"0x01"},
 {"proxyWallet":"0xbb","side":"BUY","size":70000,"price":0.19,"timestamp":1791916197,"title":"New one","outcome":"Yes","transactionHash":"0x02"}
]`

type upstream struct {
	gamma, data       *httptest.Server
	gammaFail, dFail  atomic.Bool
	gammaHit, dataHit atomic.Int32
	gammaQuery        atomic.Value
	dataQuery         atomic.Value
	agent             atomic.Value
}

func newUpstream(t *testing.T) *upstream {
	t.Helper()
	u := &upstream{}
	u.gamma = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.gammaHit.Add(1)
		u.gammaQuery.Store(r.URL.Query())
		u.agent.Store(r.UserAgent())
		if u.gammaFail.Load() {
			http.Error(w, "down", http.StatusBadGateway)
			return
		}
		fmt.Fprint(w, gammaBody)
	}))
	u.data = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.dataHit.Add(1)
		u.dataQuery.Store(r.URL.Query())
		if u.dFail.Load() {
			fmt.Fprint(w, "{not an array")
			return
		}
		fmt.Fprint(w, tradesBody)
	}))
	t.Cleanup(u.gamma.Close)
	t.Cleanup(u.data.Close)
	return u
}

func (u *upstream) poller(now func() time.Time) *live.Poller {
	return live.New(live.Options{GammaURL: u.gamma.URL, DataURL: u.data.URL, Now: now})
}

func TestNothingIsServedBeforeTheFirstPoll(t *testing.T) {
	p := newUpstream(t).poller(time.Now)
	if _, ok := p.Events(); ok {
		t.Error("events before a poll")
	}
	if _, ok := p.Trades(); ok {
		t.Error("trades before a poll")
	}
}

func TestPollParsesBothFeeds(t *testing.T) {
	u := newUpstream(t)
	at := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	p := u.poller(func() time.Time { return at })
	p.Poll(t.Context())

	ev, ok := p.Events()
	if !ok || ev.Stale || !ev.FetchedAt.Equal(at) || len(ev.Items) != 2 {
		t.Fatalf("%+v %v", ev, ok)
	}
	first := ev.Items[0]
	if first.ID != "11" || first.Slug != "big-game" || first.Volume24hUSD != 4115173.31 || first.LiquidityUSD != 7916220.4 || first.EndDate != "2026-10-13T19:00:00Z" {
		t.Errorf("%+v", first)
	}
	if len(first.Markets) != 2 {
		t.Fatalf("a closed market stays out: %+v", first.Markets)
	}
	if got := first.Markets[0].OutcomePrices; len(got) != 2 || got[0] != 0.535 || got[1] != 0.465 {
		t.Errorf("prices %v", got)
	}
	if got := first.Markets[1].OutcomePrices; got == nil || len(got) != 0 {
		t.Errorf("unreadable prices should be empty, got %#v", got)
	}
	if ev.Items[1].LiquidityUSD != 0 || ev.Items[1].Markets == nil {
		t.Errorf("%+v", ev.Items[1])
	}

	tr, ok := p.Trades()
	if !ok || tr.Stale || len(tr.Items) != 2 {
		t.Fatalf("%+v %v", tr, ok)
	}
	if got := tr.Items[0]; got.Tx != "0x02" || got.Wallet != "0xbb" || got.Question != "New one" || got.Side != "BUY" || got.Outcome != "Yes" || got.USD != 13300 || got.Time.Unix() != 1791916197 {
		t.Errorf("newest first: %+v", got)
	}
}

func TestPollAsksPolymarketForTheRightThings(t *testing.T) {
	u := newUpstream(t)
	u.poller(time.Now).Poll(t.Context())
	gq := u.gammaQuery.Load().(url.Values)
	for k, v := range map[string]string{"active": "true", "closed": "false", "order": "volume24hr", "ascending": "false", "limit": "50"} {
		if gq.Get(k) != v {
			t.Errorf("gamma %s=%v, want %s", k, gq[k], v)
		}
	}
	dq := u.dataQuery.Load().(url.Values)
	for k, v := range map[string]string{"filterType": "CASH", "filterAmount": "10000", "limit": "200"} {
		if dq.Get(k) != v {
			t.Errorf("data %s=%v, want %s", k, dq[k], v)
		}
	}
	if a := u.agent.Load().(string); !strings.HasPrefix(a, "sona-intel-pm-api/") {
		t.Errorf("user agent %q", a)
	}
}

func TestFailedPollKeepsTheLastGoodCopyAndMarksItStale(t *testing.T) {
	u := newUpstream(t)
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	p := u.poller(func() time.Time { return now })
	p.Poll(t.Context())
	u.gammaFail.Store(true)
	u.dFail.Store(true)
	later := now.Add(time.Minute)
	now = later
	p.Poll(t.Context())

	ev, ok := p.Events()
	if !ok || !ev.Stale || len(ev.Items) != 2 || ev.FetchedAt.Equal(later) {
		t.Fatalf("events %+v", ev)
	}
	tr, ok := p.Trades()
	if !ok || !tr.Stale || len(tr.Items) != 2 {
		t.Fatalf("trades %+v", tr)
	}

	u.gammaFail.Store(false)
	u.dFail.Store(false)
	p.Poll(t.Context())
	if ev, _ := p.Events(); ev.Stale || !ev.FetchedAt.Equal(later) {
		t.Errorf("a good poll should clear the flag: %+v", ev)
	}
}

func TestFailedFirstPollLeavesTheFeedUnavailable(t *testing.T) {
	u := newUpstream(t)
	u.gammaFail.Store(true)
	p := u.poller(time.Now)
	p.Poll(t.Context())
	if _, ok := p.Events(); ok {
		t.Error("events after a failed first poll")
	}
	if _, ok := p.Trades(); !ok {
		t.Error("the trade feed is independent of the events feed")
	}
}

func TestRunPollsAtOnceAndOnEveryTick(t *testing.T) {
	u := newUpstream(t)
	p := live.New(live.Options{GammaURL: u.gamma.URL, DataURL: u.data.URL, Interval: 20 * time.Millisecond})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for u.gammaHit.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	if u.gammaHit.Load() < 3 || u.dataHit.Load() < 3 {
		t.Fatalf("gamma %d, data %d", u.gammaHit.Load(), u.dataHit.Load())
	}
}
