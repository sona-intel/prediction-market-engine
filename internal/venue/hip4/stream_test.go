package hip4_test

import (
	"context"
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"pm/internal/httpx"
	"pm/internal/venue"
	"pm/internal/venue/hip4"
	"pm/internal/venue/venuetest"
)

// fake serves the info endpoint and the WebSocket from the recorded answers
type fake struct {
	t      *testing.T
	meta   func() hip4.Meta
	tape   []hip4.Trade
	wsTape []byte

	mu    sync.Mutex
	subs  []string
	unsub []string
	conns int
}

func (f *fake) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /info", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Type string `json:"type"`
		}
		if err := json.UnmarshalRead(r.Body, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		switch req.Type {
		case "outcomeMeta":
			_ = json.MarshalWrite(w, f.meta())
		case "recentTrades":
			_ = json.MarshalWrite(w, f.tape)
		default:
			http.Error(w, "unknown type", http.StatusBadRequest)
		}
	})
	mux.HandleFunc("GET /ws", func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.CloseNow() }()
		f.mu.Lock()
		f.conns++
		f.mu.Unlock()
		for {
			_, b, err := c.Read(r.Context())
			if err != nil {
				return
			}
			var m struct {
				Method       string `json:"method"`
				Subscription struct {
					Coin string `json:"coin"`
				} `json:"subscription"`
			}
			if json.Unmarshal(b, &m) != nil {
				continue
			}
			f.mu.Lock()
			switch m.Method {
			case "subscribe":
				f.subs = append(f.subs, m.Subscription.Coin)
			case "unsubscribe":
				f.unsub = append(f.unsub, m.Subscription.Coin)
			}
			f.mu.Unlock()
			if m.Method == "subscribe" && m.Subscription.Coin == "#111210" && f.wsTape != nil {
				// the same snapshot twice, as a reconnect would replay it
				for range 2 {
					_ = c.Write(r.Context(), websocket.MessageText, f.wsTape)
				}
			}
		}
	})
	return mux
}

func (f *fake) seen() (subs, unsub []string, conns int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.subs...), append([]string(nil), f.unsub...), f.conns
}

func setup(t *testing.T, f *fake, st venue.Store) *hip4.Stream {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	return &hip4.Stream{
		Store:     st,
		HTTP:      &httpx.Client{HTTP: srv.Client(), UserAgent: "test"},
		InfoURL:   srv.URL + "/info",
		WSURL:     "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws",
		Metrics:   hip4.NewMetrics(prometheus.NewRegistry()),
		MetaEvery: time.Hour,
		PingEvery: time.Hour,
		GapWeight: 1_000_000,
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func newFake(t *testing.T) *fake {
	t.Helper()
	meta := load[hip4.Meta](t, "hl_outcomemeta.json")
	ws, err := os.ReadFile(fixtures + "hl_ws_trades.json")
	if err != nil {
		t.Fatal(err)
	}
	return &fake{t: t, meta: func() hip4.Meta { return meta }, tape: load[[]hip4.Trade](t, "hl_recenttrades.json"), wsTape: ws}
}

func start(t *testing.T, s *hip4.Stream) context.Context {
	t.Helper()
	if err := s.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	return t.Context()
}

func TestStreamSubscribesSideZeroOnlyAndDedupesOnTid(t *testing.T) {
	f := newFake(t)
	st := venuetest.New()
	s := setup(t, f, st)
	start(t, s)
	waitFor(t, "the tape", func() bool { return len(st.Trades(venue.Hyperliquid)) == 6 })

	subs, _, _ := f.seen()
	if len(subs) != 3 {
		t.Fatalf("subscribed %v", subs)
	}
	for _, c := range subs {
		if !strings.HasSuffix(c, "0") {
			t.Errorf("subscribed to a mirror side %s", c)
		}
	}
	time.Sleep(100 * time.Millisecond)
	if n := len(st.Trades(venue.Hyperliquid)); n != 6 {
		t.Fatalf("replayed snapshot changed the tape to %d rows", n)
	}
	if got := testutil.ToFloat64(s.Metrics.TradesStored()); got != 6 {
		t.Fatalf("stored counter %v", got)
	}
	if m, ok := st.Market(venue.Hyperliquid, "#111210"); !ok || m.Status != "open" {
		t.Fatalf("market %+v %v", m, ok)
	}
}

func TestReconnectCountsAGapOnlyWhenThePageMissesTheTape(t *testing.T) {
	for name, tc := range map[string]struct {
		lastStored time.Time
		gaps       int
	}{
		"stored tape before the page": {time.UnixMilli(1).UTC(), 1},
		"stored tape inside the page": {time.Time{}, 0},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFake(t)
			f.wsTape = nil
			last := tc.lastStored
			if last.IsZero() {
				last = time.UnixMilli(f.tape[len(f.tape)-1].Time).UTC()
			}
			st := venuetest.New()
			if _, err := st.InsertTrades(t.Context(), []venue.Trade{{Venue: venue.Hyperliquid, ID: "old", MarketID: "#111210", TS: last, Side: "buy"}}); err != nil {
				t.Fatal(err)
			}
			s := setup(t, f, st)
			start(t, s)
			waitFor(t, "the catch-up page", func() bool { return len(st.Trades(venue.Hyperliquid)) == 7 })
			time.Sleep(100 * time.Millisecond)
			if got := testutil.ToFloat64(s.Metrics.Gaps()); int(got) != tc.gaps {
				t.Fatalf("gaps %v, want %d", got, tc.gaps)
			}
			if v, _ := st.Cursor(t.Context(), "hyperliquid/gaps"); (v == "") != (tc.gaps == 0) {
				t.Fatalf("gap cursor %q", v)
			}
		})
	}
}

func TestOutcomeListRefreshSubscribesAndUnsubscribes(t *testing.T) {
	f := newFake(t)
	full := f.meta()
	var mu sync.Mutex
	current := full
	f.meta = func() hip4.Meta {
		mu.Lock()
		defer mu.Unlock()
		return current
	}
	st := venuetest.New()
	s := setup(t, f, st)
	s.MetaEvery = 30 * time.Millisecond
	start(t, s)
	waitFor(t, "first subscriptions", func() bool { subs, _, _ := f.seen(); return len(subs) == 3 })

	mu.Lock()
	current = hip4.Meta{Outcomes: full.Outcomes[:2]}
	mu.Unlock()
	waitFor(t, "an unsubscribe", func() bool { _, un, _ := f.seen(); return len(un) == 1 })
	if _, un, _ := f.seen(); un[0] != "#103990" {
		t.Fatalf("unsubscribed %v", un)
	}
	waitFor(t, "the closed flag", func() bool { m, _ := st.Market(venue.Hyperliquid, "#103990"); return m.Status == "closed" })

	mu.Lock()
	current = full
	mu.Unlock()
	waitFor(t, "a resubscribe", func() bool { subs, _, _ := f.seen(); return len(subs) == 4 })
	if subs, _, _ := f.seen(); subs[3] != "#103990" {
		t.Fatalf("subscribed %v", subs)
	}
}

func TestSilentConnectionIsReplaced(t *testing.T) {
	f := newFake(t)
	f.wsTape = nil
	s := setup(t, f, venuetest.New())
	s.StaleAfter = 200 * time.Millisecond
	start(t, s)
	waitFor(t, "a second connection", func() bool { _, _, n := f.seen(); return n >= 2 })
}
