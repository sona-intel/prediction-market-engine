package hip4

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"

	"pm/internal/httpx"
	"pm/internal/venue"
)

const (
	gapsCursor = "hyperliquid/gaps"
	staleAfter = 2 * time.Minute
	readLimit  = 8 << 20
)

type Metrics struct {
	trades     prometheus.Counter
	skipped    prometheus.Counter
	gaps       prometheus.Counter
	reconnects prometheus.Counter
	coins      prometheus.Gauge
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		trades:     prometheus.NewCounter(prometheus.CounterOpts{Name: "pm_hip4_trades_stored_total", Help: "New HIP-4 trades stored"}),
		skipped:    prometheus.NewCounter(prometheus.CounterOpts{Name: "pm_hip4_trades_skipped_total", Help: "HIP-4 trades dropped as unusable"}),
		gaps:       prometheus.NewCounter(prometheus.CounterOpts{Name: "pm_hip4_gaps_total", Help: "Reconnects whose catch-up page did not reach the stored tape"}),
		reconnects: prometheus.NewCounter(prometheus.CounterOpts{Name: "pm_hip4_reconnects_total", Help: "WebSocket sessions started after the first"}),
		coins:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "pm_hip4_subscribed_coins", Help: "Outcome coins subscribed on the open connection"}),
	}
	reg.MustRegister(m.trades, m.skipped, m.gaps, m.reconnects, m.coins)
	return m
}

// Stream follows every outcome over one WebSocket and is polled only to report whether it is alive
type Stream struct {
	Store     venue.Store
	HTTP      *httpx.Client
	InfoURL   string
	WSURL     string
	Metrics   *Metrics
	MetaEvery time.Duration
	PingEvery time.Duration
	// SendEvery spaces subscribe messages under the venue's per-minute cap
	SendEvery time.Duration
	// GapWeight is the info weight a minute that the catch-up after a reconnect may use
	GapWeight int

	started atomic.Bool
	lastMsg atomic.Int64
	gapMu   sync.Mutex
}

func (s *Stream) Name() string { return venue.Hyperliquid + "/stream" }

func (s *Stream) Poll(ctx context.Context) error {
	if s.started.CompareAndSwap(false, true) {
		s.lastMsg.Store(time.Now().UnixNano())
		go s.run(ctx)
	}
	if age := time.Since(time.Unix(0, s.lastMsg.Load())); age > staleAfter {
		return fmt.Errorf("no message for %s", age.Round(time.Second))
	}
	return nil
}

func (s *Stream) run(ctx context.Context) {
	for attempt := 0; ctx.Err() == nil; attempt++ {
		began := time.Now()
		err := s.session(ctx, attempt > 0)
		if ctx.Err() != nil {
			return
		}
		slog.Warn("hip4 session ended", "error", err)
		if time.Since(began) > time.Minute {
			attempt = 0
		}
		if httpx.Sleep(ctx, httpx.Backoff(attempt)) != nil {
			return
		}
	}
}

func (s *Stream) session(ctx context.Context, reconnect bool) error {
	meta, err := s.fetchMeta(ctx)
	if err != nil {
		return err
	}
	coins, err := s.syncMarkets(ctx, meta)
	if err != nil {
		return err
	}
	prev := s.lastSeen(ctx, coins)
	c, _, err := websocket.Dial(ctx, s.WSURL, nil)
	if err != nil {
		return err
	}
	defer func() { _ = c.CloseNow() }()
	c.SetReadLimit(readLimit)
	if reconnect {
		s.Metrics.reconnects.Inc()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		errc <- s.manage(ctx, c, coins)
		cancel()
	}()
	go s.fill(ctx, prev)
	err = s.read(ctx, c)
	cancel()
	if merr := <-errc; errors.Is(err, context.Canceled) && merr != nil {
		err = merr
	}
	return err
}

func (s *Stream) fetchMeta(ctx context.Context) (Meta, error) {
	var m Meta
	err := s.HTTP.Post(ctx, s.InfoURL, map[string]string{"type": "outcomeMeta"}, &m)
	return m, err
}

// syncMarkets stores the live outcomes, flags the ones that left the list, and returns the coins to follow
func (s *Stream) syncMarkets(ctx context.Context, meta Meta) ([]string, error) {
	ms := make([]venue.Market, len(meta.Outcomes))
	live := make(map[string]bool, len(ms))
	coins := make([]string, len(ms))
	for i, o := range meta.Outcomes {
		ms[i] = o.Market()
		live[ms[i].ID] = true
		coins[i] = ms[i].ID
	}
	if err := s.Store.UpsertMarkets(ctx, ms); err != nil {
		return nil, err
	}
	ids, err := s.Store.OpenMarketIDs(ctx, venue.Hyperliquid)
	if err != nil {
		return nil, err
	}
	gone := slices.DeleteFunc(ids, func(id string) bool { return live[id] })
	if len(gone) > 0 {
		if err := s.Store.MarkClosed(ctx, venue.Hyperliquid, gone); err != nil {
			return nil, err
		}
	}
	return coins, nil
}

// lastSeen reads the newest stored trade of each coin before the connection adds more
func (s *Stream) lastSeen(ctx context.Context, coins []string) map[string]time.Time {
	prev := map[string]time.Time{}
	for _, c := range coins {
		ts, ok, err := s.Store.LastTradeTS(ctx, venue.Hyperliquid, c)
		if err != nil {
			slog.Warn("last trade lookup failed", "coin", c, "error", err)
		} else if ok {
			prev[c] = ts
		}
	}
	return prev
}

// manage subscribes the coins, then keeps the connection alive and follows the outcome list
func (s *Stream) manage(ctx context.Context, c *websocket.Conn, coins []string) error {
	have := map[string]bool{}
	if err := s.change(ctx, c, "subscribe", coins, have); err != nil {
		return err
	}
	ping := time.NewTicker(s.PingEvery)
	defer ping.Stop()
	refresh := time.NewTicker(s.MetaEvery)
	defer refresh.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ping.C:
			if err := c.Write(ctx, websocket.MessageText, []byte(`{"method":"ping"}`)); err != nil {
				return err
			}
		case <-refresh.C:
			meta, err := s.fetchMeta(ctx)
			if err != nil {
				slog.Warn("outcome list refresh failed", "error", err)
				continue
			}
			next, err := s.syncMarkets(ctx, meta)
			if err != nil {
				return err
			}
			now := map[string]bool{}
			for _, n := range next {
				now[n] = true
			}
			var add, drop []string
			for _, n := range next {
				if !have[n] {
					add = append(add, n)
				}
			}
			for h := range have {
				if !now[h] {
					drop = append(drop, h)
				}
			}
			slices.Sort(drop)
			if err := s.change(ctx, c, "unsubscribe", drop, have); err != nil {
				return err
			}
			if err := s.change(ctx, c, "subscribe", add, have); err != nil {
				return err
			}
		}
	}
}

func (s *Stream) change(ctx context.Context, c *websocket.Conn, method string, coins []string, have map[string]bool) error {
	for _, coin := range coins {
		msg := fmt.Sprintf(`{"method":%q,"subscription":{"type":"trades","coin":%q}}`, method, coin)
		if err := c.Write(ctx, websocket.MessageText, []byte(msg)); err != nil {
			return err
		}
		if method == "subscribe" {
			have[coin] = true
		} else {
			delete(have, coin)
		}
		s.Metrics.coins.Set(float64(len(have)))
		if err := httpx.Sleep(ctx, s.SendEvery); err != nil {
			return err
		}
	}
	return nil
}

func (s *Stream) read(ctx context.Context, c *websocket.Conn) error {
	for {
		_, b, err := c.Read(ctx)
		if err != nil {
			return err
		}
		s.lastMsg.Store(time.Now().UnixNano())
		var m struct {
			Channel string         `json:"channel"`
			Data    jsontext.Value `json:"data"`
		}
		if err := json.Unmarshal(b, &m); err != nil {
			slog.Warn("unreadable message", "error", err)
			continue
		}
		switch m.Channel {
		case "trades":
			var rows []Trade
			if err := json.Unmarshal(m.Data, &rows); err != nil {
				slog.Warn("unreadable trades", "error", err)
				continue
			}
			if _, err := s.store(ctx, rows); err != nil {
				return err
			}
		case "error":
			slog.Warn("venue error message", "data", string(m.Data))
		}
	}
}

// store writes the side-0 fills of a page and returns the converted trades, oldest first
func (s *Stream) store(ctx context.Context, rows []Trade) ([]venue.Trade, error) {
	out := make([]venue.Trade, 0, len(rows))
	for _, r := range rows {
		if !strings.HasSuffix(r.Coin, "0") {
			continue
		}
		t, err := Convert(r)
		if err != nil {
			s.Metrics.skipped.Inc()
			slog.Warn("trade skipped", "coin", r.Coin, "tid", r.TID, "error", err)
			continue
		}
		out = append(out, t)
	}
	n, err := s.Store.InsertTrades(ctx, out)
	s.Metrics.trades.Add(float64(n))
	slices.SortFunc(out, func(a, b venue.Trade) int { return a.TS.Compare(b.TS) })
	return out, err
}

// fill asks for the recent page of each coin that had a stored tape, paced under the weight cap, and counts a gap when the page starts after the last stored trade
func (s *Stream) fill(ctx context.Context, prev map[string]time.Time) {
	for _, coin := range slices.SortedFunc(maps.Keys(prev), func(a, b string) int { return prev[b].Compare(prev[a]) }) {
		var rows []Trade
		if err := s.HTTP.Post(ctx, s.InfoURL, map[string]string{"type": "recentTrades", "coin": coin}, &rows); err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("catch-up failed", "coin", coin, "error", err)
			continue
		}
		got, err := s.store(ctx, rows)
		if err != nil {
			slog.Warn("catch-up store failed", "coin", coin, "error", err)
			return
		}
		if len(got) > 0 && got[0].TS.After(prev[coin]) {
			s.addGap(ctx, coin)
		}
		pause := time.Duration(20+len(rows)/20) * time.Minute / time.Duration(s.GapWeight)
		if httpx.Sleep(ctx, pause) != nil {
			return
		}
	}
}

// addGap keeps the running count in the cursor table so the API can report it
func (s *Stream) addGap(ctx context.Context, coin string) {
	s.Metrics.gaps.Inc()
	s.gapMu.Lock()
	defer s.gapMu.Unlock()
	v, err := s.Store.Cursor(ctx, gapsCursor)
	if err != nil {
		slog.Warn("gap count read failed", "error", err)
		return
	}
	n, _ := strconv.Atoi(v)
	if err := s.Store.SetCursor(ctx, gapsCursor, strconv.Itoa(n+1)); err != nil {
		slog.Warn("gap count write failed", "error", err)
	}
	slog.Warn("gap in the tape", "coin", coin)
}

// TradesStored and Gaps expose the counters to tests
func (m *Metrics) TradesStored() prometheus.Counter { return m.trades }

func (m *Metrics) Gaps() prometheus.Counter { return m.gaps }
