// Package kalshi records Kalshi from its keyless API, which publishes no trader ids, so it keeps the catalogue, snapshots, large prints and results
package kalshi

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"pm/internal/httpx"
	"pm/internal/venue"
)

const (
	pageSize = 1000
	maxPages = 300
	// tradePages bounds one trades poll, and a poll that hits it has left a gap
	tradePages = 30
	// tradeOverlap rewinds the trades cursor so prints that landed late are still read
	tradeOverlap = 5 * time.Second
	settlePages  = 50

	// comboPrefix marks multi-leg combo tickers, which the market sweep excludes so their prints are dropped too
	comboPrefix   = "KXMVE"
	tradesCursor  = "kalshi/trades"
	settledCursor = "kalshi/settled"
	gapsCursor    = "kalshi/gaps"
)

type Market struct {
	Ticker      string `json:"ticker"`
	EventTicker string `json:"event_ticker"`
	Title       string `json:"title"`
	YesSub      string `json:"yes_sub_title"`
	Status      string `json:"status"`
	Result      string `json:"result"`
	CloseTime   string `json:"close_time"`
	SettledAt   string `json:"settlement_ts"`
	LastPrice   string `json:"last_price_dollars"`
	YesBid      string `json:"yes_bid_dollars"`
	YesAsk      string `json:"yes_ask_dollars"`
	Volume      string `json:"volume_fp"`
	Volume24h   string `json:"volume_24h_fp"`
	OpenInt     string `json:"open_interest_fp"`
}

type marketPage struct {
	Markets []Market `json:"markets"`
	Cursor  string   `json:"cursor"`
}

type Trade struct {
	ID        string `json:"trade_id"`
	Ticker    string `json:"ticker"`
	Created   string `json:"created_time"`
	Count     string `json:"count_fp"`
	YesPrice  string `json:"yes_price_dollars"`
	NoPrice   string `json:"no_price_dollars"`
	TakerSide string `json:"taker_side"`
}

type tradePage struct {
	Trades []Trade `json:"trades"`
	Cursor string  `json:"cursor"`
}

type Metrics struct {
	snaps   prometheus.Counter
	trades  prometheus.Counter
	skipped prometheus.Counter
	gaps    prometheus.Counter
	settled prometheus.Counter
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		snaps:   prometheus.NewCounter(prometheus.CounterOpts{Name: "pm_kalshi_snaps_stored_total", Help: "New Kalshi snapshots stored"}),
		trades:  prometheus.NewCounter(prometheus.CounterOpts{Name: "pm_kalshi_trades_stored_total", Help: "New large Kalshi trades stored"}),
		skipped: prometheus.NewCounter(prometheus.CounterOpts{Name: "pm_kalshi_rows_skipped_total", Help: "Kalshi rows dropped as unusable"}),
		gaps:    prometheus.NewCounter(prometheus.CounterOpts{Name: "pm_kalshi_gaps_total", Help: "Trade polls that hit the page cap before reaching the cursor"}),
		settled: prometheus.NewCounter(prometheus.CounterOpts{Name: "pm_kalshi_settled_total", Help: "Kalshi markets whose result was recorded"}),
	}
	reg.MustRegister(m.snaps, m.trades, m.skipped, m.gaps, m.settled)
	return m
}

func (m *Metrics) Trades() prometheus.Counter { return m.trades }

func (m *Metrics) Gaps() prometheus.Counter { return m.gaps }

func (m *Metrics) Skipped() prometheus.Counter { return m.skipped }

// Client reads the API through one limiter, since the venue's keyless limit is undocumented
type Client struct {
	HTTP    *httpx.Client
	BaseURL string
}

func (c *Client) markets(ctx context.Context, q url.Values) (marketPage, error) {
	var p marketPage
	err := c.HTTP.Get(ctx, c.BaseURL+"/markets", q, &p)
	return p, err
}

func (c *Client) trades(ctx context.Context, q url.Values) (tradePage, error) {
	var p tradePage
	err := c.HTTP.Get(ctx, c.BaseURL+"/markets/trades", q, &p)
	return p, err
}

func dollars(s string) (float64, bool) {
	f, err := strconv.ParseFloat(s, 64)
	return f, err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) && f >= 0
}

// micro reads a count or dollar amount, and a contract is worth one dollar at settlement so counts are notional dollars
func micro(s string) int64 {
	f, ok := dollars(s)
	if !ok || f > math.MaxInt64/venue.Micro/2 {
		return 0
	}
	return int64(math.Round(f * venue.Micro))
}

func ppm(p float64) int32 { return int32(math.Round(min(p, 1) * venue.PPM)) }

func series(event string) string {
	s, _, _ := strings.Cut(event, "-")
	return s
}

func status(s string) string {
	switch s {
	case "active", "open", "initialized":
		return "open"
	case "settled", "finalized", "determined":
		return "settled"
	}
	return strings.ToLower(s)
}

func (m Market) Venue() venue.Market {
	title := m.Title
	if m.YesSub != "" && !strings.Contains(m.Title, m.YesSub) {
		title += ": " + m.YesSub
	}
	out := venue.Market{Venue: venue.Kalshi, ID: m.Ticker, Title: title, Kind: series(m.EventTicker), Status: status(m.Status), Result: m.Result}
	if t, err := time.Parse(time.RFC3339, m.CloseTime); err == nil {
		out.Close = t.UTC()
	}
	return out
}

// Snap turns a market into a snapshot priced at the last trade, or at the middle of the book when nothing has traded
func (m Market) Snap(at time.Time) (venue.Snap, bool) {
	p, ok := dollars(m.LastPrice)
	if !ok || p == 0 || p > 1 {
		bid, okBid := dollars(m.YesBid)
		ask, okAsk := dollars(m.YesAsk)
		p, ok = (bid+ask)/2, okBid && okAsk && ask > 0 && ask <= 1
	}
	if !ok {
		return venue.Snap{}, false
	}
	return venue.Snap{
		Venue: venue.Kalshi, MarketID: m.Ticker, TS: at.UTC().Truncate(time.Second), YesPPM: ppm(p),
		Volume: micro(m.Volume), OI: micro(m.OpenInt), Vol24: micro(m.Volume24h),
	}, true
}

// Convert turns a print into a stored trade priced at what the taker paid, and reports false for rows with unusable numbers
func (t Trade) Convert() (venue.Trade, bool) {
	count, ok := dollars(t.Count)
	yes, okYes := dollars(t.YesPrice)
	no, okNo := dollars(t.NoPrice)
	if !ok || !okYes || !okNo || count <= 0 || yes > 1 || no > 1 || t.ID == "" || t.Ticker == "" {
		return venue.Trade{}, false
	}
	paid := yes
	switch t.TakerSide {
	case "yes":
	case "no":
		paid = no
	default:
		return venue.Trade{}, false
	}
	at, err := time.Parse(time.RFC3339, t.Created)
	if err != nil || count > math.MaxInt64/venue.Micro/2 {
		return venue.Trade{}, false
	}
	return venue.Trade{
		Venue: venue.Kalshi, ID: t.ID, MarketID: t.Ticker, TS: at.UTC(), Side: t.TakerSide, PricePPM: ppm(yes),
		Qty: int64(math.Round(count * venue.Micro)), USD: int64(math.Round(count * paid * venue.Micro)),
	}, true
}

// Catalogue sweeps every open market, snapshots the busiest ones and marks markets that left the list
type Catalogue struct {
	Client  *Client
	Store   venue.Store
	Top     int
	Metrics *Metrics
	Now     func() time.Time
}

func (c *Catalogue) Name() string { return venue.Kalshi + "/markets" }

func (c *Catalogue) Poll(ctx context.Context) error {
	now := time.Now
	if c.Now != nil {
		now = c.Now
	}
	at := now()
	seen := map[string]bool{}
	var snaps []venue.Snap
	cursor := ""
	for page := range maxPages {
		q := url.Values{"status": {"open"}, "mve_filter": {"exclude"}, "limit": {strconv.Itoa(pageSize)}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		p, err := c.Client.markets(ctx, q)
		if err != nil {
			return fmt.Errorf("page %d: %w", page, err)
		}
		ms := make([]venue.Market, 0, len(p.Markets))
		for _, m := range p.Markets {
			if m.Ticker == "" {
				c.Metrics.skipped.Inc()
				continue
			}
			seen[m.Ticker] = true
			ms = append(ms, m.Venue())
			if s, ok := m.Snap(at); ok && (s.Volume > 0 || s.Vol24 > 0) {
				snaps = append(snaps, s)
			}
		}
		if err := c.Store.UpsertMarkets(ctx, ms); err != nil {
			return err
		}
		if cursor = p.Cursor; cursor == "" || len(p.Markets) == 0 {
			break
		}
	}
	slices.SortFunc(snaps, func(a, b venue.Snap) int {
		return cmp.Or(cmp.Compare(b.Vol24, a.Vol24), cmp.Compare(b.Volume, a.Volume))
	})
	n, err := c.Store.InsertSnaps(ctx, snaps[:min(c.Top, len(snaps))])
	c.Metrics.snaps.Add(float64(n))
	if err != nil {
		return err
	}
	if cursor != "" {
		return nil
	}
	return c.closeGone(ctx, seen)
}

// closeGone runs only after a full sweep, since a cut-off sweep would close markets that are still listed
func (c *Catalogue) closeGone(ctx context.Context, seen map[string]bool) error {
	ids, err := c.Store.OpenMarketIDs(ctx, venue.Kalshi)
	if err != nil {
		return err
	}
	gone := slices.DeleteFunc(ids, func(id string) bool { return seen[id] })
	if len(gone) == 0 {
		return nil
	}
	slog.Info("kalshi markets left the open list", "markets", len(gone))
	return c.Store.MarkClosed(ctx, venue.Kalshi, gone)
}

// Trades follows the print tape and keeps the prints worth at least MinUSD
type Trades struct {
	Client  *Client
	Store   venue.Sink
	MinUSD  float64
	Metrics *Metrics
	Now     func() time.Time
}

func (t *Trades) Name() string { return venue.Kalshi + "/trades" }

func (t *Trades) Poll(ctx context.Context) error {
	now := time.Now
	if t.Now != nil {
		now = t.Now
	}
	since := now().Add(-tradeOverlap)
	if v, err := t.Store.Cursor(ctx, tradesCursor); err != nil {
		return err
	} else if at, err := time.Parse(time.RFC3339Nano, v); err == nil {
		since = at.Add(-tradeOverlap)
	}
	floor := int64(t.MinUSD * venue.Micro)
	var keep []venue.Trade
	var newest time.Time
	cursor, gapped := "", false
	for page := range tradePages {
		q := url.Values{"limit": {strconv.Itoa(pageSize)}, "min_ts": {strconv.FormatInt(since.Unix(), 10)}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		p, err := t.Client.trades(ctx, q)
		if err != nil {
			return fmt.Errorf("page %d: %w", page, err)
		}
		for _, r := range p.Trades {
			if strings.HasPrefix(r.Ticker, comboPrefix) {
				continue
			}
			v, ok := r.Convert()
			if !ok {
				t.Metrics.skipped.Inc()
				continue
			}
			newest = maxTime(newest, v.TS)
			if v.USD >= floor {
				keep = append(keep, v)
			}
		}
		if cursor = p.Cursor; cursor == "" || len(p.Trades) == 0 {
			break
		}
		if page == tradePages-1 {
			t.Metrics.gaps.Inc()
			gapped = true
		}
	}
	n, err := t.Store.InsertTrades(ctx, keep)
	t.Metrics.trades.Add(float64(n))
	if err != nil || newest.IsZero() {
		return err
	}
	if gapped {
		if err := t.addGap(ctx); err != nil {
			slog.Warn("kalshi gap count write failed", "error", err)
		}
	}
	return t.Store.SetCursor(ctx, tradesCursor, newest.Format(time.RFC3339Nano))
}

// addGap keeps the running count in the cursor table so the API can report it
func (t *Trades) addGap(ctx context.Context) error {
	v, err := t.Store.Cursor(ctx, gapsCursor)
	if err != nil {
		return err
	}
	n, _ := strconv.Atoi(v)
	return t.Store.SetCursor(ctx, gapsCursor, strconv.Itoa(n+1))
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// Settlements copies the result of markets that settled since the last poll onto the markets already recorded
type Settlements struct {
	Client  *Client
	Store   venue.Store
	Metrics *Metrics
	Now     func() time.Time
}

func (s *Settlements) Name() string { return venue.Kalshi + "/settled" }

func (s *Settlements) Poll(ctx context.Context) error {
	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	since := now().Add(-10 * time.Minute).Unix()
	if v, err := s.Store.Cursor(ctx, settledCursor); err != nil {
		return err
	} else if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		since = n
	}
	newest := since
	cursor := ""
	for page := range settlePages {
		q := url.Values{
			"status": {"settled"}, "mve_filter": {"exclude"}, "limit": {strconv.Itoa(pageSize)}, "min_settled_ts": {strconv.FormatInt(since, 10)},
		}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		p, err := s.Client.markets(ctx, q)
		if err != nil {
			return fmt.Errorf("page %d: %w", page, err)
		}
		for _, m := range p.Markets {
			if at, err := time.Parse(time.RFC3339, m.SettledAt); err == nil {
				newest = max(newest, at.Unix())
			}
			if m.Result == "" {
				s.Metrics.skipped.Inc()
				continue
			}
			if err := s.Store.SetResult(ctx, venue.Kalshi, m.Ticker, "settled", m.Result); err != nil {
				return err
			}
			s.Metrics.settled.Inc()
		}
		if cursor = p.Cursor; cursor == "" || len(p.Markets) == 0 {
			break
		}
	}
	return s.Store.SetCursor(ctx, settledCursor, strconv.FormatInt(newest, 10))
}
