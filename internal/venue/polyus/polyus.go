// Package polyus records Polymarket US from its keyless gateway, which has no trade tape, so it keeps the catalogue, quotes and settlements
package polyus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"pm/internal/httpx"
	"pm/internal/venue"
)

const (
	pageSize = 500
	maxPages = 400
)

type Market struct {
	Slug       string `json:"slug"`
	Question   string `json:"question"`
	Title      string `json:"title"`
	EndDate    string `json:"endDate"`
	MarketType string `json:"marketType"`
	Status     string `json:"status"`
}

type list struct {
	Markets []Market `json:"markets"`
}

type Quote struct {
	Value string `json:"value"`
}

type BBO struct {
	MarketData struct {
		CurrentPx    *Quote `json:"currentPx"`
		LastTradePx  *Quote `json:"lastTradePx"`
		LongQuote    *Quote `json:"longQuote"`
		BestBid      *Quote `json:"bestBid"`
		BestAsk      *Quote `json:"bestAsk"`
		SharesTraded string `json:"sharesTraded"`
		OpenInterest string `json:"openInterest"`
	} `json:"marketData"`
}

type settlement struct {
	Settlement *float64 `json:"settlement"`
}

type Metrics struct {
	snaps    prometheus.Counter
	failures *prometheus.CounterVec
	settled  prometheus.Counter
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		snaps: prometheus.NewCounter(prometheus.CounterOpts{Name: "pm_polyus_snaps_stored_total", Help: "New Polymarket US snapshots stored"}),
		failures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pm_polyus_request_failures_total", Help: "Polymarket US requests that failed, by feed",
		}, []string{"feed"}),
		settled: prometheus.NewCounter(prometheus.CounterOpts{Name: "pm_polyus_settled_total", Help: "Polymarket US markets whose settlement was recorded"}),
	}
	reg.MustRegister(m.snaps, m.failures, m.settled)
	return m
}

func (m *Metrics) SnapsStored() prometheus.Counter { return m.snaps }

func (m *Metrics) Failures(feed string) prometheus.Counter { return m.failures.WithLabelValues(feed) }

func (m *Metrics) Settled() prometheus.Counter { return m.settled }

// Client reads the gateway through one limiter, since the venue caps requests per address across every path
type Client struct {
	HTTP    *httpx.Client
	BaseURL string
}

func (c *Client) list(ctx context.Context, q url.Values) ([]Market, error) {
	var l list
	err := c.HTTP.Get(ctx, c.BaseURL+"/markets", q, &l)
	return l.Markets, err
}

func (c *Client) bbo(ctx context.Context, slug string) (BBO, error) {
	var b BBO
	err := c.HTTP.Get(ctx, c.BaseURL+"/markets/"+url.PathEscape(slug)+"/bbo", nil, &b)
	return b, err
}

// settled reports the value of a market's settlement, and false while the venue has none
func (c *Client) settled(ctx context.Context, slug string) (float64, bool, error) {
	var s settlement
	err := c.HTTP.Get(ctx, c.BaseURL+"/markets/"+url.PathEscape(slug)+"/settlement", nil, &s)
	if errors.Is(err, httpx.ErrStatus) {
		return 0, false, nil
	}
	if err != nil || s.Settlement == nil {
		return 0, false, err
	}
	return *s.Settlement, true, nil
}

func (m Market) Venue() venue.Market {
	title := m.Question
	if m.Title != "" && !strings.Contains(m.Question, m.Title) {
		title += ": " + m.Title
	}
	out := venue.Market{Venue: venue.PolymarketUS, ID: m.Slug, Title: title, Kind: m.MarketType, Status: "open"}
	if t, err := time.Parse(time.RFC3339, m.EndDate); err == nil {
		out.Close = t.UTC()
	}
	if s := strings.TrimPrefix(m.Status, "MARKET_STATUS_"); s != "OPEN" && s != "" {
		out.Status = strings.ToLower(s)
	}
	return out
}

func micro(s string) int64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || f <= 0 || f > math.MaxInt64/venue.Micro/2 {
		return 0
	}
	return int64(math.Round(f * venue.Micro))
}

func price(q *Quote) (float64, bool) {
	if q == nil {
		return 0, false
	}
	f, err := strconv.ParseFloat(q.Value, 64)
	return f, err == nil && !math.IsNaN(f) && f >= 0 && f <= 1
}

// Snap turns a quote into a snapshot of the long side, and reports false when the venue gave no usable price
func (b BBO) Snap(slug string, at time.Time) (venue.Snap, bool) {
	d := b.MarketData
	p, ok := price(d.CurrentPx)
	if !ok {
		p, ok = price(d.LastTradePx)
	}
	if !ok {
		p, ok = price(d.LongQuote)
	}
	if !ok {
		bid, okBid := price(d.BestBid)
		ask, okAsk := price(d.BestAsk)
		p, ok = (bid+ask)/2, okBid && okAsk
	}
	if !ok {
		return venue.Snap{}, false
	}
	return venue.Snap{
		Venue: venue.PolymarketUS, MarketID: slug, TS: at.UTC().Truncate(time.Second),
		YesPPM: int32(math.Round(p * venue.PPM)), Volume: micro(d.SharesTraded), OI: micro(d.OpenInterest),
	}, true
}

// Catalogue sweeps every open market, which is the one place new markets appear
type Catalogue struct {
	Client *Client
	Store  venue.Sink
}

func (c *Catalogue) Name() string { return venue.PolymarketUS + "/catalogue" }

func (c *Catalogue) Poll(ctx context.Context) error {
	total := 0
	for page := range maxPages {
		ms, err := c.Client.list(ctx, url.Values{
			"active": {"true"}, "closed": {"false"}, "limit": {strconv.Itoa(pageSize)}, "offset": {strconv.Itoa(page * pageSize)},
		})
		if err != nil {
			return fmt.Errorf("page %d: %w", page, err)
		}
		if len(ms) == 0 {
			break
		}
		if err := c.Store.UpsertMarkets(ctx, venueMarkets(ms)); err != nil {
			return err
		}
		total += len(ms)
	}
	slog.Info("polymarket us catalogue swept", "markets", total)
	return nil
}

func venueMarkets(ms []Market) []venue.Market {
	out := make([]venue.Market, 0, len(ms))
	for _, m := range ms {
		if m.Slug != "" {
			out = append(out, m.Venue())
		}
	}
	return out
}

// Quotes snapshots the markets with the most volume
type Quotes struct {
	Client  *Client
	Store   venue.Sink
	Top     int
	Metrics *Metrics
	Now     func() time.Time
}

func (q *Quotes) Name() string { return venue.PolymarketUS + "/quotes" }

func (q *Quotes) top(ctx context.Context) ([]Market, error) {
	var out []Market
	for off := 0; len(out) < q.Top && off < maxPages*pageSize; off += pageSize {
		ms, err := q.Client.list(ctx, url.Values{
			"active": {"true"}, "closed": {"false"}, "orderBy": {"volume"}, "orderDirection": {"desc"},
			"limit": {strconv.Itoa(min(pageSize, q.Top-len(out)))}, "offset": {strconv.Itoa(off)},
		})
		if err != nil {
			return nil, err
		}
		if len(ms) == 0 {
			break
		}
		out = append(out, ms...)
	}
	return out[:min(q.Top, len(out))], nil
}

func (q *Quotes) Poll(ctx context.Context) error {
	ms, err := q.top(ctx)
	if err != nil {
		return err
	}
	if err := q.Store.UpsertMarkets(ctx, venueMarkets(ms)); err != nil {
		return err
	}
	now := q.Now
	if now == nil {
		now = time.Now
	}
	var snaps []venue.Snap
	failed := 0
	for _, m := range ms {
		b, err := q.Client.bbo(ctx, m.Slug)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			failed++
			q.Metrics.Failures("bbo").Inc()
			slog.Warn("quote failed", "market", m.Slug, "error", err)
			continue
		}
		if s, ok := b.Snap(m.Slug, now()); ok {
			snaps = append(snaps, s)
		}
	}
	n, err := q.Store.InsertSnaps(ctx, snaps)
	q.Metrics.snaps.Add(float64(n))
	if err != nil {
		return err
	}
	if failed > 0 && failed == len(ms) {
		return fmt.Errorf("all %d quotes failed", failed)
	}
	return nil
}

// Settlements records the outcome of markets past their end date, using the venue's settlement flag since the quote's settlement price also shows on open markets
type Settlements struct {
	Client  *Client
	Store   venue.Store
	Batch   int
	Metrics *Metrics
	Now     func() time.Time
}

func (s *Settlements) Name() string { return venue.PolymarketUS + "/settlement" }

func (s *Settlements) Poll(ctx context.Context) error {
	now := s.Now
	if now == nil {
		now = time.Now
	}
	ids, err := s.Store.ExpiredMarketIDs(ctx, venue.PolymarketUS, now(), s.Batch)
	if err != nil {
		return err
	}
	for _, id := range ids {
		v, ok, err := s.Client.settled(ctx, id)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			s.Metrics.Failures("settlement").Inc()
			slog.Warn("settlement lookup failed", "market", id, "error", err)
			continue
		}
		if !ok {
			continue
		}
		if err := s.Store.SetResult(ctx, venue.PolymarketUS, id, "resolved", result(v)); err != nil {
			return err
		}
		s.Metrics.settled.Inc()
	}
	return nil
}

func result(v float64) string {
	switch v {
	case 1:
		return "yes"
	case 0:
		return "no"
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}
