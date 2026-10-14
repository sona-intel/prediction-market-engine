// Package live keeps a small in-memory copy of Polymarket's busiest markets and largest recent trades
package live

import (
	"context"
	"encoding/json/v2"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"time"

	"pm/internal/httpx"
)

const (
	GammaURL = "https://gamma-api.polymarket.com/events"
	DataURL  = "https://data-api.polymarket.com/trades"

	MinTradeUSD = 10000

	userAgent    = "sona-intel-pm-api/1.0"
	eventCount   = 50
	tradeCount   = 200
	marketsEach  = 10
	pollEvery    = 60 * time.Second
	clientWindow = 10 * time.Second
)

type Market struct {
	Question      string
	OutcomePrices []float64
}

type Event struct {
	ID             string
	Slug           string
	Title          string
	Volume24hUSD   float64
	VolumeTotalUSD *float64
	LiquidityUSD   float64
	EndDate        string
	Markets        []Market
}

type Trade struct {
	Tx       string
	Time     time.Time
	Wallet   string
	Question string
	Side     string
	Outcome  string
	Size     float64
	Price    float64
	USD      float64
}

type Snapshot[T any] struct {
	Items     []T
	FetchedAt time.Time
	Stale     bool
}

type Options struct {
	GammaURL string
	DataURL  string
	Interval time.Duration
	Client   *http.Client
	Now      func() time.Time
}

type Poller struct {
	opt Options
	cli *httpx.Client

	mu     sync.RWMutex
	events *Snapshot[Event]
	trades *Snapshot[Trade]
}

func New(opt Options) *Poller {
	if opt.GammaURL == "" {
		opt.GammaURL = GammaURL
	}
	if opt.DataURL == "" {
		opt.DataURL = DataURL
	}
	if opt.Interval <= 0 {
		opt.Interval = pollEvery
	}
	if opt.Client == nil {
		opt.Client = &http.Client{Timeout: clientWindow}
	}
	if opt.Now == nil {
		opt.Now = time.Now
	}
	return &Poller{opt: opt, cli: &httpx.Client{HTTP: opt.Client, UserAgent: userAgent}}
}

func (p *Poller) Run(ctx context.Context) {
	p.Poll(ctx)
	t := time.NewTicker(p.opt.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.Poll(ctx)
		}
	}
}

// Poll fetches the two feeds on their own so one failing never costs the other its copy
func (p *Poller) Poll(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Go(func() {
		items, err := p.fetchEvents(ctx)
		p.mu.Lock()
		defer p.mu.Unlock()
		p.events = settle(p.events, items, err, p.opt.Now(), "gamma events")
	})
	wg.Go(func() {
		items, err := p.fetchTrades(ctx)
		p.mu.Lock()
		defer p.mu.Unlock()
		p.trades = settle(p.trades, items, err, p.opt.Now(), "data trades")
	})
	wg.Wait()
}

func settle[T any](prev *Snapshot[T], items []T, err error, now time.Time, feed string) *Snapshot[T] {
	if err == nil {
		return &Snapshot[T]{Items: items, FetchedAt: now}
	}
	slog.Warn("polymarket poll failed", "feed", feed, "error", err)
	if prev == nil {
		return nil
	}
	kept := *prev
	kept.Stale = true
	return &kept
}

func (p *Poller) Events() (Snapshot[Event], bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.events == nil {
		return Snapshot[Event]{}, false
	}
	return *p.events, true
}

func (p *Poller) Trades() (Snapshot[Trade], bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.trades == nil {
		return Snapshot[Trade]{}, false
	}
	return *p.trades, true
}

/* Fetching */

type gammaEvent struct {
	ID        string        `json:"id"`
	Slug      string        `json:"slug"`
	Title     string        `json:"title"`
	Volume24h float64       `json:"volume24hr"`
	Volume    *float64      `json:"volume"`
	Liquidity float64       `json:"liquidity"`
	EndDate   string        `json:"endDate"`
	Markets   []gammaMarket `json:"markets"`
}

type gammaMarket struct {
	Question      string `json:"question"`
	Closed        bool   `json:"closed"`
	OutcomePrices string `json:"outcomePrices"`
}

func (p *Poller) fetchEvents(ctx context.Context) ([]Event, error) {
	q := url.Values{
		"active":    {"true"},
		"closed":    {"false"},
		"order":     {"volume24hr"},
		"ascending": {"false"},
		"limit":     {strconv.Itoa(eventCount)},
	}
	var raw []gammaEvent
	if err := p.cli.Get(ctx, p.opt.GammaURL, q, &raw); err != nil {
		return nil, err
	}
	out := make([]Event, 0, len(raw))
	for _, e := range raw {
		ev := Event{
			ID: e.ID, Slug: e.Slug, Title: e.Title, EndDate: e.EndDate,
			Volume24hUSD: httpx.Finite(e.Volume24h), LiquidityUSD: httpx.Finite(e.Liquidity),
			Markets: []Market{},
		}
		if e.Volume != nil {
			v := httpx.Finite(*e.Volume)
			ev.VolumeTotalUSD = &v
		}
		for _, m := range e.Markets {
			if m.Closed || len(ev.Markets) == marketsEach {
				continue
			}
			ev.Markets = append(ev.Markets, Market{Question: m.Question, OutcomePrices: parsePrices(m.OutcomePrices)})
		}
		out = append(out, ev)
	}
	return out, nil
}

// parsePrices reads the JSON array that Gamma sends inside a string
func parsePrices(s string) []float64 {
	var strs []string
	prices := []float64{}
	if json.Unmarshal([]byte(s), &strs) != nil {
		return prices
	}
	for _, v := range strs {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return []float64{}
		}
		prices = append(prices, httpx.Finite(f))
	}
	return prices
}

type dataTrade struct {
	Wallet  string  `json:"proxyWallet"`
	Side    string  `json:"side"`
	Size    float64 `json:"size"`
	Price   float64 `json:"price"`
	Time    int64   `json:"timestamp"`
	Title   string  `json:"title"`
	Outcome string  `json:"outcome"`
	Tx      string  `json:"transactionHash"`
}

func (p *Poller) fetchTrades(ctx context.Context) ([]Trade, error) {
	q := url.Values{
		"filterType":   {"CASH"},
		"filterAmount": {strconv.Itoa(MinTradeUSD)},
		"limit":        {strconv.Itoa(tradeCount)},
	}
	var raw []dataTrade
	if err := p.cli.Get(ctx, p.opt.DataURL, q, &raw); err != nil {
		return nil, err
	}
	out := make([]Trade, 0, len(raw))
	for _, t := range raw {
		usd := math.Round(httpx.Finite(t.Size)*httpx.Finite(t.Price)*100) / 100
		out = append(out, Trade{
			Tx: t.Tx, Time: time.Unix(t.Time, 0).UTC(), Wallet: t.Wallet, Question: t.Title,
			Side: t.Side, Outcome: t.Outcome, Size: httpx.Finite(t.Size), Price: httpx.Finite(t.Price), USD: usd,
		})
	}
	// the newest come first whatever order the upstream used
	slices.SortStableFunc(out, func(a, b Trade) int { return b.Time.Compare(a.Time) })
	return out, nil
}
