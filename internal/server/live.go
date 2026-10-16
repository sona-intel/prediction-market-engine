package server

import (
	"context"
	"encoding/json/v2"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"pm/internal/live"
)

const (
	topDefault, topMax     = 20, 50
	largeDefault, largeMax = 50, 100
	sourceGamma            = "polymarket-gamma"
	sourceData             = "polymarket-data"
	notPolledRetryS        = 10
)

var errNotPolled = errors.New("the first poll has not finished")

type Feed interface {
	Events() (live.Snapshot[live.Event], bool)
	Trades() (live.Snapshot[live.Trade], bool)
}

type topMarkets struct {
	Kind      string     `json:"kind"`
	Items     []topEvent `json:"items"`
	FetchedAt string     `json:"fetched_at"`
	Stale     bool       `json:"stale"`
	Source    string     `json:"source"`
}

type topEvent struct {
	EventID        string      `json:"event_id"`
	Slug           string      `json:"slug"`
	Title          string      `json:"title"`
	Volume24hUSD   float64     `json:"volume_24h_usd"`
	VolumeTotalUSD *float64    `json:"volume_total_usd"`
	LiquidityUSD   float64     `json:"liquidity_usd"`
	EndDate        *string     `json:"end_date"`
	Markets        []topMarket `json:"markets"`
}

type topMarket struct {
	Question      string    `json:"question"`
	Label         string    `json:"label"`
	Outcomes      []string  `json:"outcomes"`
	OutcomePrices []float64 `json:"outcome_prices"`
}

type largeTrades struct {
	Kind      string       `json:"kind"`
	Items     []largeTrade `json:"items"`
	FetchedAt string       `json:"fetched_at"`
	Stale     bool         `json:"stale"`
	Source    string       `json:"source"`
}

type largeTrade struct {
	Tx       string  `json:"tx"`
	Time     string  `json:"time"`
	Wallet   string  `json:"wallet"`
	Question string  `json:"question"`
	Side     string  `json:"side"`
	Outcome  string  `json:"outcome"`
	Size     float64 `json:"size"`
	Price    float64 `json:"price"`
	USD      float64 `json:"usd"`
}

func (a *app) topMarkets(w http.ResponseWriter, req *http.Request) {
	code, err := venueParam(req.URL.Query())
	if err != nil {
		writeProblem(w, problemFor(err, req.URL.EscapedPath()))
		return
	}
	if code != venuePolymarket {
		a.venueTop(w, req, code)
		return
	}
	respondLive(w, req, func() (any, bool, error) {
		limit, err := intParam(req.URL.Query(), "limit", topDefault, 1, topMax)
		if err != nil {
			return nil, false, err
		}
		snap, ok := a.events()
		if !ok {
			return nil, false, errNotPolled
		}
		out := topMarkets{
			Kind: "pm_top_markets", Items: []topEvent{}, FetchedAt: snap.FetchedAt.UTC().Format(time.RFC3339),
			Stale: snap.Stale, Source: sourceGamma,
		}
		for _, e := range snap.Items[:min(limit, len(snap.Items))] {
			ev := topEvent{
				EventID: e.ID, Slug: e.Slug, Title: e.Title, Volume24hUSD: e.Volume24hUSD, VolumeTotalUSD: e.VolumeTotalUSD,
				LiquidityUSD: e.LiquidityUSD, Markets: make([]topMarket, 0, len(e.Markets)),
			}
			if e.EndDate != "" {
				ev.EndDate = &e.EndDate
			}
			for _, m := range e.Markets {
				ev.Markets = append(ev.Markets, topMarket{Question: m.Question, Label: m.Label, Outcomes: m.Outcomes, OutcomePrices: m.OutcomePrices})
			}
			out.Items = append(out.Items, ev)
		}
		return out, snap.Stale, nil
	})
}

func (a *app) largeTrades(w http.ResponseWriter, req *http.Request) {
	code, err := venueParam(req.URL.Query())
	if err != nil {
		writeProblem(w, problemFor(err, req.URL.EscapedPath()))
		return
	}
	if code != venuePolymarket {
		a.venueLarge(w, req, code)
		return
	}
	respondLive(w, req, func() (any, bool, error) {
		q := req.URL.Query()
		limit, err := intParam(q, "limit", largeDefault, 1, largeMax)
		if err != nil {
			return nil, false, err
		}
		minUSD, err := minUSDParam(q, live.MinTradeUSD)
		if err != nil {
			return nil, false, err
		}
		snap, ok := a.trades()
		if !ok {
			return nil, false, errNotPolled
		}
		out := largeTrades{
			Kind: "pm_large_trades", Items: []largeTrade{}, FetchedAt: snap.FetchedAt.UTC().Format(time.RFC3339),
			Stale: snap.Stale, Source: sourceData,
		}
		for _, t := range snap.Items {
			if t.USD < minUSD {
				continue
			}
			if len(out.Items) == limit {
				break
			}
			out.Items = append(out.Items, largeTrade{
				Tx: t.Tx, Time: t.Time.UTC().Format(time.RFC3339), Wallet: t.Wallet, Question: t.Question,
				Side: t.Side, Outcome: t.Outcome, Size: t.Size, Price: t.Price, USD: t.USD,
			})
		}
		return out, snap.Stale, nil
	})
}

func (a *app) events() (live.Snapshot[live.Event], bool) {
	if a.feed == nil {
		return live.Snapshot[live.Event]{}, false
	}
	return a.feed.Events()
}

func (a *app) trades() (live.Snapshot[live.Trade], bool) {
	if a.feed == nil {
		return live.Snapshot[live.Trade]{}, false
	}
	return a.feed.Trades()
}

func minUSDParam(q url.Values, floor float64) (float64, error) {
	if !q.Has("min_usd") {
		return floor, nil
	}
	v, err := strconv.ParseFloat(q.Get("min_usd"), 64)
	// ParseFloat accepts NaN and Inf, which no bound would catch
	if err != nil || !(v >= floor) || v > 1e12 {
		return 0, &requestError{codeInvalidQuery, "The min_usd must be a number from " + strconv.FormatFloat(floor, 'f', -1, 64) + " to 1000000000000."}
	}
	return v, nil
}

// respondLive answers from memory, so a deadline only matters when it is already spent
func respondLive(w http.ResponseWriter, req *http.Request, build func() (body any, stale bool, err error)) {
	if budget, ok := deadline(req.Header); ok && budget <= 0 {
		writeProblem(w, problemFor(context.DeadlineExceeded, req.URL.EscapedPath()))
		return
	}
	body, stale, err := build()
	if err == nil {
		var data []byte
		if data, err = json.Marshal(body); err == nil {
			h := w.Header()
			if stale {
				h.Set("X-Sona-Cache", "stale")
			}
			h.Set("Content-Type", "application/json")
			h.Set("Content-Length", strconv.Itoa(len(data)))
			_, _ = w.Write(data)
			return
		}
	}
	p := problemFor(err, req.URL.EscapedPath())
	if errors.Is(err, errNotPolled) {
		// the poller runs every 60 s so a client gains nothing from retrying sooner than 10 s
		p.RetryAfterS = notPolledRetryS
	}
	writeProblem(w, p)
}
