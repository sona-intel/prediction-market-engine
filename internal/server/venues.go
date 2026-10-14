package server

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"pm/internal/hip4"
	"pm/internal/levr"
	"pm/internal/pgstore"
	"pm/internal/venue"
)

const (
	venuePolymarket = "polymarket"

	sourceKalshi    = "kalshi-api"
	sourcePolyUS    = "polymarket-us-gateway"
	sourceHyperInfo = "hyperliquid-info"
	sourceLevr      = "levr-squid"

	historyDefault, historyMax = 200, 1000
	historyWindow              = 24 * time.Hour
	tapeBucket                 = 5 * time.Minute
	tapeWindow                 = 24 * time.Hour
)

var errVenueDown = errors.New("the venue database is not available")

// Venues is the read side of pm-postgres, and a nil value answers 503 on every venue route
type Venues interface {
	Venues(ctx context.Context) ([]venue.Info, error)
	Gaps(ctx context.Context, code string) (int64, bool, error)
	TopRecent(ctx context.Context, code string, limit int) ([]pgstore.TopMarket, error)
	TopTape(ctx context.Context, code string, since time.Time, limit int) ([]pgstore.TopMarket, error)
	LargeNamed(ctx context.Context, code string, minUSD int64, limit int) ([]pgstore.NamedTrade, error)
	Market(ctx context.Context, code, id string) (venue.Market, bool, error)
	Snaps(ctx context.Context, code, id string, since time.Time, limit int) ([]venue.Snap, error)
	TapeSeries(ctx context.Context, code, id string, since time.Time, bucket time.Duration, limit int) ([]venue.Snap, error)
	HLWallet(ctx context.Context, addr []byte) (hip4.Wallet, error)
	HLBoard(ctx context.Context, board string, limit, offset int) (pgstore.HLPage, error)
	WalletPositions(ctx context.Context, wallet []byte) ([]levr.Position, error)
	LevrBoard(ctx context.Context, board string, limit, offset int) (pgstore.LevrPage, error)
}

type venueFacts struct{ mode, source, note string }

var venueFact = map[string]venueFacts{
	venue.Hyperliquid:  {"recorded", sourceHyperInfo, "Every Hyperliquid HIP-4 outcome fill since recording started, read from the public WebSocket, with both wallets. A gap is a reconnect whose catch-up page did not reach the stored tape. Results appear once a settlement fill is found, so some settled outcomes may still show no result."},
	venue.Kalshi:       {"partial_tape", sourceKalshi, "Market snapshots of the 500 markets with the most 24-hour volume, and only the prints that cost the taker at least $10,000. Kalshi publishes no trader ids. History starts when recording started."},
	venue.PolymarketUS: {"snapshot_only", sourcePolyUS, "Market snapshots of the busiest markets. The gateway has no trade tape and no trader ids. Volume and open interest are share counts valued in dollars. History starts when recording started."},
	venue.Levr:         {"recorded", sourceLevr, "Leveraged positions, liquidations and settlements of Levr Bet on Monad, read from the venue's unofficial indexer. Payouts are worked out from redemption rows, and void refunds and the partial return of a liquidation are not published. Positions opened before recording started are missing."},
}

var venueNames = map[string]string{venuePolymarket: "Polymarket"}

func venueParam(q url.Values) (string, error) {
	if !q.Has("venue") {
		return venuePolymarket, nil
	}
	v := q.Get("venue")
	if v != venuePolymarket && !slices.Contains(recordedVenues, v) {
		return "", &requestError{codeInvalidQuery, "The venue must be polymarket, kalshi, polymarket-us, hyperliquid or levr."}
	}
	return v, nil
}

var recordedVenues = []string{venue.Kalshi, venue.PolymarketUS, venue.Hyperliquid, venue.Levr}

// onlyVenues rejects a venue that the route cannot serve
func onlyVenues(code, detail string, allowed ...string) error {
	if slices.Contains(allowed, code) {
		return nil
	}
	return &requestError{codeInvalidQuery, detail}
}

const noTraderIDs = "The venue has no trader ids, so only hyperliquid and levr have wallet routes."

/* Coverage */

type venueCoverage struct {
	Venue   string  `json:"venue"`
	Since   string  `json:"since"`
	LastOK  *string `json:"last_ok"`
	Mode    string  `json:"mode"`
	Wallets bool    `json:"wallets"`
	Gaps    *int64  `json:"gaps"`
	Note    string  `json:"note"`
}

func (a *app) coverage(ctx context.Context, code string) (venueCoverage, error) {
	infos, err := a.v.Venues(ctx)
	if err != nil {
		return venueCoverage{}, err
	}
	i := slices.IndexFunc(infos, func(v venue.Info) bool { return v.Code == code })
	if i < 0 {
		return venueCoverage{}, pgstore.ErrNotFound
	}
	gaps, ok, err := a.v.Gaps(ctx, code)
	if err != nil {
		return venueCoverage{}, err
	}
	f := venueFact[code]
	c := venueCoverage{Venue: code, Since: rfc3339(infos[i].Since.Unix()), LastOK: optTime(infos[i].LastOK), Mode: f.mode, Wallets: infos[i].Wallets, Note: f.note}
	if ok {
		c.Gaps = &gaps
	}
	return c, nil
}

func optTime(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	s := rfc3339(t.Unix())
	return &s
}

func optString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

/* Responses */

// answerVenue serves a recorded answer, which describes a moving present and so carries no Sona-Block headers
func (a *app) answerVenue(w http.ResponseWriter, req *http.Request, read func(ctx context.Context) (any, error)) {
	answer(w, req, func(ctx context.Context) (any, error) {
		if a.v == nil {
			return nil, errVenueDown
		}
		body, err := read(ctx)
		return body, venueFailure(err)
	}, nil)
}

// venueFailure sorts an error from pm-postgres into an unreachable database and a bug
func venueFailure(err error) error {
	var pg *pgconn.PgError
	switch {
	case err == nil, errors.Is(err, pgstore.ErrNotFound), errors.Is(err, errVenueDown):
		return err
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.As(err, &pg) && !strings.HasPrefix(pg.Code, "08") && !strings.HasPrefix(pg.Code, "57"):
		return err
	case errors.As(err, new(*requestError)):
		return err
	}
	return errors.Join(errVenueDown, err)
}

func usd(micro int64) float64 { return float64(micro) / venue.Micro }

func price(ppm int32) float64 { return float64(ppm) / venue.PPM }

/* Venues */

type venuesBody struct {
	Kind  string      `json:"kind"`
	Items []venueItem `json:"items"`
}

type venueItem struct {
	Code    string  `json:"code"`
	Name    string  `json:"name"`
	Quote   string  `json:"quote"`
	Wallets bool    `json:"wallets"`
	Since   string  `json:"since"`
	LastOK  *string `json:"last_ok"`
	Mode    string  `json:"mode"`
	Note    string  `json:"note"`
}

const polymarketSince = "2022-11-21T00:00:00Z"

func (a *app) venues(w http.ResponseWriter, req *http.Request) {
	a.answerVenue(w, req, func(ctx context.Context) (any, error) {
		infos, err := a.v.Venues(ctx)
		if err != nil {
			return nil, err
		}
		poly := venueItem{
			Code: venuePolymarket, Name: venueNames[venuePolymarket], Quote: "USDC", Wallets: true, Since: polymarketSince,
			Mode: "history_and_live", Note: "Order-book history through Polygon block 85,948,288 on 2026-04-24, and a live feed of the busiest markets and largest recent trades.",
		}
		if snap, ok := a.events(); ok {
			poly.LastOK = optTime(snap.FetchedAt)
		}
		out := venuesBody{Kind: "pm_venues", Items: []venueItem{poly}}
		for _, v := range infos {
			f := venueFact[v.Code]
			out.Items = append(out.Items, venueItem{
				Code: v.Code, Name: v.Name, Quote: v.Quote, Wallets: v.Wallets, Since: rfc3339(v.Since.Unix()),
				LastOK: optTime(v.LastOK), Mode: f.mode, Note: f.note,
			})
		}
		return out, nil
	})
}

/* Top markets */

type venueTop struct {
	Kind     string        `json:"kind"`
	Venue    string        `json:"venue"`
	Items    []venueMarket `json:"items"`
	AsOf     *string       `json:"as_of"`
	Source   string        `json:"source"`
	Coverage venueCoverage `json:"coverage"`
}

type venueMarket struct {
	MarketID        string   `json:"market_id"`
	Title           string   `json:"title"`
	MarketKind      string   `json:"market_kind"`
	Status          string   `json:"status"`
	Result          *string  `json:"result"`
	CloseTime       *string  `json:"close_time"`
	YesPrice        float64  `json:"yes_price"`
	VolumeUSD       *float64 `json:"volume_usd"`
	OpenInterestUSD *float64 `json:"open_interest_usd"`
	Volume24hUSD    *float64 `json:"volume_24h_usd"`
	AsOf            string   `json:"as_of"`
}

// known says which of volume, open interest and 24-hour volume a venue really reports, since a zero would pass for a quiet market
type known struct{ total, oi, day bool }

var venueKnown = map[string]known{
	venue.Kalshi:       {true, true, true},
	venue.PolymarketUS: {true, true, false},
	venue.Hyperliquid:  {false, false, true},
}

func (a *app) venueTop(w http.ResponseWriter, req *http.Request, code string) {
	a.answerVenue(w, req, func(ctx context.Context) (any, error) {
		if err := onlyVenues(code, "The venue must be polymarket, kalshi, polymarket-us or hyperliquid.", venue.Kalshi, venue.PolymarketUS, venue.Hyperliquid); err != nil {
			return nil, err
		}
		limit, err := intParam(req.URL.Query(), "limit", topDefault, 1, topMax)
		if err != nil {
			return nil, err
		}
		var ms []pgstore.TopMarket
		if code == venue.Hyperliquid {
			ms, err = a.v.TopTape(ctx, code, time.Now().Add(-tapeWindow), limit)
		} else {
			ms, err = a.v.TopRecent(ctx, code, limit)
		}
		if err != nil {
			return nil, err
		}
		cov, err := a.coverage(ctx, code)
		if err != nil {
			return nil, err
		}
		k := venueKnown[code]
		out := venueTop{Kind: "pm_top_markets", Venue: code, Items: make([]venueMarket, 0, len(ms)), Source: venueFact[code].source, Coverage: cov}
		var newest time.Time
		for _, m := range ms {
			item := venueMarket{
				MarketID: m.ID, Title: m.Title, MarketKind: m.Kind, Status: m.Status, Result: optString(m.Result),
				YesPrice: price(m.Last.YesPPM), AsOf: rfc3339(m.Last.TS.Unix()),
			}
			if !m.Close.IsZero() {
				item.CloseTime = optTime(m.Close)
			}
			item.VolumeUSD, item.OpenInterestUSD, item.Volume24hUSD = knownUSD(k.total, m.Last.Volume), knownUSD(k.oi, m.Last.OI), knownUSD(k.day, m.Last.Vol24)
			if m.Last.TS.After(newest) {
				newest = m.Last.TS
			}
			out.Items = append(out.Items, item)
		}
		out.AsOf = optTime(newest)
		return out, nil
	})
}

func knownUSD(ok bool, micro int64) *float64 {
	if !ok {
		return nil
	}
	v := usd(micro)
	return &v
}

/* Large trades */

type venueLarge struct {
	Kind     string        `json:"kind"`
	Venue    string        `json:"venue"`
	Items    []venueTrade  `json:"items"`
	Source   string        `json:"source"`
	Coverage venueCoverage `json:"coverage"`
}

type venueTrade struct {
	ID       string  `json:"id"`
	Time     string  `json:"time"`
	MarketID string  `json:"market_id"`
	Question string  `json:"question"`
	Side     string  `json:"side"`
	Wallet   *string `json:"wallet"`
	Size     float64 `json:"size"`
	YesPrice float64 `json:"yes_price"`
	USD      float64 `json:"usd"`
}

func (a *app) venueLarge(w http.ResponseWriter, req *http.Request, code string) {
	a.answerVenue(w, req, func(ctx context.Context) (any, error) {
		if err := onlyVenues(code, "The venue must be polymarket, kalshi or hyperliquid.", venue.Kalshi, venue.Hyperliquid); err != nil {
			return nil, err
		}
		q := req.URL.Query()
		limit, err := intParam(q, "limit", largeDefault, 1, largeMax)
		if err != nil {
			return nil, err
		}
		minUSD, err := minUSDParam(q)
		if err != nil {
			return nil, err
		}
		ts, err := a.v.LargeNamed(ctx, code, int64(minUSD*venue.Micro), limit)
		if err != nil {
			return nil, err
		}
		cov, err := a.coverage(ctx, code)
		if err != nil {
			return nil, err
		}
		out := venueLarge{Kind: "pm_large_trades", Venue: code, Items: make([]venueTrade, 0, len(ts)), Source: venueFact[code].source, Coverage: cov}
		for _, t := range ts {
			item := venueTrade{
				ID: t.ID, Time: t.TS.UTC().Format(time.RFC3339), MarketID: t.MarketID, Question: t.Title, Side: t.Side,
				Size: usd(t.Qty), YesPrice: price(t.PricePPM), USD: usd(t.USD),
			}
			if len(t.Taker) > 0 {
				item.Wallet = optString(address(t.Taker))
			}
			out.Items = append(out.Items, item)
		}
		return out, nil
	})
}

/* Market history */

type venueHistory struct {
	Kind     string        `json:"kind"`
	Venue    string        `json:"venue"`
	Market   historyMarket `json:"market"`
	Items    []venuePoint  `json:"items"`
	Source   string        `json:"source"`
	Coverage venueCoverage `json:"coverage"`
}

type historyMarket struct {
	ID         string  `json:"id"`
	Title      string  `json:"title"`
	MarketKind string  `json:"market_kind"`
	Status     string  `json:"status"`
	Result     *string `json:"result"`
	CloseTime  *string `json:"close_time"`
}

type venuePoint struct {
	Time            string   `json:"time"`
	YesPrice        float64  `json:"yes_price"`
	VolumeUSD       *float64 `json:"volume_usd"`
	OpenInterestUSD *float64 `json:"open_interest_usd"`
	Volume24hUSD    *float64 `json:"volume_24h_usd"`
}

func (a *app) history(w http.ResponseWriter, req *http.Request) {
	code := req.PathValue("venue")
	a.answerVenue(w, req, func(ctx context.Context) (any, error) {
		if !slices.Contains(recordedVenues, code) && code != venuePolymarket {
			return nil, &requestError{codeInvalidQuery, "The venue must be kalshi, polymarket-us or hyperliquid."}
		}
		if err := onlyVenues(code, "The venue must be kalshi, polymarket-us or hyperliquid.", venue.Kalshi, venue.PolymarketUS, venue.Hyperliquid); err != nil {
			return nil, err
		}
		q := req.URL.Query()
		limit, err := intParam(q, "limit", historyDefault, 1, historyMax)
		if err != nil {
			return nil, err
		}
		since := time.Now().Add(-historyWindow)
		if q.Has("since") {
			if since, err = time.Parse(time.RFC3339, q.Get("since")); err != nil {
				return nil, &requestError{codeInvalidQuery, "The since must be a time like 2026-10-14T12:00:00Z."}
			}
		}
		id := req.PathValue("id")
		m, found, err := a.v.Market(ctx, code, id)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, pgstore.ErrNotFound
		}
		var snaps []venue.Snap
		if code == venue.Hyperliquid {
			snaps, err = a.v.TapeSeries(ctx, code, id, since, tapeBucket, limit)
		} else {
			snaps, err = a.v.Snaps(ctx, code, id, since, limit)
		}
		if err != nil {
			return nil, err
		}
		cov, err := a.coverage(ctx, code)
		if err != nil {
			return nil, err
		}
		k := venueKnown[code]
		out := venueHistory{
			Kind: "pm_market_history", Venue: code, Items: make([]venuePoint, 0, len(snaps)), Source: venueFact[code].source, Coverage: cov,
			Market: historyMarket{ID: m.ID, Title: m.Title, MarketKind: m.Kind, Status: m.Status, Result: optString(m.Result), CloseTime: optTime(m.Close)},
		}
		for _, s := range snaps {
			p := venuePoint{Time: rfc3339(s.TS.Unix()), YesPrice: price(s.YesPPM)}
			if code != venue.Hyperliquid {
				p.VolumeUSD, p.OpenInterestUSD, p.Volume24hUSD = knownUSD(k.total, s.Volume), knownUSD(k.oi, s.OI), knownUSD(k.day, s.Vol24)
			}
			out.Items = append(out.Items, p)
		}
		return out, nil
	})
}
