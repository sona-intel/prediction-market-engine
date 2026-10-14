// Package levr records Levr Bet on Monad from its unofficial Subsquid GraphQL, one table at a time so a broken table never stops the others
package levr

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"pm/internal/httpx"
	"pm/internal/levr"
	"pm/internal/venue"
)

const (
	cursorPrefix = "levr/"
	pageSize     = 1000
	maxPages     = 20
	// defaultLookback is how far back the first poll reads when no backfill is asked for
	defaultLookback = 24 * time.Hour
)

// Contract families share one row shape, and the over/under tables are empty today
type family struct{ prefix, kind string }

var families = []family{
	{"levrFullTimeMarketSchema", "FullTimeWinner"},
	{"levrOverUnderMarketSchema", "OverUnder"},
}

const liquidations = "levrLiquidationEngineSchemaPositionLiquidateds"

// ErrOpenedDown stops a closing table from moving past rows whose positions a failed opened table has not stored yet
var ErrOpenedDown = errors.New("positions table failed this poll")

type Opened struct {
	ID           string `json:"id"`
	Timestamp    string `json:"timestamp"`
	GameMarketID string `json:"gameMarketId"`
	GameID       string `json:"gameId"`
	PositionID   string `json:"positionId"`
	Receiver     string `json:"receiver"`
	AssetAmount  string `json:"assetAmount"`
	PositionSize string `json:"positionSize"`
	PositionFee  string `json:"positionFee"`
	EntryPrice   string `json:"entryPrice"`
	LiqPrice     string `json:"liqPrice"`
	Side         int    `json:"side"`
}

func (o Opened) RowID() string { return o.ID }

const OpenedFields = "id timestamp gameMarketId gameId positionId receiver assetAmount positionSize positionFee entryPrice liqPrice side"

type Redeemed struct {
	ID           string `json:"id"`
	Timestamp    string `json:"timestamp"`
	PositionID   string `json:"positionId"`
	GameMarketID string `json:"gameMarketId"`
	Side         int    `json:"side"`
	MarketWinner int    `json:"marketWinner"`
	Profit       string `json:"profit"`
	Loss         string `json:"loss"`
	BorrowFee    string `json:"borrowFee"`
	BalanceFee   string `json:"balanceFee"`
}

func (r Redeemed) RowID() string { return r.ID }

const RedeemedFields = "id timestamp positionId gameMarketId side marketWinner profit loss borrowFee balanceFee"

type Liquidated struct {
	ID                  string `json:"id"`
	Timestamp           string `json:"timestamp"`
	PositionID          string `json:"positionId"`
	User                string `json:"user"`
	ForfeitedCollateral string `json:"forfeitedCollateral"`
}

func (l Liquidated) RowID() string { return l.ID }

const LiquidatedFields = "id timestamp positionId user forfeitedCollateral"

type Settled struct {
	ID           string `json:"id"`
	Timestamp    string `json:"timestamp"`
	GameMarketID string `json:"gameMarketId"`
	GameID       string `json:"gameId"`
	Winner       int    `json:"winner"`
	SettledAs    int    `json:"settledAs"`
}

func (s Settled) RowID() string { return s.ID }

const SettledFields = "id timestamp gameMarketId gameId winner settledAs"

// Store is what the poller writes, beyond the shared market, snapshot and cursor sink
type Store interface {
	venue.Sink
	UpsertPositions(ctx context.Context, ps []levr.Position) (int, error)
	Positions(ctx context.Context, ids []string) (map[string]levr.Position, error)
	ClosePositions(ctx context.Context, cs []levr.Close) (int, error)
	// LoseOpen closes the open positions of a settled game that backed a side other than the winner, which the venue never redeems
	LoseOpen(ctx context.Context, market string, winner int, at time.Time) (int, error)
}

type Metrics struct {
	positions   prometheus.Counter
	closed      prometheus.Counter
	skipped     prometheus.Counter
	orphans     prometheus.Counter
	tableErrors *prometheus.CounterVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		positions: prometheus.NewCounter(prometheus.CounterOpts{Name: "pm_levr_positions_stored_total", Help: "New Levr positions stored"}),
		closed:    prometheus.NewCounter(prometheus.CounterOpts{Name: "pm_levr_positions_closed_total", Help: "Levr positions moved to a final state"}),
		skipped:   prometheus.NewCounter(prometheus.CounterOpts{Name: "pm_levr_rows_skipped_total", Help: "Levr rows dropped as unusable"}),
		orphans:   prometheus.NewCounter(prometheus.CounterOpts{Name: "pm_levr_orphans_total", Help: "Redemptions and liquidations of positions that were never recorded"}),
		tableErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pm_levr_table_errors_total", Help: "Levr GraphQL tables that failed, by table",
		}, []string{"table"}),
	}
	reg.MustRegister(m.positions, m.closed, m.skipped, m.orphans, m.tableErrors)
	return m
}

func (m *Metrics) Orphans() prometheus.Counter { return m.orphans }

func (m *Metrics) Skipped() prometheus.Counter { return m.skipped }

func (m *Metrics) TableErrors(table string) prometheus.Counter {
	return m.tableErrors.WithLabelValues(table)
}

// Client posts GraphQL queries through the shared limiter and retry
type Client struct {
	HTTP *httpx.Client
	URL  string
}

type reply[T any] struct {
	Data   map[string][]T `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

var ErrGraphQL = errors.New("graphql error")

// Rows reads one page of a table ordered by id, after a cursor id or from a time when there is no cursor, or from the start when both are empty
func Rows[T any](ctx context.Context, c *Client, table, fields, after string, since time.Time, limit int) ([]T, error) {
	where, vars := "", map[string]any{"limit": limit}
	decl := "$limit: Int!"
	switch {
	case after != "":
		where, decl, vars["after"] = ", where: {id_gt: $after}", decl+", $after: String!", after
	case !since.IsZero():
		where, decl, vars["since"] = ", where: {timestamp_gte: $since}", decl+", $since: DateTime!", since.UTC().Format(time.RFC3339)
	}
	body := map[string]any{
		"query":     fmt.Sprintf("query (%s) { %s(limit: $limit, orderBy: id_ASC%s) { %s } }", decl, table, where, fields),
		"variables": vars,
	}
	var r reply[T]
	if err := c.HTTP.Post(ctx, c.URL, body, &r); err != nil {
		return nil, err
	}
	if len(r.Errors) > 0 {
		return nil, fmt.Errorf("%w: %s", ErrGraphQL, r.Errors[0].Message)
	}
	rows, ok := r.Data[table]
	if !ok {
		return nil, fmt.Errorf("%w: no %s in the answer", ErrGraphQL, table)
	}
	return rows, nil
}

func number(s string) (int64, bool) {
	n, err := strconv.ParseInt(s, 10, 64)
	return n, err == nil && n >= 0
}

func ppm(s string) (int32, bool) {
	n, ok := number(s)
	return int32(n), ok && n <= venue.PPM
}

func wallet(s string) ([]byte, bool) {
	b, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
	return b, err == nil && len(b) == 20
}

func when(s string) (time.Time, bool) {
	t, err := time.Parse(time.RFC3339Nano, s)
	return t.UTC(), err == nil
}

// Position turns an opened row into a stored position and reports false for rows that would break the table
func (o Opened) Position() (levr.Position, bool) {
	stake, okStake := number(o.AssetAmount)
	size, okSize := number(o.PositionSize)
	fee, okFee := number(o.PositionFee)
	entry, okEntry := ppm(o.EntryPrice)
	liq, okLiq := ppm(o.LiqPrice)
	w, okW := wallet(o.Receiver)
	at, okAt := when(o.Timestamp)
	if o.PositionID == "" || o.GameMarketID == "" || !okStake || !okSize || !okFee || !okEntry || !okLiq || !okW || !okAt || stake == 0 || size < stake || o.Side < 1 || o.Side > 3 {
		return levr.Position{}, false
	}
	return levr.Position{
		ID: o.PositionID, Wallet: w, MarketID: o.GameMarketID, Opened: at, Stake: stake, Size: size, Fee: fee, Side: o.Side,
		EntryPPM: entry, LiqPPM: liq, Status: levr.Open,
	}, true
}

// Redemption reads a redeemed row, and reports false when its numbers are unusable
func (r Redeemed) Redemption() (levr.Redemption, bool) {
	profit, okP := number(r.Profit)
	loss, okL := number(r.Loss)
	borrow, okB := number(r.BorrowFee)
	balance, okF := number(r.BalanceFee)
	at, okAt := when(r.Timestamp)
	if r.PositionID == "" || !okP || !okL || !okB || !okF || !okAt || r.MarketWinner < 0 || borrow > math.MaxInt64-balance {
		return levr.Redemption{}, false
	}
	return levr.Redemption{PositionID: r.PositionID, At: at, Winner: r.MarketWinner, Profit: profit, Loss: loss, Costs: borrow + balance}, true
}

// Market names a game market, since the feed carries no team names
func (o Opened) Market(kind string) venue.Market {
	return venue.Market{Venue: venue.Levr, ID: o.GameMarketID, Title: "Game " + o.GameID, Kind: kind, Status: "open"}
}

// Market gives the result of a settled game, with the winning side as the result and void for a cancelled game
func (s Settled) Market(kind string) venue.Market {
	result := strconv.Itoa(s.Winner)
	if s.Winner == 0 {
		result = "void"
	}
	return venue.Market{Venue: venue.Levr, ID: s.GameMarketID, Title: "Game " + s.GameID, Kind: kind, Status: "settled", Result: result}
}

// Source polls every table in order, so a position is always stored before the rows that close it
type Source struct {
	Client  *Client
	Store   Store
	Metrics *Metrics
	// Backfill starts from the first row of each table, and is for the server only
	Backfill bool
	// Lookback is how far back a first poll reads without a backfill, and zero means a day
	Lookback time.Duration
	Now      func() time.Time

	// down holds the opened tables that failed in the current poll
	down map[string]bool
}

func (s *Source) Name() string { return venue.Levr + "/positions" }

type step struct {
	table string
	run   func(ctx context.Context) error
}

func (s *Source) steps() []step {
	var out []step
	for _, f := range families {
		out = append(out, step{f.prefix + "PositionOpeneds", func(ctx context.Context) error {
			return drain(ctx, s, f.prefix+"PositionOpeneds", OpenedFields, func(rows []Opened) error { return s.opened(ctx, rows, f.kind) })
		}})
	}
	out = append(out, step{liquidations, func(ctx context.Context) error {
		return drain(ctx, s, liquidations, LiquidatedFields, func(rows []Liquidated) error { return s.liquidated(ctx, rows) })
	}})
	for _, f := range families {
		out = append(out, step{f.prefix + "PositionRedeemeds", func(ctx context.Context) error {
			return drain(ctx, s, f.prefix+"PositionRedeemeds", RedeemedFields, func(rows []Redeemed) error { return s.redeemed(ctx, rows, f.prefix) })
		}})
	}
	for _, f := range families {
		out = append(out, step{f.prefix + "MarketSettleds", func(ctx context.Context) error {
			return drain(ctx, s, f.prefix+"MarketSettleds", SettledFields, func(rows []Settled) error { return s.settled(ctx, rows, f) })
		}})
	}
	return out
}

// Poll skips a table that fails and reports an error only when every table did
func (s *Source) Poll(ctx context.Context) error {
	var errs []error
	steps := s.steps()
	s.down = map[string]bool{}
	for _, st := range steps {
		if err := st.run(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if strings.HasSuffix(st.table, "PositionOpeneds") {
				s.down[st.table] = true
			}
			s.Metrics.TableErrors(st.table).Inc()
			slog.Warn("levr table failed", "table", st.table, "error", err)
			errs = append(errs, fmt.Errorf("%s: %w", st.table, err))
		}
	}
	if len(errs) == len(steps) {
		return errors.Join(errs...)
	}
	return nil
}

// drain reads pages after the stored cursor until a short page, and moves the cursor only once a page is stored
func drain[T interface{ RowID() string }](ctx context.Context, s *Source, table, fields string, handle func([]T) error) error {
	after, err := s.Store.Cursor(ctx, cursorPrefix+table)
	if err != nil {
		return err
	}
	var since time.Time
	if after == "" && !s.Backfill {
		now := time.Now
		if s.Now != nil {
			now = s.Now
		}
		look := s.Lookback
		if look == 0 {
			look = defaultLookback
		}
		since = now().Add(-look)
	}
	for range maxPages {
		rows, err := Rows[T](ctx, s.Client, table, fields, after, since, pageSize)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		if err := handle(rows); err != nil {
			return err
		}
		after = rows[len(rows)-1].RowID()
		if err := s.Store.SetCursor(ctx, cursorPrefix+table, after); err != nil {
			return err
		}
		if len(rows) < pageSize {
			return nil
		}
	}
	return nil
}

func (s *Source) opened(ctx context.Context, rows []Opened, kind string) error {
	ps := make([]levr.Position, 0, len(rows))
	var ms []venue.Market
	seen := map[string]bool{}
	for _, r := range rows {
		p, ok := r.Position()
		if !ok {
			s.Metrics.skipped.Inc()
			continue
		}
		ps = append(ps, p)
		if !seen[r.GameMarketID] {
			seen[r.GameMarketID] = true
			ms = append(ms, r.Market(kind))
		}
	}
	if err := s.Store.UpsertMarkets(ctx, ms); err != nil {
		return err
	}
	n, err := s.Store.UpsertPositions(ctx, ps)
	s.Metrics.positions.Add(float64(n))
	return err
}

func (s *Source) liquidated(ctx context.Context, rows []Liquidated) error {
	type item struct {
		id string
		at time.Time
	}
	var items []item
	var ids []string
	for _, r := range rows {
		at, ok := when(r.Timestamp)
		if r.PositionID == "" || !ok {
			s.Metrics.skipped.Inc()
			continue
		}
		items = append(items, item{r.PositionID, at})
		ids = append(ids, r.PositionID)
	}
	known, err := s.Store.Positions(ctx, ids)
	if err != nil {
		return err
	}
	var cs []levr.Close
	for _, it := range items {
		if _, ok := known[it.id]; !ok {
			if len(s.down) > 0 {
				return ErrOpenedDown
			}
			s.Metrics.orphans.Inc()
			continue
		}
		cs = append(cs, levr.Liquidation(it.id, it.at))
	}
	return s.close(ctx, cs)
}

func (s *Source) redeemed(ctx context.Context, rows []Redeemed, prefix string) error {
	var reds []levr.Redemption
	var ids []string
	for _, r := range rows {
		red, ok := r.Redemption()
		if !ok {
			s.Metrics.skipped.Inc()
			continue
		}
		reds = append(reds, red)
		ids = append(ids, red.PositionID)
	}
	known, err := s.Store.Positions(ctx, ids)
	if err != nil {
		return err
	}
	var cs []levr.Close
	for _, red := range reds {
		p, ok := known[red.PositionID]
		if !ok {
			if s.down[prefix+"PositionOpeneds"] {
				return ErrOpenedDown
			}
			s.Metrics.orphans.Inc()
			continue
		}
		cs = append(cs, levr.Settle(p, red))
	}
	return s.close(ctx, cs)
}

// close touches only positions still open, so a replayed page changes nothing
func (s *Source) close(ctx context.Context, cs []levr.Close) error {
	n, err := s.Store.ClosePositions(ctx, cs)
	s.Metrics.closed.Add(float64(n))
	return err
}

func (s *Source) settled(ctx context.Context, rows []Settled, f family) error {
	if s.down[f.prefix+"PositionOpeneds"] {
		return ErrOpenedDown
	}
	ms := make([]venue.Market, 0, len(rows))
	for _, r := range rows {
		at, ok := when(r.Timestamp)
		if r.GameMarketID == "" || r.Winner < 0 || !ok {
			s.Metrics.skipped.Inc()
			continue
		}
		ms = append(ms, r.Market(f.kind))
		if r.Winner == 0 {
			continue
		}
		n, err := s.Store.LoseOpen(ctx, r.GameMarketID, r.Winner, at)
		if err != nil {
			return err
		}
		s.Metrics.closed.Add(float64(n))
	}
	return s.Store.UpsertMarkets(ctx, ms)
}
