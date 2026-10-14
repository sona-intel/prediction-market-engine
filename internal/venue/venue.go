// Package venue holds the types every prediction-market source writes and the interface the collector runs
package venue

import (
	"context"
	"time"
)

// Venue codes as stored in every venue column
const (
	Hyperliquid  = "hyperliquid"
	Kalshi       = "kalshi"
	PolymarketUS = "polymarket-us"
	Levr         = "levr"
)

// Money is in micro-USDC and prices are in ppm, the engine's units everywhere
const (
	Micro = 1_000_000
	PPM   = 1_000_000
)

type Info struct {
	Code    string
	Name    string
	Quote   string
	Wallets bool
	Since   time.Time
	LastOK  time.Time
}

type Market struct {
	Venue  string
	ID     string
	Title  string
	Kind   string
	Close  time.Time
	Status string
	Result string
}

type Snap struct {
	Venue    string
	MarketID string
	TS       time.Time
	YesPPM   int32
	Volume   int64
	OI       int64
	Vol24    int64
}

// Maker and Taker stay nil where the venue publishes no trader ids
type Trade struct {
	Venue    string
	ID       string
	MarketID string
	TS       time.Time
	Side     string
	PricePPM int32
	Qty      int64
	USD      int64
	Maker    []byte
	Taker    []byte
}

// Source fetches one feed of one venue each time it is polled
type Source interface {
	Name() string
	Poll(ctx context.Context) error
}

// Sink is where sources write, and it holds the cursor of each source between polls
type Sink interface {
	UpsertMarkets(ctx context.Context, ms []Market) error
	InsertSnaps(ctx context.Context, ss []Snap) (int, error)
	InsertTrades(ctx context.Context, ts []Trade) (int, error)
	Cursor(ctx context.Context, source string) (string, error)
	SetCursor(ctx context.Context, source, value string) error
}

// Catalog describes the venues pm-collect can record, keyed by code
var Catalog = map[string]Info{
	Hyperliquid:  {Code: Hyperliquid, Name: "Hyperliquid HIP-4", Quote: "USD", Wallets: true},
	Kalshi:       {Code: Kalshi, Name: "Kalshi", Quote: "USD"},
	PolymarketUS: {Code: PolymarketUS, Name: "Polymarket US", Quote: "USD"},
	Levr:         {Code: Levr, Name: "Levr Bet", Quote: "USDC", Wallets: true},
}
