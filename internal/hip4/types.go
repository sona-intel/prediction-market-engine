// Package hip4 turns the recorded Hyperliquid outcome tape into settled results, wallet PnL and skill
package hip4

import (
	"context"
	"time"

	"pm/internal/venue"
)

// MarketInfo is what the ledger needs to know about a recorded market
type MarketInfo struct {
	ID        string
	Kind      string
	Result    string
	SettledAt time.Time
}

// Wallet holds the totals of one trader, with the skill fields set only when the wallet clears the rating floor
type Wallet struct {
	Address  []byte
	Fills    int64
	Volume   int64
	Gross    int64
	Markets  int
	Resolved int
	Won      int
	Rated    bool
	Brier    float64
	Edge     float64
	EdgeSE   float64
}

// SettleStore is what the settlement job reads and writes
type SettleStore interface {
	UnsettledClosed(ctx context.Context, code string, limit int) ([]string, error)
	LastTradeTS(ctx context.Context, code, marketID string) (time.Time, bool, error)
	LastTraders(ctx context.Context, code, marketID string, limit int) ([][]byte, error)
	SettleMarket(ctx context.Context, code, id, result string, at time.Time) error
}

// WalletStore is what the wallet job reads and writes
type WalletStore interface {
	EachTrade(ctx context.Context, code string, since time.Time, fn func(venue.Trade) error) error
	MarketResults(ctx context.Context, code string) ([]MarketInfo, error)
	UpsertHLWallets(ctx context.Context, ws []Wallet) error
}
