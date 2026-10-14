package hip4

import (
	"context"
	"log/slog"
	"time"

	"pm/internal/venue"
)

// Wallets rebuilds the wallet table from the whole recorded tape, which is small enough to replay every run
type Wallets struct {
	Store WalletStore
}

func (w *Wallets) Name() string { return venue.Hyperliquid + "/wallets" }

func (w *Wallets) Poll(ctx context.Context) error {
	markets, err := w.Store.MarketResults(ctx, venue.Hyperliquid)
	if err != nil {
		return err
	}
	book, err := NewBook(markets)
	if err != nil {
		return err
	}
	skipped := 0
	err = w.Store.EachTrade(ctx, venue.Hyperliquid, time.Time{}, func(t venue.Trade) error {
		if err := book.Add(t); err != nil {
			skipped++
		}
		return nil
	})
	if err != nil {
		return err
	}
	if skipped > 0 {
		slog.Warn("hip4 trades skipped by the wallet job", "trades", skipped)
	}
	return w.Store.UpsertHLWallets(ctx, book.Wallets())
}
