package levr

import (
	"bytes"
	"context"
)

const batch = 1000

type Wallet struct {
	Address []byte
	Summary
}

// WalletStore reads every position ordered by wallet and writes the per-wallet totals
type WalletStore interface {
	EachPosition(ctx context.Context, fn func(Position) error) error
	UpsertWallets(ctx context.Context, ws []Wallet) error
}

// Wallets rebuilds the wallet table from the positions, which is cheap at the volume Levr has
type Wallets struct {
	Store WalletStore
}

func (w *Wallets) Name() string { return "levr/wallets" }

func (w *Wallets) Poll(ctx context.Context) error {
	var out []Wallet
	var cur *Wallet
	flush := func(force bool) error {
		if len(out) < batch && !force {
			return nil
		}
		err := w.Store.UpsertWallets(ctx, out)
		out = out[:0]
		return err
	}
	err := w.Store.EachPosition(ctx, func(p Position) error {
		if cur != nil && !bytes.Equal(cur.Address, p.Wallet) {
			out = append(out, *cur)
			cur = nil
			if err := flush(false); err != nil {
				return err
			}
		}
		if cur == nil {
			cur = &Wallet{Address: p.Wallet}
		}
		cur.Add(p)
		return nil
	})
	if err != nil {
		return err
	}
	if cur != nil {
		out = append(out, *cur)
	}
	return flush(true)
}
