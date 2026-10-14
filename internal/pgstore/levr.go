package pgstore

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"pm/internal/levr"
)

var _ levr.WalletStore = (*Store)(nil)

// UpsertPositions stores new positions and leaves any already recorded untouched, so a replay never reopens a closed one
func (s *Store) UpsertPositions(ctx context.Context, ps []levr.Position) (int, error) {
	n := 0
	err := batched(ctx, s, ps, func(b *pgx.Batch, p levr.Position) {
		var liq *int32
		if p.LiqPPM > 0 {
			liq = &p.LiqPPM
		}
		b.Queue(`insert into levr_position (id, wallet, market_id, opened_ts, stake_micro, size_micro, fee_micro, entry_ppm, liq_ppm, status, side)
			values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) on conflict do nothing`,
			p.ID, p.Wallet, p.MarketID, p.Opened, p.Stake, p.Size, p.Fee, p.EntryPPM, liq, p.Status, p.Side)
	}, &n)
	return n, err
}

const positionCols = `id, wallet, market_id, opened_ts, stake_micro, size_micro, fee_micro, entry_ppm, coalesce(liq_ppm, 0), status, side, closed_ts, payout_micro`

func scanPosition(r pgx.CollectableRow) (p levr.Position, err error) {
	var closed *time.Time
	err = r.Scan(&p.ID, &p.Wallet, &p.MarketID, &p.Opened, &p.Stake, &p.Size, &p.Fee, &p.EntryPPM, &p.LiqPPM, &p.Status, &p.Side, &closed, &p.Payout)
	if closed != nil {
		p.Closed = *closed
	}
	return p, err
}

// Positions returns the recorded positions among ids, keyed by id
func (s *Store) Positions(ctx context.Context, ids []string) (map[string]levr.Position, error) {
	rows, err := s.Pool.Query(ctx, `select `+positionCols+` from levr_position where id = any($1)`, ids)
	if err != nil {
		return nil, err
	}
	ps, err := pgx.CollectRows(rows, scanPosition)
	if err != nil {
		return nil, err
	}
	out := make(map[string]levr.Position, len(ps))
	for _, p := range ps {
		out[p.ID] = p
	}
	return out, nil
}

// ClosePositions moves open positions to their final state and returns how many changed
func (s *Store) ClosePositions(ctx context.Context, cs []levr.Close) (int, error) {
	n := 0
	err := batched(ctx, s, cs, func(b *pgx.Batch, c levr.Close) {
		b.Queue(`update levr_position set status = $2, closed_ts = $3, payout_micro = $4 where id = $1 and status = 'open'`,
			c.ID, c.Status, c.At, c.Payout)
	}, &n)
	return n, err
}

// LoseOpen closes the open positions of a game that backed a side other than the winner, each with a zero payout
func (s *Store) LoseOpen(ctx context.Context, market string, winner int, at time.Time) (int, error) {
	tag, err := s.Pool.Exec(ctx, `update levr_position set status = 'lost', closed_ts = $3, payout_micro = 0
		where market_id = $1 and status = 'open' and side <> $2`, market, winner, at)
	return int(tag.RowsAffected()), err
}

// EachPosition streams every position ordered by wallet then time
func (s *Store) EachPosition(ctx context.Context, fn func(levr.Position) error) error {
	rows, err := s.Pool.Query(ctx, `select `+positionCols+` from levr_position order by wallet, opened_ts, id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		p, err := scanPosition(rows)
		if err != nil {
			return err
		}
		if err := fn(p); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (s *Store) UpsertWallets(ctx context.Context, ws []levr.Wallet) error {
	return batched(ctx, s, ws, func(b *pgx.Batch, w levr.Wallet) {
		b.Queue(`insert into levr_wallet (address, positions, stake_micro, size_micro, liquidations, settled, won, realized_pnl, updated_at)
			values ($1, $2, $3, $4, $5, $6, $7, $8, now())
			on conflict (address) do update set positions = excluded.positions, stake_micro = excluded.stake_micro, size_micro = excluded.size_micro,
			liquidations = excluded.liquidations, settled = excluded.settled, won = excluded.won, realized_pnl = excluded.realized_pnl, updated_at = now()`,
			w.Address, w.Positions, w.Stake, w.Size, w.Liquidated, w.Settled(), w.Won, w.Realized)
	}, nil)
}
