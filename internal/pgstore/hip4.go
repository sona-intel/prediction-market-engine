package pgstore

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"pm/internal/hip4"
)

var (
	_ hip4.SettleStore = (*Store)(nil)
	_ hip4.WalletStore = (*Store)(nil)
)

// UnsettledClosed lists markets that left the live list and have no result yet, newest close first
func (s *Store) UnsettledClosed(ctx context.Context, code string, limit int) ([]string, error) {
	rows, err := s.Pool.Query(ctx, `select id from market where venue = $1 and status = 'closed' and result = ''
		order by close_ts desc nulls last, id limit $2`, code, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// LastTraders lists the wallets of the newest trades of a market, most recent first
func (s *Store) LastTraders(ctx context.Context, code, marketID string, limit int) ([][]byte, error) {
	rows, err := s.Pool.Query(ctx, `select w from (
			select taker as w, ts from trade where venue = $1 and market_id = $2
			union all select maker, ts from trade where venue = $1 and market_id = $2
		) t where w is not null group by w order by max(ts) desc, w limit $3`, code, marketID, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[[]byte])
}

// SettleMarket records the result and when the venue settled it
func (s *Store) SettleMarket(ctx context.Context, code, id, result string, at time.Time) error {
	_, err := s.Pool.Exec(ctx, `update market set status = 'resolved', result = $3, settled_ts = $4 where venue = $1 and id = $2`, code, id, result, at)
	return err
}

func (s *Store) MarketResults(ctx context.Context, code string) ([]hip4.MarketInfo, error) {
	rows, err := s.Pool.Query(ctx, `select id, kind, result, settled_ts from market where venue = $1 order by id`, code)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (m hip4.MarketInfo, err error) {
		var at *time.Time
		err = r.Scan(&m.ID, &m.Kind, &m.Result, &at)
		if at != nil {
			m.SettledAt = *at
		}
		return m, err
	})
}

func (s *Store) UpsertHLWallets(ctx context.Context, ws []hip4.Wallet) error {
	return batched(ctx, s, ws, func(b *pgx.Batch, w hip4.Wallet) {
		var brier, edge, se *float64
		if w.Rated {
			brier, edge, se = &w.Brier, &w.Edge, &w.EdgeSE
		}
		b.Queue(`insert into hl_wallet (address, fills, volume_micro, gross_pnl, markets, resolved, won, brier, edge, edge_se, rated, updated_at)
			values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now())
			on conflict (address) do update set fills = excluded.fills, volume_micro = excluded.volume_micro, gross_pnl = excluded.gross_pnl,
			markets = excluded.markets, resolved = excluded.resolved, won = excluded.won, brier = excluded.brier, edge = excluded.edge,
			edge_se = excluded.edge_se, rated = excluded.rated, updated_at = now()`,
			w.Address, w.Fills, w.Volume, w.Gross, w.Markets, w.Resolved, w.Won, brier, edge, se, w.Rated)
	}, nil)
}
