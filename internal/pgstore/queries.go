package pgstore

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"pm/internal/venue"
)

const chunk = 1000

var _ venue.Sink = (*Store)(nil)

// Cursor returns an empty value for a source that has none yet
func (s *Store) Cursor(ctx context.Context, source string) (string, error) {
	var v string
	err := s.Pool.QueryRow(ctx, `select value from cursor where source = $1`, source).Scan(&v)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return v, err
}

func (s *Store) SetCursor(ctx context.Context, source, value string) error {
	_, err := s.Pool.Exec(ctx, `insert into cursor (source, value) values ($1, $2)
		on conflict (source) do update set value = excluded.value, updated_at = now()`, source, value)
	return err
}

func (s *Store) UpsertVenue(ctx context.Context, v venue.Info) error {
	_, err := s.Pool.Exec(ctx, `insert into venue (code, name, quote, wallets, since) values ($1, $2, $3, $4, $5)
		on conflict (code) do update set name = excluded.name, quote = excluded.quote, wallets = excluded.wallets`,
		v.Code, v.Name, v.Quote, v.Wallets, v.Since)
	return err
}

func (s *Store) MarkOK(ctx context.Context, code string, at time.Time) error {
	_, err := s.Pool.Exec(ctx, `update venue set last_ok = $2 where code = $1`, code, at)
	return err
}

func (s *Store) Venues(ctx context.Context) ([]venue.Info, error) {
	rows, err := s.Pool.Query(ctx, `select code, name, quote, wallets, since, last_ok from venue order by code`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (v venue.Info, err error) {
		var ok *time.Time
		err = r.Scan(&v.Code, &v.Name, &v.Quote, &v.Wallets, &v.Since, &ok)
		if ok != nil {
			v.LastOK = *ok
		}
		return v, err
	})
}

func (s *Store) UpsertMarkets(ctx context.Context, ms []venue.Market) error {
	return batched(ctx, s, ms, func(b *pgx.Batch, m venue.Market) {
		var closeTS *time.Time
		if !m.Close.IsZero() {
			closeTS = &m.Close
		}
		b.Queue(`insert into market (venue, id, title, kind, close_ts, status, result) values ($1, $2, $3, $4, $5, $6, $7)
			on conflict (venue, id) do update set title = excluded.title, kind = excluded.kind,
			close_ts = excluded.close_ts, status = excluded.status, result = excluded.result`,
			m.Venue, m.ID, m.Title, m.Kind, closeTS, m.Status, m.Result)
	}, nil)
}

// InsertSnaps returns how many rows were new
func (s *Store) InsertSnaps(ctx context.Context, ss []venue.Snap) (int, error) {
	n := 0
	err := batched(ctx, s, ss, func(b *pgx.Batch, v venue.Snap) {
		b.Queue(`insert into market_snap (venue, market_id, ts, yes_ppm, volume_micro, oi_micro, vol24_micro)
			values ($1, $2, $3, $4, $5, $6, $7) on conflict do nothing`,
			v.Venue, v.MarketID, v.TS, v.YesPPM, v.Volume, v.OI, v.Vol24)
	}, &n)
	return n, err
}

// InsertTrades returns how many rows were new, so a replayed page counts zero
func (s *Store) InsertTrades(ctx context.Context, ts []venue.Trade) (int, error) {
	n := 0
	err := batched(ctx, s, ts, func(b *pgx.Batch, t venue.Trade) {
		b.Queue(`insert into trade (venue, id, market_id, ts, side, price_ppm, qty_micro, usd_micro, maker, taker)
			values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) on conflict do nothing`,
			t.Venue, t.ID, t.MarketID, t.TS, t.Side, t.PricePPM, t.Qty, t.USD, t.Maker, t.Taker)
	}, &n)
	return n, err
}

func batched[T any](ctx context.Context, s *Store, items []T, queue func(*pgx.Batch, T), inserted *int) error {
	for part := range chunks(items) {
		b := &pgx.Batch{}
		for _, it := range part {
			queue(b, it)
		}
		res := s.Pool.SendBatch(ctx, b)
		for range part {
			tag, err := res.Exec()
			if err != nil {
				_ = res.Close()
				return err
			}
			if inserted != nil {
				*inserted += int(tag.RowsAffected())
			}
		}
		if err := res.Close(); err != nil {
			return err
		}
	}
	return nil
}

func chunks[T any](items []T) func(yield func([]T) bool) {
	return func(yield func([]T) bool) {
		for len(items) > 0 {
			n := min(chunk, len(items))
			if !yield(items[:n]) {
				return
			}
			items = items[n:]
		}
	}
}

func (s *Store) Market(ctx context.Context, code, id string) (venue.Market, bool, error) {
	m := venue.Market{Venue: code, ID: id}
	var closeTS *time.Time
	err := s.Pool.QueryRow(ctx, `select title, kind, close_ts, status, result from market where venue = $1 and id = $2`,
		code, id).Scan(&m.Title, &m.Kind, &closeTS, &m.Status, &m.Result)
	if errors.Is(err, pgx.ErrNoRows) {
		return m, false, nil
	}
	if closeTS != nil {
		m.Close = *closeTS
	}
	return m, err == nil, err
}

// OpenMarketIDs lists the markets of a venue that have not settled
func (s *Store) OpenMarketIDs(ctx context.Context, code string) ([]string, error) {
	rows, err := s.Pool.Query(ctx, `select id from market where venue = $1 and result = '' order by id`, code)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

type TopMarket struct {
	venue.Market
	Last venue.Snap
}

// TopMarkets ranks the markets of a venue by the volume of their latest snapshot
func (s *Store) TopMarkets(ctx context.Context, code string, limit int) ([]TopMarket, error) {
	rows, err := s.Pool.Query(ctx, `select m.id, m.title, m.kind, m.close_ts, m.status, m.result,
			l.ts, l.yes_ppm, l.volume_micro, l.oi_micro, l.vol24_micro
		from market m join lateral (
			select * from market_snap where venue = m.venue and market_id = m.id order by ts desc limit 1
		) l on true
		where m.venue = $1 order by l.vol24_micro desc, l.volume_micro desc, m.id limit $2`, code, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (t TopMarket, err error) {
		var closeTS *time.Time
		t.Venue, t.Last.Venue = code, code
		err = r.Scan(&t.ID, &t.Title, &t.Kind, &closeTS, &t.Status, &t.Result,
			&t.Last.TS, &t.Last.YesPPM, &t.Last.Volume, &t.Last.OI, &t.Last.Vol24)
		t.Last.MarketID = t.ID
		if closeTS != nil {
			t.Close = *closeTS
		}
		return t, err
	})
}

// Snaps returns the history of one market, oldest first
func (s *Store) Snaps(ctx context.Context, code, id string, since time.Time, limit int) ([]venue.Snap, error) {
	rows, err := s.Pool.Query(ctx, `select ts, yes_ppm, volume_micro, oi_micro, vol24_micro from market_snap
		where venue = $1 and market_id = $2 and ts >= $3 order by ts limit $4`, code, id, since, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (v venue.Snap, err error) {
		v.Venue, v.MarketID = code, id
		err = r.Scan(&v.TS, &v.YesPPM, &v.Volume, &v.OI, &v.Vol24)
		return v, err
	})
}

const tradeCols = `id, market_id, ts, side, price_ppm, qty_micro, usd_micro, maker, taker`

func scanTrade(code string) func(pgx.CollectableRow) (venue.Trade, error) {
	return func(r pgx.CollectableRow) (t venue.Trade, err error) {
		t.Venue = code
		err = r.Scan(&t.ID, &t.MarketID, &t.TS, &t.Side, &t.PricePPM, &t.Qty, &t.USD, &t.Maker, &t.Taker)
		return t, err
	}
}

// LargeTrades returns the newest trades of at least minUSD micro-USDC
func (s *Store) LargeTrades(ctx context.Context, code string, minUSD int64, limit int) ([]venue.Trade, error) {
	rows, err := s.Pool.Query(ctx, `select `+tradeCols+` from trade where venue = $1 and usd_micro >= $2
		order by ts desc, id desc limit $3`, code, minUSD, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanTrade(code))
}

// WalletTrades returns the trades a wallet took part in as taker or maker, newest first
func (s *Store) WalletTrades(ctx context.Context, code string, wallet []byte, limit int) ([]venue.Trade, error) {
	rows, err := s.Pool.Query(ctx, `select `+tradeCols+` from trade where venue = $1 and (taker = $2 or maker = $2)
		order by ts desc, id desc limit $3`, code, wallet, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanTrade(code))
}

// EachTrade streams every trade of a venue from since in time order, for the jobs that rebuild from the tape
func (s *Store) EachTrade(ctx context.Context, code string, since time.Time, fn func(venue.Trade) error) error {
	rows, err := s.Pool.Query(ctx, `select `+tradeCols+` from trade where venue = $1 and ts >= $2 order by ts, id`, code, since)
	if err != nil {
		return err
	}
	defer rows.Close()
	scan := scanTrade(code)
	for rows.Next() {
		t, err := scan(rows)
		if err != nil {
			return err
		}
		if err := fn(t); err != nil {
			return err
		}
	}
	return rows.Err()
}

// PruneSnaps drops snapshots older than before and returns how many went
func (s *Store) PruneSnaps(ctx context.Context, before time.Time) (int64, error) {
	tag, err := s.Pool.Exec(ctx, `delete from market_snap where ts < $1`, before)
	return tag.RowsAffected(), err
}

// PruneTrades drops the trades of one venue older than before, for venues whose tape is not the record of a wallet
func (s *Store) PruneTrades(ctx context.Context, code string, before time.Time) (int64, error) {
	tag, err := s.Pool.Exec(ctx, `delete from trade where venue = $1 and ts < $2`, code, before)
	return tag.RowsAffected(), err
}
