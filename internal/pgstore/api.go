package pgstore

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"pm/internal/hip4"
	"pm/internal/levr"
	"pm/internal/skill"
	"pm/internal/venue"
)

var ErrNotFound = errors.New("nothing recorded for this identifier")

// topWindow is how far back the latest snapshot of a market may lie to still count as on the venue's current list
const topWindow = 30 * time.Minute

// Gaps returns the running gap count a collector keeps in the cursor table, and false when the venue keeps none
func (s *Store) Gaps(ctx context.Context, code string) (int64, bool, error) {
	v, err := s.Cursor(ctx, code+"/gaps")
	if err != nil || v == "" {
		return 0, false, err
	}
	n, err := strconv.ParseInt(v, 10, 64)
	return n, err == nil, nil
}

// TopRecent ranks the markets of a venue by volume among those with a snapshot in the newest sweeps
func (s *Store) TopRecent(ctx context.Context, code string, limit int) ([]TopMarket, error) {
	rows, err := s.Pool.Query(ctx, `with latest as (
			select distinct on (market_id) market_id, ts, yes_ppm, volume_micro, oi_micro, vol24_micro from market_snap
			where venue = $1 and ts >= (select max(ts) from market_snap where venue = $1) - $3::interval
			order by market_id, ts desc
		)
		select m.id, m.title, m.kind, m.close_ts, m.status, m.result, l.ts, l.yes_ppm, l.volume_micro, l.oi_micro, l.vol24_micro
		from latest l join market m on m.venue = $1 and m.id = l.market_id
		order by l.vol24_micro desc, l.volume_micro desc, m.id limit $2`, code, limit, topWindow)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanTop(code))
}

func scanTop(code string) func(pgx.CollectableRow) (TopMarket, error) {
	return func(r pgx.CollectableRow) (t TopMarket, err error) {
		var closeTS *time.Time
		t.Venue, t.Last.Venue = code, code
		err = r.Scan(&t.ID, &t.Title, &t.Kind, &closeTS, &t.Status, &t.Result,
			&t.Last.TS, &t.Last.YesPPM, &t.Last.Volume, &t.Last.OI, &t.Last.Vol24)
		t.Last.MarketID = t.ID
		if closeTS != nil {
			t.Close = *closeTS
		}
		return t, err
	}
}

// TopTape ranks the markets of a tape-only venue by the dollars traded since a time, with the last price as the quote
func (s *Store) TopTape(ctx context.Context, code string, since time.Time, limit int) ([]TopMarket, error) {
	rows, err := s.Pool.Query(ctx, `select m.id, m.title, m.kind, m.close_ts, m.status, m.result, t.last_ts, t.last_ppm, 0::bigint, 0::bigint, t.vol
		from (
			select market_id, sum(usd_micro) as vol, max(ts) as last_ts, (array_agg(price_ppm order by ts desc, id desc))[1] as last_ppm
			from trade where venue = $1 and ts >= $2 group by market_id
		) t join market m on m.venue = $1 and m.id = t.market_id
		order by t.vol desc, m.id limit $3`, code, since, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanTop(code))
}

// NamedTrade is a recorded trade with the title of its market
type NamedTrade struct {
	venue.Trade
	Title string
}

// LargeNamed returns the newest trades of at least minUSD micro-USDC with their market titles
func (s *Store) LargeNamed(ctx context.Context, code string, minUSD int64, limit int) ([]NamedTrade, error) {
	rows, err := s.Pool.Query(ctx, `select t.id, t.market_id, t.ts, t.side, t.price_ppm, t.qty_micro, t.usd_micro, t.maker, t.taker, coalesce(m.title, '')
		from trade t left join market m on m.venue = t.venue and m.id = t.market_id
		where t.venue = $1 and t.usd_micro >= $2 order by t.ts desc, t.id desc limit $3`, code, minUSD, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (t NamedTrade, err error) {
		t.Venue = code
		err = r.Scan(&t.ID, &t.MarketID, &t.TS, &t.Side, &t.PricePPM, &t.Qty, &t.USD, &t.Maker, &t.Taker, &t.Title)
		return t, err
	})
}

// TapeSeries turns a tape-only venue's trades into one point per bucket, holding the last price, keeping the newest buckets and returning them oldest first
func (s *Store) TapeSeries(ctx context.Context, code, id string, since time.Time, bucket time.Duration, limit int) ([]venue.Snap, error) {
	rows, err := s.Pool.Query(ctx, `select * from (select to_timestamp(floor(extract(epoch from ts)::float8 / $4::float8) * $4::float8) as b, (array_agg(price_ppm order by ts desc, id desc))[1]
		from trade where venue = $1 and market_id = $2 and ts >= $3 group by b order by b desc limit $5) n order by b`, code, id, since, bucket.Seconds(), limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (v venue.Snap, err error) {
		v.Venue, v.MarketID = code, id
		err = r.Scan(&v.TS, &v.YesPPM)
		return v, err
	})
}

// HLWallet returns the recorded totals of one HIP-4 trader
func (s *Store) HLWallet(ctx context.Context, addr []byte) (hip4.Wallet, error) {
	rows, err := s.Pool.Query(ctx, `select `+hlCols+` from hl_wallet where address = $1`, addr)
	if err != nil {
		return hip4.Wallet{}, err
	}
	w, err := pgx.CollectOneRow(rows, scanHL)
	if errors.Is(err, pgx.ErrNoRows) {
		return w, ErrNotFound
	}
	return w, err
}

const hlCols = `address, fills, volume_micro, gross_pnl, markets, resolved, won, rated, coalesce(brier, 0), coalesce(edge, 0), coalesce(edge_se, 0)`

func scanHL(r pgx.CollectableRow) (w hip4.Wallet, err error) {
	err = r.Scan(&w.Address, &w.Fills, &w.Volume, &w.Gross, &w.Markets, &w.Resolved, &w.Won, &w.Rated, &w.Brier, &w.Edge, &w.EdgeSE)
	return w, err
}

type HLPage struct {
	Rows  []hip4.Wallet
	Total int64
}

var hlOrder = map[string]struct{ where, order string }{
	"pnl":    {"true", "gross_pnl desc, address"},
	"volume": {"true", "volume_micro desc, address"},
	"skill":  {"rated", "edge - " + strconv.FormatFloat(skill.Z, 'f', -1, 64) + " * edge_se desc, address"},
}

// HLBoard ranks HIP-4 traders by pnl, volume or the lower bound of their edge
func (s *Store) HLBoard(ctx context.Context, board string, limit, offset int) (HLPage, error) {
	o, ok := hlOrder[board]
	if !ok {
		return HLPage{}, ErrNotFound
	}
	var p HLPage
	if err := s.Pool.QueryRow(ctx, `select count(*) from hl_wallet where `+o.where).Scan(&p.Total); err != nil {
		return p, err
	}
	rows, err := s.Pool.Query(ctx, `select `+hlCols+` from hl_wallet where `+o.where+` order by `+o.order+` limit $1 offset $2`, limit, offset)
	if err != nil {
		return p, err
	}
	p.Rows, err = pgx.CollectRows(rows, scanHL)
	return p, err
}

// WalletPositions returns every position of one Levr wallet, oldest first
func (s *Store) WalletPositions(ctx context.Context, wallet []byte) ([]levr.Position, error) {
	rows, err := s.Pool.Query(ctx, `select `+positionCols+` from levr_position where wallet = $1 order by opened_ts, id`, wallet)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanPosition)
}

type LevrPage struct {
	Rows  []levr.Wallet
	Total int64
}

var levrOrder = map[string]string{"pnl": "realized_pnl desc, address", "volume": "size_micro desc, address"}

// LevrBoard ranks Levr wallets by realized pnl or by the size of the positions they opened
func (s *Store) LevrBoard(ctx context.Context, board string, limit, offset int) (LevrPage, error) {
	order, ok := levrOrder[board]
	if !ok {
		return LevrPage{}, ErrNotFound
	}
	var p LevrPage
	if err := s.Pool.QueryRow(ctx, `select count(*) from levr_wallet`).Scan(&p.Total); err != nil {
		return p, err
	}
	rows, err := s.Pool.Query(ctx, `select address, positions, stake_micro, size_micro, liquidations, settled, won, realized_pnl
		from levr_wallet order by `+order+` limit $1 offset $2`, limit, offset)
	if err != nil {
		return p, err
	}
	p.Rows, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (w levr.Wallet, err error) {
		var settled int
		err = r.Scan(&w.Address, &w.Positions, &w.Stake, &w.Size, &w.Liquidated, &settled, &w.Won, &w.Realized)
		w.Lost = settled - w.Won - w.Liquidated
		return w, err
	})
	return p, err
}
