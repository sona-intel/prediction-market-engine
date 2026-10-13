package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"pm/internal/skill"
)

var (
	Boards       = []string{"pnl", "volume", "skill"}
	Kinds        = []string{"other", "updown", "unknown", "all"}
	HorizonHours = []int{4, 12, 24, 168}
)

var errUnsupported = errors.New("unsupported value")

/* Trader */

type Trader struct {
	Wallet                                      [20]byte
	Owner                                       []byte
	OwnerKind                                   uint8
	Fills, Markets, TakerRows                   int64
	VolumeUSDC, BuyVolumeUSDC, TakerVolumeUSDC  int64
	FirstTS, LastTS                             int64
	AMMTrades                                   int64
	GrossPnL, Fees, BiggestWin                  int64
	ResolvedMarkets, WonMarkets, OpenUnresolved int64
	Rated                                       bool
	RatedMarkets, RatedEvents                   int64
	Brier, Edge, EdgeSE                         *float64
	Calibration                                 []WalletBucket
	WashScore                                   *float64
	PairedShares, WashShares                    int64
	Counterparties                              []Counterparty
	Trades                                      []WashTrade
	Coverage                                    Coverage
}

type WalletBucket struct {
	Bucket, N, SumF, SumO int64
}

type Counterparty struct {
	Wallet                  [20]byte
	Score                   float64
	Markets, Trades, Shares int64
}

type WashTrade struct {
	Block, LogIndex, TS int64
	Condition           [32]byte
	Question            *string
	Counterparty        [20]byte
	Buy                 bool
	Outcome             int
	Shares, USDC        int64
}

// a range filter reads the zonemaps, an equality filter pins the whole address index in memory
const traderSQL = `SELECT wallet_id, owner, owner_kind, fills, markets, taker_rows, volume_usdc, buy_volume_usdc, taker_volume_usdc,
	first_ts, last_ts, amm_trades, gross_pnl, CAST(fees AS BIGINT), biggest_win, resolved_markets, won_markets, open_unresolved,
	rated, rated_markets, rated_events, brier, edge, edge_se, wash_score, paired_shares, wash_shares
	FROM wallet WHERE address >= ? AND address <= ?`

const (
	walletCalibrationSQL = "SELECT bucket, n, sum_f, sum_o FROM wallet_calibration WHERE wallet_id = ? ORDER BY bucket"

	counterpartySQL = `SELECT w.address, w.wash_score, c.markets, c.trades, c.shares
		FROM wash_counterparty c JOIN wallet w ON w.wallet_id = c.counterparty_id
		WHERE c.wallet_id = ? ORDER BY c.rank`

	washTradeSQL = `SELECT t.block, t.log_index, t.ts, m.condition, m.question, w.address, t.is_buy, t.outcome, t.shares, t.usdc
		FROM wash_trade t JOIN market m ON m.market_id = t.market_id JOIN wallet w ON w.wallet_id = t.counterparty_id
		WHERE t.wallet_id = ? ORDER BY t.rank`
)

func (s *Store) Trader(ctx context.Context, addr [20]byte) (Trader, error) {
	var t Trader
	err := s.read(func(h *handle) error {
		id, err := scanTrader(ctx, h.db, addr, &t)
		if err != nil {
			return err
		}
		if t.Rated {
			if t.Calibration, err = walletBuckets(ctx, h.db, id); err != nil {
				return err
			}
		}
		if t.WashShares > 0 {
			if t.Counterparties, err = counterparties(ctx, h.db, id); err != nil {
				return err
			}
			if t.Trades, err = washTrades(ctx, h.db, id); err != nil {
				return err
			}
		}
		t.Wallet, t.Coverage = addr, h.meta.coverage
		return nil
	})
	if err != nil {
		return Trader{}, err
	}
	return t, nil
}

func scanTrader(ctx context.Context, db *sql.DB, addr [20]byte, t *Trader) (id uint32, err error) {
	var kind sql.Null[uint8]
	var brier, edge, se, score sql.Null[float64]
	err = db.QueryRowContext(ctx, traderSQL, addr[:], addr[:]).Scan(&id, &t.Owner, &kind, &t.Fills, &t.Markets, &t.TakerRows,
		&t.VolumeUSDC, &t.BuyVolumeUSDC, &t.TakerVolumeUSDC, &t.FirstTS, &t.LastTS, &t.AMMTrades, &t.GrossPnL, &t.Fees,
		&t.BiggestWin, &t.ResolvedMarkets, &t.WonMarkets, &t.OpenUnresolved, &t.Rated, &t.RatedMarkets, &t.RatedEvents,
		&brier, &edge, &se, &score, &t.PairedShares, &t.WashShares)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	t.OwnerKind = kind.V
	t.Brier, t.Edge, t.EdgeSE, t.WashScore = ptr(brier), ptr(edge), ptr(se), ptr(score)
	return id, nil
}

func ptr[T any](v sql.Null[T]) *T {
	if !v.Valid {
		return nil
	}
	return &v.V
}

func walletBuckets(ctx context.Context, db *sql.DB, id uint32) ([]WalletBucket, error) {
	rows, err := db.QueryContext(ctx, walletCalibrationSQL, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WalletBucket
	for rows.Next() {
		var b WalletBucket
		if err := rows.Scan(&b.Bucket, &b.N, &b.SumF, &b.SumO); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func counterparties(ctx context.Context, db *sql.DB, id uint32) ([]Counterparty, error) {
	rows, err := db.QueryContext(ctx, counterpartySQL, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Counterparty
	for rows.Next() {
		var c Counterparty
		var addr []byte
		var score sql.Null[float64]
		if err := rows.Scan(&addr, &score, &c.Markets, &c.Trades, &c.Shares); err != nil {
			return nil, err
		}
		c.Wallet, c.Score = [20]byte(addr), score.V
		out = append(out, c)
	}
	return out, rows.Err()
}

func washTrades(ctx context.Context, db *sql.DB, id uint32) ([]WashTrade, error) {
	rows, err := db.QueryContext(ctx, washTradeSQL, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WashTrade
	for rows.Next() {
		var t WashTrade
		var condition, cp []byte
		var question sql.Null[string]
		if err := rows.Scan(&t.Block, &t.LogIndex, &t.TS, &condition, &question, &cp, &t.Buy, &t.Outcome, &t.Shares, &t.USDC); err != nil {
			return nil, err
		}
		t.Condition, t.Counterparty, t.Question = [32]byte(condition), [20]byte(cp), ptr(question)
		out = append(out, t)
	}
	return out, rows.Err()
}

/* Board */

type Row struct {
	Rank                                 int
	Wallet                               [20]byte
	Fills, Markets, VolumeUSDC, Realized int64
	Final                                bool
	PairedShares, WashShares             int64
	Rated                                bool
	RatedMarkets, RatedEvents            int64
	Brier, Edge, EdgeSE, EdgeLB          float64
}

type Page struct {
	Board         string
	Limit, Offset int
	Total         int64
	Rows          []Row
	Coverage      Coverage
}

var boardOrder = map[string]struct{ where, order string }{
	"pnl":    {"", "gross_pnl - CAST(fees AS BIGINT) DESC"},
	"volume": {"", "volume_usdc DESC"},
	"skill":  {"WHERE rated", "edge - %g * edge_se DESC"},
}

func (s *Store) Board(ctx context.Context, board string, limit, offset int) (Page, error) {
	spec, ok := boardOrder[board]
	if !ok {
		return Page{}, fmt.Errorf("board %q: %w", board, errUnsupported)
	}
	order := spec.order
	if board == "skill" {
		order = fmt.Sprintf(order, skill.Z)
	}
	query := fmt.Sprintf(`SELECT address, fills, markets, volume_usdc, gross_pnl - CAST(fees AS BIGINT), open_unresolved = 0,
			paired_shares, wash_shares, rated, rated_markets, rated_events,
			coalesce(brier, 0), coalesce(edge, 0), coalesce(edge_se, 0), coalesce(edge - %g * edge_se, 0)
		FROM wallet %s ORDER BY %s, volume_usdc DESC, wallet_id LIMIT ? OFFSET ?`, skill.Z, spec.where, order)
	page := Page{Board: board, Limit: limit, Offset: offset, Rows: []Row{}}
	err := s.read(func(h *handle) error {
		rows, err := h.db.QueryContext(ctx, query, limit, offset)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var r Row
			var addr []byte
			if err := rows.Scan(&addr, &r.Fills, &r.Markets, &r.VolumeUSDC, &r.Realized, &r.Final, &r.PairedShares,
				&r.WashShares, &r.Rated, &r.RatedMarkets, &r.RatedEvents, &r.Brier, &r.Edge, &r.EdgeSE, &r.EdgeLB); err != nil {
				return err
			}
			r.Wallet, r.Rank = [20]byte(addr), offset+len(page.Rows)+1
			page.Rows = append(page.Rows, r)
		}
		page.Total, page.Coverage = h.meta.wallets, h.meta.coverage
		if board == "skill" {
			page.Total = h.meta.rated
		}
		return rows.Err()
	})
	if err != nil {
		return Page{}, err
	}
	return page, nil
}

/* Calibration */

type Bucket struct {
	Markets, SumP, SumO int64
}

type Calibration struct {
	HorizonHours int
	Kind         string
	Markets      int64
	SumSq        float64
	NoPrice      int64
	Buckets      []Bucket
	Coverage     Coverage
}

func (s *Store) Calibration(ctx context.Context, horizonH int, kind string) (Calibration, error) {
	if !slices.Contains(HorizonHours, horizonH) {
		return Calibration{}, fmt.Errorf("horizon %d: %w", horizonH, errUnsupported)
	}
	kinds := slices.Index(Kinds, kind)
	if kinds < 0 {
		return Calibration{}, fmt.Errorf("kind %q: %w", kind, errUnsupported)
	}
	lo, hi := kinds, kinds
	if kind == "all" {
		lo, hi = 0, 2
	}
	out := Calibration{HorizonHours: horizonH, Kind: kind, Buckets: make([]Bucket, 20)}
	err := s.read(func(h *handle) error {
		rows, err := h.db.QueryContext(ctx, `SELECT bucket, CAST(sum(markets) AS BIGINT), CAST(sum(sum_p) AS BIGINT),
				CAST(sum(sum_o) AS BIGINT), sum(sum_sq)
			FROM calibration WHERE horizon_h = ? AND kind BETWEEN ? AND ? GROUP BY bucket ORDER BY bucket`, horizonH, lo, hi)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var i int
			var b Bucket
			var sq float64
			if err := rows.Scan(&i, &b.Markets, &b.SumP, &b.SumO, &sq); err != nil {
				return err
			}
			out.Buckets[i] = b
			out.Markets += b.Markets
			out.SumSq += sq
		}
		if err := rows.Err(); err != nil {
			return err
		}
		missing := "SELECT CAST(count(*) AS BIGINT) FROM market WHERE resolved AND p0_" + strconv.Itoa(horizonH) + "h IS NULL AND kind BETWEEN ? AND ?"
		if err := h.db.QueryRowContext(ctx, missing, lo, hi).Scan(&out.NoPrice); err != nil {
			return err
		}
		out.Coverage = h.meta.coverage
		return nil
	})
	if err != nil {
		return Calibration{}, err
	}
	return out, nil
}
