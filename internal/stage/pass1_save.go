package stage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/duckdb/duckdb-go/v2"

	"pm/internal/skill"
)

/* Output */

type marketRow struct {
	wallet, market, f, o uint32
	volume, fees         uint64
	gross                int64
	rated                bool
}

type sink[R any] struct {
	conn  *sql.Conn
	table string
	rows  []R
	write func(*duckdb.Appender, R) error
}

func newSink[R any](conn *sql.Conn, table string, write func(*duckdb.Appender, R) error) *sink[R] {
	return &sink[R]{conn: conn, table: table, rows: make([]R, 0, flushRows), write: write}
}

func (s *sink[R]) add(r R) error {
	s.rows = append(s.rows, r)
	if len(s.rows) < flushRows {
		return nil
	}
	return s.flush()
}

func (s *sink[R]) flush() error {
	err := appendRows(s.conn, s.table, func(app *duckdb.Appender) error {
		for _, r := range s.rows {
			if err := s.write(app, r); err != nil {
				return err
			}
		}
		return nil
	})
	s.rows = s.rows[:0]
	return err
}

func appendMarketRow(app *duckdb.Appender, r marketRow) error {
	var f, o driver.Value
	if r.rated {
		f, o = r.f, r.o
	}
	return app.AppendRow(r.wallet, r.market, r.volume, r.gross, r.fees, f, o)
}

func appendRows(conn *sql.Conn, table string, fill func(*duckdb.Appender) error) error {
	return conn.Raw(func(raw any) error {
		app, err := duckdb.NewAppender(raw.(driver.Conn), "", "main", table)
		if err != nil {
			return err
		}
		return errors.Join(fill(app), app.Close())
	})
}

func (p *pass1) saveMarkets(conn *sql.Conn, b *bucket) error {
	return appendRows(conn, "market_acc", func(app *duckdb.Appender) error {
		for _, id := range b.ids {
			m := &p.markets[id]
			args := []driver.Value{id, m.trades, m.shares, m.usdc}
			for i := range horizons {
				var price driver.Value
				if m.priced[i] {
					price = uint32(m.price[i])
				}
				args = append(args, price)
			}
			if err := app.AppendRow(args...); err != nil {
				return err
			}
		}
		return nil
	})
}

func (p *pass1) acc(wallet uint32) *acc {
	i := p.slot[wallet]
	if i == 0 {
		p.slots = append(p.slots, acc{wallet: wallet})
		i = uint32(len(p.slots))
		p.slot[wallet] = i
	}
	return &p.slots[i-1]
}

func (p *pass1) reset() {
	for i := range p.slots {
		p.slot[p.slots[i].wallet] = 0
	}
	p.slots = p.slots[:0]
}

func (p *pass1) saveAccs(conn *sql.Conn) error {
	err := appendRows(conn, "wallet_acc_part", func(app *duckdb.Appender) error {
		for i := range p.slots {
			a := &p.slots[i]
			if !a.active() {
				continue
			}
			var s skill.Acc
			if a.skill != nil {
				s = *a.skill
			}
			err := app.AppendRow(a.wallet, a.fills, a.volume, a.buyVolume, a.takerRows, a.takerVolume, a.firstTS, a.lastTS,
				a.sum.Markets, a.sum.Gross, a.sum.Fees, a.sum.BestNet, a.sum.Resolved, a.sum.Won, a.open,
				s.N, a.ratedEvents, s.SumT, s.SumT2, a.x0Num, a.paired)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return appendRows(conn, "wallet_cal_part", func(app *duckdb.Appender) error {
		for i := range p.slots {
			a := &p.slots[i]
			if a.skill == nil {
				continue
			}
			for k, c := range a.skill.Cal {
				if c.N == 0 {
					continue
				}
				if err := app.AppendRow(a.wallet, uint8(k), c.N, c.SumF, c.SumO); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// finishSpans is how many wallet id ranges the sums run in, one range at a time keeps the spill small
const finishSpans = 16

// finish adds the shares of every bucket up, a wallet's first trade is the earliest and its best win the largest of buckets that had a market
func (p *pass1) finish(ctx context.Context) error {
	if _, err := p.o.Work.ExecContext(ctx, "DELETE FROM build_state WHERE stage = ? AND item = ?", pass1Stage, finishItem); err != nil {
		return fmt.Errorf("pass1 finish: %w", err)
	}
	var top uint64
	if err := p.o.Work.QueryRowContext(ctx, "SELECT CAST(coalesce(max(wallet_id), 0) AS UBIGINT) + 1 FROM wallet_dict").Scan(&top); err != nil {
		return fmt.Errorf("pass1 finish: %w", err)
	}
	step := top/finishSpans + 1
	for i := range uint64(finishSpans) {
		where := fmt.Sprintf("WHERE wallet_id >= %d AND wallet_id < %d", i*step, (i+1)*step)
		for _, sum := range []struct{ table, query string }{{"wallet_acc", accSumSQL}, {"wallet_cal", calSumSQL}} {
			stmt := "INSERT INTO " + sum.table + " " + fmt.Sprintf(sum.query, where)
			if i == 0 {
				stmt = "CREATE OR REPLACE TABLE " + sum.table + " AS " + fmt.Sprintf(sum.query, where)
			}
			if _, err := p.o.Work.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("pass1 finish: %w", err)
			}
		}
	}
	if _, err := p.o.Work.ExecContext(ctx, "INSERT OR REPLACE INTO build_state VALUES (?, ?, 'done', 0, 0, now())", pass1Stage, finishItem); err != nil {
		return fmt.Errorf("pass1 finish: %w", err)
	}
	return nil
}

const accSumSQL = `SELECT wallet_id,
	CAST(sum(fills) AS UINTEGER) AS fills, CAST(sum(volume_usdc) AS UBIGINT) AS volume_usdc, CAST(sum(buy_volume_usdc) AS UBIGINT) AS buy_volume_usdc,
	CAST(sum(taker_rows) AS UINTEGER) AS taker_rows, CAST(sum(taker_volume_usdc) AS UBIGINT) AS taker_volume_usdc,
	CAST(coalesce(min(first_ts) FILTER (WHERE first_ts > 0), 0) AS UINTEGER) AS first_ts, CAST(max(last_ts) AS UINTEGER) AS last_ts,
	CAST(sum(markets) AS UINTEGER) AS markets, CAST(sum(gross_pnl) AS BIGINT) AS gross_pnl, CAST(sum(fees) AS UBIGINT) AS fees,
	CAST(coalesce(max(best_net) FILTER (WHERE markets > 0), 0) AS BIGINT) AS best_net,
	CAST(sum(resolved_markets) AS UINTEGER) AS resolved_markets, CAST(sum(won_markets) AS UINTEGER) AS won_markets,
	CAST(sum(open_unresolved) AS UINTEGER) AS open_unresolved, CAST(sum(rated_markets) AS UINTEGER) AS rated_markets,
	CAST(sum(rated_events) AS UINTEGER) AS rated_events, CAST(sum(sum_t) AS BIGINT) AS sum_t, CAST(sum(sum_t2) AS UBIGINT) AS sum_t2,
	CAST(sum(x0_num) AS UBIGINT) AS x0_num, CAST(sum(paired_shares) AS UBIGINT) AS paired_shares
	FROM wallet_acc_part %s GROUP BY wallet_id ORDER BY wallet_id`

const calSumSQL = `SELECT wallet_id, bucket, CAST(sum(n) AS UINTEGER) AS n, CAST(sum(sum_f) AS UBIGINT) AS sum_f,
	CAST(sum(sum_o) AS UBIGINT) AS sum_o FROM wallet_cal_part %s GROUP BY wallet_id, bucket ORDER BY wallet_id, bucket`

func (p *pass1) record(ctx context.Context, tx *sql.Tx, b *bucket) error {
	counts := map[string]uint64{"duplicate_rows": b.duplicates, "unmapped_fee_rows": b.unmappedFees}
	for _, id := range b.ids {
		m := &p.markets[id]
		if !m.hasT {
			continue
		}
		for i, h := range horizons {
			if !m.priced[i] {
				counts[noPriceKey(h)]++
			}
		}
	}
	for key, n := range counts {
		if n == 0 {
			continue
		}
		if _, err := tx.ExecContext(ctx, "UPDATE audit SET value = value + ? WHERE key = ?", float64(n), key); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx, "INSERT OR REPLACE INTO build_state VALUES (?, ?, 'done', 0, 0, now())", pass1Stage, bucketName(b.n))
	return err
}
