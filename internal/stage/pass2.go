package stage

import (
	"cmp"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"pm/internal/refs"
	"pm/internal/wash"
)

const (
	pass2Stage     = "pass2"
	evidenceTrades = 20
	tradeColumns   = `(wallet_id UINTEGER, cp_id UINTEGER, block UINTEGER, log_index UINTEGER, market_id UINTEGER,
		is_buy BOOLEAN, outcome UTINYINT, shares UBIGINT, usdc UBIGINT, ts UINTEGER)`
)

var thetas = [...]float64{0.8, 0.9, 0.99}

var pass2Tables = []string{
	"CREATE TABLE IF NOT EXISTS wash_wallet (wallet_id UINTEGER, wash_shares UBIGINT)",
	"CREATE TABLE IF NOT EXISTS wash_market (market_id UINTEGER, theta UINTEGER, paired_shares UBIGINT, wash_shares UBIGINT)",
	"CREATE TABLE IF NOT EXISTS wash_cp (wallet_id UINTEGER, cp_id UINTEGER, markets UINTEGER, trades UINTEGER, shares UBIGINT)",
	"CREATE TABLE IF NOT EXISTS wash_trade " + tradeColumns,
	"CREATE TABLE IF NOT EXISTS wash_alg1 (bucket UTINYINT, a80 UBIGINT, a90 UBIGINT, a99 UBIGINT)",
}

/* State */

type pass2 struct {
	o Options
	x []float64
}

type trade struct {
	maker, taker, block, log, ts uint32
	shares, usdc                 uint64
	outcome                      uint8
	buy                          bool
}

type flagRow struct {
	wallet, cp, block, log, market, ts uint32
	shares, usdc                       uint64
	outcome                            uint8
	buy                                bool
}

type marketWash struct {
	id, theta       uint32
	paired, flagged uint64
}

type washBucket struct {
	x       []float64
	trades  []trade
	pairs   []wash.Pair
	wallets map[uint32]uint64
	markets []marketWash
	alg1    [len(thetas)]uint64
	flags   *sink[flagRow]
}

/* Entry */

func Pass2(ctx context.Context, o Options) error {
	p, err := newPass2(ctx, o)
	if err != nil {
		return err
	}
	done, err := doneItems(ctx, o.Work, pass2Stage)
	if err != nil {
		return fmt.Errorf("pass2: %w", err)
	}
	for n := range refs.Buckets {
		if !done[bucketName(n)] {
			start := time.Now()
			if err := p.bucket(ctx, n); err != nil {
				return fmt.Errorf("pass2 bucket %02d: %w", n, err)
			}
			slog.Info("pass2 bucket", "bucket", n, "secs", int(time.Since(start).Seconds()))
		}
		// a crash between the commit and this removal leaves files that the next run clears
		if err := os.RemoveAll(filepath.Join(o.StageDir, "b="+bucketName(n))); err != nil {
			return err
		}
	}
	return p.finish(ctx)
}

func newPass2(ctx context.Context, o Options) (*pass2, error) {
	solved, err := doneItems(ctx, o.Work, solveStage)
	if err != nil {
		return nil, fmt.Errorf("pass2: %w: %w", ErrIncomplete, err)
	}
	if !solved[solveItem] {
		return nil, fmt.Errorf("pass2: %w, solve has not finished", ErrIncomplete)
	}
	for _, ddl := range pass2Tables {
		if _, err := o.Work.ExecContext(ctx, ddl); err != nil {
			return nil, fmt.Errorf("pass2 setup: %w", err)
		}
	}
	if err := attachScratch(ctx, o.Work); err != nil {
		return nil, fmt.Errorf("pass2 setup: %w", err)
	}
	n, err := walletCount(ctx, o)
	if err != nil {
		return nil, err
	}
	rows, err := o.Work.QueryContext(ctx, "SELECT wallet_id, x FROM wash_score")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	x := make([]float64, n)
	for rows.Next() {
		var id uint32
		var score float64
		if err := rows.Scan(&id, &score); err != nil {
			return nil, err
		}
		if int(id) >= n {
			return nil, fmt.Errorf("pass2: score of wallet %d is outside the dictionary of %d wallets", id, n)
		}
		x[id] = score
	}
	return &pass2{o: o, x: x}, rows.Err()
}

/* Bucket */

func (p *pass2) bucket(ctx context.Context, n int) error {
	fills, err := source(filepath.Join(p.o.StageDir, "b="+bucketName(n)), "fill")
	if err != nil {
		return err
	}
	conn, err := p.o.Work.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "CREATE OR REPLACE TEMP TABLE wash_flagged "+tradeColumns); err != nil {
		return err
	}
	b := &washBucket{x: p.x, wallets: map[uint32]uint64{}, flags: newSink(conn, "wash_flagged", appendFlag)}
	if fills != "" {
		if err := p.stream(ctx, b, fills); err != nil {
			return err
		}
	}
	if err := b.flags.flush(); err != nil {
		return err
	}
	for _, stmt := range []string{
		// rank breaks ties by block and log index so the cut is the same on every run
		fmt.Sprintf(`INSERT INTO wash_trade SELECT * EXCLUDE (rank) FROM (
			SELECT *, row_number() OVER (PARTITION BY wallet_id ORDER BY shares DESC, block, log_index) AS rank FROM wash_flagged)
			WHERE rank <= %d`, evidenceTrades),
		`INSERT INTO wash_cp SELECT wallet_id, cp_id, CAST(count(DISTINCT market_id) AS UINTEGER), CAST(count(*) AS UINTEGER),
			CAST(sum(shares) AS UBIGINT) FROM wash_flagged GROUP BY wallet_id, cp_id`,
		"DROP TABLE wash_flagged",
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	if err := b.save(conn); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO wash_alg1 VALUES (?, ?, ?, ?)", n, b.alg1[0], b.alg1[1], b.alg1[2]); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT OR REPLACE INTO build_state VALUES (?, ?, 'done', 0, 0, now())", pass2Stage, bucketName(n)); err != nil {
		return err
	}
	return tx.Commit()
}

// stream holds one market at a time, so a bucket costs the memory of its largest market
func (p *pass2) stream(ctx context.Context, b *washBucket, fills string) error {
	sp, err := newSpool(ctx, p.o.Work, "trade", "market_id", `SELECT t.market_id, wm.wallet_id AS maker, wt.wallet_id AS taker, t.outcome, f.buy, f.shares, f.block, f.log_index, f.ts, f.usdc
		FROM `+fills+` f
		JOIN tok t ON t.code = f.tok
		JOIN wallet_dict wm ON wm.code = f.maker
		JOIN wallet_dict wt ON wt.code = f.taker
		WHERE f.shares > 0 AND f.maker <> f.taker`)
	if err != nil {
		return err
	}
	var market uint32
	err = sp.eachPage(ctx, "market_id, maker, taker, outcome, buy, shares, block, log_index, ts, usdc", func(rows *sql.Rows) error {
		for rows.Next() {
			var id uint32
			var t trade
			if err := rows.Scan(&id, &t.maker, &t.taker, &t.outcome, &t.buy, &t.shares, &t.block, &t.log, &t.ts, &t.usdc); err != nil {
				return err
			}
			if len(b.trades) > 0 && id != market {
				if err := b.market(market); err != nil {
					return err
				}
			}
			market = id
			b.trades = append(b.trades, t)
		}
		return rows.Err()
	})
	if err == nil && len(b.trades) > 0 {
		err = b.market(market)
	}
	return errors.Join(err, sp.drop(ctx))
}

/* Market */

func (b *washBucket) market(id uint32) error {
	b.pairs = b.pairs[:0]
	var paired uint64
	for _, t := range b.trades {
		b.pairs = append(b.pairs, wash.Pair{I: min(t.maker, t.taker), J: max(t.maker, t.taker), V: t.shares})
		paired += t.shares
	}
	slices.SortFunc(b.pairs, func(a, c wash.Pair) int { return cmp.Or(cmp.Compare(a.I, c.I), cmp.Compare(a.J, c.J)) })
	theta, found := wash.Theta(mergePairs(b.pairs), b.x)
	m := marketWash{id: id, theta: uint32(math.Round(theta * 1e6)), paired: paired}
	for _, t := range b.trades {
		xm, xt := b.x[t.maker], b.x[t.taker]
		if low := min(xm, xt); low >= thetas[0] {
			for k, cut := range thetas {
				if low >= cut {
					b.alg1[k] += t.shares
				}
			}
		}
		if !found || !wash.Flagged(theta, xm, xt) {
			continue
		}
		m.flagged += t.shares
		b.wallets[t.maker] += t.shares
		b.wallets[t.taker] += t.shares
		maker := flagRow{wallet: t.maker, cp: t.taker, block: t.block, log: t.log, market: id, ts: t.ts, shares: t.shares, usdc: t.usdc, outcome: t.outcome, buy: t.buy}
		taker := maker
		taker.wallet, taker.cp, taker.buy = t.taker, t.maker, !t.buy
		if err := b.flags.add(maker); err != nil {
			return err
		}
		if err := b.flags.add(taker); err != nil {
			return err
		}
	}
	b.markets = append(b.markets, m)
	b.trades = b.trades[:0]
	return nil
}

func mergePairs(pairs []wash.Pair) []wash.Pair {
	out := pairs[:0]
	for _, p := range pairs {
		if n := len(out); n > 0 && out[n-1].I == p.I && out[n-1].J == p.J {
			out[n-1].V += p.V
			continue
		}
		out = append(out, p)
	}
	return out
}

/* Output */

func appendFlag(app *duckdb.Appender, r flagRow) error {
	return app.AppendRow(r.wallet, r.cp, r.block, r.log, r.market, r.buy, r.outcome, r.shares, r.usdc, r.ts)
}

func (b *washBucket) save(conn *sql.Conn) error {
	err := appendRows(conn, "wash_market", func(app *duckdb.Appender) error {
		for _, m := range b.markets {
			var theta driver.Value
			if m.flagged > 0 {
				theta = m.theta
			}
			if err := app.AppendRow(m.id, theta, m.paired, m.flagged); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return appendRows(conn, "wash_wallet", func(app *duckdb.Appender) error {
		for id, shares := range b.wallets {
			if err := app.AppendRow(id, shares); err != nil {
				return err
			}
		}
		return nil
	})
}

func (p *pass2) finish(ctx context.Context) error {
	for _, stmt := range []string{
		`CREATE OR REPLACE TABLE wash_wallet AS
			SELECT wallet_id, CAST(sum(wash_shares) AS UBIGINT) AS wash_shares FROM wash_wallet GROUP BY wallet_id ORDER BY wallet_id`,
		`INSERT OR REPLACE INTO audit
			WITH t AS (SELECT CAST(coalesce(sum(paired_shares), 0) AS DOUBLE) AS paired, CAST(coalesce(sum(wash_shares), 0) AS DOUBLE) AS flagged FROM wash_market),
				a AS (SELECT CAST(coalesce(sum(a80), 0) AS DOUBLE) AS a80, CAST(coalesce(sum(a90), 0) AS DOUBLE) AS a90, CAST(coalesce(sum(a99), 0) AS DOUBLE) AS a99 FROM wash_alg1)
			SELECT 'alg1_80', coalesce(a.a80 / nullif(t.paired, 0), 0) FROM t, a
			UNION ALL SELECT 'alg1_90', coalesce(a.a90 / nullif(t.paired, 0), 0) FROM t, a
			UNION ALL SELECT 'alg1_99', coalesce(a.a99 / nullif(t.paired, 0), 0) FROM t, a
			UNION ALL SELECT 'alg2_share', coalesce(t.flagged / nullif(t.paired, 0), 0) FROM t`,
	} {
		if _, err := p.o.Work.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("pass2 finish: %w", err)
		}
	}
	return nil
}
