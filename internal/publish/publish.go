package publish

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"pm/internal/refs"
	"pm/internal/skill"
)

// walletRanges cut the sort of wallet_market so it never outgrows the memory limit
const walletRanges = 16

var ErrIncomplete = errors.New("build incomplete")

type Totals struct{ Fills, TakerRows, VolumeUSDC, Markets, Resolved, WithT, MaxWallets, LastBlock int64 }

var FullTotals = Totals{
	Fills:      1172658611,
	TakerRows:  445179390,
	VolumeUSDC: 59899137953110000,
	Markets:    1117152,
	Resolved:   992485,
	WithT:      811034,
	MaxWallets: 2630334,
	LastBlock:  CutoffBlock,
}

/* Run */

func Run(ctx context.Context, work *sql.DB, dir string, expect *Totals) error {
	conn, err := work.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := requireDone(ctx, conn); err != nil {
		return err
	}
	final, building := filepath.Join(dir, "pm.duckdb"), filepath.Join(dir, "pm.duckdb.new")
	if err := removeFiles(building, building+".wal"); err != nil {
		return err
	}
	if err := write(ctx, work, conn, building, expect); err != nil {
		return errors.Join(err, removeFiles(building, building+".wal"))
	}
	return os.Rename(building, final)
}

func removeFiles(paths ...string) error {
	for _, p := range paths {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func requireDone(ctx context.Context, conn *sql.Conn) error {
	rows, err := conn.QueryContext(ctx, "SELECT stage, item, status FROM build_state ORDER BY stage, item")
	if err != nil {
		return fmt.Errorf("%w: %w", ErrIncomplete, err)
	}
	defer rows.Close()
	done := map[string]int{}
	var open []string
	for rows.Next() {
		var stage, item, status string
		if err := rows.Scan(&stage, &item, &status); err != nil {
			return err
		}
		if status == "done" {
			done[stage]++
		} else {
			open = append(open, stage+" "+item+" is "+status)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(open) > 0 {
		return fmt.Errorf("%w: %s", ErrIncomplete, strings.Join(open, "; "))
	}
	for _, need := range []struct {
		stage string
		items int
	}{{"refs", 1}, {"stage", 1}, {"pass1", refs.Buckets + 1}, {"solve", 1}, {"pass2", refs.Buckets}} {
		if done[need.stage] < need.items {
			return fmt.Errorf("%w: %s finished %d of %d items", ErrIncomplete, need.stage, done[need.stage], need.items)
		}
	}
	return nil
}

func exec(ctx context.Context, conn *sql.Conn, name, stmt string, args ...any) error {
	if _, err := conn.ExecContext(ctx, stmt, args...); err != nil {
		return fmt.Errorf("publish %s: %w", name, err)
	}
	return nil
}

func write(ctx context.Context, work *sql.DB, conn *sql.Conn, path string, expect *Totals) (err error) {
	// a false setting lets a parallel insert reorder rows
	if err := exec(ctx, conn, "settings", "SET preserve_insertion_order = true"); err != nil {
		return err
	}
	if err := exec(ctx, conn, "attach", "ATTACH "+quote(path)+" AS pm"); err != nil {
		return err
	}
	attached := true
	defer func() {
		if attached {
			_, detachErr := conn.ExecContext(context.WithoutCancel(ctx), "DETACH pm")
			err = errors.Join(err, detachErr)
		}
	}()
	for _, t := range schema {
		if err := exec(ctx, conn, "schema "+t.name, "CREATE TABLE pm."+t.name+" "+t.columns); err != nil {
			return err
		}
	}
	if err := rate(ctx, work, conn); err != nil {
		return err
	}
	if err := fill(ctx, conn); err != nil {
		return err
	}
	if err := writeMeta(ctx, conn); err != nil {
		return err
	}
	if err := check(ctx, conn, "pm.", expect); err != nil {
		return err
	}
	if err := exec(ctx, conn, "checkpoint", "CHECKPOINT pm"); err != nil {
		return err
	}
	if err := exec(ctx, conn, "detach", "DETACH pm"); err != nil {
		return err
	}
	attached = false
	return nil
}

/* Rating */

// brier and edge come from the skill package so their formulas live in one place
func rate(ctx context.Context, work *sql.DB, conn *sql.Conn) error {
	if err := exec(ctx, conn, "rating table", "CREATE OR REPLACE TEMP TABLE wallet_skill (wallet_id UINTEGER, brier DOUBLE, edge DOUBLE, edge_se DOUBLE)"); err != nil {
		return err
	}
	rows, err := work.QueryContext(ctx, `SELECT wallet_id, rated_markets, rated_events, sum_t, sum_t2 FROM wallet_acc
		WHERE fills > 0 AND rated_markets > 0 AND rated_events > 0 ORDER BY wallet_id`)
	if err != nil {
		return fmt.Errorf("publish rating: %w", err)
	}
	defer rows.Close()
	err = conn.Raw(func(raw any) error {
		app, err := duckdb.NewAppender(raw.(driver.Conn), "temp", "main", "wallet_skill")
		if err != nil {
			return err
		}
		for rows.Next() {
			var id, events uint32
			var acc skill.Acc
			if err := rows.Scan(&id, &acc.N, &events, &acc.SumT, &acc.SumT2); err != nil {
				return errors.Join(err, app.Close())
			}
			if !skill.Rated(acc.N, events) {
				continue
			}
			se, _ := acc.EdgeSE()
			if err := app.AppendRow(id, acc.Brier(), acc.Edge(), se); err != nil {
				return errors.Join(err, app.Close())
			}
		}
		return errors.Join(rows.Err(), app.Close())
	})
	if err != nil {
		return fmt.Errorf("publish rating: %w", err)
	}
	return nil
}

/* Tables */

const (
	walletSQL = `INSERT INTO pm.wallet
		SELECT a.wallet_id, d.address, r.owner, r.owner_kind,
			a.fills, a.volume_usdc, a.buy_volume_usdc, a.taker_rows, a.taker_volume_usdc,
			a.first_ts, a.last_ts, a.markets, coalesce(m.n, 0), a.gross_pnl, a.fees,
			CASE WHEN a.best_net > 1000000 THEN a.best_net ELSE 0 END,
			a.resolved_markets, a.won_markets, a.open_unresolved,
			a.rated_markets, a.rated_events, s.brier, s.edge, s.edge_se, s.wallet_id IS NOT NULL,
			w.x, w.x0, a.paired_shares, coalesce(ww.wash_shares, 0)
		FROM wallet_acc a
		JOIN wallet_dict d ON d.wallet_id = a.wallet_id
		LEFT JOIN wallet_reg r ON r.address = d.address
		LEFT JOIN amm m ON m.address = d.address
		LEFT JOIN wallet_skill s ON s.wallet_id = a.wallet_id
		LEFT JOIN wash_score w ON w.wallet_id = a.wallet_id
		LEFT JOIN wash_wallet ww ON ww.wallet_id = a.wallet_id
		WHERE a.fills > 0
		ORDER BY a.wallet_id`

	marketSQL = `INSERT INTO pm.market
		SELECT m.market_id, m.condition, m.question, m.slug, m.end_date, m.kind, m.neg_risk, e.key, m.resolved,
			m.payout0, m.payout1, m.payout_den, r.ts,
			coalesce(a.trades, 0), coalesce(a.volume_shares, 0), coalesce(a.volume_usdc, 0), a.p0_4h, a.p0_12h, a.p0_24h, a.p0_168h,
			coalesce(w.paired_shares, 0), coalesce(w.wash_shares, 0), w.theta
		FROM market m
		JOIN ek e ON e.code = m.ek
		LEFT JOIN first_redeem r ON r.market_id = m.market_id
		LEFT JOIN market_acc a ON a.market_id = m.market_id
		LEFT JOIN wash_market w ON w.market_id = m.market_id
		ORDER BY m.market_id`

	walletMarketSQL = `INSERT INTO pm.wallet_market
		SELECT wallet_id, market_id, volume_usdc, gross_pnl, fees, f_ppm, o_ppm FROM wallet_market
		WHERE wallet_id >= %d AND wallet_id < %d AND wallet_id IN (SELECT wallet_id FROM wallet_acc WHERE fills > 0)
		ORDER BY wallet_id, market_id`

	walletCalibrationSQL = `INSERT INTO pm.wallet_calibration
		SELECT c.wallet_id, c.bucket, c.n, c.sum_f, c.sum_o FROM wallet_cal c
		JOIN wallet_skill s ON s.wallet_id = c.wallet_id
		ORDER BY c.wallet_id, c.bucket`

	// a market lives in one bucket, so its counts add across buckets
	counterpartySQL = `INSERT INTO pm.wash_counterparty
		SELECT wallet_id, CAST(rank AS UTINYINT), cp_id, markets, trades, shares FROM (
			SELECT wallet_id, cp_id, CAST(sum(markets) AS UINTEGER) AS markets, CAST(sum(trades) AS UINTEGER) AS trades,
				CAST(sum(shares) AS UBIGINT) AS shares,
				row_number() OVER (PARTITION BY wallet_id ORDER BY sum(shares) DESC, cp_id) AS rank
			FROM wash_cp GROUP BY wallet_id, cp_id)
		WHERE rank <= %d
		ORDER BY wallet_id, rank`

	// the cut is exact because the order is total, ties fall to the earlier block and log index
	evidenceSQL = `INSERT INTO pm.wash_trade
		SELECT wallet_id, CAST(rank AS UTINYINT), block, log_index, market_id, cp_id, is_buy, outcome, shares, usdc, ts FROM (
			SELECT *, row_number() OVER (PARTITION BY wallet_id ORDER BY shares DESC, block, log_index) AS rank FROM wash_trade)
		WHERE rank <= %d
		ORDER BY wallet_id, rank`
)

func fill(ctx context.Context, conn *sql.Conn) error {
	for _, s := range []struct{ name, stmt string }{
		{"wallet", walletSQL},
		{"market", marketSQL},
	} {
		if err := exec(ctx, conn, s.name, s.stmt); err != nil {
			return err
		}
	}
	var wallets int64
	if err := conn.QueryRowContext(ctx, "SELECT CAST(count(*) AS BIGINT) FROM wallet_dict").Scan(&wallets); err != nil {
		return fmt.Errorf("publish wallet_market: %w", err)
	}
	for k := range int64(walletRanges) {
		lo, hi := k*wallets/walletRanges, (k+1)*wallets/walletRanges
		if lo == hi {
			continue
		}
		if err := exec(ctx, conn, "wallet_market", fmt.Sprintf(walletMarketSQL, lo, hi)); err != nil {
			return err
		}
	}
	steps := []struct{ name, stmt string }{
		{"wallet_calibration", walletCalibrationSQL},
		{"wash_counterparty", fmt.Sprintf(counterpartySQL, evidencePeers)},
		{"wash_trade", fmt.Sprintf(evidenceSQL, evidenceTrades)},
	}
	for _, h := range horizons {
		steps = append(steps, struct{ name, stmt string }{"calibration " + strconv.Itoa(h) + "h", calibrationSQL(h)})
	}
	for _, s := range steps {
		if err := exec(ctx, conn, s.name, s.stmt); err != nil {
			return err
		}
	}
	return nil
}

func priceColumn(hours int) string { return "p0_" + strconv.Itoa(hours) + "h" }

func calibrationSQL(hours int) string {
	p := priceColumn(hours)
	return `INSERT INTO pm.calibration
		SELECT ` + strconv.Itoa(hours) + `, kind, bucket, count(*), CAST(sum(p) AS UBIGINT), CAST(sum(o) AS UBIGINT), sum(pow((p - o) / 1e6, 2))
		FROM (
			SELECT kind, CAST(least(CAST(` + p + ` AS BIGINT) * 20 // 1000000, 19) AS UTINYINT) AS bucket, CAST(` + p + ` AS BIGINT) AS p,
				CAST(CAST(payout0 AS HUGEINT) * 1000000 // payout_den AS BIGINT) AS o
			FROM pm.market WHERE resolved AND ` + p + ` IS NOT NULL)
		GROUP BY kind, bucket
		ORDER BY kind, bucket`
}

/* Meta */

func accuracySQL(hours int) string {
	p := priceColumn(hours)
	return `SELECT avg(CASE WHEN (payout0 = payout_den) = (` + p + ` > 500000) THEN 1.0 ELSE 0.0 END), count(*)
		FROM pm.market WHERE resolved AND ` + p + ` IS NOT NULL AND (payout0 = 0 OR payout0 = payout_den)`
}

func writeMeta(ctx context.Context, conn *sql.Conn) error {
	meta := append([][2]string{}, coverage...)
	meta = append(meta,
		[2]string{"cutoff_block", strconv.Itoa(CutoffBlock)},
		[2]string{"cutoff_ts", strconv.Itoa(CutoffTS)},
		[2]string{"rules_version", RulesVersion},
		[2]string{"built_at", time.Now().UTC().Format("2006-01-02T15:04:05.000Z")},
	)
	var wallets, rated, fills, markets, resolved int64
	err := conn.QueryRowContext(ctx, `SELECT CAST(count(*) AS BIGINT), CAST(count(*) FILTER (WHERE rated) AS BIGINT),
		CAST(coalesce(sum(fills), 0) AS BIGINT) FROM pm.wallet`).Scan(&wallets, &rated, &fills)
	if err != nil {
		return fmt.Errorf("publish meta: %w", err)
	}
	err = conn.QueryRowContext(ctx, "SELECT CAST(count(*) AS BIGINT), CAST(count(*) FILTER (WHERE resolved) AS BIGINT) FROM pm.market").Scan(&markets, &resolved)
	if err != nil {
		return fmt.Errorf("publish meta: %w", err)
	}
	for _, kv := range []struct {
		key string
		n   int64
	}{{"total_wallets", wallets}, {"total_rated", rated}, {"total_fills", fills}, {"total_markets", markets}, {"total_resolved_markets", resolved}} {
		meta = append(meta, [2]string{kv.key, strconv.FormatInt(kv.n, 10)})
	}
	audit, err := auditMeta(ctx, conn)
	if err != nil {
		return err
	}
	meta = append(meta, audit...)
	for _, h := range horizons {
		var acc sql.Null[float64]
		var n int64
		if err := conn.QueryRowContext(ctx, accuracySQL(h)).Scan(&acc, &n); err != nil {
			return fmt.Errorf("publish accuracy %dh: %w", h, err)
		}
		if acc.Valid {
			meta = append(meta, [2]string{"accuracy_" + strconv.Itoa(h) + "h", num(acc.V)})
		}
	}
	for _, kv := range meta {
		if err := exec(ctx, conn, "meta "+kv[0], "INSERT INTO pm.meta VALUES (?, ?)", kv[0], kv[1]); err != nil {
			return err
		}
	}
	return nil
}

func auditMeta(ctx context.Context, conn *sql.Conn) ([][2]string, error) {
	rows, err := conn.QueryContext(ctx, "SELECT key, value FROM audit ORDER BY key")
	if err != nil {
		return nil, fmt.Errorf("publish audit: %w", err)
	}
	defer rows.Close()
	var out [][2]string
	for rows.Next() {
		var key string
		var value float64
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		out = append(out, [2]string{key, num(value)})
	}
	return out, rows.Err()
}

func num(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }

func quote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
