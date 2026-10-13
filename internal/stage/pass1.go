package stage

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"math"
	"path/filepath"
	"strconv"
	"time"

	"pm/internal/pnl"
	"pm/internal/refs"
	"pm/internal/skill"
)

const (
	pass1Stage = "pass1"
	flushRows  = 1 << 18
	noMarket   = math.MaxUint32
)

const (
	kindFill uint8 = iota
	kindSplit
	kindMerge
	kindRedeem
	kindConvert
)

var horizons = [...]int{4, 12, 24, 168}

const accColumns = `(wallet_id UINTEGER, fills UINTEGER, volume_usdc UBIGINT, buy_volume_usdc UBIGINT,
		taker_rows UINTEGER, taker_volume_usdc UBIGINT, first_ts UINTEGER, last_ts UINTEGER, markets UINTEGER, gross_pnl BIGINT,
		fees UBIGINT, best_net BIGINT, resolved_markets UINTEGER, won_markets UINTEGER, open_unresolved UINTEGER,
		rated_markets UINTEGER, rated_events UINTEGER, sum_t BIGINT, sum_t2 UBIGINT, x0_num UBIGINT, paired_shares UBIGINT)`

const calColumns = "(wallet_id UINTEGER, bucket UTINYINT, n UINTEGER, sum_f UBIGINT, sum_o UBIGINT)"

var pass1Tables = []string{
	"CREATE TABLE IF NOT EXISTS wallet_acc_part " + accColumns,
	"CREATE TABLE IF NOT EXISTS wallet_cal_part " + calColumns,
	`CREATE TABLE IF NOT EXISTS wallet_market (wallet_id UINTEGER, market_id UINTEGER, volume_usdc UBIGINT, gross_pnl BIGINT,
		fees UBIGINT, f_ppm UINTEGER, o_ppm UINTEGER)`,
	`CREATE TABLE IF NOT EXISTS market_acc (market_id UINTEGER, trades UBIGINT, volume_shares UBIGINT, volume_usdc UBIGINT,
		p0_4h UINTEGER, p0_12h UINTEGER, p0_24h UINTEGER, p0_168h UINTEGER)`,
}

/* State */

type acc struct {
	wallet                                               uint32
	sum                                                  pnl.Summary
	skill                                                *skill.Acc
	fills, takerRows, firstTS, lastTS, open, ratedEvents uint32
	volume, buyVolume, takerVolume, x0Num, paired        uint64
}

func (a *acc) active() bool {
	return a.fills > 0 || a.sum.Markets > 0 || a.sum.Fees > 0 || a.paired > 0
}

type marketInfo struct {
	redeemTS             uint32
	trades, shares, usdc uint64
	payout               [2]int64
	price                [len(horizons)]int64
	priced               [len(horizons)]bool
	kind                 refs.Kind
	resolved, hasT       bool
}

func (m *marketInfo) rated() bool {
	return m.resolved && m.hasT && m.kind == refs.KindOther
}

type pass1 struct {
	o       Options
	slots   []acc
	slot    []uint32
	markets []marketInfo
}

type feeKey struct{ wallet, market uint32 }

type bucket struct {
	n              int
	fill, ctf, fee string
	ids            []uint32
	events         map[uint32][]uint32
	fees           map[feeKey]uint64
	duplicates     uint64
	unmappedFees   uint64
}

/* Entry */

func Pass1(ctx context.Context, o Options) error {
	p, err := newPass1(ctx, o)
	if err != nil {
		return err
	}
	done, err := doneItems(ctx, o.Work, pass1Stage)
	if err != nil {
		return err
	}
	for n := range refs.Buckets {
		if done[bucketName(n)] {
			continue
		}
		start := time.Now()
		if err := p.bucket(ctx, n); err != nil {
			return fmt.Errorf("pass1 bucket %02d: %w", n, err)
		}
		slog.Info("pass1 bucket", "bucket", n, "secs", int(time.Since(start).Seconds()))
	}
	return p.finish(ctx)
}

func newPass1(ctx context.Context, o Options) (*pass1, error) {
	var wallets, ranked int64
	if err := o.Work.QueryRowContext(ctx, "SELECT count(*), count(wallet_id) FROM wallet_dict").Scan(&wallets, &ranked); err != nil {
		return nil, fmt.Errorf("pass1: %w: %w", ErrIncomplete, err)
	}
	if wallets != ranked {
		return nil, fmt.Errorf("pass1: %w, the wallet dictionary is not ranked", ErrIncomplete)
	}
	var markets int64
	if err := o.Work.QueryRowContext(ctx, "SELECT coalesce(max(market_id) + 1, 0) FROM market").Scan(&markets); err != nil {
		return nil, err
	}
	if err := attachScratch(ctx, o.Work); err != nil {
		return nil, fmt.Errorf("pass1 setup: %w", err)
	}
	for _, ddl := range pass1Tables {
		if _, err := o.Work.ExecContext(ctx, ddl); err != nil {
			return nil, fmt.Errorf("pass1 setup: %w", err)
		}
	}
	for _, key := range auditKeys() {
		if _, err := o.Work.ExecContext(ctx, "INSERT INTO audit VALUES (?, 0) ON CONFLICT DO NOTHING", key); err != nil {
			return nil, fmt.Errorf("pass1 setup: %w", err)
		}
	}
	return &pass1{o: o, slot: make([]uint32, wallets), markets: make([]marketInfo, markets)}, nil
}

func auditKeys() []string {
	keys := []string{"duplicate_rows", "unmapped_fee_rows"}
	for _, h := range horizons {
		keys = append(keys, noPriceKey(h))
	}
	return keys
}

func noPriceKey(hours int) string { return "no_price_" + strconv.Itoa(hours) + "h" }

func bucketName(n int) string { return fmt.Sprintf("%02d", n) }

func (p *pass1) look(id uint32) pnl.Market {
	m := &p.markets[id]
	return pnl.Market{Resolved: m.resolved, Payout: m.payout}
}

/* Bucket */

func (p *pass1) bucket(ctx context.Context, n int) error {
	b, err := p.load(ctx, n)
	if err != nil {
		return err
	}
	defer func() {
		p.release(b)
		p.reset()
	}()
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
	out := newSink(conn, "wallet_market", appendMarketRow)
	if err := p.own(ctx, b, out); err != nil {
		return err
	}
	if err := out.flush(); err != nil {
		return err
	}
	for _, step := range []func(context.Context, *bucket) error{p.paired, p.prices, p.volumes} {
		if err := step(ctx, b); err != nil {
			return err
		}
	}
	for k, fee := range b.fees {
		p.acc(k.wallet).sum.Fees += fee
	}
	if err := p.saveMarkets(conn, b); err != nil {
		return err
	}
	if err := p.saveAccs(conn); err != nil {
		return err
	}
	if err := p.record(ctx, tx, b); err != nil {
		return err
	}
	return tx.Commit()
}

func source(dir, kind string) (string, error) {
	pattern := filepath.Join(dir, kind+"-*.parquet")
	files, err := filepath.Glob(pattern)
	if err != nil || len(files) == 0 {
		return "", err
	}
	return "read_parquet(" + quote(pattern) + ", hive_partitioning=false)", nil
}

func (p *pass1) load(ctx context.Context, n int) (*bucket, error) {
	b := &bucket{n: n, events: map[uint32][]uint32{}, fees: map[feeKey]uint64{}}
	dir := filepath.Join(p.o.StageDir, "b="+bucketName(n))
	for kind, dst := range map[string]*string{"fill": &b.fill, "ctf": &b.ctf, "fee": &b.fee} {
		var err error
		if *dst, err = source(dir, kind); err != nil {
			return nil, err
		}
	}
	if err := p.loadMarkets(ctx, b); err != nil {
		return nil, err
	}
	if err := p.loadConversions(ctx, b); err != nil {
		return nil, err
	}
	if err := p.loadFees(ctx, b); err != nil {
		return nil, err
	}
	return b, nil
}

// a fee taken in shares is worth the wallet's average taker buy price in the market, a USDC fee has no market
func (p *pass1) loadFees(ctx context.Context, b *bucket) error {
	if b.fee == "" {
		return nil
	}
	if err := p.o.Work.QueryRowContext(ctx, "SELECT CAST(coalesce(sum(unmapped), 0) AS UBIGINT) FROM "+b.fee).Scan(&b.unmappedFees); err != nil {
		return err
	}
	buys := `SELECT CAST(NULL AS UINTEGER) AS wallet, CAST(NULL AS UINTEGER) AS market_id, CAST(NULL AS HUGEINT) AS usdc, CAST(NULL AS HUGEINT) AS shares WHERE false`
	if b.fill != "" {
		buys = `SELECT f.maker AS wallet, t.market_id, sum(CAST(f.usdc AS HUGEINT)) AS usdc, sum(CAST(f.shares AS HUGEINT)) AS shares
			FROM ` + b.fill + ` f JOIN tok t ON t.code = f.tok WHERE f.buy AND f.taker IS NULL GROUP BY ALL`
	}
	rows, err := p.o.Work.QueryContext(ctx, `WITH fee AS (
			SELECT wallet, market_id, sum(CAST(fee AS HUGEINT)) AS fee FROM `+b.fee+` GROUP BY ALL),
		buys AS (`+buys+`)
		SELECT w.wallet_id, coalesce(fee.market_id, `+strconv.Itoa(noMarket)+`),
			CAST(sum(CASE WHEN buys.shares > 0 THEN fee.fee * buys.usdc // buys.shares ELSE fee.fee END) AS UBIGINT)
		FROM fee
		JOIN wallet_dict w ON w.code = fee.wallet
		LEFT JOIN buys ON buys.wallet = fee.wallet AND buys.market_id = fee.market_id
		GROUP BY ALL`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var k feeKey
		var fee uint64
		if err := rows.Scan(&k.wallet, &k.market, &fee); err != nil {
			return err
		}
		b.fees[k] = fee
	}
	return rows.Err()
}

func (p *pass1) loadMarkets(ctx context.Context, b *bucket) error {
	rows, err := p.o.Work.QueryContext(ctx, `SELECT m.market_id, m.kind, m.resolved, m.payout0, m.payout1, m.payout_den, r.ts
		FROM market m LEFT JOIN first_redeem r ON r.market_id = m.market_id WHERE m.bucket = ?`, b.n)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id uint32
		var kind uint8
		var resolved bool
		var p0, p1, den sql.Null[uint64]
		var redeem sql.Null[uint32]
		if err := rows.Scan(&id, &kind, &resolved, &p0, &p1, &den, &redeem); err != nil {
			return err
		}
		m := &p.markets[id]
		*m = marketInfo{kind: refs.Kind(kind), resolved: resolved, redeemTS: redeem.V, hasT: redeem.Valid}
		if resolved {
			m.payout = [2]int64{pnl.PricePPM(p0.V, den.V), pnl.PricePPM(p1.V, den.V)}
		}
		b.ids = append(b.ids, id)
	}
	return rows.Err()
}

func (p *pass1) loadConversions(ctx context.Context, b *bucket) error {
	rows, err := p.o.Work.QueryContext(ctx, `SELECT q.ek, q.q, q.market_id FROM nr_question q
		JOIN ek e ON e.code = q.ek WHERE e.bucket = ? ORDER BY q.ek, q.q`, b.n)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var ek, q, market uint32
		if err := rows.Scan(&ek, &q, &market); err != nil {
			return err
		}
		// questions stop at the first index with no condition so a row never reads past a gap
		if questions := b.events[ek]; q == uint32(len(questions)) {
			b.events[ek] = append(questions, market)
		}
	}
	return rows.Err()
}

func (p *pass1) release(b *bucket) {
	for _, id := range b.ids {
		p.markets[id] = marketInfo{}
	}
}
