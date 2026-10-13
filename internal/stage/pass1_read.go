package stage

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"pm/internal/pnl"
	"pm/internal/skill"
	"pm/internal/wash"
)

/* Own events */

type ownRow struct {
	wallet, ek, market, block, log, ts, questions uint32
	kind, outcome                                 uint8
	buy, summary                                  bool
	qty, usdc                                     uint64
	indexSet                                      sql.Null[string]
}

func (r *ownRow) scan(rows *sql.Rows) error {
	return rows.Scan(&r.wallet, &r.ek, &r.market, &r.kind, &r.block, &r.log, &r.ts, &r.outcome, &r.buy, &r.summary, &r.qty, &r.usdc, &r.indexSet, &r.questions)
}

const ownColumns = "wallet_id, ek, market_id, kind, block, log_index, ts, outcome, buy, summary, qty, usdc, index_set, questions"

func (b *bucket) ownSQL() []string {
	var parts []string
	if b.fill != "" {
		parts = append(parts, `SELECT w.wallet_id, m.ek, t.market_id, CAST(`+strconv.Itoa(int(kindFill))+` AS UTINYINT) AS kind, f.block, f.log_index, f.ts,
				t.outcome, f.buy, f.taker IS NULL AS summary, f.shares AS qty, f.usdc, CAST(NULL AS VARCHAR) AS index_set, CAST(0 AS UINTEGER) AS questions
			FROM `+b.fill+` f
			JOIN wallet_dict w ON w.code = f.maker
			JOIN tok t ON t.code = f.tok
			JOIN market m ON m.market_id = t.market_id`)
	}
	if b.ctf != "" {
		parts = append(parts, `SELECT w.wallet_id, m.ek, m.market_id, c.kind, c.block, c.log_index, c.ts,
				CAST(0 AS UTINYINT) AS outcome, false AS buy, false AS summary, c.amount AS qty, CAST(0 AS UBIGINT) AS usdc, CAST(NULL AS VARCHAR) AS index_set, CAST(0 AS UINTEGER) AS questions
			FROM `+b.ctf+` c
			JOIN wallet_dict w ON w.code = c.wallet
			JOIN market m ON m.market_id = c.ref
			WHERE c.kind <> `+strconv.Itoa(int(kindConvert)),
			`SELECT w.wallet_id, c.ref AS ek, CAST(0 AS UINTEGER) AS market_id, c.kind, c.block, c.log_index, c.ts,
				CAST(0 AS UTINYINT) AS outcome, false AS buy, false AS summary, c.amount AS qty, CAST(0 AS UBIGINT) AS usdc, c.index_set, CAST(coalesce(c.question_count, 0) AS UINTEGER) AS questions
			FROM `+b.ctf+` c
			JOIN wallet_dict w ON w.code = c.wallet
			WHERE c.kind = `+strconv.Itoa(int(kindConvert)))
	}
	return parts
}

func (b *bucket) decode(r *ownRow) (pnl.Event, error) {
	e := pnl.Event{Block: r.block, Log: r.log, Market: r.market, Outcome: r.outcome, Qty: int64(r.qty), USDC: r.usdc}
	switch r.kind {
	case kindFill:
		e.Kind = pnl.Sell
		if r.buy {
			e.Kind = pnl.Buy
		}
	case kindSplit:
		e.Kind = pnl.Split
	case kindMerge:
		e.Kind = pnl.Merge
	case kindRedeem:
		e.Kind = pnl.Redeem
	case kindConvert:
		e.Kind = pnl.Convert
		set, ok := pnl.ParseIndexSet(r.indexSet.V)
		if !ok {
			return e, fmt.Errorf("conversion 137_%d_%d has index set %q that is not a number of at most 256 bits", r.block, r.log, r.indexSet.V)
		}
		questions := b.events[r.ek]
		if r.questions == 0 || uint32(len(questions)) < r.questions {
			return e, fmt.Errorf("conversion 137_%d_%d belongs to an event with %d of %d question conditions", r.block, r.log, len(questions), r.questions)
		}
		e.IndexSet, e.Questions = set, questions[:r.questions]
	default:
		return e, fmt.Errorf("staged row 137_%d_%d has kind %d", r.block, r.log, r.kind)
	}
	return e, nil
}

type candidate struct {
	market uint32
	fill   skill.Fill
}

type group struct {
	wallet, ek uint32
	ledger     *pnl.Ledger
	cands      []candidate
	fills      []skill.Fill
}

func (p *pass1) own(ctx context.Context, b *bucket, out *sink[marketRow]) error {
	parts := b.ownSQL()
	if len(parts) == 0 {
		return nil
	}
	sp, err := newSpool(ctx, p.o.Work, "own", "wallet_id, ek, block, log_index", parts...)
	if err != nil {
		return err
	}
	var g group
	var r ownRow
	err = sp.eachPage(ctx, ownColumns, func(rows *sql.Rows) error {
		for rows.Next() {
			if err := r.scan(rows); err != nil {
				return err
			}
			if g.ledger != nil && (r.wallet != g.wallet || r.ek != g.ek) {
				if err := p.closeGroup(&g, b, out); err != nil {
					return err
				}
			}
			if g.ledger == nil {
				g.wallet, g.ek, g.ledger = r.wallet, r.ek, pnl.NewLedger(p.look)
			}
			e, err := b.decode(&r)
			if err != nil {
				return err
			}
			g.ledger.Apply(e)
			if r.kind == kindFill {
				p.countFill(&r)
				if p.markets[r.market].rated() {
					// block order is also timestamp order, which the forecast search relies on
					price := pnl.PricePPM(r.usdc, r.qty)
					g.cands = append(g.cands, candidate{r.market, skill.Fill{TS: r.ts, Block: r.block, Log: r.log, Buy: r.buy, Outcome: r.outcome, Price: price}})
				}
			}
		}
		return rows.Err()
	})
	if err == nil && g.ledger != nil {
		err = p.closeGroup(&g, b, out)
	}
	return errors.Join(err, sp.drop(ctx))
}

func (p *pass1) countFill(r *ownRow) {
	a := p.acc(r.wallet)
	a.fills++
	a.volume += r.usdc
	if r.buy {
		a.buyVolume += r.usdc
	}
	if r.summary {
		a.takerRows++
		a.takerVolume += r.usdc
	}
	if a.firstTS == 0 || r.ts < a.firstTS {
		a.firstTS = r.ts
	}
	a.lastTS = max(a.lastTS, r.ts)
}

func (p *pass1) closeGroup(g *group, b *bucket, out *sink[marketRow]) error {
	res := g.ledger.Close()
	if len(b.fees) > 0 {
		for i := range res.Markets {
			k := feeKey{g.wallet, res.Markets[i].Market}
			if fee, ok := b.fees[k]; ok {
				res.Markets[i].Fees = fee
				delete(b.fees, k)
			}
		}
	}
	a := p.acc(g.wallet)
	a.sum.Merge(pnl.Summarize(res.Markets))
	a.open += res.OpenUnresolved
	b.duplicates += uint64(res.Duplicates)
	slices.SortStableFunc(g.cands, func(x, y candidate) int { return cmp.Compare(x.market, y.market) })
	var rated bool
	next := 0
	for _, m := range res.Markets {
		row := marketRow{wallet: g.wallet, market: m.Market, volume: m.Volume, gross: m.Gross, fees: m.Fees}
		if info := &p.markets[m.Market]; info.rated() {
			g.fills = g.fills[:0]
			for ; next < len(g.cands) && g.cands[next].market <= m.Market; next++ {
				if g.cands[next].market == m.Market {
					g.fills = append(g.fills, g.cands[next].fill)
				}
			}
			if f, o, ok := skill.Forecast(g.fills, info.redeemTS, info.payout); ok {
				row.f, row.o, row.rated = uint32(f), uint32(o), true
				if a.skill == nil {
					a.skill = new(skill.Acc)
				}
				a.skill.Add(f, o)
				rated = true
			}
		}
		if err := out.add(row); err != nil {
			return err
		}
	}
	if rated {
		a.ratedEvents++
	}
	g.ledger, g.cands = nil, g.cands[:0]
	return nil
}

/* Paired rows */

func (p *pass1) paired(ctx context.Context, b *bucket) error {
	if b.fill == "" {
		return nil
	}
	const base = `WITH p AS (
			SELECT f.maker, f.taker, t.market_id, t.outcome, f.block, f.log_index, f.buy, f.shares
			FROM ` + "%s" + ` f JOIN tok t ON t.code = f.tok
			WHERE f.taker IS NOT NULL AND f.shares > 0 AND f.maker <> f.taker)
		SELECT w.wallet_id, p.market_id, %s AS is_taker, p.buy, p.outcome, p.shares, p.block, p.log_index
		FROM p JOIN wallet_dict w ON w.code = p.%s`
	sp, err := newSpool(ctx, p.o.Work, "pair", "wallet_id, market_id, block, log_index",
		fmt.Sprintf(base, b.fill, "false", "maker"), fmt.Sprintf(base, b.fill, "true", "taker"))
	if err != nil {
		return err
	}
	var wallet, market uint32
	var shares uint64
	var deltas []int64
	end := func() {
		a := p.acc(wallet)
		a.paired += shares
		if wash.Closures(deltas) > 0 {
			a.x0Num += shares
		}
		deltas, shares = deltas[:0], 0
	}
	err = sp.eachPage(ctx, "wallet_id, market_id, is_taker, buy, outcome, shares", func(rows *sql.Rows) error {
		for rows.Next() {
			var w, m uint32
			var taker, buy bool
			var outcome uint8
			var qty uint64
			if err := rows.Scan(&w, &m, &taker, &buy, &outcome, &qty); err != nil {
				return err
			}
			if len(deltas) > 0 && (w != wallet || m != market) {
				end()
			}
			wallet, market = w, m
			delta := wash.MakerDelta(buy, outcome, qty)
			if taker {
				delta = -delta
			}
			deltas = append(deltas, delta)
			shares += qty
		}
		return rows.Err()
	})
	if err == nil && len(deltas) > 0 {
		end()
	}
	return errors.Join(err, sp.drop(ctx))
}

/* Market figures */

func (p *pass1) prices(ctx context.Context, b *bucket) error {
	if b.fill == "" {
		return nil
	}
	sp, err := newSpool(ctx, p.o.Work, "price", "market_id, ts, block, log_index", `SELECT t.market_id, f.ts, f.block, f.log_index, f.buy, f.shares, f.usdc
			FROM `+b.fill+` f
			JOIN tok t ON t.code = f.tok AND t.outcome = 0
			JOIN first_redeem r ON r.market_id = t.market_id
			WHERE CAST(f.ts AS BIGINT) + `+strconv.Itoa(horizons[0]*3600)+` <= r.ts`)
	if err != nil {
		return err
	}
	var market uint32
	var fills []skill.Fill
	end := func() {
		m := &p.markets[market]
		for i, h := range horizons {
			m.price[i], m.priced[i] = skill.PriceAt(fills, m.redeemTS, h)
		}
		fills = fills[:0]
	}
	err = sp.eachPage(ctx, "market_id, ts, block, log_index, buy, shares, usdc", func(rows *sql.Rows) error {
		for rows.Next() {
			var id, ts, block, log uint32
			var buy bool
			var shares, usdc uint64
			if err := rows.Scan(&id, &ts, &block, &log, &buy, &shares, &usdc); err != nil {
				return err
			}
			if len(fills) > 0 && id != market {
				end()
			}
			market = id
			fills = append(fills, skill.Fill{TS: ts, Block: block, Log: log, Buy: buy, Price: pnl.PricePPM(usdc, shares)})
		}
		return rows.Err()
	})
	if err == nil && len(fills) > 0 {
		end()
	}
	return errors.Join(err, sp.drop(ctx))
}

func (p *pass1) volumes(ctx context.Context, b *bucket) error {
	if b.fill == "" {
		return nil
	}
	rows, err := p.o.Work.QueryContext(ctx, `SELECT t.market_id, CAST(count(*) AS UBIGINT), CAST(sum(f.shares) AS UBIGINT), CAST(sum(f.usdc) AS UBIGINT)
		FROM `+b.fill+` f JOIN tok t ON t.code = f.tok WHERE f.taker IS NULL GROUP BY t.market_id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id uint32
		var trades, shares, usdc uint64
		if err := rows.Scan(&id, &trades, &shares, &usdc); err != nil {
			return err
		}
		m := &p.markets[id]
		m.trades, m.shares, m.usdc = trades, shares, usdc
	}
	return rows.Err()
}
