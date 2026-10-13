package stage

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"pm/internal/pnl"
	"pm/internal/refs"
)

func Setup(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return (&runner{conn: conn}).setup(ctx)
}

func PageRows(rows int64) (restore func()) {
	was := pageRows
	pageRows = rows
	return func() { pageRows = was }
}

func WalletEvents(ctx context.Context, o Options, wallet uint32) ([]pnl.Event, error) {
	p, err := newPass1(ctx, o)
	if err != nil {
		return nil, err
	}
	var events []pnl.Event
	for n := range refs.Buckets {
		b, err := p.load(ctx, n)
		if err != nil {
			return nil, err
		}
		got, err := p.walletRows(ctx, b, wallet)
		p.release(b)
		if err != nil {
			return nil, err
		}
		events = append(events, got...)
	}
	slices.SortStableFunc(events, func(a, b pnl.Event) int {
		if a.Block != b.Block {
			return int(a.Block) - int(b.Block)
		}
		return int(a.Log) - int(b.Log)
	})
	return events, nil
}

func (p *pass1) walletRows(ctx context.Context, b *bucket, wallet uint32) ([]pnl.Event, error) {
	parts := b.ownSQL()
	if len(parts) == 0 {
		return nil, nil
	}
	rows, err := p.o.Work.QueryContext(ctx, fmt.Sprintf("SELECT * FROM (%s) WHERE wallet_id = %d ORDER BY ek, block, log_index", strings.Join(parts, " UNION ALL "), wallet))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []pnl.Event
	var r ownRow
	for rows.Next() {
		if err := r.scan(rows); err != nil {
			return nil, err
		}
		e, err := b.decode(&r)
		if err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}
