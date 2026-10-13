package stage

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// rows of one query result, the driver returns a result whole and outside the DuckDB memory limit, so a long stream is read in pages
var pageRows int64 = 250_000

const keyLimit = 1 << 32

/* Spool */

type spool struct {
	db         *sql.DB
	table, key string
	order      string
	rows       int64
}

type span struct{ lo, hi uint64 }

func attachScratch(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "ATTACH IF NOT EXISTS ':memory:' AS scratch")
	return err
}

// order starts with the paging key, each part is its own statement because a union of joins holds every hash table at once
func newSpool(ctx context.Context, db *sql.DB, table, order string, parts ...string) (*spool, error) {
	s := &spool{db: db, table: table, key: strings.SplitN(order, ",", 2)[0], order: order}
	raw := table + "_raw"
	for i, part := range parts {
		stmt := "INSERT INTO scratch." + raw + " " + part
		if i == 0 {
			stmt = "CREATE OR REPLACE TABLE scratch." + raw + " AS " + part
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return nil, fmt.Errorf("spool %s: %w", table, err)
		}
	}
	if _, err := db.ExecContext(ctx, "CREATE OR REPLACE TABLE scratch."+table+" AS SELECT * FROM scratch."+raw+" ORDER BY "+order); err != nil {
		return nil, fmt.Errorf("spool %s: %w", table, err)
	}
	if _, err := db.ExecContext(ctx, "DROP TABLE scratch."+raw); err != nil {
		return nil, fmt.Errorf("spool %s: %w", table, err)
	}
	if err := db.QueryRowContext(ctx, "SELECT CAST(count(*) AS BIGINT) FROM scratch."+table).Scan(&s.rows); err != nil {
		return nil, fmt.Errorf("spool %s: %w", table, err)
	}
	return s, nil
}

func (s *spool) spans(ctx context.Context) ([]span, error) {
	pages := (s.rows + pageRows - 1) / pageRows
	if pages < 2 {
		return []span{{0, keyLimit}}, nil
	}
	quantiles := make([]string, pages-1)
	for i := range quantiles {
		quantiles[i] = strconv.FormatFloat(float64(i+1)/float64(pages), 'f', -1, 64)
	}
	rows, err := s.db.QueryContext(ctx, "SELECT unnest(quantile_disc("+s.key+", ["+strings.Join(quantiles, ", ")+"])) FROM scratch."+s.table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var spans []span
	lo := uint64(0)
	for rows.Next() {
		var cut uint32
		if err := rows.Scan(&cut); err != nil {
			return nil, err
		}
		if uint64(cut) > lo {
			spans = append(spans, span{lo, uint64(cut)})
			lo = uint64(cut)
		}
	}
	return append(spans, span{lo, keyLimit}), rows.Err()
}

func (s *spool) eachPage(ctx context.Context, columns string, fn func(*sql.Rows) error) error {
	spans, err := s.spans(ctx)
	if err != nil {
		return err
	}
	for _, sp := range spans {
		rows, err := s.db.QueryContext(ctx, fmt.Sprintf("SELECT %s FROM scratch.%s WHERE %s >= %d AND %s < %d ORDER BY %s", columns, s.table, s.key, sp.lo, s.key, sp.hi, s.order))
		if err != nil {
			return err
		}
		err = fn(rows)
		if cerr := rows.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *spool) drop(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, "DROP TABLE IF EXISTS scratch."+s.table)
	return err
}
