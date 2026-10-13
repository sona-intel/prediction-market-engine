package publish

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/duckdb/duckdb-go/v2"
)

type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

/* Verify */

// Verify opens the file read-only, so it works while pm-api holds it
func Verify(ctx context.Context, path string, expect *Totals) error {
	db, err := openReadOnly(path)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := requireTables(ctx, db); err != nil {
		return err
	}
	return check(ctx, db, "", expect)
}

func ReadMeta(ctx context.Context, path string) (map[string]string, error) {
	db, err := openReadOnly(path)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return readMeta(ctx, db, "")
}

func openReadOnly(path string) (*sql.DB, error) {
	connector, err := duckdb.NewConnector(path+"?access_mode=READ_ONLY", nil)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	return sql.OpenDB(connector), nil
}

func requireTables(ctx context.Context, q querier) error {
	rows, err := q.QueryContext(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")
	if err != nil {
		return err
	}
	defer rows.Close()
	var have []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		have = append(have, name)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	var missing []string
	for _, t := range schema {
		if !slices.Contains(have, t.name) {
			missing = append(missing, t.name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("published file lacks tables %v", missing)
	}
	return nil
}

func readMeta(ctx context.Context, q querier, p string) (map[string]string, error) {
	rows, err := q.QueryContext(ctx, "SELECT key, value FROM "+p+"meta")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	meta := map[string]string{}
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		meta[key] = value
	}
	return meta, rows.Err()
}

/* Checks */

func check(ctx context.Context, q querier, p string, expect *Totals) error {
	meta, err := readMeta(ctx, q, p)
	if err != nil {
		return fmt.Errorf("verify meta: %w", err)
	}
	var errs []error
	for _, key := range requiredMeta {
		if meta[key] == "" {
			errs = append(errs, fmt.Errorf("meta lacks %s", key))
		}
	}
	if expect != nil {
		errs = append(errs, checkTotals(ctx, q, p, meta, *expect))
	}
	errs = append(errs, checkRules(ctx, q, p, meta), checkCalibration(ctx, q, p))
	return errors.Join(errs...)
}

func checkTotals(ctx context.Context, q querier, p string, meta map[string]string, want Totals) error {
	var got Totals
	var wallets int64
	err := q.QueryRowContext(ctx, `SELECT CAST(coalesce(sum(fills), 0) AS BIGINT), CAST(coalesce(sum(taker_rows), 0) AS BIGINT),
		CAST(coalesce(sum(volume_usdc), 0) AS BIGINT), CAST(count(*) AS BIGINT) FROM `+p+`wallet`).Scan(&got.Fills, &got.TakerRows, &got.VolumeUSDC, &wallets)
	if err != nil {
		return fmt.Errorf("verify totals: %w", err)
	}
	err = q.QueryRowContext(ctx, `SELECT CAST(count(*) AS BIGINT), CAST(count(*) FILTER (WHERE resolved) AS BIGINT),
		CAST(count(resolved_ts) AS BIGINT) FROM `+p+`market`).Scan(&got.Markets, &got.Resolved, &got.WithT)
	if err != nil {
		return fmt.Errorf("verify totals: %w", err)
	}
	got.LastBlock, _ = strconv.ParseInt(meta["cutoff_block"], 10, 64)
	var errs []error
	for _, c := range []struct {
		name      string
		got, want int64
	}{
		{"fills", got.Fills, want.Fills},
		{"taker rows", got.TakerRows, want.TakerRows},
		{"volume_usdc to the cent", toCents(got.VolumeUSDC), toCents(want.VolumeUSDC)},
		{"markets", got.Markets, want.Markets},
		{"resolved markets", got.Resolved, want.Resolved},
		{"markets with a resolution time", got.WithT, want.WithT},
		{"last block", got.LastBlock, want.LastBlock},
	} {
		if c.got != c.want {
			errs = append(errs, fmt.Errorf("%s is %d, want %d", c.name, c.got, c.want))
		}
	}
	if wallets > want.MaxWallets {
		errs = append(errs, fmt.Errorf("wallets is %d, want at most %d", wallets, want.MaxWallets))
	}
	return errors.Join(errs...)
}

// the dataset's total is published to the cent and a USDC cent is 10,000 raw units
func toCents(raw int64) int64 { return (raw + 5000) / 10000 }

func checkRules(ctx context.Context, q querier, p string, meta map[string]string) error {
	var errs []error
	for _, r := range []struct{ name, query string }{
		{
			"wallets with more wins than resolved markets or more resolved markets than markets",
			"SELECT count(*) FROM " + p + "wallet WHERE won_markets > resolved_markets OR resolved_markets > markets",
		},
		{"wallets with more flagged than paired shares", "SELECT count(*) FROM " + p + "wallet WHERE wash_shares > paired_shares"},
		{"markets with more flagged than paired shares", "SELECT count(*) FROM " + p + "market WHERE wash_shares > paired_shares"},
	} {
		var n int64
		if err := q.QueryRowContext(ctx, r.query).Scan(&n); err != nil {
			errs = append(errs, fmt.Errorf("verify %s: %w", r.name, err))
		} else if n != 0 {
			errs = append(errs, fmt.Errorf("%d %s", n, r.name))
		}
	}
	var same bool
	var wallet, perMarket string
	err := q.QueryRowContext(ctx, `WITH a AS (SELECT CAST(coalesce(sum(volume_usdc), 0) AS HUGEINT) AS v FROM `+p+`wallet),
			b AS (SELECT CAST(coalesce(sum(volume_usdc), 0) AS HUGEINT) AS v FROM `+p+`wallet_market)
		SELECT a.v = b.v, CAST(a.v AS VARCHAR), CAST(b.v AS VARCHAR) FROM a, b`).Scan(&same, &wallet, &perMarket)
	switch {
	case err != nil:
		errs = append(errs, fmt.Errorf("verify volume: %w", err))
	case !same:
		errs = append(errs, fmt.Errorf("wallet volume_usdc is %s but wallet_market adds up to %s", wallet, perMarket))
	}
	var wallets int64
	if err := q.QueryRowContext(ctx, "SELECT CAST(count(*) AS BIGINT) FROM "+p+"wallet").Scan(&wallets); err != nil {
		errs = append(errs, fmt.Errorf("verify wallets: %w", err))
	} else if meta["total_wallets"] != strconv.FormatInt(wallets, 10) {
		errs = append(errs, fmt.Errorf("wallet has %d rows but meta total_wallets is %q", wallets, meta["total_wallets"]))
	}
	return errors.Join(errs...)
}

func checkCalibration(ctx context.Context, q querier, p string) error {
	var errs []error
	for _, h := range horizons {
		col := priceColumn(h)
		rows, err := q.QueryContext(ctx, `SELECT coalesce(c.kind, m.kind), coalesce(c.n, 0), coalesce(m.n, 0) FROM
				(SELECT kind, CAST(sum(markets) AS BIGINT) AS n FROM `+p+`calibration WHERE horizon_h = `+strconv.Itoa(h)+` GROUP BY kind) c
			FULL JOIN (SELECT kind, CAST(count(*) AS BIGINT) AS n FROM `+p+`market WHERE resolved AND `+col+` IS NOT NULL GROUP BY kind) m
				ON c.kind = m.kind
			WHERE coalesce(c.n, 0) <> coalesce(m.n, 0)`)
		if err != nil {
			errs = append(errs, fmt.Errorf("verify calibration %dh: %w", h, err))
			continue
		}
		for rows.Next() {
			var kind uint8
			var inCalibration, inMarket int64
			if err := rows.Scan(&kind, &inCalibration, &inMarket); err != nil {
				errs = append(errs, err)
				break
			}
			errs = append(errs, fmt.Errorf("calibration holds %d markets of kind %d at %d h, market has %d with a price", inCalibration, kind, h, inMarket))
		}
		errs = append(errs, rows.Err(), rows.Close())
	}
	return errors.Join(errs...)
}
