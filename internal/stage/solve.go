package stage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"pm/internal/refs"
	"pm/internal/wash"
)

const (
	solveStage = "solve"
	solveItem  = "all"
	finishItem = "finish"
)

var ErrGraphTooLarge = errors.New("wash graph exceeds PM_WASH_MAX_PAIRS")

/* Entry */

func Solve(ctx context.Context, o Options, maxPairs int) error {
	done, err := doneItems(ctx, o.Work, solveStage)
	if err != nil {
		return fmt.Errorf("solve: %w", err)
	}
	if done[solveItem] {
		return nil
	}
	if err := requireBuckets(ctx, o, pass1Stage); err != nil {
		return err
	}
	if finished, err := doneItems(ctx, o.Work, pass1Stage); err != nil || !finished[finishItem] {
		return fmt.Errorf("%w, pass1 has not summed the buckets", ErrIncomplete)
	}
	if err := attachScratch(ctx, o.Work); err != nil {
		return fmt.Errorf("solve: %w", err)
	}
	start := time.Now()
	defer func() { dropSolveTables(context.WithoutCancel(ctx), o.Work) }()
	pairs, err := addEdges(ctx, o, maxPairs)
	if err != nil {
		return err
	}
	rounds, err := iterate(ctx, o.Work)
	if err != nil {
		return err
	}
	slog.Info("solved", "pairs", pairs, "rounds", rounds, "secs", int(time.Since(start).Seconds()))
	return saveScores(ctx, o, rounds)
}

func requireBuckets(ctx context.Context, o Options, stage string) error {
	done, err := doneItems(ctx, o.Work, stage)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrIncomplete, err)
	}
	delete(done, finishItem)
	if len(done) != refs.Buckets {
		return fmt.Errorf("%w, %s finished %d of %d buckets", ErrIncomplete, stage, len(done), refs.Buckets)
	}
	return nil
}

func walletCount(ctx context.Context, o Options) (int, error) {
	var n int
	err := o.Work.QueryRowContext(ctx, "SELECT CAST(count(*) AS BIGINT) FROM wallet_dict").Scan(&n)
	return n, err
}

func dropSolveTables(ctx context.Context, db *sql.DB) {
	for _, table := range []string{"wash_edge", "scratch.wash_x0", "scratch.wash_x", "scratch.wash_xn", "scratch.wash_t"} {
		_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+table)
	}
}

/* Graph */

func addEdges(ctx context.Context, o Options, maxPairs int) (int64, error) {
	if _, err := o.Work.ExecContext(ctx, "CREATE OR REPLACE TABLE wash_edge (a UINTEGER, b UINTEGER, s UBIGINT)"); err != nil {
		return 0, fmt.Errorf("solve: %w", err)
	}
	var pairs int64
	for n := range refs.Buckets {
		fills, err := source(filepath.Join(o.StageDir, "b="+bucketName(n)), "fill")
		if err != nil {
			return 0, err
		}
		if fills == "" {
			continue
		}
		start := time.Now()
		if _, err := o.Work.ExecContext(ctx, edgesSQL(fills)); err != nil {
			return 0, fmt.Errorf("solve bucket %02d: %w", n, err)
		}
		if err := o.Work.QueryRowContext(ctx, "SELECT CAST(count(*) AS BIGINT) FROM wash_edge").Scan(&pairs); err != nil {
			return 0, fmt.Errorf("solve: %w", err)
		}
		if pairs > int64(maxPairs) {
			return 0, fmt.Errorf("%w, %d directed pairs against a limit of %d", ErrGraphTooLarge, pairs, maxPairs)
		}
		slog.Info("solve edges", "bucket", n, "pairs", pairs, "secs", int(time.Since(start).Seconds()))
	}
	return pairs, nil
}

func edgesSQL(fills string) string {
	return `INSERT INTO wash_edge SELECT a, b, CAST(sum(shares) AS UBIGINT) FROM (
			SELECT CASE d.dir WHEN 0 THEN wm.wallet_id ELSE wt.wallet_id END AS a,
				CASE d.dir WHEN 0 THEN wt.wallet_id ELSE wm.wallet_id END AS b, f.shares
			FROM ` + fills + ` f
			JOIN wallet_dict wm ON wm.code = f.maker
			JOIN wallet_dict wt ON wt.code = f.taker
			CROSS JOIN (VALUES (0), (1)) d(dir)
			WHERE f.shares > 0 AND f.maker <> f.taker)
		GROUP BY a, b`
}

/* Scores */

func iterate(ctx context.Context, db *sql.DB) (int, error) {
	for _, stmt := range []string{
		`CREATE OR REPLACE TABLE scratch.wash_x0 AS
			SELECT w.wallet_id AS id, coalesce(CAST(a.x0_num AS DOUBLE) / CAST(a.paired_shares AS DOUBLE), 0) AS v
			FROM wallet_dict w LEFT JOIN wallet_acc a ON a.wallet_id = w.wallet_id AND a.paired_shares > 0`,
		"CREATE OR REPLACE TABLE scratch.wash_x AS SELECT id, v, CAST(round(v * " + scale + ") AS BIGINT) AS q FROM scratch.wash_x0",
		"CREATE OR REPLACE TABLE scratch.wash_t AS SELECT a, CAST(sum(s) AS DOUBLE) AS t FROM wash_edge GROUP BY a",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return 0, fmt.Errorf("solve: %w", err)
		}
	}
	for rounds := 1; ; rounds++ {
		diff2, prev2, err := round(ctx, db)
		if err != nil {
			return 0, fmt.Errorf("solve round %d: %w", rounds, err)
		}
		if wash.Converged(diff2, prev2, rounds) {
			return rounds, nil
		}
	}
}

// scale makes x a whole number in the sums, so a parallel sum gives the same bits on every run
const scale = "1e9"

func round(ctx context.Context, db *sql.DB) (diff2, prev2 float64, err error) {
	next := `CREATE OR REPLACE TABLE scratch.wash_xn AS
		SELECT id, v, CAST(round(v * ` + scale + `) AS BIGINT) AS q FROM (
			SELECT z.id, (z.v + coalesce(CAST(m.sx AS DOUBLE) / t.t / ` + scale + `, 0)) / 2 AS v
			FROM scratch.wash_x0 z
			LEFT JOIN (SELECT e.a, sum(CAST(e.s AS HUGEINT) * x.q) AS sx FROM wash_edge e JOIN scratch.wash_x x ON x.id = e.b GROUP BY e.a) m ON m.a = z.id
			LEFT JOIN scratch.wash_t t ON t.a = z.id)`
	if _, err := db.ExecContext(ctx, next); err != nil {
		return 0, 0, err
	}
	err = db.QueryRowContext(ctx, `SELECT CAST(coalesce(sum(CAST(n.q - c.q AS HUGEINT) * CAST(n.q - c.q AS HUGEINT)), 0) AS DOUBLE),
			CAST(coalesce(sum(CAST(c.q AS HUGEINT) * CAST(c.q AS HUGEINT)), 0) AS DOUBLE)
		FROM scratch.wash_xn n JOIN scratch.wash_x c USING (id)`).Scan(&diff2, &prev2)
	if err != nil {
		return 0, 0, err
	}
	for _, stmt := range []string{"DROP TABLE scratch.wash_x", "ALTER TABLE scratch.wash_xn RENAME TO wash_x"} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return 0, 0, err
		}
	}
	return diff2, prev2, nil
}

func saveScores(ctx context.Context, o Options, rounds int) error {
	tx, err := o.Work.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, stmt := range []string{
		`CREATE OR REPLACE TABLE wash_score AS
			SELECT x.id AS wallet_id, x.v AS x, z.v AS x0
			FROM scratch.wash_x x JOIN scratch.wash_x0 z USING (id) JOIN wallet_acc a ON a.wallet_id = x.id
			WHERE a.paired_shares > 0 ORDER BY x.id`,
		"DROP TABLE wash_edge",
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, "INSERT OR REPLACE INTO audit VALUES ('wash_rounds', ?)", float64(rounds)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT OR REPLACE INTO build_state VALUES (?, ?, 'done', 0, 0, now())", solveStage, solveItem); err != nil {
		return err
	}
	return tx.Commit()
}
