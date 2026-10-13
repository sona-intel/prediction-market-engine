package stage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"pm/internal/dataset"
)

type Options struct {
	Work     *sql.DB
	Src      *dataset.Source
	StageDir string
	Pace     *dataset.Pacer
}

var ErrIncomplete = errors.New("stage incomplete")

const (
	copyOptions = "(FORMAT parquet, COMPRESSION zstd, PARTITION_BY (bucket))"
	isSummary   = "lower(f.taker) IN ('0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e', '0xc5d563a36ae78145c45a50134d48a1215220f80a')"
	tokenMap    = "(SELECT tk.token, tk.code, tk.market_id, m.bucket FROM tok tk JOIN market m USING (market_id))"
	marketJoin  = "market m ON m.condition = unhex(substr(f.condition, 3))"
	eventJoin   = "ek m ON m.key = unhex(substr(f.negRiskMarketId, 3))"
	noIndexSet  = "CAST(NULL AS VARCHAR)"
	noQuestions = "CAST(NULL AS UINTEGER)"
)

type job struct {
	table, kind string
	cols        []string
	wallets     string
	query       string
}

var jobs = []job{
	{
		table:   "order_filled",
		kind:    "fill",
		cols:    []string{"id", "timestamp", "maker", "taker", "makerAssetId", "takerAssetId", "makerAmountFilled", "takerAmountFilled"},
		wallets: "[f.maker, CASE WHEN " + isSummary + " THEN NULL ELSE f.taker END]",
		query: `SELECT t.bucket,
				CAST(split_part(f.id, '_', 2) AS UINTEGER) AS block,
				CAST(split_part(f.id, '_', 3) AS UINTEGER) AS log_index,
				CAST(f.timestamp AS UINTEGER) AS ts,
				wm.code AS maker,
				CASE WHEN ` + isSummary + ` THEN NULL ELSE wt.code END AS taker,
				t.code AS tok,
				f.makerAssetId = '0' AS buy,
				CAST(CASE WHEN f.makerAssetId = '0' THEN f.takerAmountFilled ELSE f.makerAmountFilled END AS UBIGINT) AS shares,
				CAST(CASE WHEN f.makerAssetId = '0' THEN f.makerAmountFilled ELSE f.takerAmountFilled END AS UBIGINT) AS usdc
			FROM {src} f
			JOIN ` + tokenMap + ` t ON t.token = CASE WHEN f.makerAssetId = '0' THEN f.takerAssetId ELSE f.makerAssetId END
			JOIN wallet_dict wm ON wm.address = unhex(substr(f.maker, 3))
			LEFT JOIN wallet_dict wt ON wt.address = unhex(substr(f.taker, 3))`,
	},
	ctfJob("redemption", []string{"id", "timestamp", "redeemer", "condition"}, 3, "f.redeemer", "0", marketJoin, "m.market_id", noIndexSet, noQuestions),
	ctfJob("split", []string{"id", "timestamp", "stakeholder", "condition", "amount"}, 1, "f.stakeholder", "f.amount", marketJoin, "m.market_id", noIndexSet, noQuestions),
	ctfJob("merge", []string{"id", "timestamp", "stakeholder", "condition", "amount"}, 2, "f.stakeholder", "f.amount", marketJoin, "m.market_id", noIndexSet, noQuestions),
	ctfJob("neg_risk_conversion", []string{"id", "timestamp", "stakeholder", "negRiskMarketId", "amount", "indexSet", "questionCount"}, 4, "f.stakeholder", "f.amount", eventJoin, "m.code", "f.indexSet", "CAST(f.questionCount AS UINTEGER)"),
	{
		table:   "fee_refunded",
		kind:    "fee",
		cols:    []string{"refundee", "tokenId", "feeCharged"},
		wallets: "[f.refundee]",
		// tokenId 0 is a USDC fee with no token, it keeps the wallet total and carries no market
		query: `SELECT CAST(coalesce(t.bucket, 0) AS UTINYINT) AS bucket, w.code AS wallet, t.market_id,
				CAST(sum(CAST(f.feeCharged AS UBIGINT)) AS UBIGINT) AS fee,
				CAST(count(*) FILTER (WHERE f.tokenId <> '0' AND t.token IS NULL) AS UBIGINT) AS unmapped
			FROM {src} f
			JOIN wallet_dict w ON w.address = unhex(substr(f.refundee, 3))
			LEFT JOIN ` + tokenMap + ` t ON t.token = f.tokenId
			GROUP BY ALL`,
	},
}

func ctfJob(table string, cols []string, kind int, wallet, amount, target, ref, indexSet, questions string) job {
	return job{
		table:   table,
		kind:    "ctf",
		cols:    cols,
		wallets: "[" + wallet + "]",
		query: `SELECT m.bucket,
				CAST(split_part(f.id, '_', 2) AS UINTEGER) AS block,
				CAST(split_part(f.id, '_', 3) AS UINTEGER) AS log_index,
				CAST(f.timestamp AS UINTEGER) AS ts,
				CAST(` + strconv.Itoa(kind) + ` AS UTINYINT) AS kind,
				w.code AS wallet,
				` + ref + ` AS ref,
				CAST(` + amount + ` AS UBIGINT) AS amount,
				` + indexSet + ` AS index_set,
				` + questions + ` AS question_count
			FROM {src} f
			JOIN ` + target + `
			JOIN wallet_dict w ON w.address = unhex(substr(` + wallet + `, 3))`,
	}
}

type runner struct {
	o    Options
	conn *sql.Conn
	tmp  string
}

func Run(ctx context.Context, o Options) error {
	for _, j := range jobs {
		if len(o.Src.Files(j.table)) == 0 {
			return fmt.Errorf("stage: no files for table %s", j.table)
		}
	}
	if err := os.MkdirAll(o.StageDir, 0o750); err != nil {
		return err
	}
	conn, err := o.Work.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	r := &runner{o: o, conn: conn, tmp: filepath.Join(o.StageDir, ".tmp")}
	if err := r.setup(ctx); err != nil {
		return err
	}
	// requests logged before this run belong to an earlier step, not to the first file
	if _, _, err := dataset.Traffic(ctx, o.Src.DB); err != nil {
		return err
	}
	done, err := doneItems(ctx, r.conn, "stage")
	if err != nil {
		return err
	}
	var failed []string
	for _, j := range jobs {
		for _, file := range o.Src.Files(j.table) {
			item := r.item(file)
			if done[item] {
				continue
			}
			start := time.Now()
			unmapped, fileErr := r.stageFile(ctx, j, file, item)
			if err := ctx.Err(); err != nil {
				return err
			}
			gets, bytes, err := dataset.Traffic(ctx, o.Src.DB)
			if err != nil {
				return err
			}
			if err := r.record(ctx, item, fileErr, unmapped, bytes, gets); err != nil {
				return err
			}
			if fileErr != nil {
				failed = append(failed, item+": "+fileErr.Error())
				slog.Warn("stage file failed", "file", item, "error", fileErr)
			} else {
				slog.Info("staged", "file", item, "unmapped", unmapped, "requests", gets, "bytes", bytes, "secs", int(time.Since(start).Seconds()))
			}
			if o.Pace != nil {
				if err := o.Pace.Done(ctx, gets); err != nil {
					return err
				}
			}
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("%w: %s", ErrIncomplete, strings.Join(failed, "; "))
	}
	return r.finish(ctx)
}

func (r *runner) setup(ctx context.Context) error {
	for _, stmt := range []string{
		"CREATE TABLE IF NOT EXISTS build_state (stage VARCHAR, item VARCHAR, status VARCHAR, bytes BIGINT, requests BIGINT, done_at TIMESTAMP, PRIMARY KEY (stage, item))",
		"CREATE TABLE IF NOT EXISTS wallet_dict (address BLOB, code UINTEGER, wallet_id UINTEGER)",
		"CREATE TABLE IF NOT EXISTS audit (key VARCHAR PRIMARY KEY, value DOUBLE)",
		"INSERT INTO audit VALUES ('unmapped_rows', 0) ON CONFLICT DO NOTHING",
	} {
		if _, err := r.conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("stage setup: %w", err)
		}
	}
	return nil
}

type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func doneItems(ctx context.Context, q querier, stage string) (map[string]bool, error) {
	rows, err := q.QueryContext(ctx, "SELECT item FROM build_state WHERE stage = ? AND status = 'done'", stage)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	done := map[string]bool{}
	for rows.Next() {
		var item string
		if err := rows.Scan(&item); err != nil {
			return nil, err
		}
		done[item] = true
	}
	return done, rows.Err()
}

func (r *runner) item(file string) string {
	return strings.TrimPrefix(file, strings.TrimSuffix(r.o.Src.Root, "/")+"/")
}

func (r *runner) stageFile(ctx context.Context, j job, file, item string) (int64, error) {
	id := strings.ReplaceAll(strings.TrimSuffix(item, ".parquet"), "/", "_")
	if err := r.clean(id); err != nil {
		return 0, err
	}
	from := r.o.Src.From([]string{file}, j.cols...)
	if _, err := r.conn.ExecContext(ctx, walletsSQL(j.wallets, from)); err != nil {
		return 0, err
	}
	res, err := r.conn.ExecContext(ctx, "COPY ("+strings.Replace(j.query, "{src}", from, 1)+") TO "+quote(r.tmp)+" "+copyOptions)
	if err != nil {
		return 0, err
	}
	staged, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if err := r.move(j.kind, id); err != nil {
		return 0, err
	}
	if j.kind == "fee" {
		return 0, nil
	}
	rows, err := r.o.Src.Rows(ctx, file)
	return rows - staged, err
}

func walletsSQL(wallets, from string) string {
	return "INSERT INTO wallet_dict " +
		"SELECT n.address, b.next + row_number() OVER (ORDER BY n.address) - 1, NULL " +
		"FROM (SELECT DISTINCT unhex(substr(w, 3)) AS address FROM (SELECT unnest(" + wallets + ") AS w FROM " + from + " f) WHERE w IS NOT NULL) n " +
		"CROSS JOIN (SELECT coalesce(max(code) + 1, 0) AS next FROM wallet_dict) b " +
		"WHERE NOT EXISTS (SELECT 1 FROM wallet_dict d WHERE d.address = n.address)"
}

func (r *runner) clean(id string) error {
	if err := os.RemoveAll(r.tmp); err != nil {
		return err
	}
	old, err := filepath.Glob(filepath.Join(r.o.StageDir, "b=*", "*-"+id+".parquet"))
	if err != nil {
		return err
	}
	for _, p := range old {
		if err := os.Remove(p); err != nil {
			return err
		}
	}
	return nil
}

func (r *runner) move(kind, id string) error {
	dirs, err := os.ReadDir(r.tmp)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, d := range dirs {
		bucket, err := strconv.Atoi(strings.TrimPrefix(d.Name(), "bucket="))
		if err != nil {
			return fmt.Errorf("unexpected folder %s", d.Name())
		}
		parts, err := filepath.Glob(filepath.Join(r.tmp, d.Name(), "*.parquet"))
		if err != nil {
			return err
		}
		// DuckDB writes one file per partition, a second one would take the same name
		if len(parts) != 1 {
			return fmt.Errorf("bucket %d wrote %d files", bucket, len(parts))
		}
		dir := filepath.Join(r.o.StageDir, fmt.Sprintf("b=%02d", bucket))
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return err
		}
		if err := os.Rename(parts[0], filepath.Join(dir, kind+"-"+id+".parquet")); err != nil {
			return err
		}
	}
	return os.RemoveAll(r.tmp)
}

func (r *runner) record(ctx context.Context, item string, fileErr error, unmapped, bytes int64, gets int) error {
	tx, err := r.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	status := "done"
	if fileErr != nil {
		status = "failed"
	} else if unmapped != 0 {
		if _, err := tx.ExecContext(ctx, "UPDATE audit SET value = value + ? WHERE key = 'unmapped_rows'", unmapped); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, "INSERT OR REPLACE INTO build_state VALUES ('stage', ?, ?, ?, ?, now())", item, status, bytes, gets); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *runner) finish(ctx context.Context) error {
	rank := `CREATE OR REPLACE TABLE wallet_dict AS
		SELECT address, code, CAST(row_number() OVER (ORDER BY address) - 1 AS UINTEGER) AS wallet_id
		FROM wallet_dict ORDER BY address`
	if _, err := r.conn.ExecContext(ctx, rank); err != nil {
		return fmt.Errorf("stage rank wallets: %w", err)
	}
	pattern := filepath.Join(r.o.StageDir, "b=*", "ctf-*.parquet")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	// pass2 deletes the staged ctf files, so a rerun must keep the table built before that
	redeem := "CREATE TABLE IF NOT EXISTS first_redeem (market_id UINTEGER, ts UINTEGER)"
	if len(files) > 0 {
		redeem = `CREATE TABLE IF NOT EXISTS first_redeem AS
			SELECT ref AS market_id, min(ts) AS ts FROM read_parquet(` + quote(pattern) + `, hive_partitioning=false)
			WHERE kind = 3 GROUP BY ref`
	}
	if _, err := r.conn.ExecContext(ctx, redeem); err != nil {
		return fmt.Errorf("stage first_redeem: %w", err)
	}
	return nil
}

func quote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
