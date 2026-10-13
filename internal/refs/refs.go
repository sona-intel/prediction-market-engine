package refs

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/duckdb/duckdb-go/v2"

	"pm/internal/dataset"
)

const Buckets = 64

const maxQuestions = 256

var tables = []string{"condition", "position", "market_data", "neg_risk_event", "wallet", "fpmm_transaction"}

var scratch = []string{"cond", "md", "nr_event", "nr_derived"}

type event struct {
	key       [32]byte
	questions uint32
}

type step struct{ name, sql string }

func Build(ctx context.Context, src *dataset.Source, work *sql.DB) error {
	for _, table := range tables {
		if len(src.Files(table)) == 0 {
			return fmt.Errorf("refs: no files for table %s", table)
		}
	}
	conn, err := work.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	defer func() {
		for _, table := range scratch {
			_, _ = conn.ExecContext(context.WithoutCancel(ctx), "DROP TABLE IF EXISTS "+table)
		}
	}()
	events, err := readEvents(ctx, conn, src)
	if err != nil {
		return err
	}
	if err := loadNegRisk(ctx, conn, events); err != nil {
		return err
	}
	for _, s := range steps(src) {
		if _, err := conn.ExecContext(ctx, s.sql); err != nil {
			return fmt.Errorf("refs %s: %w", s.name, err)
		}
	}
	return nil
}

func readEvents(ctx context.Context, conn *sql.Conn, src *dataset.Source) ([]event, error) {
	rows, err := conn.QueryContext(ctx, "SELECT id, questionCount FROM "+src.From(src.Files("neg_risk_event"), "id", "questionCount"))
	if err != nil {
		return nil, fmt.Errorf("refs neg_risk_event: %w", err)
	}
	defer rows.Close()
	var events []event
	for rows.Next() {
		var id, count string
		if err := rows.Scan(&id, &count); err != nil {
			return nil, fmt.Errorf("refs neg_risk_event: %w", err)
		}
		key, err := hex.DecodeString(strings.TrimPrefix(id, "0x"))
		if err != nil || len(key) != 32 {
			return nil, fmt.Errorf("refs neg_risk_event: bad id %q", id)
		}
		n, err := strconv.ParseUint(count, 10, 32)
		if err != nil || n > maxQuestions {
			return nil, fmt.Errorf("refs neg_risk_event %s: questionCount %q is not between 0 and %d", id, count, maxQuestions)
		}
		events = append(events, event{key: [32]byte(key), questions: uint32(n)})
	}
	return events, rows.Err()
}

func loadNegRisk(ctx context.Context, conn *sql.Conn, events []event) error {
	for _, ddl := range []string{
		"CREATE OR REPLACE TEMP TABLE nr_event (key BLOB, question_count UINTEGER)",
		"CREATE OR REPLACE TEMP TABLE nr_derived (key BLOB, q UTINYINT, condition BLOB)",
	} {
		if _, err := conn.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("refs neg-risk tables: %w", err)
		}
	}
	err := conn.Raw(func(raw any) error {
		dc := raw.(driver.Conn)
		eventApp, err := duckdb.NewAppender(dc, "temp", "main", "nr_event")
		if err != nil {
			return err
		}
		derivedApp, err := duckdb.NewAppender(dc, "temp", "main", "nr_derived")
		if err != nil {
			return errors.Join(err, eventApp.Close())
		}
		err = appendNegRisk(eventApp, derivedApp, events)
		return errors.Join(err, eventApp.Close(), derivedApp.Close())
	})
	if err != nil {
		return fmt.Errorf("refs neg-risk conditions: %w", err)
	}
	return nil
}

func appendNegRisk(eventApp, derivedApp *duckdb.Appender, events []event) error {
	for _, e := range events {
		if err := eventApp.AppendRow(e.key[:], e.questions); err != nil {
			return err
		}
		for q := range e.questions {
			condition := ConditionID(NegRiskAdapter, QuestionID(e.key, uint8(q)))
			if err := derivedApp.AppendRow(e.key[:], uint8(q), condition[:]); err != nil {
				return err
			}
		}
	}
	return nil
}

func steps(src *dataset.Source) []step {
	from := func(table string, cols ...string) string { return src.From(src.Files(table), cols...) }
	return []step{
		{"cond", `CREATE OR REPLACE TEMP TABLE cond AS
			SELECT condition, resolved,
				CASE WHEN resolved THEN CAST(n[1] AS UBIGINT) END AS payout0,
				CASE WHEN resolved THEN CAST(n[2] AS UBIGINT) END AS payout1,
				CASE WHEN resolved THEN den END AS payout_den
			FROM (
				SELECT unhex(substr(id, 3)) AS condition, payoutNumerators AS n, CAST(payoutDenominator AS UBIGINT) AS den,
					coalesce(len(payoutNumerators) = 2 AND CAST(payoutDenominator AS UBIGINT) > 0, false) AS resolved
				FROM ` + from("condition", "id", "payoutNumerators", "payoutDenominator") + `
			)`},
		{"market_data", `CREATE OR REPLACE TEMP TABLE md AS
			SELECT unhex(substr(condition, 3)) AS condition, nullif(min(marketName), '') AS question,
				nullif(min(marketSlug), '') AS slug, min(CAST(try_cast(endDate AS TIMESTAMP) AS DATE)) AS end_date
			FROM ` + from("market_data", "condition", "marketName", "marketSlug", "endDate") + `
			GROUP BY 1`},
		{"ek", `CREATE OR REPLACE TABLE ek AS
			SELECT CAST(row_number() OVER (ORDER BY key) - 1 AS UINTEGER) AS code, key,
				CAST(` + bucketSQL("key") + ` AS UTINYINT) AS bucket, question_count
			FROM (
				SELECT key, question_count FROM nr_event
				UNION ALL
				SELECT c.condition, CAST(1 AS UINTEGER) FROM cond c
				WHERE NOT EXISTS (SELECT 1 FROM nr_derived d WHERE d.condition = c.condition)
			)
			ORDER BY code`},
		{"market", `CREATE OR REPLACE TABLE market AS
			SELECT CAST(row_number() OVER (ORDER BY c.condition) - 1 AS UINTEGER) AS market_id, c.condition,
				md.question, md.slug, md.end_date,
				CAST(CASE WHEN md.question IS NULL THEN ` + strconv.Itoa(int(KindUnknown)) + ` WHEN md.question ILIKE '%` + upDownPhrase + `%' THEN ` +
			strconv.Itoa(int(KindUpDown)) + ` ELSE ` + strconv.Itoa(int(KindOther)) + ` END AS UTINYINT) AS kind,
				d.condition IS NOT NULL AS neg_risk, e.code AS ek, e.bucket,
				c.resolved, c.payout0, c.payout1, c.payout_den
			FROM cond c
			LEFT JOIN md ON md.condition = c.condition
			LEFT JOIN nr_derived d ON d.condition = c.condition
			JOIN ek e ON e.key = coalesce(d.key, c.condition)
			ORDER BY market_id`},
		{"nr_question", `CREATE OR REPLACE TABLE nr_question AS
			SELECT e.code AS ek, d.q, m.market_id
			FROM nr_derived d
			JOIN market m ON m.condition = d.condition
			JOIN ek e ON e.key = d.key
			ORDER BY ek, q`},
		{"tok", `CREATE OR REPLACE TABLE tok AS
			SELECT CAST(row_number() OVER (ORDER BY p.id) - 1 AS UINTEGER) AS code, p.id AS token, m.market_id,
				CAST(p.outcomeIndex AS UTINYINT) AS outcome
			FROM ` + from("position", "id", "condition", "outcomeIndex") + ` p
			JOIN market m ON m.condition = unhex(substr(p.condition, 3))
			ORDER BY code`},
		{"wallet_reg", `CREATE OR REPLACE TABLE wallet_reg AS
			SELECT unhex(substr(lower(id), 3)) AS address, arg_min(unhex(substr(lower(signer), 3)), id) AS owner,
				CAST(arg_min(CASE type WHEN 'proxy' THEN 1 WHEN 'safe' THEN 2 END, id) AS UTINYINT) AS owner_kind
			FROM ` + from("wallet", "id", "signer", "type") + `
			GROUP BY 1`},
		{"amm", `CREATE OR REPLACE TABLE amm AS
			SELECT unhex(substr(lower(addr), 3)) AS address, CAST(count(*) AS UINTEGER) AS n
			FROM ` + from("fpmm_transaction", `"user" AS addr`) + `
			WHERE addr IS NOT NULL
			GROUP BY 1`},
	}
}

func bucketSQL(key string) string {
	return fmt.Sprintf("CAST('0x' || substr(hex(%s), 1, 16) AS UBIGINT) %% %d", key, Buckets)
}
