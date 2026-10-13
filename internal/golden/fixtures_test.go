package golden

import (
	"database/sql"
	"encoding/hex"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"pm/internal/dataset"
	"pm/internal/refs"
)

const maxFixtureBytes = 100 << 20

func fixtureSource(t *testing.T) *dataset.Source {
	t.Helper()
	db, err := dataset.Open(t.Context(), dataset.Config{ExtDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	src, err := dataset.NewSource(t.Context(), db, Path("dataset"))
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func fixtureFrom(t *testing.T, src *dataset.Source, table string, cols ...string) string {
	t.Helper()
	files := src.Files(table)
	if len(files) == 0 {
		t.Fatalf("no fixture files for %s under %s", table, Path("dataset"))
	}
	return src.From(files, cols...)
}

func fixtureSet(t *testing.T, db *sql.DB, query string) map[string]bool {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]bool{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		got[v] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestFixtureFillsMatchGolden(t *testing.T) {
	src := fixtureSource(t)
	type total struct{ fills, volume int64 }
	got := map[string]total{}
	rows, err := src.DB.QueryContext(t.Context(), `
		SELECT lower(maker), count(*),
			CAST(sum(CAST(CASE WHEN makerAssetId = '0' THEN makerAmountFilled ELSE takerAmountFilled END AS HUGEINT)) AS BIGINT)
		FROM `+fixtureFrom(t, src, "order_filled", fillCols...)+` GROUP BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var maker string
		var c total
		if err := rows.Scan(&maker, &c.fills, &c.volume); err != nil {
			t.Fatal(err)
		}
		got[maker] = c
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, name := range []string{"wallets_14", "wallets_conversion", "wallets_fee_era", "wallets_cutoff"} {
		wallets, err := LoadWallets(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, w := range wallets {
			c := got[strings.ToLower(w.Address)]
			if c.fills != int64(w.Fills) || c.volume != w.Volume {
				t.Errorf("%s %s has %d fills and volume %d, want %d and %d", name, w.Address, c.fills, c.volume, w.Fills, w.Volume)
			}
			checked++
		}
	}
	if checked != 25 {
		t.Fatalf("checked %d wallets, want 25", checked)
	}
}

func TestDerivedConditionsExist(t *testing.T) {
	src := fixtureSource(t)
	have := fixtureSet(t, src.DB, "SELECT id FROM "+fixtureFrom(t, src, "condition", "id"))
	rows, err := src.DB.QueryContext(t.Context(), "SELECT DISTINCT negRiskMarketId, questionCount FROM "+fixtureFrom(t, src, "neg_risk_conversion", "negRiskMarketId", "questionCount"))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	conversions := 0
	for rows.Next() {
		var id, count string
		if err := rows.Scan(&id, &count); err != nil {
			t.Fatal(err)
		}
		raw, err := hex.DecodeString(strings.TrimPrefix(id, "0x"))
		if err != nil || len(raw) != 32 {
			t.Fatalf("bad negRiskMarketId %q", id)
		}
		derived, err := derive(id, count)
		if err != nil {
			t.Fatal(err)
		}
		for q, cond := range derived {
			if want := refs.ConditionID(refs.NegRiskAdapter, refs.QuestionID([32]byte(raw), uint8(q))); cond != "0x"+hex.EncodeToString(want[:]) || !have[cond] {
				t.Errorf("market %s question %d: condition %s is missing from the fixtures", id, q, cond)
			}
		}
		conversions++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if conversions == 0 {
		t.Fatal("no neg_risk_conversion rows in the fixtures")
	}
}

func TestFixtureTokensAndConditionsMapped(t *testing.T) {
	src := fixtureSource(t)
	positions := fixtureSet(t, src.DB, "SELECT id FROM "+fixtureFrom(t, src, "position", "id"))
	conditions := fixtureSet(t, src.DB, "SELECT id FROM "+fixtureFrom(t, src, "condition", "id"))
	tokens := fixtureSet(t, src.DB, `
		SELECT tok FROM (
			SELECT makerAssetId AS tok FROM `+fixtureFrom(t, src, "order_filled", "makerAssetId")+`
			UNION SELECT takerAssetId FROM `+fixtureFrom(t, src, "order_filled", "takerAssetId")+`
		) WHERE tok <> '0'`)
	if len(tokens) == 0 {
		t.Fatal("no fill tokens in the fixtures")
	}
	for tok := range tokens {
		if !positions[tok] {
			t.Errorf("fill token %s has no position row", tok)
		}
	}
	for _, table := range []string{"split", "merge", "redemption"} {
		for cond := range fixtureSet(t, src.DB, "SELECT DISTINCT condition FROM "+fixtureFrom(t, src, table, "condition")) {
			if !conditions[cond] {
				t.Errorf("%s condition %s has no condition row", table, cond)
			}
		}
	}
}

func TestFixtureHasNoUnreadColumns(t *testing.T) {
	src := fixtureSource(t)
	banned := map[string]bool{"orderHash": true, "transactionHash": true, "fee": true, "year": true}
	files := 0
	for _, table := range requiredTables() {
		for _, file := range src.Files(table) {
			cols := fixtureSet(t, src.DB, "SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('"+file+"', hive_partitioning = false))")
			for col := range cols {
				if banned[col] {
					t.Errorf("%s has column %s", file, col)
				}
			}
			files++
		}
	}
	if files == 0 {
		t.Fatal("no fixture files")
	}
}

func TestFixtureSize(t *testing.T) {
	var size int64
	err := filepath.WalkDir(Path("dataset"), func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		info, err := d.Info()
		if err == nil {
			size += info.Size()
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if size == 0 || size >= maxFixtureBytes {
		t.Fatalf("fixtures are %d bytes, want more than 0 and under %d", size, maxFixtureBytes)
	}
}

func TestFixtureBuildsRefs(t *testing.T) {
	src := fixtureSource(t)
	if err := refs.Build(t.Context(), src, src.DB); err != nil {
		t.Fatal(err)
	}
	count := func(query string) int {
		t.Helper()
		var n int
		if err := src.DB.QueryRowContext(t.Context(), query).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if got, want := count("SELECT count(*) FROM market"), count("SELECT count(*) FROM "+fixtureFrom(t, src, "condition", "id")); got != want || got == 0 {
		t.Errorf("market has %d rows for %d conditions", got, want)
	}
	if got, want := count("SELECT count(*) FROM tok"), count("SELECT count(*) FROM "+fixtureFrom(t, src, "position", "id")); got != want {
		t.Errorf("tok has %d rows for %d positions", got, want)
	}
	unmapped := count(`SELECT count(*) FROM (
		SELECT makerAssetId AS tok FROM ` + fixtureFrom(t, src, "order_filled", "makerAssetId") + `
		UNION SELECT takerAssetId FROM ` + fixtureFrom(t, src, "order_filled", "takerAssetId") + `
	) WHERE tok <> '0' AND tok NOT IN (SELECT token FROM tok)`)
	if unmapped != 0 {
		t.Errorf("%d fill tokens have no tok row", unmapped)
	}
	events := count("SELECT count(*) FROM " + fixtureFrom(t, src, "neg_risk_event", "id"))
	short := count(`SELECT count(*) FROM ` + fixtureFrom(t, src, "neg_risk_event", "id") + ` ev JOIN ek e ON e.key = unhex(substr(ev.id, 3))
		WHERE e.question_count <> (SELECT count(*) FROM nr_question q WHERE q.ek = e.code)`)
	if events == 0 || short != 0 {
		t.Errorf("%d events, %d of them with missing question conditions", events, short)
	}
}
