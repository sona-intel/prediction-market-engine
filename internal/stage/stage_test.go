package stage_test

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"pm/internal/dataset"
	"pm/internal/dstest"
	"pm/internal/golden"
	"pm/internal/refs"
	"pm/internal/stage"
)

const (
	ctfExchange = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
	negExchange = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
	alice       = "0xAbCdEf0123456789aBcDeF0123456789AbCdEf01"
	bob         = "0x0123456789ABCDEF0123456789abcdef01234567"
	carol       = "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
	negMarket   = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcd00"
	wideA       = "137301486137384042495"
	wideB       = "11150372599265311570767859136324180752990208"
)

var (
	fillCols       = []string{"id", "timestamp", "maker", "taker", "makerAssetId", "takerAssetId", "makerAmountFilled", "takerAmountFilled"}
	redemptionCols = []string{"id", "redeemer", "condition", "timestamp"}
	ctfCols        = []string{"id", "timestamp", "stakeholder", "condition", "amount"}
	convertCols    = []string{"id", "timestamp", "stakeholder", "negRiskMarketId", "amount", "indexSet", "questionCount"}
	feeCols        = []string{"id", "refundee", "tokenId", "feeCharged"}

	tableCols = map[string][]string{
		"order_filled":        fillCols,
		"redemption":          redemptionCols,
		"split":               ctfCols,
		"merge":               ctfCols,
		"neg_risk_conversion": convertCols,
		"fee_refunded":        feeCols,
	}
)

const (
	condA    = 0
	condB    = 1
	condNeg0 = 2
	negCount = 5
)

type world struct {
	t      *testing.T
	set    *dstest.Set
	negKey [32]byte
	ids    []string
	toks   [][2]string
}

func hex32(b [32]byte) string {
	return "0x" + hex.EncodeToString(b[:])
}

func newWorld(t *testing.T, without ...string) *world {
	t.Helper()
	w := &world{t: t, set: dstest.New(t)}
	key, err := hex.DecodeString(negMarket)
	if err != nil {
		t.Fatal(err)
	}
	w.negKey = [32]byte(key)
	w.ids = []string{"0x" + strings.Repeat("11", 32), "0x" + strings.Repeat("22", 32)}
	for q := range uint8(negCount) {
		w.ids = append(w.ids, hex32(refs.ConditionID(refs.NegRiskAdapter, refs.QuestionID(w.negKey, q))))
	}
	var conditions, positions [][]any
	for i, id := range w.ids {
		tok := [2]string{strings.Repeat("9", 70) + strconv.Itoa(100+2*i), strings.Repeat("9", 70) + strconv.Itoa(101+2*i)}
		w.toks = append(w.toks, tok)
		numerators, den := []string{}, "0"
		if i == condA {
			numerators, den = []string{"1", "0"}, "1"
		}
		conditions = append(conditions, []any{id, []string{tok[0], tok[1]}, numerators, den})
		positions = append(positions, []any{tok[0], id, "0"}, []any{tok[1], id, "1"})
	}
	w.set.Write("condition.parquet", []string{"id", "positionIds", "payoutNumerators", "payoutDenominator"}, conditions...)
	w.set.Write("position.parquet", []string{"id", "condition", "outcomeIndex"}, positions...)
	w.set.Write("market_data.parquet", []string{"id", "endDate", "condition", "marketName", "marketSlug"},
		[]any{w.toks[condA][0], "2024-09-10T00:00:00Z", w.ids[condA], "Will A happen?", "a"})
	w.set.Write("neg_risk_event.parquet", []string{"id", "feeBps", "questionCount"}, []any{"0x" + negMarket, "0", strconv.Itoa(negCount)})
	w.set.Write("wallet.parquet", []string{"id", "signer", "type"}, []any{alice, bob, "proxy"})
	w.set.Write("fpmm_transaction/year=2022/month=06.parquet", []string{"id", "type", "user"}, []any{"137_1_1", "Buy", carol})
	for table, cols := range tableCols {
		if !slices.Contains(without, table) {
			w.set.Write(table+"/year=2024/month=01.parquet", cols)
		}
	}
	return w
}

func (w *world) write(table, month string, rows ...[]any) {
	w.t.Helper()
	w.set.Write(table+"/year=2025/month="+month+".parquet", tableCols[table], rows...)
}

func (w *world) path(table, month string) string {
	return filepath.Join(w.set.Root, table, "year=2025", "month="+month+".parquet")
}

type env struct {
	t *testing.T
	o stage.Options
}

func prepare(t *testing.T, root string) *env {
	t.Helper()
	db, err := dataset.Open(t.Context(), dataset.Config{Path: filepath.Join(t.TempDir(), "work.duckdb"), ExtDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	src, err := dataset.NewSource(t.Context(), db, root)
	if err != nil {
		t.Fatal(err)
	}
	if err := refs.Build(t.Context(), src, db); err != nil {
		t.Fatal(err)
	}
	return &env{t: t, o: stage.Options{Work: db, Src: src, StageDir: filepath.Join(t.TempDir(), "stage")}}
}

func (e *env) run() {
	e.t.Helper()
	if err := stage.Run(e.t.Context(), e.o); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) staged(kind string) string {
	return "read_parquet('" + filepath.Join(e.o.StageDir, "b=*", kind+"-*.parquet") + "', hive_partitioning=false)"
}

func (e *env) count(query string, args ...any) int64 {
	e.t.Helper()
	var n int64
	if err := e.o.Work.QueryRowContext(e.t.Context(), query, args...).Scan(&n); err != nil {
		e.t.Fatalf("%s: %v", query, err)
	}
	return n
}

func (e *env) rows(kind string) int64 {
	e.t.Helper()
	return e.count("SELECT CAST(count(*) AS BIGINT) FROM " + e.staged(kind))
}

func (e *env) code(addr string) uint32 {
	e.t.Helper()
	var code uint32
	err := e.o.Work.QueryRowContext(e.t.Context(),
		"SELECT code FROM wallet_dict WHERE address = unhex(substr(lower(?), 3))", addr).Scan(&code)
	if err != nil {
		e.t.Fatalf("code of %s: %v", addr, err)
	}
	return code
}

func (e *env) token(token string) uint32 {
	e.t.Helper()
	var code uint32
	if err := e.o.Work.QueryRowContext(e.t.Context(), "SELECT code FROM tok WHERE token = ?", token).Scan(&code); err != nil {
		e.t.Fatalf("code of token %s: %v", token, err)
	}
	return code
}

func (e *env) market(condition string) uint32 {
	e.t.Helper()
	var id uint32
	err := e.o.Work.QueryRowContext(e.t.Context(), "SELECT market_id FROM market WHERE condition = unhex(substr(?, 3))", condition).Scan(&id)
	if err != nil {
		e.t.Fatalf("market of %s: %v", condition, err)
	}
	return id
}

func (e *env) audit(key string) int64 {
	e.t.Helper()
	return e.count("SELECT CAST(value AS BIGINT) FROM audit WHERE key = ?", key)
}

func (e *env) folders() []string {
	e.t.Helper()
	dirs, err := filepath.Glob(filepath.Join(e.o.StageDir, "b=*"))
	if err != nil {
		e.t.Fatal(err)
	}
	for i, d := range dirs {
		dirs[i] = filepath.Base(d)
	}
	return dirs
}

func fill(id, ts, maker, taker, makerAsset, takerAsset, makerAmt, takerAmt string) []any {
	return []any{id, ts, maker, taker, makerAsset, takerAsset, makerAmt, takerAmt}
}

func TestStageGoldenCounts(t *testing.T) {
	e := prepare(t, golden.Path("dataset"))
	e.o.Pace = dataset.NewPacer(8)
	e.run()
	for _, tc := range []struct {
		addr                     string
		rows, summary, usdc, sum int64
	}{
		{"0x0FE9463bCfA3e3dcA586Acc12acf1D74EfB97E68", 6, 2, 121520000, 60760000},
		{"0x467ecfc0c6265683e90899C41881c0431bDEE2ed", 290, -1, 716582888996, -1},
	} {
		code := e.code(strings.ToLower(tc.addr))
		q := "SELECT CAST(%s AS BIGINT) FROM " + e.staged("fill") + " WHERE maker = ?"
		if got := e.count(fmt.Sprintf(q, "count(*)"), code); got != tc.rows {
			t.Errorf("%s rows %d, want %d", tc.addr, got, tc.rows)
		}
		if got := e.count(fmt.Sprintf(q, "sum(usdc)"), code); got != tc.usdc {
			t.Errorf("%s usdc %d, want %d", tc.addr, got, tc.usdc)
		}
		if tc.summary < 0 {
			continue
		}
		if got := e.count(fmt.Sprintf(q, "count(*)")+" AND taker IS NULL", code); got != tc.summary {
			t.Errorf("%s summary rows %d, want %d", tc.addr, got, tc.summary)
		}
		if got := e.count(fmt.Sprintf(q, "sum(usdc)")+" AND taker IS NULL", code); got != tc.sum {
			t.Errorf("%s summary usdc %d, want %d", tc.addr, got, tc.sum)
		}
	}
	if got := e.audit("unmapped_rows"); got != 0 {
		t.Errorf("unmapped_rows %d, want 0", got)
	}
}

func TestStageDecodesSide(t *testing.T) {
	w := newWorld(t)
	tok := w.toks[condA][0]
	w.write("order_filled", "01",
		fill("137_200_1", "1700000100", alice, bob, "0", tok, "3000000", "10000000"),
		fill("137_200_2", "1700000100", bob, alice, tok, "0", "10000000", "3000000"),
		fill("137_200_3", "1700000200", alice, ctfExchange, tok, "0", "7000000", "2000000"),
	)
	e := prepare(t, w.set.Root)
	e.run()
	type row struct {
		block, logIndex, ts, maker uint32
		taker                      sql.Null[uint32]
		tok                        uint32
		buy                        bool
		shares, usdc               uint64
	}
	rows, err := e.o.Work.QueryContext(t.Context(),
		"SELECT block, log_index, ts, maker, taker, tok, buy, shares, usdc FROM "+e.staged("fill")+" ORDER BY block, log_index")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.block, &r.logIndex, &r.ts, &r.maker, &r.taker, &r.tok, &r.buy, &r.shares, &r.usdc); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	a, b, code := e.code(alice), e.code(bob), e.token(tok)
	want := []row{
		{200, 1, 1700000100, a, sql.Null[uint32]{V: b, Valid: true}, code, true, 10000000, 3000000},
		{200, 2, 1700000100, b, sql.Null[uint32]{V: a, Valid: true}, code, false, 10000000, 3000000},
		{200, 3, 1700000200, a, sql.Null[uint32]{}, code, false, 7000000, 2000000},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("rows\n%v\nwant\n%v", got, want)
	}
}

func TestStageSummaryTakerStaysNull(t *testing.T) {
	w := newWorld(t)
	tok := w.toks[condA][0]
	w.write("order_filled", "01",
		fill("137_250_1", "1700000100", negExchange, bob, "0", tok, "1000000", "2000000"),
		fill("137_250_2", "1700000100", alice, negExchange, "0", tok, "1000000", "2000000"),
		fill("137_250_3", "1700000100", alice, strings.ToLower(ctfExchange), "0", tok, "1000000", "2000000"),
	)
	e := prepare(t, w.set.Root)
	e.run()
	if got := e.count("SELECT CAST(count(*) AS BIGINT) FROM " + e.staged("fill") + " WHERE taker IS NULL"); got != 2 {
		t.Fatalf("%d summary rows, want 2", got)
	}
}

func TestStageMergesAddressCase(t *testing.T) {
	w := newWorld(t)
	tok := w.toks[condA][0]
	w.write("order_filled", "01", fill("137_300_1", "1700000300", alice, alice, "0", tok, "1000000", "2000000"))
	w.write("order_filled", "02",
		fill("137_301_1", "1700000301", strings.ToLower(alice), bob, "0", tok, "1000000", "2000000"),
		fill("137_301_2", "1700000301", bob, "0x"+strings.ToUpper(carol[2:]), "0", tok, "1000000", "2000000"),
		fill("137_301_3", "1700000301", carol, bob, "0", tok, "1000000", "2000000"),
	)
	e := prepare(t, w.set.Root)
	e.run()
	if got := e.count("SELECT CAST(count(DISTINCT maker) AS BIGINT) FROM "+e.staged("fill")+" WHERE maker = ?", e.code(alice)); got != 1 {
		t.Fatalf("alice has %d codes", got)
	}
	if got := e.count("SELECT CAST(count(*) AS BIGINT) FROM "+e.staged("fill")+" WHERE maker = ?", e.code(alice)); got != 2 {
		t.Fatalf("alice has %d rows, want 2", got)
	}
	if got := e.count("SELECT CAST(count(*) AS BIGINT) FROM wallet_dict WHERE address = unhex(?)", strings.ToLower(alice[2:])); got != 1 {
		t.Fatalf("wallet_dict holds alice %d times", got)
	}
	if got := e.count("SELECT CAST(count(*) AS BIGINT) FROM wallet_dict WHERE octet_length(address) <> 20"); got != 0 {
		t.Fatalf("%d addresses are not 20 bytes", got)
	}
	if got := e.count("SELECT CAST(count(*) AS BIGINT) FROM wallet_dict"); got != 3 {
		t.Fatalf("wallet_dict has %d entries, want 3", got)
	}
	rows, err := e.o.Work.QueryContext(t.Context(), "SELECT address, wallet_id FROM wallet_dict ORDER BY wallet_id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var prev []byte
	for i := 0; rows.Next(); i++ {
		var addr []byte
		var id uint32
		if err := rows.Scan(&addr, &id); err != nil {
			t.Fatal(err)
		}
		if int(id) != i {
			t.Fatalf("wallet_id %d at rank %d", id, i)
		}
		if prev != nil && string(prev) >= string(addr) {
			t.Fatalf("wallet_id %d is out of address order", id)
		}
		prev = addr
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestStageKeepsWideIndexSet(t *testing.T) {
	w := newWorld(t)
	w.write("neg_risk_conversion", "01",
		[]any{"137_400_1", "1700000400", alice, "0x" + negMarket, "5000000", wideA, strconv.Itoa(negCount)},
		[]any{"137_400_2", "1700000401", alice, "0x" + negMarket, "6000000", wideB, strconv.Itoa(negCount)},
		[]any{"137_400_3", "1700000402", alice, "0x" + negMarket, "7000000", "3", strconv.Itoa(negCount)},
	)
	e := prepare(t, w.set.Root)
	e.run()
	rows, err := e.o.Work.QueryContext(t.Context(),
		"SELECT index_set, typeof(index_set) FROM "+e.staged("ctf")+" WHERE kind = 4 ORDER BY log_index")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var s, typ string
		if err := rows.Scan(&s, &typ); err != nil {
			t.Fatal(err)
		}
		if typ != "VARCHAR" {
			t.Fatalf("index_set has type %s", typ)
		}
		got = append(got, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if want := []string{wideA, wideB, "3"}; !slices.Equal(got, want) {
		t.Fatalf("index sets %v, want %v", got, want)
	}
}

func TestStageDecodesCtf(t *testing.T) {
	w := newWorld(t)
	a := w.ids[condA]
	w.write("split", "01", []any{"137_500_1", "1700000500", alice, a, "9000000"})
	w.write("merge", "01", []any{"137_500_2", "1700000501", bob, a, "4000000"})
	w.write("redemption", "01", []any{"137_500_3", alice, a, "1700000502"})
	w.write("neg_risk_conversion", "01", []any{"137_500_4", "1700000503", carol, "0x" + negMarket, "2000000", "5", "4"})
	e := prepare(t, w.set.Root)
	e.run()
	var ek uint32
	if err := e.o.Work.QueryRowContext(t.Context(), "SELECT code FROM ek WHERE key = unhex(?)", negMarket).Scan(&ek); err != nil {
		t.Fatal(err)
	}
	market := e.market(a)
	type row struct {
		block, logIndex, ts uint32
		kind                uint8
		wallet, ref         uint32
		amount              uint64
		indexSet            sql.Null[string]
		questions           sql.Null[uint32]
	}
	rows, err := e.o.Work.QueryContext(t.Context(),
		"SELECT block, log_index, ts, kind, wallet, ref, amount, index_set, question_count FROM "+e.staged("ctf")+" ORDER BY log_index")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.block, &r.logIndex, &r.ts, &r.kind, &r.wallet, &r.ref, &r.amount, &r.indexSet, &r.questions); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []row{
		{500, 1, 1700000500, 1, e.code(alice), market, 9000000, sql.Null[string]{}, sql.Null[uint32]{}},
		{500, 2, 1700000501, 2, e.code(bob), market, 4000000, sql.Null[string]{}, sql.Null[uint32]{}},
		{500, 3, 1700000502, 3, e.code(alice), market, 0, sql.Null[string]{}, sql.Null[uint32]{}},
		{500, 4, 1700000503, 4, e.code(carol), ek, 2000000, sql.Null[string]{V: "5", Valid: true}, sql.Null[uint32]{V: 4, Valid: true}},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("rows\n%v\nwant\n%v", got, want)
	}
}

func TestStageSortsNumerically(t *testing.T) {
	w := newWorld(t)
	tok := w.toks[condA][0]
	w.write("order_filled", "01",
		fill("137_99999_9", "1700000600", alice, bob, "0", tok, "1000000", "2000000"),
		fill("137_100000_1", "1700000600", alice, bob, "0", tok, "1000000", "2000000"),
	)
	e := prepare(t, w.set.Root)
	e.run()
	if got := e.count("SELECT CAST(count(*) AS BIGINT) FROM (DESCRIBE SELECT block, log_index FROM " + e.staged("fill") +
		") WHERE column_type = 'UINTEGER'"); got != 2 {
		t.Fatalf("block and log_index are not both UINTEGER")
	}
	rows, err := e.o.Work.QueryContext(t.Context(), "SELECT block, log_index FROM "+e.staged("fill")+" ORDER BY block, log_index")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got [][2]uint32
	for rows.Next() {
		var p [2]uint32
		if err := rows.Scan(&p[0], &p[1]); err != nil {
			t.Fatal(err)
		}
		got = append(got, p)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if want := [][2]uint32{{99999, 9}, {100000, 1}}; !slices.Equal(got, want) {
		t.Fatalf("order %v, want %v", got, want)
	}
}

func TestStageBucketsByEvent(t *testing.T) {
	w := newWorld(t)
	var fills [][]any
	for q := range negCount {
		fills = append(fills, fill("137_700_"+strconv.Itoa(q), "1700000700", alice, bob, "0", w.toks[condNeg0+q][0], "1000000", "2000000"))
	}
	w.write("order_filled", "01", fills...)
	w.write("neg_risk_conversion", "01",
		[]any{"137_701_1", "1700000701", alice, "0x" + negMarket, "1000000", "3", strconv.Itoa(negCount)})
	w.write("split", "01", []any{"137_701_2", "1700000701", alice, w.ids[condNeg0+3], "1000000"})
	e := prepare(t, w.set.Root)
	e.run()
	want := "b=" + fmt.Sprintf("%02d", refs.Bucket(w.negKey, refs.Buckets))
	if got := e.folders(); !slices.Equal(got, []string{want}) {
		t.Fatalf("folders %v, want [%s]", got, want)
	}
	for _, kind := range []string{"fill", "ctf"} {
		if got := e.rows(kind); got == 0 {
			t.Errorf("no %s rows staged", kind)
		}
	}
}

func TestStageCountsUnmapped(t *testing.T) {
	w := newWorld(t)
	tok := w.toks[condA][0]
	w.write("order_filled", "01",
		fill("137_800_1", "1700000800", alice, bob, "0", tok, "1000000", "2000000"),
		fill("137_800_2", "1700000800", alice, bob, "0", "424242", "1000000", "2000000"),
	)
	e := prepare(t, w.set.Root)
	e.run()
	if got := e.rows("fill"); got != 1 {
		t.Fatalf("%d fills staged, want 1", got)
	}
	if got := e.audit("unmapped_rows"); got != 1 {
		t.Fatalf("unmapped_rows %d, want 1", got)
	}
	e.run()
	if got := e.audit("unmapped_rows"); got != 1 {
		t.Fatalf("unmapped_rows %d after a second run, want 1", got)
	}
}

func TestStageResumeLeavesNoDuplicates(t *testing.T) {
	w := newWorld(t)
	tok := w.toks[condA][0]
	w.write("order_filled", "01",
		fill("137_900_1", "1700000900", alice, bob, "0", tok, "1000000", "2000000"),
		fill("137_900_2", "1700000900", alice, bob, "0", tok, "1000000", "2000000"),
		fill("137_900_3", "1700000900", alice, bob, "0", tok, "1000000", "2000000"),
	)
	e := prepare(t, w.set.Root)
	home := int(refs.Bucket([32]byte(mustHex(t, w.ids[condA])), refs.Buckets))
	stale := filepath.Join(e.o.StageDir, fmt.Sprintf("b=%02d", (home+1)%refs.Buckets), "fill-order_filled_year=2025_month=01.parquet")
	if err := os.MkdirAll(filepath.Dir(stale), 0o750); err != nil {
		t.Fatal(err)
	}
	_, err := e.o.Work.ExecContext(t.Context(), `COPY (SELECT CAST(1 AS UINTEGER) AS block, CAST(1 AS UINTEGER) AS log_index,
		CAST(1 AS UINTEGER) AS ts, CAST(1 AS UINTEGER) AS maker, CAST(1 AS UINTEGER) AS taker, CAST(1 AS UINTEGER) AS tok,
		true AS buy, CAST(1 AS UBIGINT) AS shares, CAST(1 AS UBIGINT) AS usdc) TO '`+stale+`' (FORMAT parquet)`)
	if err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(e.o.StageDir, ".tmp")
	junk := filepath.Join(tmp, "bucket=00", "data_0.parquet")
	if err := os.MkdirAll(filepath.Dir(junk), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(junk, []byte("killed mid copy"), 0o600); err != nil {
		t.Fatal(err)
	}
	e.run()
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale file survived: %v", err)
	}
	if _, err := os.Stat(tmp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp folder survived: %v", err)
	}
	if got := e.rows("fill"); got != 3 {
		t.Fatalf("%d fills staged, want 3", got)
	}
	for _, table := range []string{"order_filled", "redemption", "split", "merge", "neg_risk_conversion", "fee_refunded"} {
		for _, p := range w.sources(table) {
			if err := os.WriteFile(p, []byte("not parquet"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	e.run()
	if got := e.rows("fill"); got != 3 {
		t.Fatalf("%d fills staged after a second run, want 3", got)
	}
	if got := e.count("SELECT CAST(count(*) AS BIGINT) FROM build_state WHERE stage = 'stage' AND status = 'done'"); got != 7 {
		t.Fatalf("%d done files, want 7", got)
	}
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func (w *world) sources(table string) []string {
	w.t.Helper()
	files, err := filepath.Glob(filepath.Join(w.set.Root, table, "year=*", "month=*.parquet"))
	if err != nil {
		w.t.Fatal(err)
	}
	return files
}

func TestStageRecordsFailedFile(t *testing.T) {
	w := newWorld(t)
	tok := w.toks[condA][0]
	w.write("order_filled", "01", fill("137_1000_1", "1700001000", alice, bob, "0", tok, "1000000", "2000000"))
	w.write("order_filled", "02",
		fill("137_1001_1", "1700001001", alice, bob, "0", tok, "1000000", "2000000"),
		fill("137_1001_2", "1700001001", alice, bob, "0", tok, "1000000", "2000000"),
	)
	e := prepare(t, w.set.Root)
	good, bad := "order_filled/year=2025/month=01.parquet", "order_filled/year=2025/month=02.parquet"
	original, err := os.ReadFile(w.path("order_filled", "02"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(w.path("order_filled", "02"), []byte("not parquet"), 0o600); err != nil {
		t.Fatal(err)
	}
	err = stage.Run(t.Context(), e.o)
	if !errors.Is(err, stage.ErrIncomplete) || !strings.Contains(err.Error(), bad) {
		t.Fatalf("Run returned %v, want ErrIncomplete naming %s", err, bad)
	}
	if strings.Contains(err.Error(), good) {
		t.Fatalf("error names the healthy file: %v", err)
	}
	if got := e.rows("fill"); got != 1 {
		t.Fatalf("%d fills staged, want the 1 of the healthy file", got)
	}
	status := func(item string) string {
		var s string
		if err := e.o.Work.QueryRowContext(t.Context(), "SELECT status FROM build_state WHERE stage = 'stage' AND item = ?", item).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	if status(bad) != "failed" || status(good) != "done" {
		t.Fatalf("states are %s and %s", status(bad), status(good))
	}
	doneAt := func(item string) time.Time {
		var at time.Time
		if err := e.o.Work.QueryRowContext(t.Context(), "SELECT done_at FROM build_state WHERE stage = 'stage' AND item = ?", item).Scan(&at); err != nil {
			t.Fatal(err)
		}
		return at
	}
	before := doneAt(good)
	if err := os.WriteFile(w.path("order_filled", "02"), original, 0o600); err != nil {
		t.Fatal(err)
	}
	e.run()
	if got := e.rows("fill"); got != 3 {
		t.Fatalf("%d fills staged after the fix, want 3", got)
	}
	if status(bad) != "done" {
		t.Fatalf("fixed file is %s", status(bad))
	}
	if !doneAt(good).Equal(before) {
		t.Fatal("the healthy file was staged again")
	}
}

func TestStageNeedsEveryTable(t *testing.T) {
	w := newWorld(t, "split")
	e := prepare(t, w.set.Root)
	err := stage.Run(t.Context(), e.o)
	if err == nil || !strings.Contains(err.Error(), "split") {
		t.Fatalf("Run returned %v, want an error naming split", err)
	}
}

func TestFirstRedeem(t *testing.T) {
	w := newWorld(t)
	a, b := w.ids[condA], w.ids[condB]
	w.write("redemption", "01",
		[]any{"137_1100_1", alice, a, "1000"},
		[]any{"137_1100_2", bob, a, "900"},
		[]any{"137_1100_3", alice, b, "2000"},
	)
	w.write("split", "01", []any{"137_1100_4", "100", alice, a, "1000000"})
	e := prepare(t, w.set.Root)
	e.run()
	want := map[uint32]uint32{e.market(a): 900, e.market(b): 2000}
	rows, err := e.o.Work.QueryContext(t.Context(), "SELECT market_id, ts FROM first_redeem")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[uint32]uint32{}
	for rows.Next() {
		var id, ts uint32
		if err := rows.Scan(&id, &ts); err != nil {
			t.Fatal(err)
		}
		got[id] = ts
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("first_redeem %v, want %v", got, want)
	}
}

func TestFirstRedeemWithoutRedemptions(t *testing.T) {
	w := newWorld(t)
	e := prepare(t, w.set.Root)
	e.run()
	if got := e.count("SELECT CAST(count(*) AS BIGINT) FROM first_redeem"); got != 0 {
		t.Fatalf("first_redeem has %d rows, want 0", got)
	}
}

func TestStageFeesSummed(t *testing.T) {
	w := newWorld(t)
	a, b := w.toks[condA][0], w.toks[condB][1]
	w.write("fee_refunded", "01",
		[]any{"137_1200_1", alice, a, "100"},
		[]any{"137_1200_2", strings.ToLower(alice), a, "250"},
		[]any{"137_1200_3", alice, b, "40"},
		[]any{"137_1200_4", bob, a, "7"},
	)
	e := prepare(t, w.set.Root)
	e.run()
	fee := func(wallet string, token string) int64 {
		return e.count("SELECT CAST(coalesce(sum(fee), -1) AS BIGINT) FROM "+e.staged("fee")+" WHERE wallet = ? AND market_id = (SELECT market_id FROM tok WHERE token = ?)",
			e.code(wallet), token)
	}
	if got := fee(alice, a); got != 350 {
		t.Errorf("alice fee on A is %d, want 350", got)
	}
	if got := e.count("SELECT CAST(count(*) AS BIGINT) FROM "+e.staged("fee")+" WHERE wallet = ? AND market_id = ?", e.code(alice), e.market(w.ids[condA])); got != 1 {
		t.Errorf("alice has %d fee rows on A, want 1", got)
	}
	if got := fee(alice, b); got != 40 {
		t.Errorf("alice fee on B is %d, want 40", got)
	}
	if got := fee(bob, a); got != 7 {
		t.Errorf("bob fee on A is %d, want 7", got)
	}
}

func TestStageKeepsUnattributedFees(t *testing.T) {
	w := newWorld(t)
	a := w.toks[condA][0]
	w.write("fee_refunded", "01",
		[]any{"137_1300_1", alice, "0", "300"},
		[]any{"137_1300_2", alice, "0", "20"},
		[]any{"137_1300_3", alice, a, "5"},
		[]any{"137_1300_4", bob, "0", "0"},
	)
	e := prepare(t, w.set.Root)
	e.run()
	if got := e.count("SELECT CAST(sum(fee) AS BIGINT) FROM "+e.staged("fee")+" WHERE wallet = ? AND market_id IS NULL", e.code(alice)); got != 320 {
		t.Errorf("alice unattributed fee %d, want 320", got)
	}
	if got := e.count("SELECT CAST(count(*) AS BIGINT) FROM "+e.staged("fee")+" WHERE wallet = ? AND market_id IS NULL", e.code(alice)); got != 1 {
		t.Errorf("alice has %d unattributed rows, want 1", got)
	}
	if got := e.count("SELECT CAST(sum(fee) AS BIGINT) FROM " + e.staged("fee")); got != 325 {
		t.Errorf("total fee %d, want 325", got)
	}
	if got := e.audit("unmapped_rows"); got != 0 {
		t.Errorf("unmapped_rows %d, want 0", got)
	}
}

func TestStageDropsEarlierRequests(t *testing.T) {
	w := newWorld(t)
	w.write("order_filled", "01", fill("137_1400_1", "1700001400", alice, bob, "0", w.toks[condA][0], "1000000", "2000000"))
	e := prepare(t, w.set.Root)
	ctx := t.Context()
	if _, err := e.o.Work.ExecContext(ctx, "CALL enable_logging('HTTP')"); err != nil {
		t.Fatal(err)
	}
	msg := "{''request'': {''type'': GET, ''url'': ''x''}, ''response'': {''headers'': {content-length=5000}}}"
	if _, err := e.o.Work.ExecContext(ctx, "SELECT write_log('"+msg+"', log_type := 'HTTP')"); err != nil {
		t.Fatal(err)
	}
	e.run()
	if got := e.count("SELECT CAST(sum(requests) + sum(bytes) AS BIGINT) FROM build_state WHERE stage = 'stage'"); got != 0 {
		t.Fatalf("stage was charged %d for earlier requests", got)
	}
}
