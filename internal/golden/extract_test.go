package golden

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"pm/internal/dataset"
	"pm/internal/dstest"
	"pm/internal/refs"
)

const (
	decoy     = "0x1111111111111111111111111111111111111111"
	exchange  = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
	decoyTok  = "9999"
	decoyCond = "0xd0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0"
	decoyNR   = "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd00"
	marketNR  = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcd00"
)

var sourceCols = map[string][]string{
	"order_filled":        {"id", "orderHash", "transactionHash", "fee", "timestamp", "maker", "taker", "makerAssetId", "takerAssetId", "makerAmountFilled", "takerAmountFilled"},
	"redemption":          {"id", "timestamp", "redeemer", "condition", "indexSets", "payout"},
	"split":               {"id", "timestamp", "stakeholder", "condition", "amount"},
	"merge":               {"id", "timestamp", "stakeholder", "condition", "amount"},
	"neg_risk_conversion": {"id", "timestamp", "stakeholder", "negRiskMarketId", "amount", "indexSet", "questionCount"},
	"fee_refunded":        {"id", "orderHash", "tokenId", "timestamp", "refundee", "feeRefunded", "feeCharged"},
	"fpmm_transaction":    {"id", "type", "timestamp", "market_id", "user", "tradeAmount", "feeAmount", "outcomeIndex", "outcomeTokensAmount"},
	"user_position":       {"id", "user", "tokenId", "amount", "avgPrice", "realizedPnl", "totalBought"},
	"condition":           {"id", "positionIds", "payoutNumerators", "payoutDenominator"},
	"position":            {"id", "condition", "outcomeIndex"},
	"market_data":         {"id", "endDate", "condition", "outcomeIndex", "marketName", "marketSlug", "description"},
	"neg_risk_event":      {"id", "feeBps", "questionCount"},
	"wallet":              {"id", "signer", "type", "balance"},
}

var outputCols = map[string][]string{
	"order_filled":        {"id", "timestamp", "maker", "taker", "makerAssetId", "takerAssetId", "makerAmountFilled", "takerAmountFilled"},
	"redemption":          {"id", "redeemer", "condition", "timestamp"},
	"split":               {"id", "timestamp", "stakeholder", "condition", "amount"},
	"merge":               {"id", "timestamp", "stakeholder", "condition", "amount"},
	"neg_risk_conversion": {"id", "timestamp", "stakeholder", "negRiskMarketId", "amount", "indexSet", "questionCount"},
	"fee_refunded":        {"id", "refundee", "tokenId", "feeCharged"},
	"fpmm_transaction":    {"user"},
	"user_position":       {"user", "tokenId", "amount", "avgPrice", "realizedPnl", "totalBought"},
	"condition":           {"id", "positionIds", "payoutNumerators", "payoutDenominator"},
	"position":            {"id", "condition", "outcomeIndex"},
	"market_data":         {"condition", "marketName", "marketSlug", "endDate"},
	"neg_risk_event":      {"id", "questionCount"},
	"wallet":              {"id", "signer", "type"},
}

type world struct {
	t     *testing.T
	files map[string][][]any
}

type result struct {
	t    *testing.T
	src  *dataset.Source
	pace *dataset.Pacer
	out  string
}

func newWorld(t *testing.T) *world {
	t.Helper()
	w := &world{t: t, files: map[string][][]any{}}
	w.add("order_filled/year=2025/month=01.parquet", fillRow("137_0_1", decoy, "0", decoyTok))
	w.add("redemption/year=2025/month=01.parquet", redemptionRow("137_0_2", decoy, decoyCond))
	w.add("split/year=2025/month=01.parquet", ctfRow("137_0_3", decoy, decoyCond))
	w.add("merge/year=2025/month=01.parquet", ctfRow("137_0_4", decoy, decoyCond))
	w.add("neg_risk_conversion/year=2025/month=01.parquet", conversionRow("137_0_5", decoy, decoyNR, 2))
	w.add("fee_refunded/year=2026/month=01.parquet", feeRow("137_0_6", decoy, decoyTok))
	w.add("fpmm_transaction/year=2022/month=06.parquet", []any{"137_0_7", "Buy", "1", "0xpool", decoy, "1", "0", "0", "1"})
	w.add("user_position.parquet", []any{decoy + "-" + decoyTok, decoy, decoyTok, "1", "1", "1", "1"})
	w.add("condition.parquet", conditionRow(decoyCond, decoyTok, decoyTok+"1"))
	w.add("position.parquet", []any{decoyTok, decoyCond, "0"})
	w.add("market_data.parquet", marketDataRow(decoyTok, decoyCond))
	w.add("neg_risk_event.parquet", []any{decoyNR, "0", "2"})
	w.add("wallet.parquet", []any{decoy, decoy, "proxy", "0"})
	return w
}

func (w *world) add(rel string, rows ...[]any) {
	w.files[rel] = append(w.files[rel], rows...)
}

func (w *world) run() (*result, error) {
	w.t.Helper()
	r, err := w.prepare()
	if err != nil {
		return nil, err
	}
	return r, r.rerun()
}

func (w *world) prepare() (*result, error) {
	w.t.Helper()
	set := dstest.New(w.t)
	for rel, rows := range w.files {
		table, _, _ := strings.Cut(strings.TrimSuffix(rel, ".parquet"), "/")
		set.Write(rel, sourceCols[table], rows...)
	}
	db, err := dataset.Open(w.t.Context(), dataset.Config{ExtDir: w.t.TempDir()})
	if err != nil {
		w.t.Fatal(err)
	}
	w.t.Cleanup(func() { _ = db.Close() })
	src, err := dataset.NewSource(w.t.Context(), db, set.Root)
	if err != nil {
		w.t.Fatal(err)
	}
	return &result{t: w.t, src: src, pace: dataset.NewPacer(1e9), out: filepath.Join(w.t.TempDir(), "dataset")}, nil
}

func (w *world) extract() *result {
	w.t.Helper()
	r, err := w.run()
	if err != nil {
		w.t.Fatal(err)
	}
	return r
}

func (r *result) rerun() error {
	return Extract(r.t.Context(), r.src, r.pace, r.out)
}

func (r *result) values(rel, col string) []string {
	r.t.Helper()
	path := filepath.Join(r.out, filepath.FromSlash(rel))
	if _, err := os.Stat(path); err != nil {
		return nil
	}
	return query(r.t, r.src.DB, fmt.Sprintf("SELECT %s FROM read_parquet(?) ORDER BY 1", col), path)
}

func (r *result) columns(rel string) []string {
	r.t.Helper()
	path := filepath.Join(r.out, filepath.FromSlash(rel))
	return query(r.t, r.src.DB, "SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('"+path+"', hive_partitioning = false))")
}

func (r *result) files() []string {
	r.t.Helper()
	var rels []string
	err := filepath.WalkDir(r.out, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(r.out, path)
		rels = append(rels, filepath.ToSlash(rel))
		return err
	})
	if err != nil && !os.IsNotExist(err) {
		r.t.Fatal(err)
	}
	slices.Sort(rels)
	return rels
}

func query(t *testing.T, db *sql.DB, stmt string, args ...any) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), stmt, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var v sql.NullString
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		got = append(got, v.String)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return got
}

func expect(t *testing.T, got []string, want ...string) {
	t.Helper()
	slices.Sort(want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func fillRow(id, maker, makerAsset, takerAsset string) []any {
	return []any{id, "0xorder", "0xtx", "7", "1700000000", maker, exchange, makerAsset, takerAsset, "5000000", "2200000"}
}

func redemptionRow(id, redeemer, condition string) []any {
	return []any{id, "1700000000", redeemer, condition, []string{"1", "2"}, "9640000"}
}

func ctfRow(id, stakeholder, condition string) []any {
	return []any{id, "1700000000", stakeholder, condition, "5000000"}
}

func conversionRow(id, stakeholder, market string, questions int) []any {
	return []any{id, "1700000000", stakeholder, market, "5000000", "7", strconv.Itoa(questions)}
}

func feeRow(id, refundee, token string) []any {
	return []any{id, "0xorder", token, "1700000000", refundee, "10", "25"}
}

func conditionRow(id, token0, token1 string) []any {
	return []any{id, []string{token0, token1}, []string{"1", "0"}, "1"}
}

func marketDataRow(token, condition string) []any {
	return []any{token, "2025-05-01T00:00:00Z", condition, nil, "Will it?", "will-it", "text"}
}

func positionRows(condition, token0, token1 string) [][]any {
	return [][]any{{token0, condition, "0"}, {token1, condition, "1"}}
}

func condHex(tag byte) string {
	return "0x" + strings.Repeat(hex.EncodeToString([]byte{tag}), 32)
}

func monthFile(table string, ts uint32) string {
	return monthFileOf(table, monthOf(int64(ts)))
}

func monthOf(ts int64) time.Time {
	t := time.Unix(ts, 0).UTC()
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
}

func monthFileOf(table string, month time.Time) string {
	return fmt.Sprintf("%s/%s.parquet", table, month.Format("year=2006/month=01"))
}

func wallet(t *testing.T, name string, i int) Wallet {
	t.Helper()
	wallets, err := LoadWallets(name)
	if err != nil {
		t.Fatal(err)
	}
	return wallets[i]
}

func market(t *testing.T, name string, i int) Market {
	t.Helper()
	markets, err := LoadMarkets(name)
	if err != nil {
		t.Fatal(err)
	}
	return markets[i]
}

func TestExtractKeepsBothCaseForms(t *testing.T) {
	ranged := wallet(t, "wallets_14", 1)
	if ranged.Address == strings.ToLower(ranged.Address) {
		t.Fatalf("golden wallet %s is not mixed case", ranged.Address)
	}
	rel := monthFile("order_filled", ranged.FirstTS)
	w := newWorld(t)
	w.add(rel,
		fillRow("137_1_1", ranged.Address, "0", decoyTok),
		fillRow("137_1_2", strings.ToLower(ranged.Address), decoyTok, "0"),
		fillRow("137_1_3", decoy, "0", decoyTok),
	)
	expect(t, w.extract().values(rel, "id"), "137_1_1", "137_1_2")
}

func TestExtractSkipsMonthsOutsideRange(t *testing.T) {
	ranged := wallet(t, "wallets_14", 1)
	free := wallet(t, "tokens_user_position", 0)
	wide := wallet(t, "wallets_14", 13)
	if free.FirstTS != 0 || free.LastTS != 0 {
		t.Fatalf("wallet %s has a range", free.Address)
	}
	inside := monthFile("order_filled", ranged.FirstTS)
	if inside != monthFile("order_filled", ranged.LastTS) {
		t.Fatalf("range of %s spans two months", ranged.Address)
	}
	before, after := "order_filled/year=2024/month=06.parquet", "order_filled/year=2024/month=07.parquet"
	middle := "order_filled/year=2025/month=03.parquet"
	w := newWorld(t)
	w.add(inside, fillRow("137_1_1", ranged.Address, "0", decoyTok), fillRow("137_1_2", free.Address, "0", decoyTok))
	w.add(before, fillRow("137_2_1", ranged.Address, "0", decoyTok), fillRow("137_2_2", free.Address, "0", decoyTok))
	w.add(after, fillRow("137_3_1", ranged.Address, "0", decoyTok))
	w.add(middle, fillRow("137_4_1", wide.Address, "0", decoyTok), fillRow("137_4_2", ranged.Address, "0", decoyTok))
	r := w.extract()
	expect(t, r.values(inside, "id"), "137_1_1", "137_1_2")
	expect(t, r.values(before, "id"), "137_2_2")
	expect(t, r.values(middle, "id"), "137_4_1")
	if got := r.values(after, "id"); got != nil {
		t.Errorf("%s was written with %v", after, got)
	}
	if slices.Contains(r.files(), after) {
		t.Errorf("%s exists", after)
	}
}

func TestExtractIncludesMarketRows(t *testing.T) {
	m := market(t, "markets_volume_5", 0)
	trader := wallet(t, "wallets_14", 1)
	fills, redeems, both := "order_filled/year=2023/month=02.parquet", "redemption/year=2025/month=09.parquet", monthFile("order_filled", trader.FirstTS)
	w := newWorld(t)
	w.add(fills,
		fillRow("137_1_1", decoy, m.Tokens[0], "0"),
		fillRow("137_1_2", decoy, "0", m.Tokens[1]),
		fillRow("137_1_3", decoy, "0", decoyTok),
	)
	w.add(both, fillRow("137_3_1", trader.Address, m.Tokens[0], m.Tokens[1]))
	w.add(redeems, redemptionRow("137_2_1", decoy, m.Condition), redemptionRow("137_2_2", decoy, decoyCond))
	r := w.extract()
	expect(t, r.values(fills, "id"), "137_1_1", "137_1_2")
	expect(t, r.values(both, "id"), "137_3_1")
	expect(t, r.values(redeems, "id"), "137_2_1")
}

func TestExtractBoundsMarketWindow(t *testing.T) {
	m := market(t, "markets_may25_29", 0)
	first, last := monthOf(int64(m.ClosedTime)-8*day), monthOf(int64(m.ClosedTime)+day)
	w := newWorld(t)
	want := map[string]bool{}
	for i, month := 0, first.AddDate(0, -1, 0); !month.After(last.AddDate(0, 1, 0)); i, month = i+1, month.AddDate(0, 1, 0) {
		rel := monthFileOf("order_filled", month)
		w.add(rel, fillRow(fmt.Sprintf("137_%d_1", i+1), decoy, m.Tokens[0], "0"))
		want[rel] = !month.Before(first) && !month.After(last)
	}
	r := w.extract()
	for rel, inside := range want {
		if got := len(r.values(rel, "id")) == 1; got != inside {
			t.Errorf("%s extracted %v, want %v", rel, got, inside)
		}
	}
}

func TestWindowCoversMonths(t *testing.T) {
	feb, mar := monthSpan("order_filled/year=2025/month=02.parquet"), monthSpan("order_filled/year=2025/month=03.parquet")
	win := window{from: time.Date(2025, 2, 25, 0, 0, 0, 0, time.UTC).Unix(), to: time.Date(2025, 3, 2, 0, 0, 0, 0, time.UTC).Unix()}
	for _, tc := range []struct {
		name string
		file string
		want bool
	}{
		{"january", "order_filled/year=2025/month=01.parquet", false},
		{"february", "order_filled/year=2025/month=02.parquet", true},
		{"march", "order_filled/year=2025/month=03.parquet", true},
		{"april", "order_filled/year=2025/month=04.parquet", false},
		{"single file", "condition.parquet", true},
	} {
		if got := win.covers(monthSpan(tc.file)); got != tc.want {
			t.Errorf("%s covers = %v, want %v", tc.name, got, tc.want)
		}
	}
	if (window{}).covers(feb) != true || (window{}).union(win) != (window{}) || win.union(window{}) != (window{}) {
		t.Error("an empty window must cover every month and absorb unions")
	}
	if got, want := win.union(window{from: win.from - day, to: win.to + day}), (window{win.from - day, win.to + day}); got != want || !got.covers(mar) {
		t.Errorf("union = %+v, want %+v", got, want)
	}
}

func TestExtractIncludesDerivedSiblings(t *testing.T) {
	checkSiblings(t, 3)
}

func TestExtractMatchesManyKeys(t *testing.T) {
	checkSiblings(t, maxPoints+22)
}

func checkSiblings(t *testing.T, questions int) {
	t.Helper()
	conv := wallet(t, "wallets_conversion", 1)
	var market [32]byte
	if _, err := hex.Decode(market[:], []byte(marketNR[2:])); err != nil {
		t.Fatal(err)
	}
	derived := func(q int) string {
		c := refs.ConditionID(refs.NegRiskAdapter, refs.QuestionID(market, uint8(q)))
		return "0x" + hex.EncodeToString(c[:])
	}
	w := newWorld(t)
	w.add("neg_risk_conversion/year=2025/month=05.parquet", conversionRow("137_1_1", conv.Address, marketNR, questions))
	var wantCond, wantTok, wantData []string
	for q := 0; q <= questions; q++ {
		cond, t0, t1 := derived(q), fmt.Sprintf("5%03d0", q), fmt.Sprintf("5%03d1", q)
		w.add("condition.parquet", conditionRow(cond, t0, t1))
		w.add("position.parquet", positionRows(cond, t0, t1)...)
		w.add("market_data.parquet", marketDataRow(t0, cond), marketDataRow(t1, cond))
		if q < questions {
			wantCond = append(wantCond, cond)
			wantTok = append(wantTok, t0, t1)
			wantData = append(wantData, cond, cond)
		}
	}
	w.add("neg_risk_event.parquet", []any{marketNR, "0", strconv.Itoa(questions)})
	r := w.extract()
	expect(t, r.values("condition.parquet", "id"), wantCond...)
	expect(t, r.values("position.parquet", "id"), wantTok...)
	expect(t, r.values("market_data.parquet", "condition"), wantData...)
	expect(t, r.values("neg_risk_event.parquet", "id"), marketNR)
}

func TestExtractIncludesStateRows(t *testing.T) {
	trader := wallet(t, "wallets_14", 1)
	fills := monthFile("order_filled", trader.FirstTS)
	w := newWorld(t)
	w.add(fills, fillRow("137_1_1", trader.Address, "111", "0"))
	w.add("split/year=2024/month=01.parquet", ctfRow("137_2_1", trader.Address, condHex(0xb1)))
	w.add("redemption/year=2024/month=02.parquet", redemptionRow("137_3_1", strings.ToLower(trader.Address), condHex(0xc1)))
	for _, c := range []struct {
		cond   string
		t0, t1 string
	}{{condHex(0xa1), "111", "112"}, {condHex(0xb1), "121", "122"}, {condHex(0xc1), "131", "132"}} {
		w.add("condition.parquet", conditionRow(c.cond, c.t0, c.t1))
		w.add("position.parquet", positionRows(c.cond, c.t0, c.t1)...)
		w.add("market_data.parquet", marketDataRow(c.t0, c.cond))
	}
	w.add("wallet.parquet", []any{trader.Address, decoy, "safe", "0"}, []any{strings.ToLower(trader.Address), decoy, "safe", "0"})
	r := w.extract()
	expect(t, r.values("condition.parquet", "id"), condHex(0xa1), condHex(0xb1), condHex(0xc1))
	expect(t, r.values("position.parquet", "id"), "111", "112", "121", "122", "131", "132")
	expect(t, r.values("market_data.parquet", "condition"), condHex(0xa1), condHex(0xb1), condHex(0xc1))
	expect(t, r.values("wallet.parquet", "id"), trader.Address, strings.ToLower(trader.Address))
}

func TestExtractLayout(t *testing.T) {
	trader := wallet(t, "wallets_14", 1)
	conv := wallet(t, "wallets_conversion", 1)
	fee := wallet(t, "wallets_fee_era", 0)
	token := wallet(t, "tokens_user_position", 0)
	w := newWorld(t)
	want := map[string]string{
		monthFile("order_filled", trader.FirstTS):        "order_filled",
		"redemption/year=2025/month=03.parquet":          "redemption",
		"split/year=2025/month=03.parquet":               "split",
		"merge/year=2025/month=03.parquet":               "merge",
		"neg_risk_conversion/year=2025/month=03.parquet": "neg_risk_conversion",
		monthFile("fee_refunded", fee.FirstTS):           "fee_refunded",
		"fpmm_transaction/year=2022/month=06.parquet":    "fpmm_transaction",
		"user_position.parquet":                          "user_position",
		"condition.parquet":                              "condition",
		"position.parquet":                               "position",
		"market_data.parquet":                            "market_data",
		"neg_risk_event.parquet":                         "neg_risk_event",
		"wallet.parquet":                                 "wallet",
	}
	w.add(monthFile("order_filled", trader.FirstTS), fillRow("137_1_1", trader.Address, "111", "0"))
	w.add("redemption/year=2025/month=03.parquet", redemptionRow("137_1_2", trader.Address, condHex(0xa1)))
	w.add("split/year=2025/month=03.parquet", ctfRow("137_1_3", trader.Address, condHex(0xa1)))
	w.add("merge/year=2025/month=03.parquet", ctfRow("137_1_4", trader.Address, condHex(0xa1)))
	w.add("neg_risk_conversion/year=2025/month=03.parquet", conversionRow("137_1_5", conv.Address, marketNR, 1))
	w.add(monthFile("fee_refunded", fee.FirstTS), feeRow("137_1_6", fee.Address, "111"))
	w.add("fpmm_transaction/year=2022/month=06.parquet", []any{"137_1_7", "Buy", "1", "0xpool", trader.Address, "1", "0", "0", "1"})
	w.add("user_position.parquet", []any{token.Address + "-111", token.Address, "111", "1", "1", "1", "1"})
	w.add("condition.parquet", conditionRow(condHex(0xa1), "111", "112"))
	w.add("position.parquet", positionRows(condHex(0xa1), "111", "112")...)
	w.add("market_data.parquet", marketDataRow("111", condHex(0xa1)))
	w.add("neg_risk_event.parquet", []any{marketNR, "0", "1"})
	w.add("wallet.parquet", []any{trader.Address, decoy, "proxy", "0"})
	r := w.extract()

	got := r.files()
	var wantFiles []string
	for rel := range want {
		wantFiles = append(wantFiles, rel)
	}
	slices.Sort(wantFiles)
	if !reflect.DeepEqual(got, wantFiles) {
		t.Fatalf("files %v, want %v", got, wantFiles)
	}
	for rel, table := range want {
		if cols := r.columns(rel); !reflect.DeepEqual(cols, outputCols[table]) {
			t.Errorf("%s has columns %v, want %v", rel, cols, outputCols[table])
		}
	}
}

func TestExtractKeepsRepeatedRows(t *testing.T) {
	trader := wallet(t, "wallets_14", 1)
	rel := "fpmm_transaction/year=2022/month=06.parquet"
	w := newWorld(t)
	w.add(rel,
		[]any{"137_1_1", "Buy", "1", "0xpool", trader.Address, "1", "0", "0", "1"},
		[]any{"137_1_2", "Sell", "2", "0xpool", trader.Address, "1", "0", "0", "1"},
	)
	expect(t, w.extract().values(rel, `"user"`), trader.Address, trader.Address)
}

func TestExtractWritesSchemaForEmptyTable(t *testing.T) {
	trader := wallet(t, "wallets_14", 1)
	rel := monthFile("order_filled", trader.FirstTS)
	w := newWorld(t)
	w.add(rel, fillRow("137_1_1", trader.Address, "0", decoyTok))
	r := w.extract()
	want := "fpmm_transaction/year=2022/month=06.parquet"
	if !slices.Contains(r.files(), want) {
		t.Fatalf("%s is missing from %v", want, r.files())
	}
	expect(t, r.values(want, `"user"`))
	expect(t, r.columns(want), "user")
	if slices.Contains(r.files(), "order_filled/year=2025/month=01.parquet") {
		t.Error("a table with matching rows got a placeholder for a file without any")
	}
}

func TestExtractRerunKeepsFiles(t *testing.T) {
	trader := wallet(t, "wallets_14", 1)
	w := newWorld(t)
	w.add(monthFile("order_filled", trader.FirstTS), fillRow("137_1_1", trader.Address, "0", decoyTok))
	w.add("wallet.parquet", []any{trader.Address, decoy, "proxy", "0"})
	r := w.extract()
	const rel = "wallet.parquet"
	path := filepath.Join(r.out, rel)
	if err := os.WriteFile(path, []byte("sentinel"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := r.rerun(); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "sentinel" {
		t.Fatalf("rerun rewrote %s: %q %v", rel, data, err)
	}
	for _, f := range r.files() {
		if strings.HasSuffix(f, ".tmp") {
			t.Errorf("left %s behind", f)
		}
	}
}

func TestExtractReplacesStaleTmp(t *testing.T) {
	trader := wallet(t, "wallets_14", 1)
	rel := monthFile("order_filled", trader.FirstTS)
	w := newWorld(t)
	w.add(rel, fillRow("137_1_1", trader.Address, "0", decoyTok))
	r, err := w.prepare()
	if err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(r.out, filepath.FromSlash(rel)) + ".tmp"
	if err := os.MkdirAll(filepath.Dir(stale), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stale, []byte("half written"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := r.rerun(); err != nil {
		t.Fatal(err)
	}
	expect(t, r.values(rel, "id"), "137_1_1")
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("%s still exists: %v", stale, err)
	}
}

func TestExtractFailsOnMissingTable(t *testing.T) {
	w := newWorld(t)
	delete(w.files, "condition.parquet")
	_, err := w.run()
	if err == nil || !strings.Contains(err.Error(), "condition") {
		t.Fatalf("want an error naming condition, got %v", err)
	}
}
