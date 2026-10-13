package refs_test

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"math/rand/v2"
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
	adapterHex = "d91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
	marketHex  = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcd00"
)

func decode32(t testing.TB, s string) [32]byte {
	t.Helper()
	b, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
	if err != nil || len(b) != 32 {
		t.Fatalf("bad 32 byte hex %q", s)
	}
	return [32]byte(b)
}

func hex0x(b [32]byte) string {
	return "0x" + hex.EncodeToString(b[:])
}

func TestConditionIDVector(t *testing.T) {
	adapter, err := hex.DecodeString(adapterHex)
	if err != nil || !bytes.Equal(adapter, refs.NegRiskAdapter[:]) {
		t.Fatalf("NegRiskAdapter is %x, want %s", refs.NegRiskAdapter, adapterHex)
	}
	market := decode32(t, marketHex)
	for _, tc := range []struct {
		q                           uint8
		wantQuestion, wantCondition string
	}{
		{
			3, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcd03",
			"473dd35df1975d3bd28d6105709a1f24fc5039ed818300a82a79ed747be91575",
		},
		{0, marketHex, "eb2b6c0117f61ebf5cff3f9bcf6e324ae273684e4ab9e4b387eaf4423cece2f3"},
	} {
		question := refs.QuestionID(market, tc.q)
		if got := hex.EncodeToString(question[:]); got != tc.wantQuestion {
			t.Fatalf("q=%d question %s, want %s", tc.q, got, tc.wantQuestion)
		}
		condition := refs.ConditionID(refs.NegRiskAdapter, question)
		if got := hex.EncodeToString(condition[:]); got != tc.wantCondition {
			t.Fatalf("q=%d condition %s, want %s", tc.q, got, tc.wantCondition)
		}
	}
}

func TestKindOf(t *testing.T) {
	for _, tc := range []struct {
		question string
		want     refs.Kind
	}{
		{"Bitcoin Up or Down - May 3, 5PM ET", refs.KindUpDown},
		{"ETH UP OR DOWN 5m", refs.KindUpDown},
		{"", refs.KindUnknown},
		{"Will Spain win La Liga?", refs.KindOther},
	} {
		if got := refs.KindOf(tc.question); got != tc.want {
			t.Errorf("KindOf(%q) = %d, want %d", tc.question, got, tc.want)
		}
	}
}

func TestBucketKeepsEventTogether(t *testing.T) {
	market := decode32(t, marketHex)
	want := refs.Bucket(market, 64)
	for q := range uint8(5) {
		if got := refs.Bucket(refs.QuestionID(market, q), 64); got != want {
			t.Fatalf("q=%d bucket %d, want %d", q, got, want)
		}
	}
	rng := rand.New(rand.NewPCG(1, 2))
	var seen [64]bool
	for range 1000 {
		var key [32]byte
		for i := range key {
			key[i] = byte(rng.UintN(256))
		}
		b := refs.Bucket(key, 64)
		if b >= 64 {
			t.Fatalf("bucket %d out of range", b)
		}
		seen[b] = true
	}
	for b, ok := range seen {
		if !ok {
			t.Errorf("bucket %d never used", b)
		}
	}
}

type fixture struct {
	set                  *dstest.Set
	negMarket, emptyKey  [32]byte
	nr0, nr1             [32]byte
	plainA, plainB       string
	plainC, plainD       string
	tokens               map[string][2]string
	registryAddr, signer string
}

func plain(b byte) string {
	return "0x" + strings.Repeat(hex.EncodeToString([]byte{b}), 32)
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{
		set:       dstest.New(t),
		negMarket: decode32(t, marketHex),
		emptyKey:  decode32(t, strings.Repeat("ee", 31)+"00"),
		plainA:    plain(0x11),
		plainB:    plain(0x22),
		plainC:    plain(0x33),
		plainD:    plain(0x44),
		tokens:    map[string][2]string{},
	}
	f.nr0 = refs.ConditionID(refs.NegRiskAdapter, refs.QuestionID(f.negMarket, 0))
	f.nr1 = refs.ConditionID(refs.NegRiskAdapter, refs.QuestionID(f.negMarket, 1))
	ids := []string{hex0x(f.nr0), hex0x(f.nr1), f.plainA, f.plainB, f.plainC, f.plainD}
	for i, id := range ids {
		f.tokens[id] = [2]string{strconv.Itoa(1000 + 2*i), strconv.Itoa(1001 + 2*i)}
	}
	big := "1000000000000000000"
	var conditions, positions [][]any
	payouts := map[string][]any{
		hex0x(f.nr0): {[]string{"0", "1"}, "1"},
		hex0x(f.nr1): {[]string{}, "0"},
		f.plainA:     {[]string{"1", "0"}, "1"},
		f.plainB:     {[]string{big, "0"}, big},
		f.plainC:     {[]string{"500000", "500000"}, "1000000"},
		f.plainD:     {[]string{}, "0"},
	}
	for _, id := range ids {
		tok := f.tokens[id]
		conditions = append(conditions, []any{id, []string{tok[0], tok[1]}, payouts[id][0], payouts[id][1]})
		positions = append(positions, []any{tok[0], id, "0"}, []any{tok[1], id, "1"})
	}
	f.set.Write("condition.parquet", []string{"id", "positionIds", "payoutNumerators", "payoutDenominator"}, conditions...)
	f.set.Write("position.parquet", []string{"id", "condition", "outcomeIndex"}, positions...)
	f.set.Write("market_data.parquet", []string{"id", "endDate", "condition", "marketName", "marketSlug"},
		[]any{f.tokens[f.plainA][0], "2024-09-10T00:00:00Z", f.plainA, "Bitcoin Up or Down - May 3, 5PM ET", "btc-up-or-down"},
		[]any{f.tokens[f.plainA][1], "2024-09-10T00:00:00Z", f.plainA, "Bitcoin Up or Down - May 3, 5PM ET", "btc-up-or-down"},
		[]any{f.tokens[hex0x(f.nr0)][0], "2025-01-02T12:30:00Z", hex0x(f.nr0), "Will Spain win La Liga?", "spain-la-liga"},
		[]any{f.tokens[hex0x(f.nr0)][1], "2025-01-02T12:30:00Z", hex0x(f.nr0), "Will Spain win La Liga?", "spain-la-liga"},
		[]any{f.tokens[f.plainC][0], "not a date", f.plainC, "ETH UP OR DOWN 5m", "eth-5m"},
		[]any{f.tokens[f.plainC][1], nil, f.plainC, "ETH UP OR DOWN 5m", "eth-5m"},
	)
	f.set.Write("neg_risk_event.parquet", []string{"id", "feeBps", "questionCount"},
		[]any{hex0x(f.negMarket), "0", "2"},
		[]any{hex0x(f.emptyKey), "0", "3"},
	)
	f.registryAddr = "0xAbCdEf0123456789aBcDeF0123456789AbCdEf01"
	f.signer = "0x0123456789ABCDEF0123456789abcdef01234567"
	f.set.Write("wallet.parquet", []string{"id", "signer", "type"},
		[]any{f.registryAddr, f.signer, "proxy"},
		[]any{"0x1111111111111111111111111111111111111111", "0x2222222222222222222222222222222222222222", "safe"},
		[]any{strings.ToLower(f.registryAddr), f.signer, "proxy"},
	)
	f.set.Write("fpmm_transaction/year=2022/month=06.parquet", []string{"id", "type", "user"},
		[]any{"137_1_1", "Buy", "0xAAaaAAaaAAaaAAaaAAaaAAaaAAaaAAaaAAaaAAaa"},
		[]any{"137_2_1", "Sell", "0xaaAAaaAAaaAAaaAAaaAAaaAAaaAAaaAAaaAAaaAA"},
	)
	return f
}

func (f *fixture) build(t *testing.T) *sql.DB {
	t.Helper()
	db, err := dataset.Open(t.Context(), dataset.Config{ExtDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	src, err := dataset.NewSource(t.Context(), db, f.set.Root)
	if err != nil {
		t.Fatal(err)
	}
	if err := refs.Build(t.Context(), src, db); err != nil {
		t.Fatal(err)
	}
	return db
}

func count(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(t.Context(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

type marketRow struct {
	id                    uint32
	condition             []byte
	question, slug        sql.Null[string]
	endDate               sql.Null[time.Time]
	kind                  uint8
	negRisk, resolved     bool
	ek                    uint32
	bucket                uint8
	payout0, payout1, den sql.Null[uint64]
}

func readMarkets(t *testing.T, db *sql.DB) []marketRow {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `
		SELECT market_id, condition, question, slug, end_date, kind, neg_risk, ek, bucket, resolved, payout0, payout1, payout_den
		FROM market ORDER BY market_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []marketRow
	for rows.Next() {
		var m marketRow
		if err := rows.Scan(&m.id, &m.condition, &m.question, &m.slug, &m.endDate, &m.kind, &m.negRisk, &m.ek,
			&m.bucket, &m.resolved, &m.payout0, &m.payout1, &m.den); err != nil {
			t.Fatal(err)
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestBuildMarkets(t *testing.T) {
	f := newFixture(t)
	db := f.build(t)
	markets := readMarkets(t, db)
	if len(markets) != 6 {
		t.Fatalf("got %d markets, want 6", len(markets))
	}
	byCond := map[string]marketRow{}
	for i, m := range markets {
		if m.id != uint32(i) {
			t.Fatalf("market_id %d at position %d", m.id, i)
		}
		if i > 0 && bytes.Compare(markets[i-1].condition, m.condition) >= 0 {
			t.Fatalf("market_id order differs from condition byte order at %d", i)
		}
		byCond["0x"+hex.EncodeToString(m.condition)] = m
		if want := refs.KindOf(m.question.V); refs.Kind(m.kind) != want {
			t.Errorf("market %d kind %d, want %d for %q", m.id, m.kind, want, m.question.V)
		}
	}
	nr0, nr1 := byCond[hex0x(f.nr0)], byCond[hex0x(f.nr1)]
	a, b, c, d := byCond[f.plainA], byCond[f.plainB], byCond[f.plainC], byCond[f.plainD]

	if !nr0.negRisk || !nr1.negRisk || nr0.ek != nr1.ek || nr0.bucket != nr1.bucket {
		t.Errorf("neg-risk markets %+v and %+v must share one event", nr0, nr1)
	}
	for _, m := range []marketRow{a, b, c, d} {
		if m.negRisk || m.ek == nr0.ek {
			t.Errorf("plain market %d must be its own event", m.id)
		}
	}
	if count(t, db, "SELECT count(DISTINCT ek) FROM market") != 5 {
		t.Error("want 5 distinct event codes over 6 markets")
	}
	var ekKey []byte
	var qc uint32
	if err := db.QueryRowContext(t.Context(), "SELECT key, question_count FROM ek WHERE code = ?", a.ek).Scan(&ekKey, &qc); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ekKey, a.condition) || qc != 1 {
		t.Errorf("plain event key %x count %d, want its condition and 1", ekKey, qc)
	}
	if err := db.QueryRowContext(t.Context(), "SELECT key, question_count FROM ek WHERE code = ?", nr0.ek).Scan(&ekKey, &qc); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ekKey, f.negMarket[:]) || qc != 2 {
		t.Errorf("neg-risk event key %x count %d, want the market id and 2", ekKey, qc)
	}
	if count(t, db, "SELECT count(*) FROM ek") != 6 {
		t.Error("ek must hold 4 plain events and both neg-risk events, even one with no condition rows")
	}

	type payout struct{ p0, p1, den uint64 }
	for _, tc := range []struct {
		name string
		m    marketRow
		want payout
	}{
		{"a", a, payout{1, 0, 1}},
		{"b", b, payout{1_000_000_000_000_000_000, 0, 1_000_000_000_000_000_000}},
		{"c", c, payout{500_000, 500_000, 1_000_000}},
		{"neg-risk 0", nr0, payout{0, 1, 1}},
	} {
		got := payout{tc.m.payout0.V, tc.m.payout1.V, tc.m.den.V}
		if !tc.m.resolved || got != tc.want {
			t.Errorf("%s resolved=%v payouts %+v, want %+v", tc.name, tc.m.resolved, got, tc.want)
		}
	}
	for name, m := range map[string]marketRow{"d": d, "neg-risk 1": nr1} {
		if m.resolved || m.payout0.Valid || m.payout1.Valid || m.den.Valid {
			t.Errorf("%s must be unresolved with no payouts, got %+v", name, m)
		}
	}

	ppm := func(cond string) (int64, int64) {
		var p0, p1 int64
		err := db.QueryRowContext(t.Context(), `
			SELECT CAST(CAST(payout0 AS HUGEINT) * 1000000 // payout_den AS BIGINT),
			       CAST(CAST(payout1 AS HUGEINT) * 1000000 // payout_den AS BIGINT)
			FROM market WHERE condition = unhex(substr(?, 3))`, cond).Scan(&p0, &p1)
		if err != nil {
			t.Fatal(err)
		}
		return p0, p1
	}
	if p0, p1 := ppm(f.plainB); p0 != 1_000_000 || p1 != 0 {
		t.Errorf("second resolved market pays %d and %d ppm", p0, p1)
	}
	if p0, p1 := ppm(f.plainC); p0 != 500_000 || p1 != 500_000 {
		t.Errorf("third resolved market pays %d and %d ppm", p0, p1)
	}

	if a.question.V != "Bitcoin Up or Down - May 3, 5PM ET" || a.slug.V != "btc-up-or-down" || refs.Kind(a.kind) != refs.KindUpDown {
		t.Errorf("market a text %+v", a)
	}
	if d.question.Valid || d.slug.Valid || refs.Kind(d.kind) != refs.KindUnknown {
		t.Errorf("market d must have no text and an unknown kind, got %+v", d)
	}
	if refs.Kind(nr0.kind) != refs.KindOther || refs.Kind(c.kind) != refs.KindUpDown {
		t.Errorf("kinds nr0=%d c=%d", nr0.kind, c.kind)
	}
	if !a.endDate.Valid || !a.endDate.V.Equal(time.Date(2024, 9, 10, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("end date of a is %v", a.endDate)
	}
	if !nr0.endDate.Valid || !nr0.endDate.V.Equal(time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("end date of nr0 is %v", nr0.endDate)
	}
	if c.endDate.Valid || d.endDate.Valid {
		t.Error("an unparseable or missing end date must be NULL")
	}

	rows, err := db.QueryContext(t.Context(), "SELECT key, bucket FROM ek")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var key []byte
		var bucket uint8
		if err := rows.Scan(&key, &bucket); err != nil {
			t.Fatal(err)
		}
		if want := refs.Bucket([32]byte(key), 64); bucket != want {
			t.Errorf("ek %x bucket %d, want %d", key, bucket, want)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	checkTokens(t, db, f, byCond)
	checkNegRiskQuestions(t, db, nr0, nr1)
	checkRegistry(t, db, f)
}

func checkTokens(t *testing.T, db *sql.DB, f *fixture, byCond map[string]marketRow) {
	t.Helper()
	type tokRow struct {
		code, market uint32
		token        string
		outcome      uint8
	}
	var want []tokRow
	for cond, toks := range f.tokens {
		for outcome, token := range toks {
			want = append(want, tokRow{market: byCond[cond].id, token: token, outcome: uint8(outcome)})
		}
	}
	slices.SortFunc(want, func(x, y tokRow) int { return strings.Compare(x.token, y.token) })
	for i := range want {
		want[i].code = uint32(i)
	}
	rows, err := db.QueryContext(t.Context(), "SELECT code, token, market_id, outcome FROM tok ORDER BY code")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []tokRow
	for rows.Next() {
		var r tokRow
		if err := rows.Scan(&r.code, &r.token, &r.market, &r.outcome); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		t.Errorf("tok\n got %v\nwant %v", got, want)
	}
}

func checkNegRiskQuestions(t *testing.T, db *sql.DB, nr0, nr1 marketRow) {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "SELECT ek, q, market_id FROM nr_question ORDER BY ek, q")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type qRow struct {
		ek, market uint32
		q          uint8
	}
	var got []qRow
	for rows.Next() {
		var r qRow
		if err := rows.Scan(&r.ek, &r.q, &r.market); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []qRow{{ek: nr0.ek, q: 0, market: nr0.id}, {ek: nr0.ek, q: 1, market: nr1.id}}
	if !slices.Equal(got, want) {
		t.Errorf("nr_question got %v, want %v", got, want)
	}
}

func checkRegistry(t *testing.T, db *sql.DB, f *fixture) {
	t.Helper()
	if n := count(t, db, "SELECT count(*) FROM wallet_reg"); n != 2 {
		t.Errorf("wallet_reg has %d rows, want 2 (the same address in two cases is one wallet)", n)
	}
	var owner []byte
	var kind uint8
	err := db.QueryRowContext(t.Context(), "SELECT owner, owner_kind FROM wallet_reg WHERE address = unhex(?)",
		strings.ToLower(strings.TrimPrefix(f.registryAddr, "0x"))).Scan(&owner, &kind)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(owner) != strings.ToLower(strings.TrimPrefix(f.signer, "0x")) || kind != 1 {
		t.Errorf("owner %x kind %d", owner, kind)
	}
	if k := count(t, db, "SELECT owner_kind FROM wallet_reg WHERE address = unhex('1111111111111111111111111111111111111111')"); k != 2 {
		t.Errorf("safe wallet kind %d, want 2", k)
	}
	var amm []byte
	var n uint32
	if err := db.QueryRowContext(t.Context(), "SELECT address, n FROM amm").Scan(&amm, &n); err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(amm) != strings.Repeat("aa", 20) || n != 2 {
		t.Errorf("amm %x count %d, want 20 bytes of aa and 2", amm, n)
	}
}

func TestBuildRerun(t *testing.T) {
	f := newFixture(t)
	db := f.build(t)
	src, err := dataset.NewSource(t.Context(), db, f.set.Root)
	if err != nil {
		t.Fatal(err)
	}
	if err := refs.Build(t.Context(), src, db); err != nil {
		t.Fatal(err)
	}
	for table, want := range map[string]int{"market": 6, "tok": 12, "ek": 6, "nr_question": 2, "wallet_reg": 2, "amm": 1} {
		if got := count(t, db, "SELECT count(*) FROM "+table); got != want {
			t.Errorf("%s has %d rows after a rerun, want %d", table, got, want)
		}
	}
	if got := count(t, db, "SELECT count(*) FROM duckdb_tables() WHERE temporary"); got != 0 {
		t.Errorf("%d temp tables left on the pooled connection after Build, want 0", got)
	}
}

func TestBuildNeedsEveryTable(t *testing.T) {
	set := dstest.New(t)
	set.Write("condition.parquet", []string{"id", "positionIds", "payoutNumerators", "payoutDenominator"},
		[]any{plain(0x11), []string{"1", "2"}, []string{}, "0"})
	db, err := dataset.Open(t.Context(), dataset.Config{ExtDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	src, err := dataset.NewSource(t.Context(), db, set.Root)
	if err != nil {
		t.Fatal(err)
	}
	err = refs.Build(t.Context(), src, db)
	if err == nil || !strings.Contains(err.Error(), "position") {
		t.Fatalf("want an error naming the missing table position, got %v", err)
	}
}

func TestBuildRejectsWideQuestionCount(t *testing.T) {
	f := newFixture(t)
	f.set.Write("neg_risk_event.parquet", []string{"id", "feeBps", "questionCount"},
		[]any{hex0x(f.negMarket), "0", "257"})
	db, err := dataset.Open(t.Context(), dataset.Config{ExtDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	src, err := dataset.NewSource(t.Context(), db, f.set.Root)
	if err != nil {
		t.Fatal(err)
	}
	err = refs.Build(t.Context(), src, db)
	if err == nil || !strings.Contains(err.Error(), "257") {
		t.Fatalf("want an error naming questionCount 257, got %v", err)
	}
}
