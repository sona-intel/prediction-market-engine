package golden

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"pm/internal/dataset"
	"pm/internal/refs"
)

const (
	day          = 86400
	maxQuestions = 256
	maxPoints    = 128
	allMonths    = "markets_volume_5"
)

var (
	walletFiles = []string{"wallets_14", "wallets_conversion", "wallets_fee_era", "wallets_cutoff", "tokens_user_position"}
	marketFiles = []string{"markets_volume_5", "markets_may25_29", "prices_12"}
	monthRe     = regexp.MustCompile(`/year=(\d{4})/month=(\d{2})\.parquet$`)
)

type set map[string]struct{}

func (s set) add(vals ...string) {
	for _, v := range vals {
		s[v] = struct{}{}
	}
}

func (s set) sorted() []string {
	return slices.Sorted(maps.Keys(s))
}

type span struct {
	lo, hi int64
	month  bool
}

type window struct{ from, to int64 }

func (w window) covers(s span) bool {
	return !s.month || w == window{} || w.from < s.hi && w.to >= s.lo
}

func (w window) union(o window) window {
	if w == (window{}) || o == (window{}) {
		return window{}
	}
	return window{min(w.from, o.from), max(w.to, o.to)}
}

type owner struct {
	window
	key        string
	forms      []string
	fee, token bool
}

type pool struct {
	window
	condition string
	tokens    []string
}

type targets struct {
	owners []owner
	pools  []pool
}

type touched struct {
	tokens, conditions, events, wallets set
}

type values func(t *targets, s span) []string

type branch struct {
	col    string
	values values
}

type scan struct {
	table    string
	cols     []string
	ranged   bool
	branches []branch
}

type lookup struct {
	table string
	cols  []string
	key   string
	pick  func(*touched) set
}

type extractor struct {
	src     *dataset.Source
	pace    *dataset.Pacer
	out     string
	t       *targets
	written int
	gets    int
	bytes   int64
}

func addresses(keep func(owner) bool) values {
	return func(t *targets, s span) []string {
		var out []string
		for _, o := range t.owners {
			if keep(o) && o.covers(s) {
				out = append(out, o.forms...)
			}
		}
		return out
	}
}

func everyone(owner) bool     { return true }
func feePayer(o owner) bool   { return o.fee }
func tokenLevel(o owner) bool { return o.token }

func tokens(t *targets, s span) []string {
	var out []string
	for _, p := range t.pools {
		if p.covers(s) {
			out = append(out, p.tokens...)
		}
	}
	return out
}

func conditions(t *targets, _ span) []string {
	out := make([]string, len(t.pools))
	for i, p := range t.pools {
		out[i] = p.condition
	}
	return out
}

var (
	fillCols = []string{"id", "timestamp", "maker", "taker", "makerAssetId", "takerAssetId", "makerAmountFilled", "takerAmountFilled"}
	ctfCols  = []string{"id", "timestamp", "stakeholder", "condition", "amount"}

	scans = []scan{
		{"order_filled", fillCols, true, []branch{{"maker", addresses(everyone)}, {"makerAssetId", tokens}, {"takerAssetId", tokens}}},
		{
			"redemption",
			[]string{"id", "redeemer", "condition", "timestamp"},
			false,
			[]branch{{"redeemer", addresses(everyone)}, {"condition", conditions}},
		},
		{"split", ctfCols, false, []branch{{"stakeholder", addresses(everyone)}}},
		{"merge", ctfCols, false, []branch{{"stakeholder", addresses(everyone)}}},
		{
			"neg_risk_conversion",
			[]string{"id", "timestamp", "stakeholder", "negRiskMarketId", "amount", "indexSet", "questionCount"},
			false,
			[]branch{{"stakeholder", addresses(everyone)}},
		},
		{"fee_refunded", []string{"id", "refundee", "tokenId", "feeCharged"}, true, []branch{{"refundee", addresses(feePayer)}}},
		{"fpmm_transaction", []string{`"user"`}, false, []branch{{`"user"`, addresses(everyone)}}},
		{
			"user_position",
			[]string{`"user"`, "tokenId", "amount", "avgPrice", "realizedPnl", "totalBought"},
			false,
			[]branch{{`"user"`, addresses(tokenLevel)}},
		},
	}

	lookups = []lookup{
		{"position", []string{"id", "condition", "outcomeIndex"}, "condition", func(t *touched) set { return t.conditions }},
		{"condition", []string{"id", "positionIds", "payoutNumerators", "payoutDenominator"}, "id", func(t *touched) set { return t.conditions }},
		{"market_data", []string{"condition", "marketName", "marketSlug", "endDate"}, "condition", func(t *touched) set { return t.conditions }},
		{"neg_risk_event", []string{"id", "questionCount"}, "id", func(t *touched) set { return t.events }},
		{"wallet", []string{"id", "signer", "type"}, "id", func(t *touched) set { return t.wallets }},
	}
)

func Extract(ctx context.Context, src *dataset.Source, pace *dataset.Pacer, out string) error {
	t, err := loadTargets()
	if err != nil {
		return err
	}
	for _, table := range requiredTables() {
		if len(src.Files(table)) == 0 {
			return fmt.Errorf("golden: no files for table %s", table)
		}
	}
	e := &extractor{src: src, pace: pace, out: out, t: t}
	start := time.Now()
	for _, sc := range scans {
		files := src.Files(sc.table)
		for _, file := range files {
			if err := e.scan(ctx, sc, file); err != nil {
				return err
			}
		}
		if len(e.local(sc.table)) == 0 {
			// refs.Build refuses a table without files
			if err := e.placeholder(ctx, sc, files[0]); err != nil {
				return err
			}
		}
	}
	if err := e.references(ctx); err != nil {
		return err
	}
	slog.Info("fixtures extracted", "files", e.written, "requests", e.gets, "bytes", e.bytes, "secs", int(time.Since(start).Seconds()))
	return nil
}

func requiredTables() []string {
	var tables []string
	for _, sc := range scans {
		tables = append(tables, sc.table)
	}
	for _, lk := range lookups {
		tables = append(tables, lk.table)
	}
	return tables
}

func loadTargets() (*targets, error) {
	t := &targets{}
	for _, name := range walletFiles {
		wallets, err := LoadWallets(name)
		if err != nil {
			return nil, err
		}
		for _, w := range wallets {
			t.addOwner(w)
		}
	}
	for _, name := range marketFiles {
		markets, err := LoadMarkets(name)
		if err != nil {
			return nil, err
		}
		for _, m := range markets {
			t.addPool(m, name == allMonths)
		}
	}
	return t, nil
}

func (t *targets) addOwner(w Wallet) {
	key := strings.ToLower(w.Address)
	win := window{int64(w.FirstTS), int64(w.LastTS)}
	if i := slices.IndexFunc(t.owners, func(o owner) bool { return o.key == key }); i >= 0 {
		o := &t.owners[i]
		o.window = o.union(win)
		o.fee = o.fee || w.FeesPaid > 0
		o.token = o.token || w.Tokens > 0
		if !slices.Contains(o.forms, w.Address) {
			o.forms = append(o.forms, w.Address)
		}
		return
	}
	forms := []string{key}
	if w.Address != key {
		forms = append(forms, w.Address)
	}
	t.owners = append(t.owners, owner{window: win, key: key, forms: forms, fee: w.FeesPaid > 0, token: w.Tokens > 0})
}

func (t *targets) addPool(m Market, everyMonth bool) {
	var win window
	if !everyMonth && m.ClosedTime != 0 {
		win = window{int64(m.ClosedTime) - 8*day, int64(m.ClosedTime) + day}
	}
	var toks []string
	for _, tok := range []string{m.Tokens[0], m.Tokens[1], m.Token0} {
		if tok != "" && !slices.Contains(toks, tok) {
			toks = append(toks, tok)
		}
	}
	if i := slices.IndexFunc(t.pools, func(p pool) bool { return p.condition == m.Condition }); i >= 0 {
		p := &t.pools[i]
		p.window = p.union(win)
		for _, tok := range toks {
			if !slices.Contains(p.tokens, tok) {
				p.tokens = append(p.tokens, tok)
			}
		}
		return
	}
	t.pools = append(t.pools, pool{window: win, condition: m.Condition, tokens: toks})
}

func (e *extractor) scan(ctx context.Context, sc scan, file string) error {
	rel := e.rel(file)
	if e.done(rel) {
		return nil
	}
	var s span
	if sc.ranged {
		s = monthSpan(file)
	}
	from := e.src.From([]string{file}, sc.cols...)
	var parts []string
	for _, b := range sc.branches {
		if vals := b.values(e.t, s); len(vals) > 0 {
			parts = append(parts, match(from, b.col, vals))
		}
	}
	if len(parts) == 0 {
		return nil
	}
	return e.copyOut(ctx, rel, strings.Join(parts, " UNION "), false)
}

func (e *extractor) placeholder(ctx context.Context, sc scan, file string) error {
	return e.copyOut(ctx, e.rel(file), "SELECT * FROM "+e.src.From([]string{file}, sc.cols...)+" WHERE false", true)
}

func (e *extractor) references(ctx context.Context) error {
	need, err := e.gather(ctx)
	if err != nil {
		return err
	}
	if len(need.tokens) > 0 {
		files := e.src.Files("position")
		conds, err := e.column(ctx, "SELECT DISTINCT condition FROM "+e.src.From(files, "id", "condition")+" WHERE id IN "+list(need.tokens.sorted()))
		if err != nil {
			return fmt.Errorf("golden position: %w", err)
		}
		need.conditions.add(conds...)
	}
	for _, lk := range lookups {
		keys := lk.pick(need)
		for _, file := range e.src.Files(lk.table) {
			rel := e.rel(file)
			if len(keys) == 0 || e.done(rel) {
				continue
			}
			if err := e.copyOut(ctx, rel, match(e.src.From([]string{file}, lk.cols...), lk.key, keys.sorted()), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *extractor) gather(ctx context.Context) (*touched, error) {
	need := &touched{tokens: set{}, conditions: set{}, events: set{}, wallets: set{}}
	for _, p := range e.t.pools {
		need.tokens.add(p.tokens...)
		need.conditions.add(p.condition)
	}
	for _, o := range e.t.owners {
		need.wallets.add(o.forms...)
	}
	for _, c := range [][2]string{{"order_filled", "makerAssetId"}, {"order_filled", "takerAssetId"}, {"fee_refunded", "tokenId"}} {
		toks, err := e.distinct(ctx, c[0], c[1])
		if err != nil {
			return nil, err
		}
		need.tokens.add(toks...)
	}
	delete(need.tokens, "0")
	for _, table := range []string{"split", "merge", "redemption"} {
		conds, err := e.distinct(ctx, table, "condition")
		if err != nil {
			return nil, err
		}
		need.conditions.add(conds...)
	}
	if err := e.conversions(ctx, need); err != nil {
		return nil, err
	}
	return need, nil
}

func (e *extractor) conversions(ctx context.Context, need *touched) error {
	files := e.local("neg_risk_conversion")
	if len(files) == 0 {
		return nil
	}
	rows, err := e.src.DB.QueryContext(ctx, "SELECT DISTINCT negRiskMarketId, questionCount FROM "+e.src.From(files, "negRiskMarketId", "questionCount"))
	if err != nil {
		return fmt.Errorf("golden conversions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, count string
		if err := rows.Scan(&id, &count); err != nil {
			return fmt.Errorf("golden conversions: %w", err)
		}
		derived, err := derive(id, count)
		if err != nil {
			return err
		}
		need.events.add(id)
		need.conditions.add(derived...)
	}
	return rows.Err()
}

func derive(id, count string) ([]string, error) {
	raw, err := hex.DecodeString(strings.TrimPrefix(id, "0x"))
	n, countErr := strconv.ParseUint(count, 10, 32)
	if err != nil || len(raw) != 32 || countErr != nil || n > maxQuestions {
		return nil, fmt.Errorf("golden conversion %s with questionCount %q is not usable", id, count)
	}
	out := make([]string, n)
	for q := range out {
		c := refs.ConditionID(refs.NegRiskAdapter, refs.QuestionID([32]byte(raw), uint8(q)))
		out[q] = "0x" + hex.EncodeToString(c[:])
	}
	return out, nil
}

func (e *extractor) copyOut(ctx context.Context, rel, query string, keepEmpty bool) error {
	dst := e.path(rel)
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		return err
	}
	tmp := dst + ".tmp"
	var rows int64
	err := e.src.DB.QueryRowContext(ctx, "COPY ("+query+" ORDER BY ALL) TO "+lit(tmp)+" (FORMAT parquet, COMPRESSION zstd)").Scan(&rows)
	if err == nil && (rows > 0 || keepEmpty) {
		err = os.Rename(tmp, dst)
	} else {
		_ = os.Remove(tmp)
	}
	if err != nil {
		return fmt.Errorf("golden %s: %w", rel, err)
	}
	gets, bytes, err := dataset.Traffic(ctx, e.src.DB)
	if err != nil {
		return err
	}
	if rows > 0 {
		e.written++
	}
	e.gets += gets
	e.bytes += bytes
	slog.Info("fixture file", "file", rel, "rows", rows, "requests", gets, "bytes", bytes)
	return e.pace.Done(ctx, gets)
}

func (e *extractor) column(ctx context.Context, query string) ([]string, error) {
	rows, err := e.src.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (e *extractor) distinct(ctx context.Context, table, col string) ([]string, error) {
	files := e.local(table)
	if len(files) == 0 {
		return nil, nil
	}
	out, err := e.column(ctx, "SELECT DISTINCT "+col+" FROM "+e.src.From(files, col))
	if err != nil {
		return nil, fmt.Errorf("golden %s: %w", table, err)
	}
	return out, nil
}

func (e *extractor) local(table string) []string {
	var files []string
	for _, file := range e.src.Files(table) {
		if rel := e.rel(file); e.done(rel) {
			files = append(files, e.path(rel))
		}
	}
	return files
}

func (e *extractor) rel(file string) string {
	return strings.TrimPrefix(file, strings.TrimSuffix(e.src.Root, "/")+"/")
}

func (e *extractor) path(rel string) string {
	return filepath.Join(e.out, filepath.FromSlash(rel))
}

func (e *extractor) done(rel string) bool {
	_, err := os.Stat(e.path(rel))
	return err == nil
}

func monthSpan(file string) span {
	m := monthRe.FindStringSubmatch(file)
	if m == nil {
		return span{}
	}
	year, _ := strconv.Atoi(m[1])
	month, _ := strconv.Atoi(m[2])
	lo := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
	return span{lo: lo.Unix(), hi: lo.AddDate(0, 1, 0).Unix(), month: true}
}

func match(from, col string, vals []string) string {
	vals = slices.Compact(slices.Sorted(slices.Values(vals)))
	if len(vals) > maxPoints {
		return "(SELECT * FROM " + from + " WHERE " + col + " IN " + list(vals) + ")"
	}
	// an equality filter prunes row groups with Parquet bloom filters and an IN list does not
	parts := make([]string, len(vals))
	for i, v := range vals {
		parts[i] = "SELECT * FROM " + from + " WHERE " + col + " = " + lit(v)
	}
	// parentheses keep a UNION ALL chain from skipping the dedup of the UNION that joins columns
	return "(" + strings.Join(parts, " UNION ALL ") + ")"
}

func lit(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func list(vals []string) string {
	quoted := make([]string, len(vals))
	for i, v := range vals {
		quoted[i] = lit(v)
	}
	return "(" + strings.Join(quoted, ", ") + ")"
}
