package levr_test

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"pm/internal/httpx"
	"pm/internal/levr"
	"pm/internal/venue"
	vl "pm/internal/venue/levr"
	"pm/internal/venue/venuetest"
)

const (
	fixtures = "../../../testdata/venues/"
	ft       = "levrFullTimeMarketSchema"
	liqTable = "levrLiquidationEngineSchemaPositionLiquidateds"
	winID    = "0x46d28c3c7b524cc42590e8a73718d5cc452e0194afcf60ed7f10de48c10cd0cf"
	voidID   = "0x352331706070f53fd43d54222b0dfdd2527774ed09fe1985b22dc6989c0875c5"
	liqID    = "0xb6485f20c4709667d4e5cbfa80f34dc2eb7ddc0b62f6873627119c33f4739e3c"
)

var now = time.Date(2026, 10, 14, 22, 0, 0, 0, time.UTC)

func raw(t testing.TB, name, table string) []jsontext.Value {
	t.Helper()
	b, err := os.ReadFile(fixtures + name)
	if err != nil {
		t.Fatal(err)
	}
	var d struct {
		Data map[string][]jsontext.Value `json:"data"`
	}
	if err := json.Unmarshal(b, &d); err != nil {
		t.Fatal(err)
	}
	return d.Data[table]
}

func rows[T any](t testing.TB, name, table string) []T {
	t.Helper()
	var out []T
	for _, r := range raw(t, name, table) {
		var v T
		if err := json.Unmarshal(r, &v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	return out
}

// store keeps positions in memory on top of the shared in-memory sink
type store struct {
	*venuetest.Mem
	mu  sync.Mutex
	pos map[string]levr.Position
}

func newStore() *store { return &store{Mem: venuetest.New(), pos: map[string]levr.Position{}} }

func (s *store) UpsertPositions(_ context.Context, ps []levr.Position) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, p := range ps {
		if _, ok := s.pos[p.ID]; !ok {
			s.pos[p.ID] = p
			n++
		}
	}
	return n, nil
}

func (s *store) Positions(_ context.Context, ids []string) (map[string]levr.Position, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]levr.Position{}
	for _, id := range ids {
		if p, ok := s.pos[id]; ok {
			out[id] = p
		}
	}
	return out, nil
}

func (s *store) ClosePositions(_ context.Context, cs []levr.Close) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range cs {
		if p, ok := s.pos[c.ID]; ok && p.Status == levr.Open {
			p.Status, p.Closed, p.Payout = c.Status, c.At, c.Payout
			s.pos[c.ID] = p
			n++
		}
	}
	return n, nil
}

func (s *store) LoseOpen(_ context.Context, market string, winner int, at time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, p := range s.pos {
		if p.MarketID == market && p.Status == levr.Open && p.Side != winner {
			p.Status, p.Closed, p.Payout = levr.Lost, at, new(int64)
			s.pos[id] = p
			n++
		}
	}
	return n, nil
}

type call struct {
	Query string         `json:"query"`
	Vars  map[string]any `json:"variables"`
}

// squid answers like the GraphQL does, from rows per table, after the id cursor, and fails the tables named in broken
type squid struct {
	mu     sync.Mutex
	tables map[string][]jsontext.Value
	broken map[string]bool
	calls  []call
}

func (q *squid) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var c call
	if err := json.UnmarshalRead(r.Body, &c); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	q.calls = append(q.calls, c)
	table := strings.Fields(c.Query[strings.Index(c.Query, "{")+1:])[0]
	table = table[:strings.Index(table, "(")]
	if q.broken[table] {
		_ = json.MarshalWrite(w, map[string]any{"data": nil, "errors": []map[string]string{{"message": "internal error"}}})
		return
	}
	limit := int(c.Vars["limit"].(float64))
	var out []jsontext.Value
	for _, row := range q.tables[table] {
		var id struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(row, &id)
		if after, ok := c.Vars["after"].(string); ok && id.ID <= after {
			continue
		}
		if len(out) < limit {
			out = append(out, row)
		}
	}
	_ = json.MarshalWrite(w, map[string]any{"data": map[string]any{table: out}})
}

func fixtureSquid(t testing.TB) *squid {
	return &squid{
		tables: map[string][]jsontext.Value{
			ft + "PositionOpeneds":   raw(t, "levr_opened.json", ft+"PositionOpeneds"),
			ft + "PositionRedeemeds": raw(t, "levr_redeemed.json", ft+"PositionRedeemeds"),
			liqTable:                 raw(t, "levr_liquidated.json", liqTable),
			ft + "MarketSettleds":    raw(t, "levr_settled.json", ft+"MarketSettleds"),
			"levrOverUnderMarketSchemaPositionOpeneds": nil, "levrOverUnderMarketSchemaPositionRedeemeds": nil, "levrOverUnderMarketSchemaMarketSettleds": nil,
		},
		broken: map[string]bool{},
	}
}

func source(t testing.TB, q *squid, st vl.Store) (*vl.Source, *vl.Metrics) {
	t.Helper()
	srv := httptest.NewServer(q)
	t.Cleanup(srv.Close)
	m := vl.NewMetrics(prometheus.NewRegistry())
	cli := &vl.Client{HTTP: &httpx.Client{HTTP: srv.Client(), UserAgent: "test"}, URL: srv.URL}
	return &vl.Source{Client: cli, Store: st, Metrics: m, Now: func() time.Time { return now }}, m
}

func TestPositionFromRecordedRow(t *testing.T) {
	op := rows[vl.Opened](t, "levr_opened.json", ft+"PositionOpeneds")
	p, ok := op[1].Position()
	if !ok || p.ID != winID || p.Stake != 6_000_001 || p.Size != 12_000_000 || p.Fee != 120_000 || p.EntryPPM != 523809 || p.LiqPPM != 373931 || p.Status != levr.Open {
		t.Fatalf("%+v %v", p, ok)
	}
	if len(p.Wallet) != 20 || p.Opened.Format(time.RFC3339) != "2026-10-12T20:57:46Z" {
		t.Fatalf("%x %v", p.Wallet, p.Opened)
	}
	sum := levr.Summarize([]levr.Position{p})
	if got := sum.Leverage(); got < 1.999 || got > 2.0 {
		t.Fatalf("leverage %v", got)
	}
	bad := op[0]
	for _, mutate := range []func(*vl.Opened){
		func(o *vl.Opened) { o.AssetAmount = "-5" },
		func(o *vl.Opened) { o.AssetAmount = "0" },
		func(o *vl.Opened) { o.EntryPrice = "2000000" },
		func(o *vl.Opened) { o.Receiver = "0x12" },
		func(o *vl.Opened) { o.PositionSize = "1" },
		func(o *vl.Opened) { o.Timestamp = "soon" },
	} {
		o := bad
		mutate(&o)
		if _, ok := o.Position(); ok {
			t.Fatalf("accepted %+v", o)
		}
	}
}

// the venue's own aggregate reports a payout of 15,318,784 for this position, within a micro-USDC of the sum of the stake, profit and fees
func TestPositionWithAnUnknownSideIsRejected(t *testing.T) {
	o := rows[vl.Opened](t, "levr_opened.json", ft+"PositionOpeneds")[1]
	for _, side := range []int{0, 4, -1} {
		o.Side = side
		if _, ok := o.Position(); ok {
			t.Fatalf("side %d was accepted", side)
		}
	}
}

func TestRedemptionPayoutMatchesTheVenue(t *testing.T) {
	op := rows[vl.Opened](t, "levr_opened.json", ft+"PositionOpeneds")
	red := rows[vl.Redeemed](t, "levr_redeemed.json", ft+"PositionRedeemeds")
	p, _ := op[1].Position()
	r, ok := red[1].Redemption()
	if !ok {
		t.Fatal("redemption")
	}
	c := levr.Settle(p, r)
	if c.Status != levr.Won || c.Payout == nil || *c.Payout < 15_318_783 || *c.Payout > 15_318_785 {
		t.Fatalf("%+v", c)
	}
	p.Status, p.Payout = c.Status, c.Payout
	if pnl := p.PnL(); pnl != *c.Payout-p.Stake {
		t.Fatalf("pnl %d", pnl)
	}
	v, _ := op[0].Position()
	vr, _ := red[0].Redemption()
	if c := levr.Settle(v, vr); c.Status != levr.Void || c.Payout != nil {
		t.Fatalf("cancelled game %+v", c)
	}
}

func TestLiquidationLosesTheStake(t *testing.T) {
	p := levr.Position{ID: "x", Stake: 5_000_000, Size: 25_000_000, Status: levr.Open}
	c := levr.Liquidation("x", now)
	p.Status, p.Payout = c.Status, c.Payout
	if p.PnL() != -5_000_000 {
		t.Fatalf("pnl %d", p.PnL())
	}
	s := levr.Summarize([]levr.Position{p})
	if s.Liquidated != 1 || s.LiquidationRate() != 1 || s.Realized != -5_000_000 || s.Leverage() != 5 || s.WinRate() != 0 {
		t.Fatalf("%+v", s)
	}
}

func TestPollStoresPositionsAndTheirEnds(t *testing.T) {
	q := fixtureSquid(t)
	st := newStore()
	src, m := source(t, q, st)
	if err := src.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(st.pos) != 3 {
		t.Fatalf("%d positions", len(st.pos))
	}
	win, void, liq := st.pos[winID], st.pos[voidID], st.pos[liqID]
	if win.Status != levr.Won || win.Payout == nil || void.Status != levr.Void || void.Payout != nil || liq.Status != levr.Liquidated || *liq.Payout != 0 {
		t.Fatalf("%+v\n%+v\n%+v", win, void, liq)
	}
	if mk, _ := st.Market(venue.Levr, win.MarketID); mk.Result != "1" || mk.Status != "settled" || mk.Title != "Game 4885" || mk.Kind != "FullTimeWinner" {
		t.Fatalf("%+v", mk)
	}
	if mk, _ := st.Market(venue.Levr, void.MarketID); mk.Result != "void" {
		t.Fatalf("%+v", mk)
	}
	if mk, _ := st.Market(venue.Levr, liq.MarketID); mk.Status != "open" || mk.Result != "" {
		t.Fatalf("%+v", mk)
	}
	if cur, _ := st.Cursor(t.Context(), "levr/"+ft+"PositionRedeemeds"); cur != "0113428263-9b5ca-000100" {
		t.Fatalf("cursor %q", cur)
	}
	// every table is asked again, now after its cursor, and nothing changes
	before := len(q.calls)
	if err := src.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, c := range q.calls[before:] {
		// the over/under tables are empty, so they have no cursor to follow
		if _, ok := c.Vars["after"]; !ok && !strings.Contains(c.Query, "OverUnder") {
			t.Fatalf("second poll fell back to the lookback %v", c.Vars)
		}
	}
	if n := testutil.ToFloat64(m.Orphans()); n != 0 || len(st.pos) != 3 || st.pos[winID].Status != levr.Won {
		t.Fatalf("orphans %v", n)
	}
}

// the venue never redeems a position on the losing side, so settling the game must close it
func TestSettlementLosesOpenPositionsOnTheOtherSide(t *testing.T) {
	var o vl.Opened
	if err := json.Unmarshal(raw(t, "levr_opened.json", ft+"PositionOpeneds")[1], &o); err != nil {
		t.Fatal(err)
	}
	o.ID, o.PositionID, o.Side = "0113428999-zzzzz-000001", "0x"+strings.Repeat("ab", 32), 2
	extra, _ := json.Marshal(o)
	other := o
	other.ID, other.PositionID, other.Side = "0113428999-zzzzz-000002", "0x"+strings.Repeat("cd", 32), 1
	pending, _ := json.Marshal(other)
	q := fixtureSquid(t)
	q.tables[ft+"PositionOpeneds"] = append(q.tables[ft+"PositionOpeneds"], extra, pending)
	st := newStore()
	src, _ := source(t, q, st)
	if err := src.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	lost := st.pos[o.PositionID]
	if lost.Status != levr.Lost || lost.Payout == nil || *lost.Payout != 0 || lost.PnL() != -lost.Stake || lost.Side != 2 {
		t.Fatalf("%+v", lost)
	}
	// the winning side waits for its own redemption
	if p := st.pos[other.PositionID]; p.Status != levr.Open {
		t.Fatalf("%+v", p)
	}
	if p := st.pos[voidID]; p.Status != levr.Void {
		t.Fatalf("a cancelled game lost a position %+v", p)
	}
}

func TestFirstPollReadsALookbackAndBackfillReadsEverything(t *testing.T) {
	q := fixtureSquid(t)
	src, _ := source(t, q, newStore())
	if err := src.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	c := q.calls[0]
	if c.Vars["since"] != now.Add(-24*time.Hour).Format(time.RFC3339) || !strings.Contains(c.Query, "timestamp_gte: $since") || !strings.Contains(c.Query, "orderBy: id_ASC") {
		t.Fatalf("%+v", c)
	}
	q = fixtureSquid(t)
	src, _ = source(t, q, newStore())
	src.Backfill = true
	if err := src.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if c := q.calls[0]; c.Vars["since"] != nil || c.Vars["after"] != nil || strings.Contains(c.Query, "where") {
		t.Fatalf("%+v", c)
	}
}

func TestBrokenTableIsSkippedAndCounted(t *testing.T) {
	q := fixtureSquid(t)
	q.broken[ft+"PositionRedeemeds"] = true
	st := newStore()
	src, m := source(t, q, st)
	if err := src.Poll(t.Context()); err != nil {
		t.Fatalf("one broken table failed the poll: %v", err)
	}
	if st.pos[liqID].Status != levr.Liquidated || st.pos[winID].Status != levr.Open {
		t.Fatalf("%+v", st.pos)
	}
	if n := testutil.ToFloat64(m.TableErrors(ft + "PositionRedeemeds")); n != 1 {
		t.Fatalf("errors %v", n)
	}
	for table := range q.tables {
		q.broken[table] = true
	}
	if err := src.Poll(t.Context()); err == nil {
		t.Fatal("every table failed and the poll reported success")
	}
}

func TestClosesWaitWhilePositionsCannotBeRead(t *testing.T) {
	q := fixtureSquid(t)
	q.broken[ft+"PositionOpeneds"] = true
	st := newStore()
	src, m := source(t, q, st)
	if err := src.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n := testutil.ToFloat64(m.Orphans()); n != 0 || len(st.pos) != 0 {
		t.Fatalf("orphans %v, positions %d", n, len(st.pos))
	}
	q.broken[ft+"PositionOpeneds"] = false
	if err := src.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if st.pos[winID].Status != levr.Won || st.pos[liqID].Status != levr.Liquidated || st.pos[voidID].Status != levr.Void {
		t.Fatalf("%+v", st.pos)
	}
}

func TestClosesOfUnknownPositionsAreOrphans(t *testing.T) {
	q := fixtureSquid(t)
	q.tables[ft+"PositionOpeneds"] = nil
	st := newStore()
	src, m := source(t, q, st)
	if err := src.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n := testutil.ToFloat64(m.Orphans()); n != 3 || len(st.pos) != 0 {
		t.Fatalf("orphans %v, positions %d", n, len(st.pos))
	}
}

func TestPagesFollowTheCursorUntilAShortPage(t *testing.T) {
	base := raw(t, "levr_opened.json", ft+"PositionOpeneds")[1]
	var o vl.Opened
	if err := json.Unmarshal(base, &o); err != nil {
		t.Fatal(err)
	}
	q := fixtureSquid(t)
	q.tables[ft+"PositionOpeneds"] = nil
	for i := range 2500 {
		o.ID = "0113400000-aaaaa-" + strconv.Itoa(100000+i)
		o.PositionID = "0x" + strings.Repeat("0", 60) + strconv.Itoa(1000+i)
		b, _ := json.Marshal(o)
		q.tables[ft+"PositionOpeneds"] = append(q.tables[ft+"PositionOpeneds"], b)
	}
	q.tables[ft+"PositionRedeemeds"], q.tables[liqTable], q.tables[ft+"MarketSettleds"] = nil, nil, nil
	st := newStore()
	src, _ := source(t, q, st)
	if err := src.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	var afters []any
	for _, c := range q.calls {
		if strings.Contains(c.Query, ft+"PositionOpeneds") {
			afters = append(afters, c.Vars["after"])
		}
	}
	if len(st.pos) != 2500 || len(afters) != 3 || afters[0] != nil || afters[1] != "0113400000-aaaaa-100999" || afters[2] != "0113400000-aaaaa-101999" {
		t.Fatalf("%d positions, afters %v", len(st.pos), afters)
	}
	if cur, _ := st.Cursor(t.Context(), "levr/"+ft+"PositionOpeneds"); cur != "0113400000-aaaaa-102499" {
		t.Fatalf("cursor %q", cur)
	}
}

type walletStore struct {
	ps  []levr.Position
	out []levr.Wallet
}

func (w *walletStore) EachPosition(_ context.Context, fn func(levr.Position) error) error {
	for _, p := range w.ps {
		if err := fn(p); err != nil {
			return err
		}
	}
	return nil
}

func (w *walletStore) UpsertWallets(_ context.Context, ws []levr.Wallet) error {
	w.out = append(w.out, slices.Clone(ws)...)
	return nil
}

func TestWalletTotals(t *testing.T) {
	pay := int64(9_000_000)
	a, b := []byte{1}, []byte{2}
	ws := &walletStore{ps: []levr.Position{
		{Wallet: a, Stake: 5_000_000, Size: 10_000_000, Status: levr.Won, Payout: &pay},
		{Wallet: a, Stake: 5_000_000, Size: 25_000_000, Status: levr.Liquidated},
		{Wallet: a, Stake: 1_000_000, Size: 2_000_000, Status: levr.Open},
		{Wallet: b, Stake: 2_000_000, Size: 2_000_000, Status: levr.Void},
	}}
	if err := (&levr.Wallets{Store: ws}).Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(ws.out) != 2 {
		t.Fatalf("%+v", ws.out)
	}
	x := ws.out[0]
	if x.Positions != 3 || x.Won != 1 || x.Liquidated != 1 || x.Settled() != 2 || x.Stake != 11_000_000 || x.Size != 37_000_000 || x.Realized != -1_000_000 {
		t.Fatalf("%+v", x)
	}
	if x.WinRate() != 0.5 || x.LiquidationRate() != 0.5 {
		t.Fatalf("rates %v %v", x.WinRate(), x.LiquidationRate())
	}
	if y := ws.out[1]; y.Voided != 1 || y.Realized != 0 || y.Settled() != 0 {
		t.Fatalf("%+v", y)
	}
}
