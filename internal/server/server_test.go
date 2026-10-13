package server_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/prometheus/client_golang/prometheus"

	"pm/internal/dataset"
	"pm/internal/server"
	"pm/internal/skill"
	"pm/internal/store"
	"pm/internal/testdb"
)

const (
	goldWallet   = "0x467ecfc0c6265683e90899C41881c0431bDEE2ed"
	ratedWallet  = "0x6326fC8A7361717B3926287f2998c91ee4fAADE2"
	cutoffWallet = "0xAaDce4CcC5a50Ed1CCDC7a0c88dc6adf9B93B7BA"
	statusPath   = "/internal/v1/status"
)

var requestID = regexp.MustCompile(`^[0-9a-f]{16}$`)

/* Fixtures */

type built struct {
	once      sync.Once
	dir, path string
	err       error
}

var goldenBuild, pairBuild built

func TestMain(m *testing.M) {
	code := m.Run()
	for _, b := range []*built{&goldenBuild, &pairBuild} {
		if b.dir != "" {
			_ = os.RemoveAll(b.dir)
		}
	}
	os.Exit(code)
}

func (b *built) get(t *testing.T, build func(string) (string, error)) string {
	t.Helper()
	b.once.Do(func() {
		b.dir, b.err = os.MkdirTemp("", "pm-server-")
		if b.err == nil {
			b.path, b.err = build(b.dir)
		}
	})
	if b.err != nil {
		t.Fatal(b.err)
	}
	return b.path
}

func goldenPath(t *testing.T) string {
	t.Helper()
	return goldenBuild.get(t, func(dir string) (string, error) { return testdb.Build(context.Background(), dir) })
}

func pairPath(t *testing.T) string {
	t.Helper()
	return pairBuild.get(t, func(dir string) (string, error) { return testdb.Pair(t, dir) })
}

func copyFile(t testing.TB, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func openStore(t *testing.T, path string, every time.Duration) *store.Store {
	t.Helper()
	s := store.New(path, store.Options{Every: every})
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func serve(t *testing.T, r server.Reader) (*httptest.Server, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	srv := httptest.NewServer(server.New(r, server.NewMetrics(reg)))
	t.Cleanup(srv.Close)
	return srv, reg
}

func goldenServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv, _ := serve(t, openStore(t, goldenPath(t), time.Hour))
	return srv
}

func pairServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv, _ := serve(t, openStore(t, pairPath(t), time.Hour))
	return srv
}

type reply struct {
	status int
	header http.Header
	body   []byte
}

func request(t testing.TB, srv *httptest.Server, method, path string, headers ...string) reply {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, srv.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return reply{resp.StatusCode, resp.Header, body}
}

func get(t testing.TB, srv *httptest.Server, path string, headers ...string) reply {
	t.Helper()
	return request(t, srv, http.MethodGet, path, headers...)
}

func (r reply) json(t testing.TB) any {
	t.Helper()
	var v any
	if err := json.Unmarshal(r.body, &v); err != nil {
		t.Fatalf("body is not JSON (%v): %s", err, r.body)
	}
	return v
}

func dig(t testing.TB, v any, path ...any) any {
	t.Helper()
	for _, p := range path {
		switch k := p.(type) {
		case string:
			m, ok := v.(map[string]any)
			if !ok {
				t.Fatalf("%v is not an object at %q", v, k)
			}
			if v, ok = m[k]; !ok {
				t.Fatalf("key %q is missing in %v", k, m)
			}
		case int:
			a, ok := v.([]any)
			if !ok || k >= len(a) {
				t.Fatalf("%v has no element %d", v, k)
			}
			v = a[k]
		}
	}
	return v
}

func raw(t testing.TB, v any, path ...any) int64 {
	t.Helper()
	n, err := strconv.ParseInt(dig(t, v, append(path, "raw")...).(string), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func expectProblem(t testing.TB, r reply, status int, code string) {
	t.Helper()
	if r.status != status {
		t.Fatalf("status %d, want %d (%s)", r.status, status, r.body)
	}
	if got := r.header.Get("Content-Type"); got != "application/problem+json" {
		t.Errorf("content type %q, want application/problem+json", got)
	}
	if got := r.header.Get("Cache-Control"); got != "no-store" {
		t.Errorf("cache control %q, want no-store", got)
	}
	if !requestID.MatchString(r.header.Get("X-Request-Id")) {
		t.Errorf("request id %q is not 16 hex digits", r.header.Get("X-Request-Id"))
	}
	body := r.json(t)
	if got := dig(t, body, "code"); got != code {
		t.Errorf("code %v, want %s", got, code)
	}
	if got := dig(t, body, "type"); got != "urn:sona:problem:"+code {
		t.Errorf("type %v, want urn:sona:problem:%s", got, code)
	}
	if got := dig(t, body, "status"); got != float64(status) {
		t.Errorf("body status %v, want %d", got, status)
	}
}

func metaOf(t testing.TB, path string) map[string]string {
	t.Helper()
	connector, err := duckdb.NewConnector(path+"?access_mode=READ_ONLY", nil)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	rows, err := db.QueryContext(t.Context(), "SELECT key, value FROM meta")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	meta := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			t.Fatal(err)
		}
		meta[k] = v
	}
	return meta
}

/* Probes and availability */

func TestHealthz(t *testing.T) {
	srv, _ := serve(t, openStore(t, filepath.Join(t.TempDir(), "pm.duckdb"), time.Hour))
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		if r := request(t, srv, method, "/healthz"); r.status != http.StatusOK {
			t.Errorf("%s /healthz is %d, want 200 with no database", method, r.status)
		}
	}
}

func TestReadyzFollowsStore(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pm.duckdb")
	s := openStore(t, path, 50*time.Millisecond)
	srv, _ := serve(t, s)
	if r := get(t, srv, "/readyz"); r.status != http.StatusServiceUnavailable {
		t.Fatalf("/readyz is %d before a database exists, want 503", r.status)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go s.Run(ctx)
	next := filepath.Join(dir, "next.duckdb")
	copyFile(t, goldenPath(t), next)
	if err := os.Rename(next, path); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for get(t, srv, "/readyz").status != http.StatusOK {
		if time.Now().After(deadline) {
			t.Fatal("/readyz stayed 503 after a valid database appeared")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestUnavailableOverGarbageFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pm.duckdb")
	if err := os.WriteFile(path, bytes.Repeat([]byte("not a database "), 4000), 0o600); err != nil {
		t.Fatal(err)
	}
	srv, _ := serve(t, openStore(t, path, time.Hour))
	for _, p := range []string{statusPath, "/internal/v1/traders/" + goldWallet, "/internal/v1/leaderboards/pnl", "/internal/v1/calibration"} {
		r := get(t, srv, p)
		expectProblem(t, r, http.StatusServiceUnavailable, "upstream_unavailable")
		if got := r.header.Get("Retry-After"); got != "1" {
			t.Errorf("%s Retry-After %q, want 1", p, got)
		}
		if got := dig(t, r.json(t), "retry_after_s"); got != float64(1) {
			t.Errorf("%s retry_after_s %v, want 1", p, got)
		}
	}
	if r := get(t, srv, "/readyz"); r.status != http.StatusServiceUnavailable {
		t.Errorf("/readyz is %d over a garbage file, want 503", r.status)
	}
}

/* Wallet */

func TestTraderAnyCase(t *testing.T) {
	srv := goldenServer(t)
	hexDigits := strings.ToLower(goldWallet[2:])
	forms := []string{"0x" + hexDigits, "0x" + strings.ToUpper(hexDigits), goldWallet}
	first := get(t, srv, "/internal/v1/traders/"+forms[0])
	if first.status != http.StatusOK {
		t.Fatalf("lowercase form is %d: %s", first.status, first.body)
	}
	for _, form := range forms[1:] {
		r := get(t, srv, "/internal/v1/traders/"+form)
		if r.status != http.StatusOK || !bytes.Equal(r.body, first.body) {
			t.Errorf("form %s answered %d with a different body", form, r.status)
		}
	}
	if got := dig(t, first.json(t), "wallet"); got != "0x"+hexDigits {
		t.Errorf("wallet %v, want the lowercase address", got)
	}
}

func TestTraderInvalidAndUnknown(t *testing.T) {
	srv := goldenServer(t)
	for _, tc := range []struct {
		path   string
		status int
		code   string
		name   string
	}{
		{"0x12", 400, "invalid_id", "short"},
		{"0x" + strings.Repeat("Z", 40), 400, "invalid_id", "not hex"},
		{strings.Repeat("a", 40), 400, "invalid_id", "no prefix"},
		{"0x" + strings.Repeat("a", 41), 400, "invalid_id", "too long"},
		{"0x1234567890123456789012345678901234567890", 404, "not_found", "unknown wallet"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expectProblem(t, get(t, srv, "/internal/v1/traders/"+tc.path), tc.status, tc.code)
		})
	}
}

func TestTraderContent(t *testing.T) {
	srv := goldenServer(t)
	body := get(t, srv, "/internal/v1/traders/"+goldWallet).json(t)
	if got := dig(t, body, "kind"); got != "pm_trader" {
		t.Errorf("kind %v", got)
	}
	if got := raw(t, body, "activity", "volume"); got != 716582888996 {
		t.Errorf("volume %d, want 716582888996", got)
	}
	gross, fees, realized := raw(t, body, "pnl", "gross"), raw(t, body, "pnl", "fees"), raw(t, body, "pnl", "realized")
	if realized != gross-fees {
		t.Errorf("realized %d, want gross %d minus fees %d", realized, gross, fees)
	}
	if d := realized - -16628825094; d > 200 || -d > 200 {
		t.Errorf("realized %d, want -16628825094 within 200", realized)
	}
	if got := dig(t, body, "pnl", "win_rate"); !mapEquals(got, map[string]any{"value": nil, "wilson_lower": nil, "shown": false, "floor": float64(30)}) {
		t.Errorf("win_rate %v, want the hidden form", got)
	}
	for key, want := range map[string]float64{"resolved_markets": 11, "won_markets": 4, "open_unresolved_positions": 0} {
		if got := dig(t, body, "pnl", key); got != want {
			t.Errorf("pnl.%s %v, want %v", key, got, want)
		}
	}
	if got := dig(t, body, "pnl", "final"); got != true {
		t.Errorf("final %v, want true", got)
	}
	if got := raw(t, body, "pnl", "biggest_win"); got != 95527217758 {
		t.Errorf("biggest_win %d", got)
	}
	if got := dig(t, body, "activity", "volume", "value"); got != "716582.888996" {
		t.Errorf("volume value %v", got)
	}
	if got := dig(t, body, "activity", "volume", "symbol"); got != "USDC" {
		t.Errorf("volume symbol %v", got)
	}
	sk := dig(t, body, "skill")
	if dig(t, sk, "rated") != false || dig(t, sk, "brier") != nil || dig(t, sk, "edge") != nil || dig(t, sk, "edge_se") != nil {
		t.Errorf("an unrated wallet shows skill figures: %v", sk)
	}
	if cal, ok := dig(t, sk, "calibration").([]any); !ok || len(cal) != 0 {
		t.Errorf("calibration %v, want an empty array", dig(t, sk, "calibration"))
	}
	if dig(t, sk, "horizon_hours") != float64(12) || !mapEquals(dig(t, sk, "floor"), map[string]any{"markets": float64(30), "events": float64(3)}) {
		t.Errorf("skill floor or horizon wrong: %v", sk)
	}
	if got := dig(t, body, "owner_kind"); got != nil && got != "proxy" && got != "safe" {
		t.Errorf("owner_kind %v", got)
	}
}

func mapEquals(got any, want map[string]any) bool {
	m, ok := got.(map[string]any)
	if !ok || len(m) != len(want) {
		return false
	}
	for k, v := range want {
		if m[k] != v {
			return false
		}
	}
	return true
}

func TestTraderWinRateShown(t *testing.T) {
	body := get(t, goldenServer(t), "/internal/v1/traders/"+ratedWallet).json(t)
	won, resolved := dig(t, body, "pnl", "won_markets").(float64), dig(t, body, "pnl", "resolved_markets").(float64)
	if resolved < 30 {
		t.Fatalf("fixture wallet has %v resolved markets, want at least 30", resolved)
	}
	win := dig(t, body, "pnl", "win_rate")
	if dig(t, win, "shown") != true || dig(t, win, "floor") != float64(30) {
		t.Fatalf("win_rate %v, want shown", win)
	}
	if got, want := dig(t, win, "value").(float64), won/resolved; math.Abs(got-want) > 5e-5 {
		t.Errorf("win_rate value %v, want %v to 4 decimals", got, want)
	}
	if got, want := dig(t, win, "wilson_lower").(float64), skill.Wilson(int(won), int(resolved)); math.Abs(got-want) > 5e-5 {
		t.Errorf("wilson_lower %v, want %v to 4 decimals", got, want)
	}
}

func TestTraderRatedSkill(t *testing.T) {
	body := get(t, goldenServer(t), "/internal/v1/traders/"+ratedWallet).json(t)
	sk := dig(t, body, "skill")
	if dig(t, sk, "rated") != true {
		t.Fatalf("wallet is not rated: %v", sk)
	}
	for _, key := range []string{"brier", "edge", "edge_se"} {
		if _, ok := dig(t, sk, key).(float64); !ok {
			t.Errorf("%s is %v, want a number", key, dig(t, sk, key))
		}
	}
	markets := dig(t, sk, "markets").(float64)
	if markets < 30 || dig(t, sk, "events").(float64) < 3 {
		t.Errorf("rated with %v markets and %v events", markets, dig(t, sk, "events"))
	}
	cal := dig(t, sk, "calibration").([]any)
	if len(cal) != 5 {
		t.Fatalf("calibration has %d buckets, want 5", len(cal))
	}
	var n float64
	for i, b := range cal {
		lo, hi := dig(t, b, "lo").(float64), dig(t, b, "hi").(float64)
		if lo != float64(i)/5 || hi != float64(i+1)/5 {
			t.Errorf("bucket %d spans %v to %v", i, lo, hi)
		}
		n += dig(t, b, "markets").(float64)
	}
	if n != markets {
		t.Errorf("calibration holds %v markets, want %v", n, markets)
	}
}

func TestTraderOpenPositions(t *testing.T) {
	body := get(t, goldenServer(t), "/internal/v1/traders/"+cutoffWallet).json(t)
	if got := dig(t, body, "pnl", "open_unresolved_positions"); got != float64(26) {
		t.Errorf("open_unresolved_positions %v, want 26", got)
	}
	if got := dig(t, body, "pnl", "final"); got != false {
		t.Errorf("final %v, want false", got)
	}
}

func TestTraderCoverage(t *testing.T) {
	meta := metaOf(t, goldenPath(t))
	body := get(t, goldenServer(t), "/internal/v1/traders/"+goldWallet).json(t)
	cov := dig(t, body, "coverage")
	for key, metaKey := range map[string]string{
		"note": "note", "revision": "revision", "attribution": "attribution", "license_url": "license_url",
		"modified": "modified", "source_url": "source_url",
	} {
		if got := dig(t, cov, key); got != meta[metaKey] || got == "" {
			t.Errorf("coverage.%s is %v, want the meta string %q", key, got, meta[metaKey])
		}
	}
	if got := dig(t, cov, "revision"); got != dataset.Revision {
		t.Errorf("revision %v", got)
	}
	if !mapEquals(dig(t, cov, "through"), map[string]any{"block": float64(85948288), "time": "2026-04-24T07:43:40Z"}) {
		t.Errorf("through %v", dig(t, cov, "through"))
	}
}

func TestTraderWashEvidence(t *testing.T) {
	srv := pairServer(t)
	body := get(t, srv, "/internal/v1/traders/"+testdb.Alice).json(t)
	w := dig(t, body, "wash")
	score, share := dig(t, w, "score").(float64), dig(t, w, "flagged_share").(float64)
	if dig(t, w, "scored") != true || score <= 0.8 || score >= 1 || math.Abs(share-200.0/240) > 1e-9 || dig(t, w, "algorithm") != "columbia-2" {
		t.Errorf("wash %v, want scored with a score in (0.8, 1) and flagged_share 200/240", w)
	}
	if got := dig(t, w, "paired_volume", "symbol"); got != "shares" {
		t.Errorf("paired_volume symbol %v, want shares", got)
	}
	if raw(t, w, "paired_volume") != 240_000_000 || raw(t, w, "flagged_volume") != 200_000_000 {
		t.Errorf("paired %v, flagged %v", dig(t, w, "paired_volume"), dig(t, w, "flagged_volume"))
	}
	cps := dig(t, w, "evidence", "counterparties").([]any)
	if len(cps) != 1 {
		t.Fatalf("%d counterparties, want 1", len(cps))
	}
	if got := dig(t, cps[0], "wallet"); got != strings.ToLower(testdb.Bob) {
		t.Errorf("counterparty %v, want bob", got)
	}
	if dig(t, cps[0], "trades") != float64(2) || dig(t, cps[0], "markets") != float64(1) || dig(t, cps[0], "score").(float64) <= 0.9 {
		t.Errorf("counterparty %v", cps[0])
	}
	trades := dig(t, w, "evidence", "trades").([]any)
	if len(trades) != 2 {
		t.Fatalf("%d trades, want 2", len(trades))
	}
	first, second := trades[0], trades[1]
	if dig(t, first, "id") != "137_100_1" || dig(t, second, "id") != "137_101_1" {
		t.Errorf("ids %v and %v, want 137_100_1 and 137_101_1", dig(t, first, "id"), dig(t, second, "id"))
	}
	if dig(t, first, "side") != "buy" || dig(t, second, "side") != "sell" || dig(t, first, "outcome") != float64(0) {
		t.Errorf("sides %v and %v, outcome %v", dig(t, first, "side"), dig(t, second, "side"), dig(t, first, "outcome"))
	}
	if got := dig(t, first, "time"); got != time.Unix(1700000100, 0).UTC().Format(time.RFC3339) {
		t.Errorf("time %v", got)
	}
	if dig(t, first, "counterparty") != strings.ToLower(testdb.Bob) {
		t.Errorf("trade counterparty %v", dig(t, first, "counterparty"))
	}
	if got := dig(t, first, "market", "condition"); got != "0x"+strings.Repeat("1", 64) {
		t.Errorf("condition %v", got)
	}
	if got := dig(t, first, "market", "question"); got != "Will A happen?" {
		t.Errorf("question %v", got)
	}
	shares, usdc := float64(raw(t, first, "shares")), float64(raw(t, first, "usdc"))
	if got := dig(t, first, "price").(float64); math.Abs(got-usdc/shares) > 1e-9 || got != 0.5 {
		t.Errorf("price %v, want usdc / shares = %v", got, usdc/shares)
	}
}

func TestTraderUnflaggedEvidence(t *testing.T) {
	body := get(t, goldenServer(t), "/internal/v1/traders/"+ratedWallet).json(t)
	w := dig(t, body, "wash")
	cps, trades := dig(t, w, "evidence", "counterparties"), dig(t, w, "evidence", "trades")
	if a, ok := cps.([]any); !ok || a == nil {
		t.Errorf("counterparties %v, want an array", cps)
	}
	if a, ok := trades.([]any); !ok || a == nil {
		t.Errorf("trades %v, want an array", trades)
	}
}

/* Boards */

func TestLeaderboards(t *testing.T) {
	srv := goldenServer(t)
	body := get(t, srv, "/internal/v1/leaderboards/pnl?limit=3").json(t)
	if dig(t, body, "kind") != "pm_leaderboard" || dig(t, body, "board") != "pnl" || dig(t, body, "limit") != float64(3) || dig(t, body, "offset") != float64(0) {
		t.Errorf("header fields wrong: %v", body)
	}
	rows := dig(t, body, "rows").([]any)
	if len(rows) != 3 {
		t.Fatalf("%d rows, want 3", len(rows))
	}
	var prev, prevVol int64 = math.MaxInt64, math.MaxInt64
	for i, r := range rows {
		if dig(t, r, "rank") != float64(i+1) {
			t.Errorf("row %d has rank %v", i, dig(t, r, "rank"))
		}
		net, vol := raw(t, r, "realized"), raw(t, r, "volume")
		if net > prev || (net == prev && vol > prevVol) {
			t.Errorf("row %d (%d, %d) breaks the order after (%d, %d)", i, net, vol, prev, prevVol)
		}
		prev, prevVol = net, vol
		if _, ok := dig(t, r, "wallet").(string); !ok {
			t.Errorf("row %d wallet %v", i, dig(t, r, "wallet"))
		}
		for _, key := range []string{"fills", "markets", "final", "wash_share", "skill"} {
			dig(t, r, key)
		}
	}
	if total := dig(t, body, "total").(float64); total < 1000 {
		t.Errorf("total %v", total)
	}
}

func TestLeaderboardQueries(t *testing.T) {
	srv := goldenServer(t)
	for _, q := range []string{"limit=0", "limit=101", "limit=abc", "limit=", "limit=-1", "offset=10001", "offset=-1", "offset=x"} {
		t.Run(q, func(t *testing.T) {
			expectProblem(t, get(t, srv, "/internal/v1/leaderboards/pnl?"+q), http.StatusBadRequest, "invalid_query")
		})
	}
	expectProblem(t, get(t, srv, "/internal/v1/leaderboards/foo"), http.StatusBadRequest, "invalid_id")
	full := get(t, srv, "/internal/v1/leaderboards/volume?limit=100&offset=10000&ignored=1").json(t)
	rows, ok := dig(t, full, "rows").([]any)
	if !ok || len(rows) != 0 {
		t.Errorf("rows %v, want an empty array", dig(t, full, "rows"))
	}
	if total := dig(t, full, "total").(float64); total == 0 {
		t.Error("total is 0 past the end, want the board size")
	}
	small := get(t, pairServer(t), "/internal/v1/leaderboards/pnl?offset=5000")
	if small.status != http.StatusOK || dig(t, small.json(t), "total") != float64(3) {
		t.Fatalf("offset 5000 over three wallets answered %d: %s", small.status, small.body)
	}
	if rows, ok := dig(t, small.json(t), "rows").([]any); !ok || len(rows) != 0 {
		t.Errorf("rows %v, want an empty array", dig(t, small.json(t), "rows"))
	}
	mid := get(t, srv, "/internal/v1/leaderboards/volume?offset=5000&limit=1")
	if mid.status != http.StatusOK || dig(t, mid.json(t), "rows", 0, "rank") != float64(5001) {
		t.Errorf("offset 5000 answered %d: %s", mid.status, mid.body)
	}
}

func TestLeaderboardSkill(t *testing.T) {
	body := get(t, goldenServer(t), "/internal/v1/leaderboards/skill").json(t)
	rows := dig(t, body, "rows").([]any)
	if dig(t, body, "total") != float64(3) || len(rows) != 3 {
		t.Fatalf("skill board has total %v and %d rows, want the 3 rated wallets", dig(t, body, "total"), len(rows))
	}
	prev := math.Inf(1)
	for i, r := range rows {
		sk := dig(t, r, "skill")
		if sk == nil {
			t.Fatalf("row %d has no skill object", i)
		}
		lb := dig(t, sk, "edge_lb").(float64)
		if lb > dig(t, sk, "edge").(float64) || lb > prev {
			t.Errorf("row %d edge_lb %v is out of order or above edge", i, lb)
		}
		prev = lb
		for _, key := range []string{"markets", "events", "brier", "edge"} {
			dig(t, sk, key)
		}
	}
	empty := get(t, pairServer(t), "/internal/v1/leaderboards/skill").json(t)
	if dig(t, empty, "total") != float64(0) {
		t.Errorf("total %v, want 0", dig(t, empty, "total"))
	}
	if a, ok := dig(t, empty, "rows").([]any); !ok || len(a) != 0 {
		t.Errorf("rows %v, want an empty array", dig(t, empty, "rows"))
	}
}

func TestLeaderboardPnlSkillNull(t *testing.T) {
	body := get(t, pairServer(t), "/internal/v1/leaderboards/volume").json(t)
	rows := dig(t, body, "rows").([]any)
	if len(rows) != 3 {
		t.Fatalf("%d rows, want 3", len(rows))
	}
	for _, r := range rows {
		share := dig(t, r, "wash_share").(float64)
		if dig(t, r, "skill") != nil || share < 0 || share > 1 {
			t.Errorf("row %v, want null skill and a wash_share from 0 to 1", r)
		}
	}
}

/* Calibration */

func TestCalibration(t *testing.T) {
	srv := goldenServer(t)
	def := get(t, srv, "/internal/v1/calibration").json(t)
	if dig(t, def, "kind") != "pm_calibration" || dig(t, def, "horizon_hours") != float64(12) || dig(t, def, "market_kind") != "other" {
		t.Errorf("defaults wrong: horizon %v, kind %v", dig(t, def, "horizon_hours"), dig(t, def, "market_kind"))
	}
	buckets := dig(t, def, "buckets").([]any)
	if len(buckets) != 20 {
		t.Fatalf("%d buckets, want 20", len(buckets))
	}
	var n float64
	for i, b := range buckets {
		if dig(t, b, "lo") != float64(i)/20 || dig(t, b, "hi") != float64(i+1)/20 {
			t.Errorf("bucket %d spans %v to %v", i, dig(t, b, "lo"), dig(t, b, "hi"))
		}
		n += dig(t, b, "markets").(float64)
	}
	if n != dig(t, def, "markets").(float64) || n == 0 {
		t.Errorf("buckets hold %v markets, body says %v", n, dig(t, def, "markets"))
	}
	if b := dig(t, def, "brier").(float64); b <= 0 || b >= 1 {
		t.Errorf("brier %v", b)
	}
	if dig(t, def, "excluded", "no_price").(float64) <= 0 {
		t.Errorf("excluded.no_price %v, want a positive count on the fixtures", dig(t, def, "excluded", "no_price"))
	}
	for q, hours := range map[string]float64{"horizon=4h": 4, "horizon=12h": 12, "horizon=1d": 24, "horizon=1w": 168} {
		if got := dig(t, get(t, srv, "/internal/v1/calibration?"+q).json(t), "horizon_hours"); got != hours {
			t.Errorf("%s gave horizon_hours %v, want %v", q, got, hours)
		}
	}
	for _, kind := range []string{"other", "updown", "unknown", "all"} {
		r := get(t, srv, "/internal/v1/calibration?market_kind="+kind)
		if r.status != http.StatusOK || dig(t, r.json(t), "market_kind") != kind {
			t.Errorf("market_kind=%s answered %d", kind, r.status)
		}
	}
	for _, q := range []string{"horizon=2h", "horizon=", "market_kind=x", "market_kind="} {
		t.Run(q, func(t *testing.T) {
			expectProblem(t, get(t, srv, "/internal/v1/calibration?"+q), http.StatusBadRequest, "invalid_query")
		})
	}
}

func TestCalibrationEmptyKind(t *testing.T) {
	body := get(t, pairServer(t), "/internal/v1/calibration?market_kind=unknown").json(t)
	if dig(t, body, "markets") != float64(0) || dig(t, body, "brier") != nil {
		t.Errorf("markets %v, brier %v, want 0 and null", dig(t, body, "markets"), dig(t, body, "brier"))
	}
}

/* Status */

func TestStatus(t *testing.T) {
	meta := metaOf(t, goldenPath(t))
	r := get(t, goldenServer(t), statusPath)
	if r.status != http.StatusOK {
		t.Fatalf("status %d: %s", r.status, r.body)
	}
	body := r.json(t)
	if dig(t, body, "kind") != "pm_status" || dig(t, body, "ready") != true || dig(t, body, "rules_version") != "pm-rules-1" || dig(t, body, "built_at") != meta["built_at"] {
		t.Errorf("status body %v", body)
	}
	for key, metaKey := range map[string]string{
		"fills": "total_fills", "wallets": "total_wallets", "rated_wallets": "total_rated",
		"markets": "total_markets", "resolved_markets": "total_resolved_markets",
	} {
		if got := dig(t, body, "totals", key); strconv.FormatFloat(got.(float64), 'f', -1, 64) != meta[metaKey] {
			t.Errorf("totals.%s is %v, want %s", key, got, meta[metaKey])
		}
	}
	dig(t, body, "coverage", "through", "block")
}

/* Deadline and transport */

func TestDeadline(t *testing.T) {
	srv := goldenServer(t)
	for _, v := range []string{"0", "-5"} {
		expectProblem(t, get(t, srv, statusPath, "Sona-Deadline-Ms", v), http.StatusGatewayTimeout, "deadline_exceeded")
	}
	if r := get(t, srv, statusPath, "Sona-Deadline-Ms", "abc"); r.status != http.StatusOK {
		t.Errorf("a malformed deadline answered %d, want the default budget and 200", r.status)
	}
	if r := get(t, srv, statusPath, "Sona-Deadline-Ms", "250"); r.status != http.StatusOK {
		t.Errorf("a 250 ms budget answered %d, want 200", r.status)
	}
}

type slow struct{ ready bool }

func (s slow) Ready() bool { return s.ready }

func (slow) Status(ctx context.Context) (store.Status, error) {
	<-ctx.Done()
	return store.Status{}, ctx.Err()
}

func (slow) Trader(ctx context.Context, _ [20]byte) (store.Trader, error) {
	<-ctx.Done()
	return store.Trader{}, ctx.Err()
}

func (slow) Board(ctx context.Context, _ string, _, _ int) (store.Page, error) {
	<-ctx.Done()
	return store.Page{}, ctx.Err()
}

func (slow) Calibration(ctx context.Context, _ int, _ string) (store.Calibration, error) {
	<-ctx.Done()
	return store.Calibration{}, ctx.Err()
}

func TestDeadlineRunsOut(t *testing.T) {
	srv, _ := serve(t, slow{ready: true})
	start := time.Now()
	expectProblem(t, get(t, srv, statusPath, "Sona-Deadline-Ms", "30"), http.StatusGatewayTimeout, "deadline_exceeded")
	if time.Since(start) > 2*time.Second {
		t.Errorf("the 30 ms budget took %v", time.Since(start))
	}
}

func TestTransport(t *testing.T) {
	srv := goldenServer(t)
	head := request(t, srv, http.MethodHead, statusPath)
	if head.status != http.StatusOK || len(head.body) != 0 {
		t.Errorf("HEAD answered %d with %d body bytes, want 200 and none", head.status, len(head.body))
	}
	if head.header.Get("Sona-Block") != "85948288" || head.header.Get("Content-Type") != "application/json" {
		t.Errorf("HEAD headers %v", head.header)
	}
	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, statusPath}, {http.MethodGet, "/nope"}, {http.MethodPut, "/healthz"}, {http.MethodDelete, "/internal/v1/traders/" + goldWallet},
	} {
		r := request(t, srv, tc.method, tc.path)
		expectProblem(t, r, http.StatusNotFound, "not_found")
		if got := dig(t, r.json(t), "detail"); got != "No such route." {
			t.Errorf("%s %s detail %v", tc.method, tc.path, got)
		}
		if got := dig(t, r.json(t), "instance"); got != tc.path {
			t.Errorf("%s %s instance %v", tc.method, tc.path, got)
		}
	}
	ok := get(t, srv, statusPath)
	for key, want := range map[string]string{"Sona-Finality": "finalized", "Sona-Block": "85948288", "Sona-Block-Time": "1777016620", "Content-Type": "application/json"} {
		if got := ok.header.Get(key); got != want {
			t.Errorf("%s is %q, want %q", key, got, want)
		}
	}
	if !requestID.MatchString(ok.header.Get("X-Request-Id")) {
		t.Errorf("request id %q", ok.header.Get("X-Request-Id"))
	}
	if get(t, srv, statusPath).header.Get("X-Request-Id") == ok.header.Get("X-Request-Id") {
		t.Error("two requests share a request id")
	}
	bad := get(t, srv, "/internal/v1/traders/0x12")
	if bad.header.Get("Sona-Block") != "" {
		t.Error("an error carries the data headers")
	}
	for _, p := range []string{"/internal/v1/leaderboards/pnl", "/internal/v1/calibration", "/internal/v1/traders/" + goldWallet} {
		r := get(t, srv, p)
		if r.header.Get("Sona-Finality") != "finalized" || r.header.Get("Sona-Block") != "85948288" {
			t.Errorf("%s headers %v", p, r.header)
		}
	}
}

/* Failures */

type failing struct{ err error }

func (failing) Ready() bool { return true }

func (f failing) Status(context.Context) (store.Status, error) { return store.Status{}, f.err }

func (f failing) Trader(context.Context, [20]byte) (store.Trader, error) {
	return store.Trader{}, f.err
}

func (f failing) Board(context.Context, string, int, int) (store.Page, error) {
	return store.Page{}, f.err
}

func (f failing) Calibration(context.Context, int, string) (store.Calibration, error) {
	return store.Calibration{}, f.err
}

type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func TestInternalErrorIsLoggedUnderRequestID(t *testing.T) {
	var logs logBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	srv, _ := serve(t, failing{err: errors.New("disk exploded")})
	r := get(t, srv, statusPath)
	expectProblem(t, r, http.StatusInternalServerError, "internal")
	id := r.header.Get("X-Request-Id")
	if line := logs.String(); !strings.Contains(line, id) || !strings.Contains(line, "disk exploded") {
		t.Errorf("log %q does not hold request id %s and the cause", line, id)
	}
	if strings.Contains(string(r.body), "disk exploded") {
		t.Error("the cause leaked into the response body")
	}
}

func TestStoreErrorsMap(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		code   string
	}{
		{store.ErrNotBuilt, 503, "upstream_unavailable"},
		{store.ErrNotFound, 404, "not_found"},
		{context.DeadlineExceeded, 504, "deadline_exceeded"},
		{errors.New("boom"), 500, "internal"},
	} {
		srv, _ := serve(t, failing{err: tc.err})
		expectProblem(t, get(t, srv, "/internal/v1/traders/"+goldWallet), tc.status, tc.code)
	}
}

/* Metrics */

func TestMetrics(t *testing.T) {
	srv, reg := serve(t, openStore(t, goldenPath(t), time.Hour))
	get(t, srv, statusPath)
	get(t, srv, "/internal/v1/traders/0x12")
	get(t, srv, "/unknown")
	get(t, srv, "/healthz")
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			seen[f.GetName()+" "+labels["route"]+" "+labels["code"]] = true
		}
	}
	for _, want := range []string{
		"pm_api_requests_total GET /internal/v1/status 200",
		"pm_api_requests_total GET /internal/v1/traders/{wallet} 400",
		"pm_api_requests_total unmatched 404",
		"pm_api_request_duration_seconds GET /internal/v1/status 200",
	} {
		if !seen[want] {
			t.Errorf("metric %q is missing, have %v", want, seen)
		}
	}
	for key := range seen {
		if strings.Contains(key, "/healthz") {
			t.Errorf("probe counted: %s", key)
		}
	}
}
