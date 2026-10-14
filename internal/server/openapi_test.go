package server_test

import (
	"cmp"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/prometheus/client_golang/prometheus"

	"pm/internal/server"
)

const (
	flaggedWallet = "0x0ef286b836e4efa2aa3403cfba1baa6c54d8e986"
	unknownWallet = "0x1234567890123456789012345678901234567890"
	tradersPath   = "/internal/v1/traders/"
	boardsPath    = "/internal/v1/leaderboards/"
	calibrationAt = "/internal/v1/calibration"
)

type specCase struct {
	name, method, path string
	// like names the documented path whose operation answers for a path that no operation names
	like   string
	header []string
	status int
	via    http.Handler
	// malformed marks a request that the spec itself rejects
	malformed bool
}

/* Helpers */

func loadSpec(t testing.TB) *openapi3.T {
	t.Helper()
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromFile(filepath.Join("..", "..", "pm-api", "openapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func handlerFor(t *testing.T, r server.Reader) http.Handler {
	t.Helper()
	return server.New(r, server.NewMetrics(prometheus.NewRegistry()))
}

func specCases(ok, empty, broken http.Handler) []specCase {
	cases := []specCase{
		{name: "healthz", path: "/healthz", via: ok},
		{name: "readyz", path: "/readyz", via: ok},
		{name: "status", path: statusPath, via: ok},
		{name: "trader in mixed case", path: tradersPath + goldWallet, via: ok},
		{name: "trader in lowercase", path: tradersPath + strings.ToLower(goldWallet), via: ok},
		{name: "rated trader", path: tradersPath + ratedWallet, via: ok},
		{name: "trader with open positions", path: tradersPath + cutoffWallet, via: ok},
		{name: "flagged trader", path: tradersPath + flaggedWallet, via: ok},
		{name: "pnl board", path: boardsPath + "pnl", via: ok},
		{name: "volume board", path: boardsPath + "volume", via: ok},
		{name: "skill board", path: boardsPath + "skill", via: ok},
		{name: "board page", path: boardsPath + "pnl?limit=2&offset=1", via: ok},
		{name: "board past its end", path: boardsPath + "volume?limit=100&offset=10000", via: ok},
		{name: "default calibration", path: calibrationAt, via: ok},
	}
	for _, horizon := range []string{"4h", "12h", "1d", "1w"} {
		for _, kind := range []string{"other", "updown", "unknown", "all"} {
			cases = append(cases, specCase{name: "calibration " + horizon + " " + kind, path: calibrationAt + "?horizon=" + horizon + "&market_kind=" + kind, via: ok})
		}
	}
	bad := http.StatusBadRequest
	cases = append(cases,
		specCase{name: "short wallet", path: tradersPath + "0x12", status: bad, malformed: true, via: ok},
		specCase{name: "wallet without a prefix", path: tradersPath + strings.Repeat("a", 40), status: bad, malformed: true, via: ok},
		specCase{name: "unknown board", path: boardsPath + "nope", status: bad, malformed: true, via: ok},
		specCase{name: "limit below 1", path: boardsPath + "pnl?limit=0", status: bad, malformed: true, via: ok},
		specCase{name: "limit above 100", path: boardsPath + "pnl?limit=101", status: bad, malformed: true, via: ok},
		specCase{name: "limit that is not a number", path: boardsPath + "pnl?limit=abc", status: bad, malformed: true, via: ok},
		specCase{name: "negative offset", path: boardsPath + "pnl?offset=-1", status: bad, malformed: true, via: ok},
		specCase{name: "offset above 10000", path: boardsPath + "pnl?offset=10001", status: bad, malformed: true, via: ok},
		specCase{name: "unknown horizon", path: calibrationAt + "?horizon=2h", status: bad, malformed: true, via: ok},
		specCase{name: "unknown market kind", path: calibrationAt + "?market_kind=nope", status: bad, malformed: true, via: ok},

		specCase{name: "unknown wallet", path: tradersPath + unknownWallet, status: http.StatusNotFound, via: ok},
		specCase{name: "unknown route", path: "/nope", like: tradersPath + goldWallet, status: http.StatusNotFound, via: ok},
		specCase{name: "other method", method: http.MethodPost, path: statusPath, like: tradersPath + goldWallet, status: http.StatusNotFound, via: ok},

		specCase{name: "readyz without a database", path: "/readyz", status: http.StatusServiceUnavailable, via: empty},
		specCase{name: "status without a database", path: statusPath, status: http.StatusServiceUnavailable, via: empty},
		specCase{name: "trader without a database", path: tradersPath + goldWallet, status: http.StatusServiceUnavailable, via: empty},
		specCase{name: "board without a database", path: boardsPath + "pnl", status: http.StatusServiceUnavailable, via: empty},
		specCase{name: "calibration without a database", path: calibrationAt, status: http.StatusServiceUnavailable, via: empty},

		specCase{name: "status past its deadline", path: statusPath, header: []string{"Sona-Deadline-Ms", "0"}, status: http.StatusGatewayTimeout, via: ok},
		specCase{name: "trader past its deadline", path: tradersPath + goldWallet, header: []string{"Sona-Deadline-Ms", "0"}, status: http.StatusGatewayTimeout, via: ok},
		specCase{name: "board past its deadline", path: boardsPath + "pnl", header: []string{"Sona-Deadline-Ms", "0"}, status: http.StatusGatewayTimeout, via: ok},
		specCase{name: "calibration past its deadline", path: calibrationAt, header: []string{"Sona-Deadline-Ms", "0"}, status: http.StatusGatewayTimeout, via: ok},

		specCase{name: "status that fails", path: statusPath, status: http.StatusInternalServerError, via: broken},
		specCase{name: "trader that fails", path: tradersPath + goldWallet, status: http.StatusInternalServerError, via: broken},
		specCase{name: "board that fails", path: boardsPath + "pnl", status: http.StatusInternalServerError, via: broken},
		specCase{name: "calibration that fails", path: calibrationAt, status: http.StatusInternalServerError, via: broken},
	)
	return cases
}

// otherVenue is true for a request that the collector's venues answer, which has no Sona stamp and its own retry time
func otherVenue(path string, req *http.Request) bool {
	if strings.HasSuffix(path, "/pm/venues") || strings.HasSuffix(path, "/history") {
		return true
	}
	v := req.URL.Query().Get("venue")
	return v != "" && v != "polymarket"
}

func checkHeaders(t *testing.T, path string, req *http.Request, rec *httptest.ResponseRecorder) {
	t.Helper()
	h := rec.Header()
	if !requestID.MatchString(h.Get("X-Request-Id")) {
		t.Errorf("X-Request-Id %q is not 16 hex digits", h.Get("X-Request-Id"))
	}
	probe := path == "/healthz" || path == "/readyz"
	// the live feed is a moving present, so it has no block to stamp
	venue := otherVenue(path, req)
	stamped := rec.Code == http.StatusOK && !probe && !strings.Contains(path, "/pm/") && !venue
	for name, want := range map[string]string{"Sona-Finality": "finalized", "Sona-Block": "85948288", "Sona-Block-Time": "1777016620"} {
		got := h.Get(name)
		if stamped && got != want {
			t.Errorf("%s is %q, want %q", name, got, want)
		}
		if !stamped && got != "" {
			t.Errorf("%s is %q on an answer that is not data", name, got)
		}
	}
	problem := strings.HasPrefix(h.Get("Content-Type"), "application/problem+json")
	if problem && h.Get("Cache-Control") != "no-store" {
		t.Errorf("problem Cache-Control %q", h.Get("Cache-Control"))
	}
	if !problem && h.Get("Cache-Control") != "" {
		t.Errorf("Cache-Control %q on an answer that is not a problem", h.Get("Cache-Control"))
	}
	wantRetry := "1"
	if strings.Contains(path, "/pm/") && !venue {
		wantRetry = "10"
	}
	if rec.Code == http.StatusServiceUnavailable && !probe && h.Get("Retry-After") != wantRetry {
		t.Errorf("Retry-After %q, want %s", h.Get("Retry-After"), wantRetry)
	}
}

func checkDeclared(t *testing.T, op *openapi3.Operation, rec *httptest.ResponseRecorder) {
	t.Helper()
	ref := cmp.Or(op.Responses.Status(rec.Code), op.Responses.Default())
	declared := make(map[string]bool)
	for name, header := range ref.Value.Headers {
		declared[http.CanonicalHeaderKey(name)] = true
		if header.Value.Required && rec.Header().Get(name) == "" {
			t.Errorf("%d omits %s, which the spec requires", rec.Code, name)
		}
	}
	for name := range rec.Header() {
		if !declared[name] && name != "Content-Type" && name != "Content-Length" {
			t.Errorf("%d sends %s, which the spec does not declare", rec.Code, name)
		}
	}
}

func firstDiff(path string, got, want any) string {
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			return path + " is not an object"
		}
		for _, k := range slices.Sorted(maps.Keys(w)) {
			if _, ok := g[k]; !ok {
				return path + "." + k + " is missing"
			}
			if d := firstDiff(path+"."+k, g[k], w[k]); d != "" {
				return d
			}
		}
		for _, k := range slices.Sorted(maps.Keys(g)) {
			if _, ok := w[k]; !ok {
				return path + "." + k + " is not in the example"
			}
		}
	case []any:
		g, ok := got.([]any)
		if !ok || len(g) != len(w) {
			return fmt.Sprintf("%s is %v, the example has %d items", path, got, len(w))
		}
		for i := range w {
			if d := firstDiff(fmt.Sprintf("%s[%d]", path, i), g[i], w[i]); d != "" {
				return d
			}
		}
	default:
		if got != want {
			return fmt.Sprintf("%s is %v, the example has %v", path, got, want)
		}
	}
	return ""
}

/* Tests */

func TestSpecIsValid(t *testing.T) {
	doc := loadSpec(t)
	if err := doc.Validate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if doc.OpenAPI != "3.1.2" {
		t.Errorf("openapi %q, want 3.1.2", doc.OpenAPI)
	}
	var urls []string
	for _, s := range doc.Servers {
		urls = append(urls, s.URL)
	}
	if want := []string{"http://127.0.0.1:8191"}; !slices.Equal(urls, want) {
		t.Fatalf("servers %q, want %q", urls, want)
	}
}

func TestSpecOperationsAreTheRoutes(t *testing.T) {
	doc := loadSpec(t)
	var documented []string
	for path, item := range doc.Paths.Map() {
		for method := range item.Operations() {
			documented = append(documented, method+" "+path)
		}
	}
	served := server.Patterns()
	slices.Sort(documented)
	slices.Sort(served)
	if !slices.Equal(documented, served) {
		t.Fatalf("spec has %q, routes are %q", documented, served)
	}
}

func TestSpecValidatesResponses(t *testing.T) {
	doc := loadSpec(t)
	// the server URL is the host address, which the handler under test never sees
	doc.Servers = nil
	router, err := legacy.NewRouter(doc)
	if err != nil {
		t.Fatal(err)
	}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.DiscardHandler))
	t.Cleanup(func() { slog.SetDefault(prev) })
	ok := handlerFor(t, openStore(t, goldenPath(t), time.Hour))
	empty := handlerFor(t, openStore(t, filepath.Join(t.TempDir(), "pm.duckdb"), time.Hour))
	broken := handlerFor(t, failing{err: errors.New("boom")})
	cases := append(specCases(ok, empty, broken), liveSpecCases(liveHandler(t, false), liveHandler(t, true), empty)...)
	cases = append(cases, venueSpecCases(venueHandler(t, fakeVenues{}), venueHandler(t, nil))...)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), cmp.Or(c.method, http.MethodGet), c.path, nil)
			for i := 0; i+1 < len(c.header); i += 2 {
				req.Header.Set(c.header[i], c.header[i+1])
			}
			lookup := req
			if c.like != "" {
				lookup = httptest.NewRequestWithContext(t.Context(), http.MethodGet, c.like, nil)
			}
			route, params, err := router.FindRoute(lookup)
			if err != nil {
				t.Fatal(err)
			}
			in := &openapi3filter.RequestValidationInput{Request: req, PathParams: params, Route: route}
			if c.like == "" {
				if err := openapi3filter.ValidateRequest(t.Context(), in); (err != nil) != c.malformed {
					t.Fatalf("malformed %v, request: %v", c.malformed, err)
				}
			}
			want := cmp.Or(c.status, http.StatusOK)
			rec := httptest.NewRecorder()
			c.via.ServeHTTP(rec, req)
			if rec.Code != want {
				t.Fatalf("status %d, want %d: %s", rec.Code, want, rec.Body)
			}
			out := &openapi3filter.ResponseValidationInput{
				RequestValidationInput: in,
				Status:                 rec.Code,
				Header:                 rec.Header(),
				Options:                &openapi3filter.Options{IncludeResponseStatus: true},
			}
			if err := openapi3filter.ValidateResponse(t.Context(), out.SetBodyBytes(rec.Body.Bytes())); err != nil {
				t.Fatalf("response: %v\n%s", err, rec.Body)
			}
			checkHeaders(t, route.Path, req, rec)
			checkDeclared(t, route.Operation, rec)
		})
	}
}

func TestSpecExamplesMatchHandler(t *testing.T) {
	doc := loadSpec(t)
	h := handlerFor(t, openStore(t, goldenPath(t), time.Hour))
	for _, c := range []struct{ route, example, request string }{
		{"/internal/v1/traders/{wallet}", "golden", tradersPath + goldWallet},
		{"/internal/v1/traders/{wallet}", "rated", tradersPath + ratedWallet},
		{"/internal/v1/traders/{wallet}", "open_positions", tradersPath + cutoffWallet},
		{"/internal/v1/traders/{wallet}", "flagged", tradersPath + flaggedWallet},
		{"/internal/v1/leaderboards/{board}", "pnl", boardsPath + "pnl?limit=3"},
		{"/internal/v1/leaderboards/{board}", "volume", boardsPath + "volume?limit=3"},
		{"/internal/v1/leaderboards/{board}", "skill", boardsPath + "skill"},
		{"/internal/v1/calibration", "default", calibrationAt},
		{"/internal/v1/calibration", "week_all", calibrationAt + "?horizon=1w&market_kind=all"},
	} {
		t.Run(c.route+" "+c.example, func(t *testing.T) {
			item := doc.Paths.Find(c.route)
			if item == nil || item.Get == nil {
				t.Fatalf("the spec has no GET %s", c.route)
			}
			example := item.Get.Responses.Status(http.StatusOK).Value.Content.Get("application/json").Examples[c.example]
			if example == nil {
				t.Fatalf("GET %s has no example %q", c.route, c.example)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, c.request, nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d: %s", rec.Code, rec.Body)
			}
			var got, want any
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(example.Value.Value)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &want); err != nil {
				t.Fatal(err)
			}
			if d := firstDiff("$", got, want); d != "" {
				t.Fatal(d)
			}
		})
	}
}
