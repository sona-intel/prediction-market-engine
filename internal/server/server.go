package server

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"pm/internal/store"
)

const (
	defaultDeadline = 5000 * time.Millisecond
	// nginx's code for a client that closed the request before the answer
	statusClientClosed = 499

	defaultLimit = 50
	maxLimit     = 100
	maxOffset    = 10000
)

type Reader interface {
	Ready() bool
	Status(ctx context.Context) (store.Status, error)
	Trader(ctx context.Context, addr [20]byte) (store.Trader, error)
	Board(ctx context.Context, board string, limit, offset int) (store.Page, error)
	Calibration(ctx context.Context, horizonH int, kind string) (store.Calibration, error)
}

type app struct {
	s    Reader
	feed Feed
}

type build func(ctx context.Context) (body any, through store.Coverage, err error)

type route struct {
	pattern string
	handler http.HandlerFunc
}

func (a *app) routes() []route {
	return []route{
		{"GET /healthz", func(http.ResponseWriter, *http.Request) {}},
		{"GET /readyz", a.readyz},
		{"GET /internal/v1/status", a.status},
		{"GET /internal/v1/traders/{wallet}", a.trader},
		{"GET /internal/v1/leaderboards/{board}", a.leaderboard},
		{"GET /internal/v1/calibration", a.calibration},
		{"GET /internal/v1/pm/markets/top", a.topMarkets},
		{"GET /internal/v1/pm/trades/large", a.largeTrades},
	}
}

// New serves the history only, so the live routes answer 503
func New(s Reader, m *Metrics) http.Handler { return NewLive(s, nil, m) }

func NewLive(s Reader, feed Feed, m *Metrics) http.Handler {
	mux := http.NewServeMux()
	for _, rt := range (&app{s: s, feed: feed}).routes() {
		mux.HandleFunc(rt.pattern, rt.handler)
	}
	mux.HandleFunc("/", notFound)
	return instrument(m, mux)
}

func notFound(w http.ResponseWriter, req *http.Request) {
	writeProblem(w, newProblem(codeNotFound, "No such route.", req.URL.EscapedPath()))
}

func (a *app) readyz(w http.ResponseWriter, _ *http.Request) {
	if !a.s.Ready() {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}

/* Handlers */

func (a *app) status(w http.ResponseWriter, req *http.Request) {
	respond(w, req, func(ctx context.Context) (any, store.Coverage, error) {
		s, err := a.s.Status(ctx)
		if err != nil {
			return nil, store.Coverage{}, err
		}
		return newStatus(s), s.Coverage, nil
	})
}

func (a *app) trader(w http.ResponseWriter, req *http.Request) {
	respond(w, req, func(ctx context.Context) (any, store.Coverage, error) {
		addr, ok := parseWallet(req.PathValue("wallet"))
		if !ok {
			return nil, store.Coverage{}, &requestError{codeInvalidID, "The wallet must be 0x and 40 hex digits."}
		}
		t, err := a.s.Trader(ctx, addr)
		if err != nil {
			return nil, store.Coverage{}, err
		}
		return newTrader(t), t.Coverage, nil
	})
}

func (a *app) leaderboard(w http.ResponseWriter, req *http.Request) {
	respond(w, req, func(ctx context.Context) (any, store.Coverage, error) {
		board := req.PathValue("board")
		if !slices.Contains(store.Boards, board) {
			return nil, store.Coverage{}, &requestError{codeInvalidID, "The board must be pnl, volume or skill."}
		}
		q := req.URL.Query()
		limit, err := intParam(q, "limit", defaultLimit, 1, maxLimit)
		if err != nil {
			return nil, store.Coverage{}, err
		}
		offset, err := intParam(q, "offset", 0, 0, maxOffset)
		if err != nil {
			return nil, store.Coverage{}, err
		}
		p, err := a.s.Board(ctx, board, limit, offset)
		if err != nil {
			return nil, store.Coverage{}, err
		}
		return newBoard(p), p.Coverage, nil
	})
}

var horizons = map[string]int{"4h": 4, "12h": 12, "1d": 24, "1w": 168}

func (a *app) calibration(w http.ResponseWriter, req *http.Request) {
	respond(w, req, func(ctx context.Context) (any, store.Coverage, error) {
		q := req.URL.Query()
		hours, ok := 12, true
		if q.Has("horizon") {
			hours, ok = horizons[q.Get("horizon")]
		}
		if !ok {
			return nil, store.Coverage{}, &requestError{codeInvalidQuery, "The horizon must be 4h, 12h, 1d or 1w."}
		}
		kind := "other"
		if q.Has("market_kind") {
			kind = q.Get("market_kind")
		}
		if !slices.Contains(store.Kinds, kind) {
			return nil, store.Coverage{}, &requestError{codeInvalidQuery, "The market_kind must be other, updown, unknown or all."}
		}
		c, err := a.s.Calibration(ctx, hours, kind)
		if err != nil {
			return nil, store.Coverage{}, err
		}
		return newCalibration(c), c.Coverage, nil
	})
}

/* Responses */

func respond(w http.ResponseWriter, req *http.Request, read build) {
	budget, ok := deadline(req.Header)
	if !ok {
		budget = defaultDeadline
	}
	if budget <= 0 {
		writeProblem(w, problemFor(context.DeadlineExceeded, req.URL.EscapedPath()))
		return
	}
	ctx, cancel := context.WithTimeout(req.Context(), budget)
	defer cancel()
	body, through, err := read(ctx)
	var data []byte
	if err == nil {
		data, err = json.Marshal(body)
	}
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			err = context.DeadlineExceeded
		}
		if errors.Is(err, context.Canceled) && req.Context().Err() != nil {
			w.WriteHeader(statusClientClosed)
			return
		}
		p := problemFor(err, req.URL.EscapedPath())
		if p.Code == codeInternal {
			slog.ErrorContext(ctx, "request failed", "request_id", w.Header().Get("X-Request-Id"), "route", req.Pattern, "error", err)
		}
		writeProblem(w, p)
		return
	}
	h := w.Header()
	h.Set("Sona-Finality", "finalized")
	h.Set("Sona-Block", strconv.FormatInt(through.Block, 10))
	h.Set("Sona-Block-Time", strconv.FormatInt(through.Time, 10))
	h.Set("Content-Type", "application/json")
	h.Set("Content-Length", strconv.Itoa(len(data)))
	_, _ = w.Write(data)
}

func deadline(h http.Header) (time.Duration, bool) {
	ms, err := strconv.ParseInt(h.Get("Sona-Deadline-Ms"), 10, 64)
	if err != nil {
		return 0, false
	}
	const limit = math.MaxInt64 / int64(time.Millisecond)
	return time.Duration(max(min(ms, limit), -limit)) * time.Millisecond, true
}

/* Request parts */

func parseWallet(s string) (addr [20]byte, ok bool) {
	digits, found := strings.CutPrefix(s, "0x")
	if !found || len(digits) != 40 {
		return addr, false
	}
	_, err := hex.Decode(addr[:], []byte(digits))
	return addr, err == nil
}

func intParam(q url.Values, name string, def, lo, hi int) (int, error) {
	if !q.Has(name) {
		return def, nil
	}
	n, err := strconv.Atoi(q.Get(name))
	if err != nil || n < lo || n > hi {
		return 0, &requestError{codeInvalidQuery, "The " + name + " must be a whole number from " + strconv.Itoa(lo) + " to " + strconv.Itoa(hi) + "."}
	}
	return n, nil
}

/* Middleware */

func instrument(m *Metrics, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Request-Id", newRequestID())
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(sw, r)
		route := r.Pattern
		if route == "" || route == "/" {
			route = "unmatched"
		}
		m.observe(route, sw.code(), time.Since(start))
	})
}

func newRequestID() string {
	return hex.EncodeToString(binary.BigEndian.AppendUint64(nil, rand.Uint64()))
}
