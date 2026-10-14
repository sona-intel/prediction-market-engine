package server_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"

	"pm/internal/hip4"
	"pm/internal/levr"
	"pm/internal/pgstore"
	"pm/internal/server"
	"pm/internal/venue"
)

const (
	venuesPath   = "/internal/v1/pm/venues"
	historyPath  = "/internal/v1/pm/markets/"
	hlWallet     = "0x91c4374f795110c4f39806ad4ed46657b1f507e7"
	levrWallet   = "0x1111111111111111111111111111111111111111"
	missingAddr  = "0x2222222222222222222222222222222222222222"
	kalshiMarket = "KXTEST-26OCT15-A"
)

var (
	at    = time.Date(2026, 10, 14, 22, 30, 0, 0, time.UTC)
	since = time.Date(2026, 10, 14, 20, 0, 0, 0, time.UTC)
)

func hlBytes() []byte {
	b, err := hex.DecodeString(hlWallet[2:])
	if err != nil {
		panic(err)
	}
	return b
}

// fakeVenues answers the way pm-postgres would for a few rows, and err makes every read fail
type fakeVenues struct{ err error }

func (f fakeVenues) Venues(context.Context) ([]venue.Info, error) {
	if f.err != nil {
		return nil, f.err
	}
	return []venue.Info{
		{Code: venue.Hyperliquid, Name: "Hyperliquid HIP-4", Quote: "USDC", Wallets: true, Since: since, LastOK: at},
		{Code: venue.Kalshi, Name: "Kalshi", Quote: "USD", Since: since, LastOK: at},
		{Code: venue.Levr, Name: "Levr Bet", Quote: "USDC", Wallets: true, Since: since},
		{Code: venue.PolymarketUS, Name: "Polymarket US", Quote: "USD", Since: since, LastOK: at},
	}, nil
}

func (f fakeVenues) Gaps(_ context.Context, code string) (int64, bool, error) {
	return 2, code == venue.Hyperliquid || code == venue.Kalshi, f.err
}

func (f fakeVenues) top(code string) []pgstore.TopMarket {
	m := venue.Market{Venue: code, ID: kalshiMarket, Title: "Will it rain?", Kind: "KXTEST", Close: at.Add(24 * time.Hour), Status: "open"}
	last := venue.Snap{Venue: code, MarketID: m.ID, TS: at, YesPPM: 455000, Volume: 120_000_000, OI: 40_000_000, Vol24: 9_500_000}
	return []pgstore.TopMarket{{Market: m, Last: last}}
}

func (f fakeVenues) TopRecent(_ context.Context, code string, _ int) ([]pgstore.TopMarket, error) {
	return f.top(code), f.err
}

func (f fakeVenues) TopTape(_ context.Context, code string, _ time.Time, _ int) ([]pgstore.TopMarket, error) {
	ms := f.top(code)
	ms[0].ID, ms[0].Result, ms[0].Status = "#102220", "yes", "resolved"
	return ms, f.err
}

func (f fakeVenues) LargeNamed(_ context.Context, code string, minUSD int64, _ int) ([]pgstore.NamedTrade, error) {
	t := pgstore.NamedTrade{
		Title: "Will it rain?", Venue: code, ID: "t1", MarketID: kalshiMarket, TS: at, Side: "no", PricePPM: 510000, Qty: 60_000_000_000, USD: 29_400_000_000,
	}
	if code == venue.Hyperliquid {
		t.Taker, t.Maker = hlBytes(), []byte{1}
	}
	if minUSD > t.USD {
		return nil, f.err
	}
	return []pgstore.NamedTrade{t}, f.err
}

func (f fakeVenues) Market(_ context.Context, code, id string) (venue.Market, bool, error) {
	if id != kalshiMarket && id != "#102220" {
		return venue.Market{}, false, f.err
	}
	return f.top(code)[0].Market, true, f.err
}

func (f fakeVenues) Snaps(_ context.Context, code, id string, _ time.Time, _ int) ([]venue.Snap, error) {
	return []venue.Snap{
		{Venue: code, MarketID: id, TS: at.Add(-10 * time.Minute), YesPPM: 450000, Volume: 100_000_000, OI: 30_000_000, Vol24: 9_000_000},
		{Venue: code, MarketID: id, TS: at, YesPPM: 455000, Volume: 120_000_000, OI: 40_000_000, Vol24: 9_500_000},
	}, f.err
}

func (f fakeVenues) TapeSeries(_ context.Context, code, id string, _ time.Time, _ time.Duration, _ int) ([]venue.Snap, error) {
	return []venue.Snap{{Venue: code, MarketID: id, TS: at, YesPPM: 455000}}, f.err
}

func (f fakeVenues) HLWallet(_ context.Context, addr []byte) (hip4.Wallet, error) {
	if f.err != nil {
		return hip4.Wallet{}, f.err
	}
	if addr[0] != 0x91 {
		return hip4.Wallet{}, pgstore.ErrNotFound
	}
	return hip4.Wallet{Address: addr, Fills: 40, Volume: 900_000_000, Gross: 25_500_000, Markets: 6, Resolved: 4, Won: 3, Rated: true, Brier: 0.19, Edge: 0.04, EdgeSE: 0.01}, nil
}

func (f fakeVenues) HLBoard(_ context.Context, _ string, _, _ int) (pgstore.HLPage, error) {
	w, _ := f.HLWallet(context.Background(), hlBytes())
	return pgstore.HLPage{Rows: []hip4.Wallet{w, {Address: bytes.Repeat([]byte{2}, 20), Fills: 1}}, Total: 2}, f.err
}

func (f fakeVenues) WalletPositions(_ context.Context, wallet []byte) ([]levr.Position, error) {
	if wallet[0] != 0x11 {
		return nil, f.err
	}
	pay := int64(7_000_000)
	return []levr.Position{
		{ID: "p1", Opened: at, Stake: 5_000_000, Size: 25_000_000, Status: levr.Won, Payout: &pay},
		{ID: "p2", Opened: at.Add(time.Hour), Stake: 2_000_000, Size: 10_000_000, Status: levr.Liquidated},
		{ID: "p3", Opened: at.Add(2 * time.Hour), Stake: 1_000_000, Size: 3_000_000, Status: levr.Open},
	}, f.err
}

func (f fakeVenues) LevrBoard(_ context.Context, _ string, _, _ int) (pgstore.LevrPage, error) {
	w := levr.Wallet{Address: bytes.Repeat([]byte{0x11}, 20), Positions: 3, Won: 1, Liquidated: 1, Stake: 8_000_000, Size: 38_000_000, Realized: -2_000_000}
	return pgstore.LevrPage{Rows: []levr.Wallet{w}, Total: 1}, f.err
}

func venueHandler(t testing.TB, v server.Venues) http.Handler {
	t.Helper()
	f := newFakePolymarket(t)
	p := f.poller()
	p.Poll(t.Context())
	return server.NewFull(failing{}, p, v, server.NewMetrics(prometheus.NewRegistry()))
}

func venueServer(t *testing.T, v server.Venues) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(venueHandler(t, v))
	t.Cleanup(srv.Close)
	return srv
}

func TestVenueDefaultsToPolymarketWithTheSameBytes(t *testing.T) {
	srv := venueServer(t, fakeVenues{})
	for _, path := range []string{topPath, largePath, topPath + "?limit=2", largePath + "?min_usd=13300"} {
		plain := get(t, srv, path)
		sep := "?"
		if strings.Contains(path, "?") {
			sep = "&"
		}
		named := get(t, srv, path+sep+"venue=polymarket")
		if plain.status != 200 || named.status != 200 || string(plain.body) != string(named.body) {
			t.Errorf("%s\n %s\n %s", path, plain.body, named.body)
		}
	}
}

func TestUnknownVenueIsRejectedOnEveryRoute(t *testing.T) {
	srv := venueServer(t, fakeVenues{})
	for _, path := range []string{topPath, largePath, tradersPath + hlWallet, boardsPath + "pnl"} {
		expectProblem(t, get(t, srv, path+"?venue=nope"), http.StatusBadRequest, "invalid_query")
		expectProblem(t, get(t, srv, path+"?venue="), http.StatusBadRequest, "invalid_query")
	}
}

func TestVenuesBody(t *testing.T) {
	r := get(t, venueServer(t, fakeVenues{}), venuesPath)
	if r.status != 200 || r.header.Get("Sona-Block") != "" {
		t.Fatalf("%d %v", r.status, r.header)
	}
	items := dig(t, r.json(t), "items").([]any)
	var codes []string
	for _, it := range items {
		codes = append(codes, it.(map[string]any)["code"].(string))
	}
	if len(codes) != 5 || codes[0] != "polymarket" || codes[1] != "hyperliquid" || codes[4] != "polymarket-us" {
		t.Fatalf("%v", codes)
	}
	if dig(t, items[0], "last_ok") != "2026-10-13T12:00:00Z" || dig(t, items[3], "last_ok") != nil || dig(t, items[2], "mode") != "partial_tape" && dig(t, items[2], "mode") != "recorded" {
		t.Fatalf("%v", items)
	}
	if dig(t, items[2], "code") != "kalshi" || dig(t, items[2], "mode") != "partial_tape" || dig(t, items[2], "wallets") != false {
		t.Fatalf("%v", items[2])
	}
}

func TestTopMarketsOfARecordedVenue(t *testing.T) {
	srv := venueServer(t, fakeVenues{})
	body := get(t, srv, topPath+"?venue=kalshi").json(t)
	if dig(t, body, "kind") != "pm_top_markets" || dig(t, body, "venue") != "kalshi" || dig(t, body, "source") != "kalshi-api" || dig(t, body, "as_of") != "2026-10-14T22:30:00Z" {
		t.Fatalf("%v", body)
	}
	item := dig(t, body, "items", 0)
	for k, want := range map[string]any{
		"market_id": kalshiMarket, "title": "Will it rain?", "yes_price": 0.455, "volume_usd": float64(120), "open_interest_usd": float64(40),
		"volume_24h_usd": 9.5, "result": nil, "close_time": "2026-10-15T22:30:00Z",
	} {
		if got := dig(t, item, k); got != want {
			t.Errorf("%s is %v, want %v", k, got, want)
		}
	}
	if gaps := dig(t, body, "coverage", "gaps"); gaps != float64(2) || dig(t, body, "coverage", "mode") != "partial_tape" {
		t.Errorf("coverage %v", dig(t, body, "coverage"))
	}
	// the gateway reports no 24-hour volume and the tape venue no totals, so those stay null rather than zero
	if dig(t, dig(t, get(t, srv, topPath+"?venue=polymarket-us").json(t), "items", 0), "volume_24h_usd") != nil {
		t.Error("polymarket-us 24-hour volume should be null")
	}
	hl := dig(t, get(t, srv, topPath+"?venue=hyperliquid").json(t), "items", 0)
	if dig(t, hl, "volume_usd") != nil || dig(t, hl, "open_interest_usd") != nil || dig(t, hl, "result") != "yes" || dig(t, hl, "volume_24h_usd") != 9.5 {
		t.Errorf("%v", hl)
	}
	expectProblem(t, get(t, srv, topPath+"?venue=levr"), http.StatusBadRequest, "invalid_query")
	expectProblem(t, get(t, srv, topPath+"?venue=kalshi&limit=51"), http.StatusBadRequest, "invalid_query")
}

func TestLargeTradesOfARecordedVenue(t *testing.T) {
	srv := venueServer(t, fakeVenues{})
	k := get(t, srv, largePath+"?venue=kalshi").json(t)
	item := dig(t, k, "items", 0)
	if dig(t, item, "wallet") != nil || dig(t, item, "usd") != float64(29400) || dig(t, item, "yes_price") != 0.51 || dig(t, item, "side") != "no" || dig(t, item, "size") != float64(60000) {
		t.Fatalf("%v", item)
	}
	if hl := dig(t, get(t, srv, largePath+"?venue=hyperliquid").json(t), "items", 0, "wallet"); hl != hlWallet {
		t.Fatalf("wallet %v", hl)
	}
	if items := dig(t, get(t, srv, largePath+"?venue=kalshi&min_usd=50000").json(t), "items").([]any); len(items) != 0 {
		t.Fatalf("%v", items)
	}
	expectProblem(t, get(t, srv, largePath+"?venue=polymarket-us"), http.StatusBadRequest, "invalid_query")
	expectProblem(t, get(t, srv, largePath+"?venue=kalshi&min_usd=9999"), http.StatusBadRequest, "invalid_query")
}

func TestMarketHistory(t *testing.T) {
	srv := venueServer(t, fakeVenues{})
	body := get(t, srv, historyPath+"kalshi/"+kalshiMarket+"/history").json(t)
	if dig(t, body, "kind") != "pm_market_history" || dig(t, body, "market", "title") != "Will it rain?" {
		t.Fatalf("%v", body)
	}
	items := dig(t, body, "items").([]any)
	if len(items) != 2 || dig(t, items[0], "time") != "2026-10-14T22:20:00Z" || dig(t, items[1], "yes_price") != 0.455 || dig(t, items[1], "volume_24h_usd") != 9.5 {
		t.Fatalf("%v", items)
	}
	// a tape venue has only prices
	hl := dig(t, get(t, srv, historyPath+"hyperliquid/%23102220/history?since=2026-10-14T00:00:00Z&limit=5").json(t), "items", 0)
	if dig(t, hl, "yes_price") != 0.455 || dig(t, hl, "volume_usd") != nil {
		t.Fatalf("%v", hl)
	}
	expectProblem(t, get(t, srv, historyPath+"kalshi/NOPE/history"), http.StatusNotFound, "not_found")
	for _, p := range []string{"polymarket/x/history", "levr/x/history", "nope/x/history", "kalshi/" + kalshiMarket + "/history?since=yesterday", "kalshi/" + kalshiMarket + "/history?limit=1001", "kalshi/" + kalshiMarket + "/history?limit=0"} {
		expectProblem(t, get(t, srv, historyPath+p), http.StatusBadRequest, "invalid_query")
	}
}

func TestHyperliquidTraderAndBoard(t *testing.T) {
	srv := venueServer(t, fakeVenues{})
	body := get(t, srv, tradersPath+hlWallet+"?venue=hyperliquid").json(t)
	if dig(t, body, "kind") != "pm_trader" || dig(t, body, "venue") != "hyperliquid" || raw(t, body, "pnl", "gross") != 25_500_000 || raw(t, body, "activity", "volume") != 900_000_000 {
		t.Fatalf("%v", body)
	}
	if dig(t, body, "skill", "rated") != true || dig(t, body, "skill", "edge") != 0.04 || dig(t, body, "pnl", "win_rate", "shown") != false {
		t.Fatalf("%v", body)
	}
	expectProblem(t, get(t, srv, tradersPath+missingAddr+"?venue=hyperliquid"), http.StatusNotFound, "not_found")
	expectProblem(t, get(t, srv, tradersPath+"0x12?venue=hyperliquid"), http.StatusBadRequest, "invalid_id")
	board := get(t, srv, boardsPath+"skill?venue=hyperliquid").json(t)
	if dig(t, board, "venue") != "hyperliquid" || dig(t, board, "total") != float64(2) || dig(t, board, "rows", 0, "rank") != float64(1) || dig(t, board, "rows", 1, "rank") != float64(2) {
		t.Fatalf("%v", board)
	}
	if lb := dig(t, board, "rows", 0, "skill", "edge_lb").(float64); lb < 0.0203 || lb > 0.0205 {
		t.Errorf("edge_lb %v", lb)
	}
	if dig(t, board, "rows", 1, "skill") != nil {
		t.Errorf("unrated row has skill %v", board)
	}
}

func TestLevrTraderAndBoard(t *testing.T) {
	srv := venueServer(t, fakeVenues{})
	body := get(t, srv, tradersPath+levrWallet+"?venue=levr").json(t)
	want := map[string]any{
		"kind": "pm_levr_trader", "venue": "levr", "positions": float64(3), "open": float64(1), "won": float64(1), "lost": float64(0),
		"liquidated": float64(1), "voided": float64(0), "leverage": 4.75, "liquidation_rate": 0.5, "win_rate": 0.5,
		"first_position": "2026-10-14T22:30:00Z", "last_position": "2026-10-15T00:30:00Z",
	}
	for k, w := range want {
		if got := dig(t, body, k); got != w {
			t.Errorf("%s is %v, want %v", k, got, w)
		}
	}
	if raw(t, body, "realized") != 0 || raw(t, body, "stake") != 8_000_000 {
		t.Errorf("%v", body)
	}
	expectProblem(t, get(t, srv, tradersPath+missingAddr+"?venue=levr"), http.StatusNotFound, "not_found")
	board := get(t, srv, boardsPath+"volume?venue=levr&limit=5").json(t)
	if dig(t, board, "rows", 0, "liquidation_rate") != 0.5 || raw(t, board, "rows", 0, "size") != 38_000_000 || dig(t, board, "limit") != float64(5) {
		t.Fatalf("%v", board)
	}
	expectProblem(t, get(t, srv, boardsPath+"skill?venue=levr"), http.StatusBadRequest, "invalid_id")
}

func TestVenuesWithoutTraderIDsHaveNoWalletRoutes(t *testing.T) {
	srv := venueServer(t, fakeVenues{})
	for _, v := range []string{"kalshi", "polymarket-us"} {
		expectProblem(t, get(t, srv, tradersPath+hlWallet+"?venue="+v), http.StatusBadRequest, "invalid_query")
		expectProblem(t, get(t, srv, boardsPath+"pnl?venue="+v), http.StatusBadRequest, "invalid_query")
	}
}

func TestVenueRoutesAnswer503WhenPostgresIsDown(t *testing.T) {
	down := &pgconn.PgError{Code: "57P01"}
	for name, srv := range map[string]*httptest.Server{
		"not configured": venueServer(t, nil),
		"unreachable":    venueServer(t, fakeVenues{err: errors.New("dial tcp: connection refused")}),
		"shutting down":  venueServer(t, fakeVenues{err: down}),
	} {
		for _, path := range []string{
			venuesPath, topPath + "?venue=kalshi", largePath + "?venue=kalshi", historyPath + "kalshi/" + kalshiMarket + "/history",
			tradersPath + hlWallet + "?venue=hyperliquid", tradersPath + levrWallet + "?venue=levr", boardsPath + "pnl?venue=hyperliquid", boardsPath + "pnl?venue=levr",
		} {
			r := get(t, srv, path)
			expectProblem(t, r, http.StatusServiceUnavailable, "upstream_unavailable")
			if r.header.Get("Retry-After") != "1" {
				t.Errorf("%s %s: Retry-After %q", name, path, r.header.Get("Retry-After"))
			}
		}
	}
}

func TestAQueryErrorIsAnInternalFailureNotAnOutage(t *testing.T) {
	srv := venueServer(t, fakeVenues{err: &pgconn.PgError{Code: "42P01", Message: "no such table"}})
	expectProblem(t, get(t, srv, venuesPath), http.StatusInternalServerError, "internal")
}

func TestVenueRoutesHonourAnSpentDeadline(t *testing.T) {
	srv := venueServer(t, fakeVenues{})
	expectProblem(t, get(t, srv, venuesPath, "Sona-Deadline-Ms", "0"), http.StatusGatewayTimeout, "deadline_exceeded")
	expectProblem(t, get(t, srv, topPath+"?venue=kalshi", "Sona-Deadline-Ms", "-1"), http.StatusGatewayTimeout, "deadline_exceeded")
}

func venueSpecCases(ok, down http.Handler) []specCase {
	bad, nf, un := http.StatusBadRequest, http.StatusNotFound, http.StatusServiceUnavailable
	hist := historyPath + "kalshi/" + kalshiMarket + "/history"
	return []specCase{
		{name: "venues", path: venuesPath, via: ok},
		{name: "top markets of polymarket named", path: topPath + "?venue=polymarket", via: ok},
		{name: "top markets of kalshi", path: topPath + "?venue=kalshi&limit=5", via: ok},
		{name: "top markets of polymarket-us", path: topPath + "?venue=polymarket-us", via: ok},
		{name: "top markets of hyperliquid", path: topPath + "?venue=hyperliquid", via: ok},
		{name: "large trades of kalshi", path: largePath + "?venue=kalshi", via: ok},
		{name: "large trades of hyperliquid", path: largePath + "?venue=hyperliquid&limit=3&min_usd=10000", via: ok},
		{name: "large trades of kalshi with no match", path: largePath + "?venue=kalshi&min_usd=1000000", via: ok},
		{name: "kalshi history", path: hist, via: ok},
		{name: "kalshi history window", path: hist + "?since=2026-10-14T00:00:00Z&limit=1000", via: ok},
		{name: "polymarket-us history", path: historyPath + "polymarket-us/" + kalshiMarket + "/history", via: ok},
		{name: "hyperliquid history", path: historyPath + "hyperliquid/%23102220/history", via: ok},
		{name: "hyperliquid trader", path: tradersPath + hlWallet + "?venue=hyperliquid", via: ok},
		{name: "levr trader", path: tradersPath + levrWallet + "?venue=levr", via: ok},
		{name: "hyperliquid pnl board", path: boardsPath + "pnl?venue=hyperliquid", via: ok},
		{name: "hyperliquid skill board", path: boardsPath + "skill?venue=hyperliquid&limit=2", via: ok},
		{name: "levr pnl board", path: boardsPath + "pnl?venue=levr", via: ok},
		{name: "levr volume board", path: boardsPath + "volume?venue=levr&limit=1&offset=0", via: ok},
		{name: "unknown venue", path: topPath + "?venue=nope", status: bad, malformed: true, via: ok},
		{name: "venue without a market list", path: topPath + "?venue=levr", status: bad, via: ok},
		{name: "venue without a trade tape", path: largePath + "?venue=polymarket-us", status: bad, via: ok},
		{name: "history of a venue without snapshots", path: historyPath + "levr/x/history", status: bad, malformed: true, via: ok},
		{name: "history with a bad time", path: hist + "?since=yesterday", status: bad, malformed: true, via: ok},
		{name: "history limit above 1000", path: hist + "?limit=1001", status: bad, malformed: true, via: ok},
		{name: "wallet route of a venue without wallets", path: tradersPath + hlWallet + "?venue=kalshi", status: bad, via: ok},
		{name: "levr skill board", path: boardsPath + "skill?venue=levr", status: bad, via: ok},
		{name: "unknown history market", path: historyPath + "kalshi/NOPE/history", status: nf, via: ok},
		{name: "unknown hyperliquid trader", path: tradersPath + missingAddr + "?venue=hyperliquid", status: nf, via: ok},
		{name: "unknown levr trader", path: tradersPath + missingAddr + "?venue=levr", status: nf, via: ok},
		{name: "venues without postgres", path: venuesPath, status: un, via: down},
		{name: "top markets without postgres", path: topPath + "?venue=kalshi", status: un, via: down},
		{name: "history without postgres", path: hist, status: un, via: down},
		{name: "levr board without postgres", path: boardsPath + "pnl?venue=levr", status: un, via: down},
		{name: "venues past their deadline", path: venuesPath, header: []string{"Sona-Deadline-Ms", "0"}, status: http.StatusGatewayTimeout, via: ok},
	}
}
