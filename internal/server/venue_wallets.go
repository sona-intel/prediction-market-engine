package server

import (
	"context"
	"net/http"
	"slices"

	"pm/internal/hip4"
	"pm/internal/levr"
	"pm/internal/pgstore"
	"pm/internal/skill"
	"pm/internal/store"
	"pm/internal/venue"
)

type hlTrader struct {
	Kind     string        `json:"kind"`
	Venue    string        `json:"venue"`
	Wallet   string        `json:"wallet"`
	Activity hlActivity    `json:"activity"`
	PnL      hlPnL         `json:"pnl"`
	Skill    hlSkill       `json:"skill"`
	Coverage venueCoverage `json:"coverage"`
}

type hlActivity struct {
	Fills   int64  `json:"fills"`
	Markets int    `json:"markets"`
	Volume  amount `json:"volume"`
}

type hlPnL struct {
	Realized amount  `json:"realized"`
	Gross    amount  `json:"gross"`
	Fees     amount  `json:"fees"`
	Resolved int     `json:"resolved_markets"`
	Won      int     `json:"won_markets"`
	WinRate  winRate `json:"win_rate"`
}

type hlSkill struct {
	Rated  bool     `json:"rated"`
	Floor  floor    `json:"floor"`
	Brier  *float64 `json:"brier"`
	Edge   *float64 `json:"edge"`
	EdgeSE *float64 `json:"edge_se"`
}

func newHLSkill(w hip4.Wallet) hlSkill {
	s := hlSkill{Rated: w.Rated, Floor: floor{Markets: skill.MinMarkets, Events: skill.MinEvents}}
	if w.Rated {
		s.Brier, s.Edge, s.EdgeSE = &w.Brier, &w.Edge, &w.EdgeSE
	}
	return s
}

func newHLPnL(w hip4.Wallet) hlPnL {
	return hlPnL{
		Realized: usdc(w.Gross), Gross: usdc(w.Gross), Fees: usdc(0), Resolved: w.Resolved, Won: w.Won,
		WinRate: newWinRate(int64(w.Won), int64(w.Resolved)),
	}
}

type levrTrader struct {
	Kind            string        `json:"kind"`
	Venue           string        `json:"venue"`
	Wallet          string        `json:"wallet"`
	Positions       int           `json:"positions"`
	Open            int           `json:"open"`
	Won             int           `json:"won"`
	Lost            int           `json:"lost"`
	Liquidated      int           `json:"liquidated"`
	Voided          int           `json:"voided"`
	Stake           amount        `json:"stake"`
	Size            amount        `json:"size"`
	Leverage        float64       `json:"leverage"`
	LiquidationRate float64       `json:"liquidation_rate"`
	WinRate         float64       `json:"win_rate"`
	Realized        amount        `json:"realized"`
	FirstPosition   string        `json:"first_position"`
	LastPosition    string        `json:"last_position"`
	Coverage        venueCoverage `json:"coverage"`
}

func (a *app) venueTrader(w http.ResponseWriter, req *http.Request, code string) {
	a.answerVenue(w, req, func(ctx context.Context) (any, error) {
		addr, ok := parseWallet(req.PathValue("wallet"))
		if !ok {
			return nil, &requestError{codeInvalidID, "The wallet must be 0x and 40 hex digits."}
		}
		switch code {
		case venue.Kalshi, venue.PolymarketUS:
			return nil, &requestError{codeInvalidQuery, noTraderIDs}
		case venue.Hyperliquid:
			return a.hlTrader(ctx, addr)
		}
		return a.levrTrader(ctx, addr)
	})
}

func (a *app) hlTrader(ctx context.Context, addr [20]byte) (any, error) {
	w, err := a.v.HLWallet(ctx, addr[:])
	if err != nil {
		return nil, err
	}
	cov, err := a.coverage(ctx, venue.Hyperliquid)
	if err != nil {
		return nil, err
	}
	return hlTrader{
		Kind: "pm_trader", Venue: venue.Hyperliquid, Wallet: address(addr[:]),
		Activity: hlActivity{Fills: w.Fills, Markets: w.Markets, Volume: usdc(w.Volume)},
		PnL:      newHLPnL(w), Skill: newHLSkill(w), Coverage: cov,
	}, nil
}

func (a *app) levrTrader(ctx context.Context, addr [20]byte) (any, error) {
	ps, err := a.v.WalletPositions(ctx, addr[:])
	if err != nil {
		return nil, err
	}
	if len(ps) == 0 {
		return nil, pgstore.ErrNotFound
	}
	cov, err := a.coverage(ctx, venue.Levr)
	if err != nil {
		return nil, err
	}
	s := levr.Summarize(ps)
	return levrTrader{
		Kind: "pm_levr_trader", Venue: venue.Levr, Wallet: address(addr[:]),
		Positions: s.Positions, Open: s.Positions - s.Settled() - s.Voided, Won: s.Won, Lost: s.Lost, Liquidated: s.Liquidated, Voided: s.Voided,
		Stake: usdc(s.Stake), Size: usdc(s.Size), Leverage: round4(s.Leverage()), LiquidationRate: round4(s.LiquidationRate()), WinRate: round4(s.WinRate()),
		Realized: usdc(s.Realized), FirstPosition: rfc3339(ps[0].Opened.Unix()), LastPosition: rfc3339(ps[len(ps)-1].Opened.Unix()), Coverage: cov,
	}, nil
}

/* Leaderboards */

type hlBoard struct {
	Kind     string        `json:"kind"`
	Venue    string        `json:"venue"`
	Board    string        `json:"board"`
	Limit    int           `json:"limit"`
	Offset   int           `json:"offset"`
	Total    int64         `json:"total"`
	Rows     []hlBoardRow  `json:"rows"`
	Coverage venueCoverage `json:"coverage"`
}

type hlBoardRow struct {
	Rank     int           `json:"rank"`
	Wallet   string        `json:"wallet"`
	Fills    int64         `json:"fills"`
	Markets  int           `json:"markets"`
	Volume   amount        `json:"volume"`
	Realized amount        `json:"realized"`
	Skill    *hlBoardSkill `json:"skill"`
}

type hlBoardSkill struct {
	Brier  float64 `json:"brier"`
	Edge   float64 `json:"edge"`
	EdgeLB float64 `json:"edge_lb"`
}

type levrBoard struct {
	Kind     string         `json:"kind"`
	Venue    string         `json:"venue"`
	Board    string         `json:"board"`
	Limit    int            `json:"limit"`
	Offset   int            `json:"offset"`
	Total    int64          `json:"total"`
	Rows     []levrBoardRow `json:"rows"`
	Coverage venueCoverage  `json:"coverage"`
}

type levrBoardRow struct {
	Rank            int     `json:"rank"`
	Wallet          string  `json:"wallet"`
	Positions       int     `json:"positions"`
	Stake           amount  `json:"stake"`
	Size            amount  `json:"size"`
	Leverage        float64 `json:"leverage"`
	Liquidations    int     `json:"liquidations"`
	LiquidationRate float64 `json:"liquidation_rate"`
	WinRate         float64 `json:"win_rate"`
	Realized        amount  `json:"realized"`
}

func (a *app) venueBoard(w http.ResponseWriter, req *http.Request, code string) {
	a.answerVenue(w, req, func(ctx context.Context) (any, error) {
		if err := onlyVenues(code, noTraderIDs, venue.Hyperliquid, venue.Levr); err != nil {
			return nil, err
		}
		board := req.PathValue("board")
		if !slices.Contains(store.Boards, board) {
			return nil, &requestError{codeInvalidID, "The board must be pnl, volume or skill."}
		}
		// the board exists, so a venue without it is a query the venue cannot answer
		if code == venue.Levr && board == "skill" {
			return nil, &requestError{codeInvalidQuery, "Levr has no skill board, only pnl and volume."}
		}
		q := req.URL.Query()
		limit, err := intParam(q, "limit", defaultLimit, 1, maxLimit)
		if err != nil {
			return nil, err
		}
		offset, err := intParam(q, "offset", 0, 0, maxOffset)
		if err != nil {
			return nil, err
		}
		if code == venue.Hyperliquid {
			return a.hlBoard(ctx, board, limit, offset)
		}
		return a.levrBoard(ctx, board, limit, offset)
	})
}

func (a *app) hlBoard(ctx context.Context, board string, limit, offset int) (any, error) {
	p, err := a.v.HLBoard(ctx, board, limit, offset)
	if err != nil {
		return nil, err
	}
	cov, err := a.coverage(ctx, venue.Hyperliquid)
	if err != nil {
		return nil, err
	}
	out := hlBoard{Kind: "pm_leaderboard", Venue: venue.Hyperliquid, Board: board, Limit: limit, Offset: offset, Total: p.Total, Rows: make([]hlBoardRow, 0, len(p.Rows)), Coverage: cov}
	for i, r := range p.Rows {
		row := hlBoardRow{Rank: offset + i + 1, Wallet: address(r.Address), Fills: r.Fills, Markets: r.Markets, Volume: usdc(r.Volume), Realized: usdc(r.Gross)}
		if r.Rated {
			row.Skill = &hlBoardSkill{Brier: r.Brier, Edge: r.Edge, EdgeLB: r.Edge - skill.Z*r.EdgeSE}
		}
		out.Rows = append(out.Rows, row)
	}
	return out, nil
}

func (a *app) levrBoard(ctx context.Context, board string, limit, offset int) (any, error) {
	p, err := a.v.LevrBoard(ctx, board, limit, offset)
	if err != nil {
		return nil, err
	}
	cov, err := a.coverage(ctx, venue.Levr)
	if err != nil {
		return nil, err
	}
	out := levrBoard{Kind: "pm_leaderboard", Venue: venue.Levr, Board: board, Limit: limit, Offset: offset, Total: p.Total, Rows: make([]levrBoardRow, 0, len(p.Rows)), Coverage: cov}
	for i, r := range p.Rows {
		out.Rows = append(out.Rows, levrBoardRow{
			Rank: offset + i + 1, Wallet: address(r.Address), Positions: r.Positions, Stake: usdc(r.Stake), Size: usdc(r.Size),
			Leverage: round4(r.Leverage()), Liquidations: r.Liquidated, LiquidationRate: round4(r.LiquidationRate()), WinRate: round4(r.WinRate()), Realized: usdc(r.Realized),
		})
	}
	return out, nil
}
