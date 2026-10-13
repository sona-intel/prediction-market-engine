package server

import (
	"encoding/hex"
	"math"
	"strconv"
	"strings"
	"time"

	"pm/internal/skill"
	"pm/internal/store"
)

const (
	decimals     = 6
	unit         = 1_000_000
	ppm          = 1e6
	winRateFloor = 30
	algorithm    = "columbia-2"
)

/* Amounts and times */

type amount struct {
	Raw      string `json:"raw"`
	Decimals int    `json:"decimals"`
	Symbol   string `json:"symbol"`
	Value    string `json:"value"`
}

func usdc(raw int64) amount   { return newAmount(raw, "USDC") }
func shares(raw int64) amount { return newAmount(raw, "shares") }

func newAmount(raw int64, symbol string) amount {
	return amount{Raw: strconv.FormatInt(raw, 10), Decimals: decimals, Symbol: symbol, Value: formatAmount(raw)}
}

func formatAmount(raw int64) string {
	// negating the unsigned value is exact for the smallest int64 too
	abs, sign := uint64(raw), ""
	if raw < 0 {
		abs, sign = -abs, "-"
	}
	out := sign + strconv.FormatUint(abs/unit, 10)
	if frac := abs % unit; frac != 0 {
		out += "." + strings.TrimRight(strconv.FormatUint(frac+unit, 10)[1:], "0")
	}
	return out
}

func rfc3339(ts int64) string { return time.Unix(ts, 0).UTC().Format(time.RFC3339) }

func address(b []byte) string { return "0x" + hex.EncodeToString(b) }

func round4(v float64) float64 { return math.Round(v*1e4) / 1e4 }

func ratio(part, whole int64) float64 {
	if whole <= 0 {
		return 0
	}
	return float64(part) / float64(whole)
}

/* Coverage */

type coverage struct {
	Note        string  `json:"note"`
	Through     through `json:"through"`
	Revision    string  `json:"revision"`
	Attribution string  `json:"attribution"`
	LicenseURL  string  `json:"license_url"`
	Modified    string  `json:"modified"`
	SourceURL   string  `json:"source_url"`
}

type through struct {
	Block int64  `json:"block"`
	Time  string `json:"time"`
}

func newCoverage(c store.Coverage) coverage {
	return coverage{
		Note:        c.Note,
		Through:     through{Block: c.Block, Time: rfc3339(c.Time)},
		Revision:    c.Revision,
		Attribution: c.Attribution,
		LicenseURL:  c.LicenseURL,
		Modified:    c.Modified,
		SourceURL:   c.SourceURL,
	}
}

/* Status */

type statusBody struct {
	Kind         string   `json:"kind"`
	Ready        bool     `json:"ready"`
	BuiltAt      string   `json:"built_at"`
	RulesVersion string   `json:"rules_version"`
	Totals       totals   `json:"totals"`
	Coverage     coverage `json:"coverage"`
}

type totals struct {
	Fills           int64 `json:"fills"`
	Wallets         int64 `json:"wallets"`
	RatedWallets    int64 `json:"rated_wallets"`
	Markets         int64 `json:"markets"`
	ResolvedMarkets int64 `json:"resolved_markets"`
}

func newStatus(s store.Status) statusBody {
	return statusBody{
		Kind:         "pm_status",
		Ready:        true,
		BuiltAt:      s.BuiltAt,
		RulesVersion: s.RulesVersion,
		Totals:       totals{s.Fills, s.Wallets, s.RatedWallets, s.Markets, s.ResolvedMarkets},
		Coverage:     newCoverage(s.Coverage),
	}
}

/* Trader */

type traderBody struct {
	Kind      string   `json:"kind"`
	Wallet    string   `json:"wallet"`
	Owner     *string  `json:"owner"`
	OwnerKind *string  `json:"owner_kind"`
	Activity  activity `json:"activity"`
	PnL       pnl      `json:"pnl"`
	Skill     skillOut `json:"skill"`
	Wash      wash     `json:"wash"`
	Coverage  coverage `json:"coverage"`
}

type activity struct {
	Fills       int64  `json:"fills"`
	Markets     int64  `json:"markets"`
	Volume      amount `json:"volume"`
	BuyVolume   amount `json:"buy_volume"`
	TakerFills  int64  `json:"taker_fills"`
	TakerVolume amount `json:"taker_volume"`
	FirstTrade  string `json:"first_trade"`
	LastTrade   string `json:"last_trade"`
}

type pnl struct {
	Realized    amount  `json:"realized"`
	Gross       amount  `json:"gross"`
	Fees        amount  `json:"fees"`
	BiggestWin  amount  `json:"biggest_win"`
	Resolved    int64   `json:"resolved_markets"`
	Won         int64   `json:"won_markets"`
	WinRate     winRate `json:"win_rate"`
	Open        int64   `json:"open_unresolved_positions"`
	Final       bool    `json:"final"`
	AMMExcluded int64   `json:"amm_trades_excluded"`
}

type winRate struct {
	Value       *float64 `json:"value"`
	WilsonLower *float64 `json:"wilson_lower"`
	Shown       bool     `json:"shown"`
	Floor       int      `json:"floor"`
}

type skillOut struct {
	Rated        bool          `json:"rated"`
	Floor        floor         `json:"floor"`
	HorizonHours int           `json:"horizon_hours"`
	Markets      int64         `json:"markets"`
	Events       int64         `json:"events"`
	Brier        *float64      `json:"brier"`
	Edge         *float64      `json:"edge"`
	EdgeSE       *float64      `json:"edge_se"`
	Calibration  []walletCurve `json:"calibration"`
}

type floor struct {
	Markets int `json:"markets"`
	Events  int `json:"events"`
}

type walletCurve struct {
	Lo           float64 `json:"lo"`
	Hi           float64 `json:"hi"`
	Markets      int64   `json:"markets"`
	MeanForecast float64 `json:"mean_forecast"`
	Observed     float64 `json:"observed"`
}

type wash struct {
	Scored        bool     `json:"scored"`
	Score         *float64 `json:"score"`
	PairedVolume  amount   `json:"paired_volume"`
	FlaggedVolume amount   `json:"flagged_volume"`
	FlaggedShare  float64  `json:"flagged_share"`
	Algorithm     string   `json:"algorithm"`
	Evidence      evidence `json:"evidence"`
}

type evidence struct {
	Counterparties []peer      `json:"counterparties"`
	Trades         []washTrade `json:"trades"`
}

type peer struct {
	Wallet  string  `json:"wallet"`
	Score   float64 `json:"score"`
	Markets int64   `json:"markets"`
	Trades  int64   `json:"trades"`
	Shares  amount  `json:"shares"`
}

type washTrade struct {
	ID           string    `json:"id"`
	Time         string    `json:"time"`
	Market       marketRef `json:"market"`
	Counterparty string    `json:"counterparty"`
	Side         string    `json:"side"`
	Outcome      int       `json:"outcome"`
	Shares       amount    `json:"shares"`
	USDC         amount    `json:"usdc"`
	Price        float64   `json:"price"`
}

type marketRef struct {
	Condition string  `json:"condition"`
	Question  *string `json:"question"`
}

func newTrader(t store.Trader) traderBody {
	return traderBody{
		Kind:      "pm_trader",
		Wallet:    address(t.Wallet[:]),
		Owner:     owner(t.Owner),
		OwnerKind: ownerKind(t.OwnerKind),
		Activity: activity{
			Fills: t.Fills, Markets: t.Markets, Volume: usdc(t.VolumeUSDC), BuyVolume: usdc(t.BuyVolumeUSDC),
			TakerFills: t.TakerRows, TakerVolume: usdc(t.TakerVolumeUSDC),
			FirstTrade: rfc3339(t.FirstTS), LastTrade: rfc3339(t.LastTS),
		},
		PnL: pnl{
			Realized: usdc(t.GrossPnL - t.Fees), Gross: usdc(t.GrossPnL), Fees: usdc(t.Fees), BiggestWin: usdc(t.BiggestWin),
			Resolved: t.ResolvedMarkets, Won: t.WonMarkets, WinRate: newWinRate(t.WonMarkets, t.ResolvedMarkets),
			Open: t.OpenUnresolved, Final: t.OpenUnresolved == 0, AMMExcluded: t.AMMTrades,
		},
		Skill:    newSkill(t),
		Wash:     newWash(t),
		Coverage: newCoverage(t.Coverage),
	}
}

func owner(b []byte) *string {
	if len(b) == 0 {
		return nil
	}
	s := address(b)
	return &s
}

func ownerKind(kind uint8) *string {
	var s string
	switch kind {
	case 1:
		s = "proxy"
	case 2:
		s = "safe"
	default:
		return nil
	}
	return &s
}

func newWinRate(won, resolved int64) winRate {
	w := winRate{Floor: winRateFloor}
	if resolved >= winRateFloor {
		value, lower := round4(ratio(won, resolved)), round4(skill.Wilson(int(won), int(resolved)))
		w.Value, w.WilsonLower, w.Shown = &value, &lower, true
	}
	return w
}

func newSkill(t store.Trader) skillOut {
	s := skillOut{
		Rated:        t.Rated,
		Floor:        floor{Markets: skill.MinMarkets, Events: skill.MinEvents},
		HorizonHours: skill.Horizon / 3600,
		Markets:      t.RatedMarkets,
		Events:       t.RatedEvents,
	}
	if !t.Rated {
		return s
	}
	s.Brier, s.Edge, s.EdgeSE = t.Brier, t.Edge, t.EdgeSE
	var by [5]store.WalletBucket
	for _, b := range t.Calibration {
		by[b.Bucket] = b
	}
	for i, b := range by {
		c := walletCurve{Lo: float64(i) / 5, Hi: float64(i+1) / 5, Markets: b.N}
		if b.N > 0 {
			c.MeanForecast, c.Observed = float64(b.SumF)/float64(b.N)/ppm, float64(b.SumO)/float64(b.N)/ppm
		}
		s.Calibration = append(s.Calibration, c)
	}
	return s
}

func newWash(t store.Trader) wash {
	w := wash{
		Scored:        t.WashScore != nil,
		Score:         t.WashScore,
		PairedVolume:  shares(t.PairedShares),
		FlaggedVolume: shares(t.WashShares),
		FlaggedShare:  ratio(t.WashShares, t.PairedShares),
		Algorithm:     algorithm,
	}
	for _, c := range t.Counterparties {
		w.Evidence.Counterparties = append(w.Evidence.Counterparties, peer{
			Wallet: address(c.Wallet[:]), Score: c.Score, Markets: c.Markets, Trades: c.Trades, Shares: shares(c.Shares),
		})
	}
	for _, x := range t.Trades {
		side := "sell"
		if x.Buy {
			side = "buy"
		}
		price := 0.0
		if x.Shares > 0 {
			price = float64(x.USDC) / float64(x.Shares)
		}
		w.Evidence.Trades = append(w.Evidence.Trades, washTrade{
			ID:           "137_" + strconv.FormatInt(x.Block, 10) + "_" + strconv.FormatInt(x.LogIndex, 10),
			Time:         rfc3339(x.TS),
			Market:       marketRef{Condition: address(x.Condition[:]), Question: x.Question},
			Counterparty: address(x.Counterparty[:]),
			Side:         side,
			Outcome:      x.Outcome,
			Shares:       shares(x.Shares),
			USDC:         usdc(x.USDC),
			Price:        price,
		})
	}
	return w
}

/* Leaderboard */

type boardBody struct {
	Kind     string     `json:"kind"`
	Board    string     `json:"board"`
	Limit    int        `json:"limit"`
	Offset   int        `json:"offset"`
	Total    int64      `json:"total"`
	Rows     []boardRow `json:"rows"`
	Coverage coverage   `json:"coverage"`
}

type boardRow struct {
	Rank      int         `json:"rank"`
	Wallet    string      `json:"wallet"`
	Fills     int64       `json:"fills"`
	Markets   int64       `json:"markets"`
	Volume    amount      `json:"volume"`
	Realized  amount      `json:"realized"`
	Final     bool        `json:"final"`
	WashShare float64     `json:"wash_share"`
	Skill     *boardSkill `json:"skill"`
}

type boardSkill struct {
	Markets int64   `json:"markets"`
	Events  int64   `json:"events"`
	Brier   float64 `json:"brier"`
	Edge    float64 `json:"edge"`
	EdgeLB  float64 `json:"edge_lb"`
}

func newBoard(p store.Page) boardBody {
	b := boardBody{
		Kind: "pm_leaderboard", Board: p.Board, Limit: p.Limit, Offset: p.Offset, Total: p.Total,
		Rows: make([]boardRow, 0, len(p.Rows)), Coverage: newCoverage(p.Coverage),
	}
	for _, r := range p.Rows {
		row := boardRow{
			Rank: r.Rank, Wallet: address(r.Wallet[:]), Fills: r.Fills, Markets: r.Markets,
			Volume: usdc(r.VolumeUSDC), Realized: usdc(r.Realized), Final: r.Final, WashShare: ratio(r.WashShares, r.PairedShares),
		}
		if r.Rated {
			row.Skill = &boardSkill{Markets: r.RatedMarkets, Events: r.RatedEvents, Brier: r.Brier, Edge: r.Edge, EdgeLB: r.EdgeLB}
		}
		b.Rows = append(b.Rows, row)
	}
	return b
}

/* Calibration */

type calibrationBody struct {
	Kind         string        `json:"kind"`
	HorizonHours int           `json:"horizon_hours"`
	MarketKind   string        `json:"market_kind"`
	Markets      int64         `json:"markets"`
	Brier        *float64      `json:"brier"`
	Buckets      []priceBucket `json:"buckets"`
	Excluded     excluded      `json:"excluded"`
	Coverage     coverage      `json:"coverage"`
}

type priceBucket struct {
	Lo        float64 `json:"lo"`
	Hi        float64 `json:"hi"`
	Markets   int64   `json:"markets"`
	MeanPrice float64 `json:"mean_price"`
	Observed  float64 `json:"observed"`
}

type excluded struct {
	NoPrice int64 `json:"no_price"`
}

func newCalibration(c store.Calibration) calibrationBody {
	b := calibrationBody{
		Kind: "pm_calibration", HorizonHours: c.HorizonHours, MarketKind: c.Kind, Markets: c.Markets,
		Buckets: make([]priceBucket, 0, len(c.Buckets)), Excluded: excluded{NoPrice: c.NoPrice}, Coverage: newCoverage(c.Coverage),
	}
	if c.Markets > 0 {
		brier := c.SumSq / float64(c.Markets)
		b.Brier = &brier
	}
	for i, x := range c.Buckets {
		p := priceBucket{Lo: float64(i) / 20, Hi: float64(i+1) / 20, Markets: x.Markets}
		if x.Markets > 0 {
			p.MeanPrice, p.Observed = float64(x.SumP)/float64(x.Markets)/ppm, float64(x.SumO)/float64(x.Markets)/ppm
		}
		b.Buckets = append(b.Buckets, p)
	}
	return b
}
