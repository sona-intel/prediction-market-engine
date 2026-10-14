// Package levr summarises leveraged Levr Bet positions, whose house vault pays out so there is no outcome price to rate
package levr

import (
	"time"
)

const (
	Open       = "open"
	Won        = "won"
	Lost       = "lost"
	Liquidated = "liquidated"
	Void       = "void"
)

// Position amounts are micro-USDC and prices are ppm of a dollar, and Payout stays nil until a payout is known
type Position struct {
	ID       string
	Wallet   []byte
	MarketID string
	Opened   time.Time
	Stake    int64
	Size     int64
	Fee      int64
	// Side is 1 or 2, and the winner of a settled game is one of them or 3 for a draw
	Side     int
	EntryPPM int32
	LiqPPM   int32
	Status   string
	Closed   time.Time
	Payout   *int64
}

// Close moves an open position to its final state
type Close struct {
	ID     string
	Status string
	At     time.Time
	Payout *int64
}

// Redemption is the settlement row of a position, and Winner is zero when the game was cancelled
type Redemption struct {
	PositionID string
	At         time.Time
	Winner     int
	Profit     int64
	Loss       int64
	// Costs are the borrow and balance fees charged at redemption
	Costs int64
}

// Settle works out the payout of a redeemed position, which is the stake plus profit less loss and every fee, checked against the venue's own totals
func Settle(p Position, r Redemption) Close {
	c := Close{ID: p.ID, At: r.At}
	if r.Winner == 0 {
		c.Status = Void
		return c
	}
	pay := max(0, p.Stake+r.Profit-r.Loss-r.Costs-p.Fee)
	c.Payout = &pay
	c.Status = Lost
	if r.Profit > 0 {
		c.Status = Won
	}
	return c
}

// Liquidation closes a position with no payout
func Liquidation(id string, at time.Time) Close {
	zero := int64(0)
	return Close{ID: id, Status: Liquidated, At: at, Payout: &zero}
}

// PnL is the realized result in micro-USDC, and a liquidated position loses its whole stake
func (p Position) PnL() int64 {
	switch p.Status {
	case Won, Lost:
		if p.Payout != nil {
			return *p.Payout - p.Stake
		}
	case Liquidated:
		return -p.Stake
	}
	return 0
}

type Summary struct {
	Positions, Won, Lost, Liquidated, Voided int
	Stake, Size, Realized                    int64
}

func Summarize(ps []Position) Summary {
	var s Summary
	for _, p := range ps {
		s.Add(p)
	}
	return s
}

func (s *Summary) Add(p Position) {
	s.Positions++
	s.Stake += p.Stake
	s.Size += p.Size
	s.Realized += p.PnL()
	switch p.Status {
	case Won:
		s.Won++
	case Lost:
		s.Lost++
	case Liquidated:
		s.Liquidated++
	case Void:
		s.Voided++
	}
}

// Settled counts positions that ended in a win, a loss or a liquidation
func (s Summary) Settled() int { return s.Won + s.Lost + s.Liquidated }

// Leverage is the position size over the stake across all positions
func (s Summary) Leverage() float64 {
	if s.Stake == 0 {
		return 0
	}
	return float64(s.Size) / float64(s.Stake)
}

func (s Summary) LiquidationRate() float64 { return ratio(s.Liquidated, s.Settled()) }

func (s Summary) WinRate() float64 { return ratio(s.Won, s.Settled()) }

func ratio(n, d int) float64 {
	if d == 0 {
		return 0
	}
	return float64(n) / float64(d)
}
