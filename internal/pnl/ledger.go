package pnl

import (
	"cmp"
	"encoding/binary"
	"math"
	"math/big"
	"math/bits"
	"slices"
)

const (
	scale   = 1000000
	halfPPM = 500000
)

/* Index sets */

type IndexSet [4]uint64

func ParseIndexSet(s string) (IndexSet, bool) {
	if s == "" {
		return IndexSet{}, false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return IndexSet{}, false
		}
	}
	n, ok := new(big.Int).SetString(s, 10)
	if !ok || n.BitLen() > 256 {
		return IndexSet{}, false
	}
	var buf [32]byte
	n.FillBytes(buf[:])
	var set IndexSet
	for i := range set {
		set[i] = binary.BigEndian.Uint64(buf[24-8*i:])
	}
	return set, true
}

func (s IndexSet) Has(q int) bool {
	return uint(q) < 256 && s[q>>6]>>(q&63)&1 == 1
}

func (s IndexSet) Count() int {
	return bits.OnesCount64(s[0]) + bits.OnesCount64(s[1]) + bits.OnesCount64(s[2]) + bits.OnesCount64(s[3])
}

/* Types */

type Kind uint8

const (
	Buy Kind = iota
	Sell
	Split
	Merge
	Redeem
	Convert
)

type Event struct {
	Block, Log uint32
	Kind       Kind
	Market     uint32
	Outcome    uint8
	Qty        int64
	USDC       uint64
	IndexSet   IndexSet
	Questions  []uint32
}

type Market struct {
	Resolved bool
	Payout   [2]int64
}

type Position struct {
	Market                        uint32
	Outcome                       uint8
	Amount, Avg, Realized, Bought int64
}

type MarketPnL struct {
	Market   uint32
	Resolved bool
	Volume   uint64
	Gross    int64
	Fees     uint64
}

type Result struct {
	Markets                    []MarketPnL
	OpenUnresolved, Duplicates uint32
}

/* Prices */

func PricePPM(usdc, shares uint64) int64 {
	if shares == 0 {
		return 0
	}
	hi, lo := bits.Mul64(usdc, scale)
	return div128(hi, lo, shares)
}

// saturates instead of panicking when the quotient leaves 64 bits
func div128(hi, lo, d uint64) int64 {
	if hi >= d {
		return math.MaxInt64
	}
	q, _ := bits.Div64(hi, lo, d)
	return int64(min(q, math.MaxInt64))
}

// not floored at zero because the dataset's own positions keep the negative average
func yesPrice(noPrice, noCount, questions int64) int64 {
	return (noPrice*noCount - scale*(noCount-1)) / (questions - noCount)
}

func signedAvg(avg, amount, price, qty int64) int64 {
	sum := new(big.Int).Mul(big.NewInt(avg), big.NewInt(amount))
	sum.Add(sum, new(big.Int).Mul(big.NewInt(price), big.NewInt(qty)))
	return sum.Quo(sum, big.NewInt(amount+qty)).Int64()
}

/* Positions */

func (p *Position) buy(price, qty int64) {
	if qty <= 0 {
		return
	}
	if p.Avg < 0 || price < 0 {
		p.Avg = signedAvg(p.Avg, p.Amount, price, qty)
	} else {
		hi, lo := bits.Mul64(uint64(p.Avg), uint64(p.Amount))
		hi2, lo2 := bits.Mul64(uint64(price), uint64(qty))
		lo, carry := bits.Add64(lo, lo2, 0)
		hi, _ = bits.Add64(hi, hi2, carry)
		p.Avg = div128(hi, lo, uint64(p.Amount)+uint64(qty))
	}
	p.Amount += qty
	p.Bought += qty
}

func (p *Position) sell(price, qty int64) {
	n := min(qty, p.Amount)
	if n <= 0 {
		return
	}
	if d := price - p.Avg; d > 0 {
		hi, lo := bits.Mul64(uint64(n), uint64(d))
		p.Realized += div128(hi, lo, scale)
	} else if d < 0 {
		hi, lo := bits.Mul64(uint64(n), uint64(-d))
		p.Realized -= div128(hi, lo, scale)
	}
	p.Amount -= n
}

type slot struct {
	id     uint32
	live   [2]bool
	volume uint64
	pos    [2]Position
}

func (s *slot) at(outcome uint8) *Position {
	if !s.live[outcome] {
		s.live[outcome] = true
		s.pos[outcome] = Position{Market: s.id, Outcome: outcome}
	}
	return &s.pos[outcome]
}

/* Ledger */

type Ledger struct {
	look       func(market uint32) Market
	slots      []slot
	index      map[uint32]int
	duplicates uint32
	started    bool
	block, log uint32
}

func NewLedger(look func(market uint32) Market) *Ledger {
	return &Ledger{look: look, index: make(map[uint32]int)}
}

// the pointer is valid until the next call that can add a market
func (l *Ledger) slot(id uint32) *slot {
	i, ok := l.index[id]
	if !ok {
		i = len(l.slots)
		l.slots = append(l.slots, slot{id: id})
		l.index[id] = i
	}
	return &l.slots[i]
}

func (l *Ledger) Apply(e Event) {
	if l.started && e.Block == l.block && e.Log == l.log {
		l.duplicates++
		return
	}
	l.started, l.block, l.log = true, e.Block, e.Log
	switch e.Kind {
	case Buy:
		s := l.slot(e.Market)
		s.volume += e.USDC
		if e.Qty > 0 {
			s.at(e.Outcome).buy(PricePPM(e.USDC, uint64(e.Qty)), e.Qty)
		}
	case Sell:
		s := l.slot(e.Market)
		s.volume += e.USDC
		s.at(e.Outcome).sell(PricePPM(e.USDC, uint64(e.Qty)), e.Qty)
	case Split:
		s := l.slot(e.Market)
		s.at(0).buy(halfPPM, e.Qty)
		s.at(1).buy(halfPPM, e.Qty)
	case Merge:
		s := l.slot(e.Market)
		s.at(0).sell(halfPPM, e.Qty)
		s.at(1).sell(halfPPM, e.Qty)
	case Redeem:
		if m := l.look(e.Market); m.Resolved {
			s := l.slot(e.Market)
			for o := range uint8(2) {
				p := s.at(o)
				p.sell(m.Payout[o], p.Amount)
			}
		}
	case Convert:
		l.convert(e)
	}
}

func (l *Ledger) convert(e Event) {
	var sum, no int64
	for q, market := range e.Questions {
		if e.IndexSet.Has(q) {
			p := l.slot(market).at(1)
			sum += p.Avg
			p.sell(p.Avg, e.Qty)
			no++
		}
	}
	total := int64(len(e.Questions))
	if no == 0 || no == total {
		return
	}
	price := yesPrice(sum/no, no, total)
	for q, market := range e.Questions {
		if !e.IndexSet.Has(q) {
			l.slot(market).at(0).buy(price, e.Qty)
		}
	}
}

func (l *Ledger) Positions() []Position {
	out := make([]Position, 0, 2*len(l.slots))
	for i := range l.slots {
		s := &l.slots[i]
		for o := range s.pos {
			if s.live[o] {
				out = append(out, s.pos[o])
			}
		}
	}
	slices.SortFunc(out, func(a, b Position) int {
		return cmp.Or(cmp.Compare(a.Market, b.Market), cmp.Compare(a.Outcome, b.Outcome))
	})
	return out
}

func (l *Ledger) Close() Result {
	res := Result{Markets: make([]MarketPnL, 0, len(l.slots)), Duplicates: l.duplicates}
	for i := range l.slots {
		s := &l.slots[i]
		m := l.look(s.id)
		var gross int64
		for o := range s.pos {
			p := &s.pos[o]
			if p.Amount > 0 {
				if m.Resolved {
					p.sell(m.Payout[o], p.Amount)
				} else {
					res.OpenUnresolved++
				}
			}
			gross += p.Realized
		}
		res.Markets = append(res.Markets, MarketPnL{Market: s.id, Resolved: m.Resolved, Volume: s.volume, Gross: gross})
	}
	slices.SortFunc(res.Markets, func(a, b MarketPnL) int { return cmp.Compare(a.Market, b.Market) })
	return res
}
