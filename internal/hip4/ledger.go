package hip4

import (
	"errors"
	"fmt"
	"math/bits"
	"slices"
	"strconv"
	"strings"

	"pm/internal/pnl"
	"pm/internal/skill"
	"pm/internal/venue"
)

var errMarket = errors.New("not a side-0 outcome coin")

// ratedPrefixes name the question-type outcomes, since recurring price outcomes and fallbacks have no forecast to rate
var ratedPrefixes = []string{"sports", "award", "policyRate", "companyIpo"}

// Rated reports whether an outcome kind is a question that skill can be rated on
func Rated(kind string) bool {
	return slices.ContainsFunc(ratedPrefixes, func(p string) bool { return strings.HasPrefix(kind, p) })
}

type holding struct {
	held  [2]int64
	fills []skill.Fill
}

type trader struct {
	ledger  *pnl.Ledger
	markets map[uint32]*holding
	fills   int64
	volume  int64
}

// Book replays the recorded tape into one ledger per wallet, and the tape must arrive in time order
type Book struct {
	info    map[uint32]MarketInfo
	traders map[[20]byte]*trader
	second  int64
	rank    uint32
}

func NewBook(markets []MarketInfo) (*Book, error) {
	b := &Book{info: make(map[uint32]MarketInfo, len(markets)), traders: map[[20]byte]*trader{}}
	for _, m := range markets {
		id, err := outcome(m.ID)
		if err != nil {
			return nil, err
		}
		b.info[id] = m
	}
	return b, nil
}

// outcome reads the outcome number from the side-0 coin that names a market
func outcome(coin string) (uint32, error) {
	n, err := strconv.ParseUint(strings.TrimPrefix(coin, "#"), 10, 32)
	if err != nil || !strings.HasPrefix(coin, "#") || n%coinFactor != 0 {
		return 0, fmt.Errorf("%w: %q", errMarket, coin)
	}
	return uint32(n / coinFactor), nil
}

const coinFactor = 10

func (b *Book) look(id uint32) pnl.Market {
	yes, no, ok := Payout(b.info[id].Result)
	if !ok {
		return pnl.Market{}
	}
	return pnl.Market{Resolved: true, Payout: [2]int64{yes, no}}
}

func (b *Book) trader(w []byte) *trader {
	var k [20]byte
	copy(k[:], w)
	t, ok := b.traders[k]
	if !ok {
		t = &trader{ledger: pnl.NewLedger(b.look), markets: map[uint32]*holding{}}
		b.traders[k] = t
	}
	return t
}

// Add applies one recorded trade to the buyer and the seller of the Yes coin
func (b *Book) Add(t venue.Trade) error {
	id, err := outcome(t.MarketID)
	if err != nil {
		return err
	}
	if len(t.Taker) != 20 || len(t.Maker) != 20 || t.Qty <= 0 {
		return fmt.Errorf("unusable trade %s", t.ID)
	}
	secs := t.TS.Unix()
	if secs != b.second {
		b.second, b.rank = secs, 0
	}
	slot := b.rank * 4
	b.rank++
	buyer, seller := t.Taker, t.Maker
	if t.Side == "sell" {
		buyer, seller = t.Maker, t.Taker
	}
	b.trader(buyer).apply(id, t, true, uint32(secs), slot)
	b.trader(seller).apply(id, t, false, uint32(secs), slot+2)
	return nil
}

// apply books a fill of the Yes coin for one wallet. A buyer holding No closes it and a seller holding Yes closes that, and the rest opens the other
// side at the complementary price, which leaves the same profit at settlement as the venue's own accounting
func (tr *trader) apply(market uint32, t venue.Trade, buy bool, secs, slot uint32) {
	h := tr.markets[market]
	if h == nil {
		h = &holding{}
		tr.markets[market] = h
	}
	tr.fills++
	tr.volume += t.USD
	price := int64(t.PricePPM)
	h.fills = append(h.fills, skill.Fill{TS: secs, Block: secs, Log: slot, Buy: buy, Outcome: 0, Price: price})
	open, closing := uint8(0), uint8(1)
	px, closePx := price, venue.PPM-price
	if !buy {
		open, closing = 1, 0
		px, closePx = venue.PPM-price, price
	}
	closed := min(t.Qty, h.held[closing])
	if closed > 0 {
		tr.ledger.Apply(pnl.Event{Block: secs, Log: slot, Kind: pnl.Sell, Market: market, Outcome: closing, Qty: closed, USDC: amount(closed, closePx)})
		h.held[closing] -= closed
	}
	if rest := t.Qty - closed; rest > 0 {
		tr.ledger.Apply(pnl.Event{Block: secs, Log: slot + 1, Kind: pnl.Buy, Market: market, Outcome: open, Qty: rest, USDC: amount(rest, px)})
		h.held[open] += rest
	}
}

// amount is shares times a ppm price in micro-USDC, rounded to the nearest unit
func amount(qty, ppm int64) uint64 {
	hi, lo := bits.Mul64(uint64(qty), uint64(min(max(ppm, 0), venue.PPM)))
	lo, carry := bits.Add64(lo, venue.PPM/2, 0)
	q, _ := bits.Div64(hi+carry, lo, venue.PPM)
	return q
}

// Wallets closes every ledger and returns the totals in wallet order
func (b *Book) Wallets() []Wallet {
	out := make([]Wallet, 0, len(b.traders))
	for addr, tr := range b.traders {
		res := tr.ledger.Close()
		sum := pnl.Summarize(res.Markets)
		w := Wallet{
			Address: slices.Clone(addr[:]), Fills: tr.fills, Volume: tr.volume, Gross: sum.Gross,
			Markets: int(sum.Markets), Resolved: int(sum.Resolved), Won: int(sum.Won),
		}
		var acc skill.Acc
		for id, h := range tr.markets {
			info := b.info[id]
			yes, no, ok := Payout(info.Result)
			if !ok || info.SettledAt.IsZero() || !Rated(info.Kind) {
				continue
			}
			if f, o, ok := skill.Forecast(h.fills, uint32(info.SettledAt.Unix()), [2]int64{yes, no}); ok {
				acc.Add(f, o)
			}
		}
		// each outcome is its own event because the recorded markets carry no question id
		if skill.Rated(acc.N, acc.N) {
			se, _ := acc.EdgeSE()
			w.Rated, w.Brier, w.Edge, w.EdgeSE = true, acc.Brier(), acc.Edge(), se
		}
		out = append(out, w)
	}
	slices.SortFunc(out, func(a, c Wallet) int { return slices.Compare(a.Address, c.Address) })
	return out
}
