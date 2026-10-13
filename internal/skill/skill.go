package skill

import (
	"math"
	"math/bits"
	"sort"
)

const (
	Horizon    = 12 * 3600
	scale      = 1000000
	MinMarkets = 30
	MinEvents  = 3
	Z          = 1.96
)

/* Horizon price and forecast */

type Fill struct {
	TS, Block, Log uint32
	Buy            bool
	Outcome        uint8
	Price          int64
}

// fills must be sorted by ts, block and log
func lastAtOrBefore(fills []Fill, t uint32, secs int64) (Fill, bool) {
	cutoff := int64(t) - secs
	if cutoff < 0 {
		return Fill{}, false
	}
	i := sort.Search(len(fills), func(i int) bool { return int64(fills[i].TS) > cutoff })
	if i == 0 {
		return Fill{}, false
	}
	return fills[i-1], true
}

func PriceAt(fills []Fill, t uint32, hours int) (p int64, ok bool) {
	last, ok := lastAtOrBefore(fills, t, int64(hours)*3600)
	return last.Price, ok
}

func Forecast(fills []Fill, t uint32, payout [2]int64) (f, o int64, ok bool) {
	last, ok := lastAtOrBefore(fills, t, Horizon)
	if !ok {
		return 0, 0, false
	}
	backed, price := last.Outcome, last.Price
	if !last.Buy {
		backed, price = 1-backed, scale-price
	}
	return min(max(price, 0), scale), payout[backed], true
}

/* Accumulator */

type Acc struct {
	N     uint32
	SumT  int64
	SumT2 uint64
	Cal   [5]struct {
		N          uint32
		SumF, SumO uint64
	}
}

func (a *Acc) Add(f, o int64) {
	t := o - f
	a.N++
	a.SumT += t
	a.SumT2 += uint64(t * t)
	c := &a.Cal[Bucket5(f)]
	c.N++
	c.SumF += uint64(f)
	c.SumO += uint64(o)
}

func (a *Acc) Merge(b Acc) {
	a.N += b.N
	a.SumT += b.SumT
	a.SumT2 += b.SumT2
	for i := range a.Cal {
		a.Cal[i].N += b.Cal[i].N
		a.Cal[i].SumF += b.Cal[i].SumF
		a.Cal[i].SumO += b.Cal[i].SumO
	}
}

func (a Acc) Brier() float64 {
	if a.N == 0 {
		return 0
	}
	return float64(a.SumT2) / float64(a.N) / (scale * scale)
}

func (a Acc) Edge() float64 {
	if a.N == 0 {
		return 0
	}
	return float64(a.SumT) / float64(a.N) / scale
}

func (a Acc) EdgeSE() (float64, bool) {
	if a.N < 2 {
		return 0, false
	}
	n := uint64(a.N)
	sum := uint64(max(a.SumT, -a.SumT))
	hi, lo := bits.Mul64(n, a.SumT2)
	sqHi, sqLo := bits.Mul64(sum, sum)
	lo, borrow := bits.Sub64(lo, sqLo, 0)
	hi, _ = bits.Sub64(hi, sqHi, borrow)
	spread := float64(hi)*0x1p64 + float64(lo)
	return math.Sqrt(spread/float64(n*(n-1))/float64(n)) / scale, true
}

func (a Acc) EdgeLB() float64 {
	se, _ := a.EdgeSE()
	return a.Edge() - Z*se
}

/* Floor, buckets and intervals */

func Rated(markets, events uint32) bool {
	return markets >= MinMarkets && events >= MinEvents
}

func Bucket5(f int64) int { return int(min(max(f*5/scale, 0), 4)) }

func Bucket20(p int64) int { return int(min(max(p*20/scale, 0), 19)) }

func Wilson(k, n int) float64 {
	if n <= 0 {
		return 0
	}
	fn := float64(n)
	p := float64(k) / fn
	z2 := Z * Z
	center := p + z2/(2*fn)
	margin := Z * math.Sqrt(p*(1-p)/fn+z2/(4*fn*fn))
	return max(0, (center-margin)/(1+z2/fn))
}
