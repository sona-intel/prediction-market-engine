package wash

import (
	"cmp"
	"math"
	"slices"
)

const (
	thetaMin = 0.8
	thetaMax = 0.99
	yMax     = 0.1
	yFloor   = 0.001
)

type Pair struct {
	I, J uint32
	V    uint64
}

type entry struct {
	r float64
	v uint64
}

type tail struct {
	r   []float64
	sum []uint64
}

func newTail(e []entry) tail {
	slices.SortFunc(e, func(a, b entry) int { return cmp.Compare(a.r, b.r) })
	t := tail{r: make([]float64, len(e)), sum: make([]uint64, len(e)+1)}
	for k := len(e) - 1; k >= 0; k-- {
		t.r[k] = e[k].r
		t.sum[k] = t.sum[k+1] + e[k].v
	}
	return t
}

func (t tail) from(bound float64) uint64 {
	k, _ := slices.BinarySearch(t.r, bound)
	return t.sum[k]
}

func Theta(pairs []Pair, x []float64) (theta float64, found bool) {
	ids := make([]uint32, 0, 2*len(pairs))
	for _, p := range pairs {
		ids = append(ids, p.I, p.J)
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)

	ends := make([]int, 2*len(pairs))
	r := make([]float64, len(ids))
	for k, p := range pairs {
		i, _ := slices.BinarySearch(ids, p.I)
		j, _ := slices.BinarySearch(ids, p.J)
		ends[2*k], ends[2*k+1] = i, j
		r[i] = max(r[i], x[p.J])
		r[j] = max(r[j], x[p.I])
	}
	for i, id := range ids {
		r[i] = min(r[i], x[id])
	}

	lo, hi := make([]entry, len(pairs)), make([]entry, len(pairs))
	for k, p := range pairs {
		a, b := r[ends[2*k]], r[ends[2*k+1]]
		lo[k], hi[k] = entry{min(a, b), p.V}, entry{max(a, b), p.V}
	}
	byLo, byHi := newTail(lo), newTail(hi)

	cands := slices.DeleteFunc(r, func(v float64) bool { return v < thetaMin || v > thetaMax })
	slices.Sort(cands)
	cands = slices.Compact(cands)

	theta, best := 1.0, math.Inf(1)
	for _, c := range cands {
		den := byHi.from(c)
		// the integer difference keeps equal fractions bitwise equal so ties go to the smallest candidate
		y := float64(den-byLo.from(c)) / float64(den)
		if y > yMax {
			continue
		}
		if s := max(yFloor, y); s < best {
			theta, best, found = c, s, true
		}
	}
	return theta, found
}

func Flagged(theta, xi, xj float64) bool {
	return xi >= theta && xj >= theta
}
