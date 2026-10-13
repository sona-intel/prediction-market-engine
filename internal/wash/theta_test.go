package wash_test

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"pm/internal/wash"
)

func TestThetaPicksSmallestMinimiser(t *testing.T) {
	pairs := []wash.Pair{{I: 0, J: 1, V: 100}, {I: 2, J: 3, V: 100}}
	theta, found := wash.Theta(pairs, []float64{0.85, 0.85, 0.95, 0.95})
	if theta != 0.85 || !found {
		t.Fatalf("got %v, %v, want 0.85, true", theta, found)
	}
}

func TestThetaPrefersLowerY(t *testing.T) {
	pairs := []wash.Pair{{I: 0, J: 1, V: 1000}, {I: 2, J: 3, V: 1000}, {I: 0, J: 4, V: 50}, {I: 2, J: 5, V: 100}}
	theta, found := wash.Theta(pairs, []float64{0.95, 0.95, 0.85, 0.85, 0.5, 0.5})
	if theta != 0.95 || !found {
		t.Fatalf("got %v, %v, want 0.95, true", theta, found)
	}
}

func TestThetaRejectsMixedPairs(t *testing.T) {
	pairs := []wash.Pair{{I: 0, J: 1, V: 100}, {I: 0, J: 2, V: 300}}
	theta, found := wash.Theta(pairs, []float64{0.9, 0.9, 0.1})
	if theta != 1 || found {
		t.Fatalf("got %v, %v, want 1, false", theta, found)
	}
}

func TestThetaEmptyCandidates(t *testing.T) {
	if theta, found := wash.Theta(nil, nil); theta != 1 || found {
		t.Fatalf("no pairs gave %v, %v, want 1, false", theta, found)
	}
	pairs := []wash.Pair{{I: 0, J: 1, V: 100}}
	if theta, found := wash.Theta(pairs, []float64{1, 1}); theta != 1 || found {
		t.Fatalf("isolated pair at 1.0 gave %v, %v, want 1, false", theta, found)
	}
	if theta, found := wash.Theta(pairs, []float64{0.5, 0.5}); theta != 1 || found {
		t.Fatalf("scores below range gave %v, %v, want 1, false", theta, found)
	}
}

func TestThetaCandidateRangeIsClosed(t *testing.T) {
	pairs := []wash.Pair{{I: 0, J: 1, V: 100}}
	for _, v := range []float64{0.79, 0.995} {
		if theta, found := wash.Theta(pairs, []float64{v, v}); theta != 1 || found {
			t.Fatalf("%v gave %v, %v, want 1, false", v, theta, found)
		}
	}
	if theta, found := wash.Theta(pairs, []float64{0.8, 0.8}); theta != 0.8 || !found {
		t.Fatalf("0.8 gave %v, %v, want 0.8, true", theta, found)
	}
	if theta, found := wash.Theta(pairs, []float64{0.99, 0.99}); theta != 0.99 || !found {
		t.Fatalf("0.99 gave %v, %v, want 0.99, true", theta, found)
	}
}

type naive struct {
	theta float64
	found bool
}

func naiveTheta(pairs []wash.Pair, x []float64) naive {
	maxCP := map[uint32]float64{}
	for _, p := range pairs {
		maxCP[p.I] = max(maxCP[p.I], x[p.J])
		maxCP[p.J] = max(maxCP[p.J], x[p.I])
	}
	r := func(i uint32) float64 { return min(x[i], maxCP[i]) }
	var cands []float64
	for i := range maxCP {
		if v := r(i); v >= 0.8 && v <= 0.99 {
			cands = append(cands, v)
		}
	}
	slices.Sort(cands)
	cands = slices.Compact(cands)
	best, bestScore := naive{theta: 1}, math.Inf(1)
	for _, c := range cands {
		var num, den uint64
		for _, p := range pairs {
			lo, hi := min(r(p.I), r(p.J)), max(r(p.I), r(p.J))
			if lo >= c {
				num += p.V
			}
			if hi >= c {
				den += p.V
			}
		}
		y := float64(den-num) / float64(den)
		if y > 0.1 {
			continue
		}
		if s := max(0.001, y); s < bestScore {
			best, bestScore = naive{c, true}, s
		}
	}
	return best
}

func TestThetaMatchesNaive(t *testing.T) {
	const wallets, half = 40, 20
	found, missed := 0, 0
	for seed := range uint64(30) {
		rng := rand.New(rand.NewPCG(seed, 7))
		x := make([]float64, wallets)
		for i := range x {
			if i < half {
				x[i] = 0.8 + 0.2*rng.Float64()
			} else {
				x[i] = 0.5 + 0.3*rng.Float64()
			}
		}
		crossPct := int(seed%10) * 3
		seen := map[[2]uint32]bool{}
		var pairs []wash.Pair
		for len(pairs) < 200 {
			a := rng.IntN(wallets)
			side := a / half
			if rng.IntN(100) < crossPct {
				side = 1 - side
			}
			i, j := uint32(a), uint32(side*half+rng.IntN(half))
			if i > j {
				i, j = j, i
			}
			if i == j || seen[[2]uint32{i, j}] {
				continue
			}
			seen[[2]uint32{i, j}] = true
			pairs = append(pairs, wash.Pair{I: i, J: j, V: 1 + rng.Uint64N(1000)})
		}
		want := naiveTheta(pairs, x)
		theta, ok := wash.Theta(pairs, x)
		if theta != want.theta || ok != want.found {
			t.Fatalf("seed %d: got %v, %v, want %v, %v", seed, theta, ok, want.theta, want.found)
		}
		if ok {
			found++
		} else {
			missed++
		}
	}
	if found == 0 || missed == 0 {
		t.Fatalf("%d seeds found a candidate and %d did not, the comparison needs both", found, missed)
	}
}

func TestFlagged(t *testing.T) {
	if !wash.Flagged(0.85, 0.85, 0.9) {
		t.Error("both at or above theta must be flagged")
	}
	if wash.Flagged(0.85, 0.84, 0.9) {
		t.Error("one below theta must not be flagged")
	}
	if !wash.Flagged(1, 1, 1) {
		t.Error("theta 1 with both scores 1 must be flagged")
	}
}
