package wash_test

import (
	"math"
	"slices"
	"testing"

	"pm/internal/wash"
)

func pairGraph() *wash.Graph {
	g := wash.NewGraph(2)
	g.AddRow(0, []uint32{1}, []uint64{50e6})
	g.AddRow(1, []uint32{0}, []uint64{50e6})
	return g
}

func TestAddRowWeights(t *testing.T) {
	g := wash.NewGraph(4)
	g.AddRow(0, []uint32{1, 2}, []uint64{1e6, 3e6})
	g.AddRow(3, []uint32{0}, []uint64{7e6})
	g.AddRow(4, []uint32{1}, []uint64{0})
	if want := []uint32{0, 2, 2, 2, 3, 3}; !slices.Equal(g.Off, want) {
		t.Fatalf("Off = %v, want %v", g.Off, want)
	}
	if want := []uint32{1, 2, 0}; !slices.Equal(g.Col, want) {
		t.Fatalf("Col = %v, want %v", g.Col, want)
	}
	if want := []float32{0.25, 0.75, 1}; !slices.Equal(g.W, want) {
		t.Fatalf("W = %v, want %v", g.W, want)
	}
}

func TestConverged(t *testing.T) {
	for _, tc := range []struct {
		diff2, prev2 float64
		round        int
		want         bool
	}{
		{1e-12, 1, 3, true},
		{1e-9, 1, 3, false},
		{1, 1, 200, true},
		{0, 0, 1, true},
	} {
		if got := wash.Converged(tc.diff2, tc.prev2, tc.round); got != tc.want {
			t.Errorf("Converged(%v, %v, %d) = %v, want %v", tc.diff2, tc.prev2, tc.round, got, tc.want)
		}
	}
}

func TestSolveTwoWallets(t *testing.T) {
	x, rounds := wash.Solve(pairGraph(), []float64{1, 0})
	if math.Abs(x[0]-2.0/3) > 1e-4 || math.Abs(x[1]-1.0/3) > 1e-4 {
		t.Fatalf("x = %v, want (2/3, 1/3)", x)
	}
	if rounds >= 30 {
		t.Fatalf("rounds = %d, want under 30", rounds)
	}
}

func TestSolveIsolatedPair(t *testing.T) {
	x, rounds := wash.Solve(pairGraph(), []float64{1, 1})
	if x[0] != 1 || x[1] != 1 || rounds != 1 {
		t.Fatalf("x = %v, rounds = %d, want (1, 1) in 1 round", x, rounds)
	}
}

func TestSolveKeepsStartVector(t *testing.T) {
	x0 := []float64{1, 0}
	wash.Solve(pairGraph(), x0)
	if x0[0] != 1 || x0[1] != 0 {
		t.Fatalf("x0 changed to %v", x0)
	}
}

func TestSolveZeroStart(t *testing.T) {
	x, rounds := wash.Solve(pairGraph(), []float64{0, 0})
	if x[0] != 0 || x[1] != 0 || rounds != 1 {
		t.Fatalf("x = %v, rounds = %d, want (0, 0) in 1 round", x, rounds)
	}
}

func TestSolveRowsWithoutEdges(t *testing.T) {
	g := wash.NewGraph(3)
	g.AddRow(0, []uint32{1}, []uint64{1e6})
	x, _ := wash.Solve(g, []float64{0, 1, 1})
	want := []float64{0.25, 0.5, 0.5}
	for i := range want {
		if math.Abs(x[i]-want[i]) > 1e-4 {
			t.Fatalf("x = %v, want %v", x, want)
		}
	}
}
