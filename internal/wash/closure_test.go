package wash_test

import (
	"math"
	"testing"

	"pm/internal/wash"
)

func mil(v float64) int64 { return int64(math.Round(v * 1e6)) }

func TestMakerDelta(t *testing.T) {
	cases := []struct {
		name    string
		buy     bool
		outcome uint8
		want    int64
	}{
		{"buy outcome 0", true, 0, 100},
		{"buy outcome 1", true, 1, -100},
		{"sell outcome 0", false, 0, -100},
		{"sell outcome 1", false, 1, 100},
	}
	for _, c := range cases {
		if got := wash.MakerDelta(c.buy, c.outcome, 100); got != c.want {
			t.Errorf("%s = %d, want %d", c.name, got, c.want)
		}
	}
}

func TestClosures(t *testing.T) {
	cases := []struct {
		name   string
		deltas []float64
		want   int
	}{
		{"full exit", []float64{100, -100}, 1},
		{"residue equals c times M", []float64{100, -99.5}, 1},
		{"residue just above c times M", []float64{100, -99.4}, 0},
		{"reversal splits", []float64{100, -150, 50}, 2},
		{"small residue then expansion", []float64{100, -99.9, 50}, 1},
		{"only the last contraction of a run is terminal", []float64{100, -60, -39.8}, 1},
		{"half exit", []float64{100, -50}, 0},
		{"two round trips", []float64{100, -100, 10, -10}, 2},
		{"M restarts after a closure", []float64{100, -100, 10, -9.8}, 1},
		{"no trades", nil, 0},
		{"only expansions", []float64{10, 20, 30}, 0},
		{"short side mirrors long", []float64{-100, 100}, 1},
	}
	for _, c := range cases {
		deltas := make([]int64, len(c.deltas))
		for i, d := range c.deltas {
			deltas[i] = mil(d)
		}
		if got := wash.Closures(deltas); got != c.want {
			t.Errorf("%s = %d, want %d", c.name, got, c.want)
		}
	}
}
