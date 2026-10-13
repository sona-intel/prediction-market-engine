package wash

func MakerDelta(buy bool, outcome uint8, shares uint64) int64 {
	if buy == (outcome == 0) {
		return int64(shares)
	}
	return -int64(shares)
}

type tracker struct {
	x, m, prevX int64
	contracted  bool
	closures    int
}

func (t *tracker) terminal() bool { return t.contracted && t.prevX*1000 <= 5*t.m }

func (t *tracker) step(nx int64) {
	switch {
	case nx > t.x:
		if t.terminal() {
			t.closures++
			t.m = 0
		}
		t.contracted = false
		t.m = max(t.m, nx)
	case nx < t.x:
		t.contracted, t.prevX = true, nx
	}
	t.x = nx
}

func abs(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

// deltas must be in (block, log) order
func Closures(deltas []int64) int {
	var t tracker
	var p int64
	for _, d := range deltas {
		np := p + d
		if p > 0 && np < 0 || p < 0 && np > 0 {
			t.step(0)
		}
		t.step(abs(np))
		p = np
	}
	if t.terminal() {
		t.closures++
	}
	return t.closures
}
