package wash

const (
	tolerance = 1e-5
	maxRounds = 200
)

type Graph struct {
	Off []uint32
	Col []uint32
	W   []float32
}

func NewGraph(rows int) *Graph {
	return &Graph{Off: make([]uint32, 1, rows+1)}
}

func (g *Graph) AddRow(i uint32, cols []uint32, shares []uint64) {
	for uint32(len(g.Off)-1) < i {
		g.Off = append(g.Off, uint32(len(g.Col)))
	}
	var sum uint64
	for _, s := range shares {
		sum += s
	}
	if sum != 0 {
		for k, c := range cols {
			g.Col = append(g.Col, c)
			g.W = append(g.W, float32(float64(shares[k])/float64(sum)))
		}
	}
	g.Off = append(g.Off, uint32(len(g.Col)))
}

func Converged(diff2, prev2 float64, round int) bool {
	return diff2 <= tolerance*tolerance*prev2 || round == maxRounds
}

func Solve(g *Graph, x0 []float64) (x []float64, rounds int) {
	n := len(x0)
	rows := min(len(g.Off)-1, n)
	cur, next := make([]float64, n), make([]float64, n)
	copy(cur, x0)
	for rounds = 1; ; rounds++ {
		var diff2, prev2 float64
		for i := range rows {
			var s float64
			for k := g.Off[i]; k < g.Off[i+1]; k++ {
				s += float64(g.W[k]) * cur[g.Col[k]]
			}
			next[i] = (x0[i] + s) / 2
		}
		for i := rows; i < n; i++ {
			next[i] = x0[i] / 2
		}
		for i, v := range next {
			d := v - cur[i]
			diff2 += d * d
			prev2 += cur[i] * cur[i]
		}
		cur, next = next, cur
		if Converged(diff2, prev2, rounds) {
			return cur, rounds
		}
	}
}
