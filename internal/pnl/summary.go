package pnl

type Summary struct {
	Markets, Resolved, Won uint32
	Gross                  int64
	Fees, Volume           uint64
	BestNet                int64
}

func Summarize(ms []MarketPnL) Summary {
	s := Summary{Markets: uint32(len(ms))}
	for i, m := range ms {
		net := m.Gross - int64(m.Fees)
		if i == 0 || net > s.BestNet {
			s.BestNet = net
		}
		s.Gross += m.Gross
		s.Fees += m.Fees
		s.Volume += m.Volume
		if m.Resolved {
			s.Resolved++
			if net > 0 {
				s.Won++
			}
		}
	}
	return s
}

func (s *Summary) Merge(o Summary) {
	if o.Markets > 0 && (s.Markets == 0 || o.BestNet > s.BestNet) {
		s.BestNet = o.BestNet
	}
	s.Markets += o.Markets
	s.Resolved += o.Resolved
	s.Won += o.Won
	s.Gross += o.Gross
	s.Fees += o.Fees
	s.Volume += o.Volume
}

func (s Summary) BiggestWin() int64 {
	if s.BestNet <= scale {
		return 0
	}
	return s.BestNet
}
