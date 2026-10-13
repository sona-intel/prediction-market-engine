package pnl_test

import (
	"testing"

	"pm/internal/pnl"
)

func TestSummarize(t *testing.T) {
	s := pnl.Summarize([]pnl.MarketPnL{
		{Market: 1, Resolved: true, Gross: 5e6, Fees: 0.2e6, Volume: 7e6},
		{Market: 2, Resolved: true, Gross: -2e6, Volume: 1e6},
		{Market: 3, Gross: 1.5e6, Fees: 0.6e6, Volume: 2e6},
	})
	want := pnl.Summary{Markets: 3, Resolved: 2, Won: 1, Gross: 4.5e6, Fees: 0.8e6, Volume: 10e6, BestNet: 4.8e6}
	if s != want {
		t.Fatalf("got %+v, want %+v", s, want)
	}
}

func TestSummarizeFeesFlipAWin(t *testing.T) {
	s := pnl.Summarize([]pnl.MarketPnL{{Resolved: true, Gross: 1e6, Fees: 1e6}, {Resolved: true, Gross: 1e6, Fees: 999999}})
	if s.Won != 1 {
		t.Fatalf("Won %d, want 1", s.Won)
	}
}

func TestSummarizeBestNetKeepsNegative(t *testing.T) {
	s := pnl.Summarize([]pnl.MarketPnL{{Resolved: true, Gross: -5e6}, {Resolved: true, Gross: -2e6, Fees: 1e6}})
	if s.BestNet != -3e6 || s.BiggestWin() != 0 {
		t.Fatalf("BestNet %d BiggestWin %d", s.BestNet, s.BiggestWin())
	}
}

func TestSummarizeEmpty(t *testing.T) {
	if s := pnl.Summarize(nil); s != (pnl.Summary{}) {
		t.Fatalf("got %+v", s)
	}
}

func TestSummarizeBiggestWinFloor(t *testing.T) {
	if got := (pnl.Summary{BestNet: 1000000}).BiggestWin(); got != 0 {
		t.Fatalf("1000000 gave %d, want 0", got)
	}
	if got := (pnl.Summary{BestNet: 1000001}).BiggestWin(); got != 1000001 {
		t.Fatalf("1000001 gave %d, want 1000001", got)
	}
}

func TestSummaryMerge(t *testing.T) {
	a := pnl.Summary{Markets: 3, Resolved: 2, Won: 1, Gross: 4e6, Fees: 1e6, Volume: 10e6, BestNet: 4e6}
	b := pnl.Summary{Markets: 2, Resolved: 1, Won: 1, Gross: 6e6, Fees: 2e6, Volume: 5e6, BestNet: 6e6}
	a.Merge(b)
	want := pnl.Summary{Markets: 5, Resolved: 3, Won: 2, Gross: 10e6, Fees: 3e6, Volume: 15e6, BestNet: 6e6}
	if a != want {
		t.Fatalf("got %+v, want %+v", a, want)
	}
	a.Merge(pnl.Summary{Markets: 1, BestNet: 1e6})
	if a.BestNet != 6e6 {
		t.Fatalf("BestNet %d, want 6e6", a.BestNet)
	}
}

func TestSummaryMergeNegativeBestNet(t *testing.T) {
	var s pnl.Summary
	s.Merge(pnl.Summary{Markets: 1, BestNet: -4e6})
	s.Merge(pnl.Summary{})
	s.Merge(pnl.Summary{Markets: 1, BestNet: -2e6})
	if s.BestNet != -2e6 || s.Markets != 2 {
		t.Fatalf("got %+v", s)
	}
}
