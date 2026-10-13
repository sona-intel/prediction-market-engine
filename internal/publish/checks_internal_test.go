package publish

import "testing"

func TestToCentsRoundsTheFullHistoryVolumeToTheDatasetTotal(t *testing.T) {
	if got, want := toCents(59899137953107476), toCents(FullTotals.VolumeUSDC); got != want {
		t.Errorf("cents %d, want %d", got, want)
	}
	if toCents(59899137953100000) == toCents(FullTotals.VolumeUSDC) {
		t.Error("a volume a cent off must not match")
	}
}
