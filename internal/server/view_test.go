package server

import (
	"encoding/json/v2"
	"math"
	"testing"
)

func TestFormatAmount(t *testing.T) {
	for _, tc := range []struct {
		raw  int64
		want string
	}{
		{0, "0"},
		{121520000, "121.52"},
		{-113752331, "-113.752331"},
		{1, "0.000001"},
		{-477, "-0.000477"},
		{5000000, "5"},
		{716582888996, "716582.888996"},
		{-1, "-0.000001"},
		{999999, "0.999999"},
		{1000000, "1"},
		{-5000000, "-5"},
		{math.MaxInt64, "9223372036854.775807"},
		{math.MinInt64, "-9223372036854.775808"},
	} {
		if got := formatAmount(tc.raw); got != tc.want {
			t.Errorf("formatAmount(%d) = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

func TestAmountJSON(t *testing.T) {
	got, err := json.Marshal(usdc(-113752331))
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"raw":"-113752331","decimals":6,"symbol":"USDC","value":"-113.752331"}`; string(got) != want {
		t.Errorf("amount is %s, want %s", got, want)
	}
	got, err = json.Marshal(shares(200000000))
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"raw":"200000000","decimals":6,"symbol":"shares","value":"200"}`; string(got) != want {
		t.Errorf("amount is %s, want %s", got, want)
	}
}

func TestParseWallet(t *testing.T) {
	want := [20]byte{0x46, 0x7e, 0xcf, 0xc0, 0xc6, 0x26, 0x56, 0x83, 0xe9, 0x08, 0x99, 0xC4, 0x18, 0x81, 0xc0, 0x43, 0x1b, 0xDE, 0xE2, 0xed}
	for _, in := range []string{"0x467ecfc0c6265683e90899C41881c0431bDEE2ed", "0x467ECFC0C6265683E90899C41881C0431BDEE2ED", "0x467ecfc0c6265683e90899c41881c0431bdee2ed"} {
		if got, ok := parseWallet(in); !ok || got != want {
			t.Errorf("parseWallet(%q) = %x, %v", in, got, ok)
		}
	}
	for _, in := range []string{"", "0x", "0x12", "467ecfc0c6265683e90899c41881c0431bdee2ed00", "0x467ecfc0c6265683e90899c41881c0431bdee2eg", "0X467ecfc0c6265683e90899c41881c0431bdee2ed", "0x467ecfc0c6265683e90899c41881c0431bdee2ed0"} {
		if _, ok := parseWallet(in); ok {
			t.Errorf("parseWallet(%q) accepted", in)
		}
	}
}

func TestRoundWinRate(t *testing.T) {
	if got := round4(4.0 / 11); got != 0.3636 {
		t.Errorf("round4(4/11) = %v, want 0.3636", got)
	}
}
