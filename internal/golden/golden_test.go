package golden

import (
	"path/filepath"
	"testing"
)

func TestPath(t *testing.T) {
	got := Path("dataset", "condition.parquet")
	if !filepath.IsAbs(got) || filepath.Base(filepath.Dir(got)) != "dataset" || filepath.Base(filepath.Dir(filepath.Dir(got))) != "golden" {
		t.Fatalf("Path = %q", got)
	}
}

func TestLoadWallets(t *testing.T) {
	for name, n := range map[string]int{
		"wallets_14": 14, "wallets_conversion": 6, "wallets_fee_era": 3, "wallets_cutoff": 2, "tokens_user_position": 3,
	} {
		got, err := LoadWallets(name)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != n {
			t.Errorf("%s has %d wallets, want %d", name, len(got), n)
		}
	}
	w14, err := LoadWallets("wallets_14")
	if err != nil {
		t.Fatal(err)
	}
	first := Wallet{
		Address: "0x0FE9463bCfA3e3dcA586Acc12acf1D74EfB97E68", Fills: 6, TakerRows: 2, Markets: 1, Volume: 121520000,
		Tolerance: 200, FirstTS: 1740515919, LastTS: 1740642347,
	}
	if w14[0] != first {
		t.Errorf("wallets_14[0] = %+v, want %+v", w14[0], first)
	}
	fee, _ := LoadWallets("wallets_fee_era")
	cutoff, _ := LoadWallets("wallets_cutoff")
	tokens, _ := LoadWallets("tokens_user_position")
	if fee[0].FeesPaid != 37262 || cutoff[0].OpenUnresolved != 26 || cutoff[0].Settled != 79444405 || cutoff[0].PolymarketRealized != -997491 {
		t.Errorf("fee %+v cutoff %+v", fee[0], cutoff[0])
	}
	if tokens[0].FirstTS != 0 || tokens[0].LastTS != 0 || tokens[0].Tokens != 9 {
		t.Errorf("token wallet without a range: %+v", tokens[0])
	}
	if tokens[2].Conversions != 3 || tokens[2].WithoutConversions != -1073204661 || tokens[2].Tokens != 50 {
		t.Errorf("token wallet with conversions: %+v", tokens[2])
	}
}

func TestLoadMarkets(t *testing.T) {
	volume, err := LoadMarkets("markets_volume_5")
	if err != nil {
		t.Fatal(err)
	}
	may, err := LoadMarkets("markets_may25_29")
	if err != nil {
		t.Fatal(err)
	}
	prices, err := LoadMarkets("prices_12")
	if err != nil {
		t.Fatal(err)
	}
	if len(volume) != 5 || len(may) != 29 || len(prices) != 12 {
		t.Fatalf("got %d, %d and %d markets, want 5, 29 and 12", len(volume), len(may), len(prices))
	}
	if volume[0].Volume != 39940.33 || volume[0].Tokens[1] == "" || volume[0].ClosedTime != 1757052186 {
		t.Errorf("markets_volume_5[0] = %+v", volume[0])
	}
	if may[0].Volume != 5039821.548948 || may[0].Condition != prices[0].Condition || may[0].Tokens[0] != prices[0].Token0 {
		t.Errorf("markets_may25_29[0] = %+v, prices_12[0] = %+v", may[0], prices[0])
	}
	if p := prices[0].Prices["12h"]; p == nil || *p != 0.0045 {
		t.Errorf("prices_12[0] 12h = %v", p)
	}
}
