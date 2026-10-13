package golden

import (
	"bytes"
	"cmp"
	"encoding/json/v2"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

type Wallet struct {
	Address            string `json:"wallet"`
	Fills              uint32 `json:"fills"`
	TakerRows          uint32 `json:"taker_rows"`
	Markets            uint32 `json:"markets"`
	Tokens             uint32 `json:"tokens"`
	Volume             int64  `json:"volume"`
	Realized           int64  `json:"realized"`
	BiggestWin         int64  `json:"biggest_win"`
	Tolerance          int64  `json:"tolerance"`
	FeesPaid           int64  `json:"fees_paid"`
	Settled            int64  `json:"settled"`
	PolymarketRealized int64  `json:"polymarket_realized"`
	Conversions        uint32 `json:"conversions"`
	OpenUnresolved     uint32 `json:"open_unresolved"`
	WithoutConversions int64  `json:"without_conversions"`
	FirstTS            uint32 `json:"first_ts"`
	LastTS             uint32 `json:"last_ts"`
}

type Market struct {
	Condition  string              `json:"condition"`
	Tokens     [2]string           `json:"tokens"`
	Volume     float64             `json:"volume"`
	ClosedTime uint32              `json:"closed_time"`
	Token0     string              `json:"token0"`
	Prices     map[string]*float64 `json:"prices"`
}

type wireMarket struct {
	Market
	VolumeNum float64 `json:"volume_num"`
}

func Path(parts ...string) string {
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(file), "..", "..", "testdata", "golden")
	return filepath.Join(append([]string{root}, parts...)...)
}

func LoadWallets(name string) ([]Wallet, error) {
	data, err := os.ReadFile(Path(name + ".json"))
	if err != nil {
		return nil, err
	}
	var wallets []Wallet
	if err := json.Unmarshal(data, &wallets); err != nil {
		return nil, fmt.Errorf("golden %s: %w", name, err)
	}
	return wallets, nil
}

func LoadMarkets(name string) ([]Market, error) {
	data, err := os.ReadFile(Path(name + ".json"))
	if err != nil {
		return nil, err
	}
	var wire []wireMarket
	if bytes.HasPrefix(bytes.TrimSpace(data), []byte("{")) {
		var file struct {
			Markets []wireMarket `json:"markets"`
		}
		err = json.Unmarshal(data, &file)
		wire = file.Markets
	} else {
		err = json.Unmarshal(data, &wire)
	}
	if err != nil {
		return nil, fmt.Errorf("golden %s: %w", name, err)
	}
	markets := make([]Market, len(wire))
	for i, w := range wire {
		markets[i] = w.Market
		markets[i].Volume = cmp.Or(w.Volume, w.VolumeNum)
	}
	return markets, nil
}
