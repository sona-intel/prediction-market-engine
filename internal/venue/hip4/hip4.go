// Package hip4 records Hyperliquid outcome markets from the WebSocket trades channel and the info endpoint
package hip4

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"pm/internal/venue"
)

const (
	// Both sides of an outcome carry the same fills mirrored, so only side 0 is recorded
	coinFactor = 10
	deadline   = "20060102-1504"
)

var errBadTrade = errors.New("unusable trade")

type Meta struct {
	Outcomes []Outcome `json:"outcomes"`
}

type Outcome struct {
	ID          int    `json:"outcome"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type Trade struct {
	Coin  string   `json:"coin"`
	Side  string   `json:"side"`
	Px    string   `json:"px"`
	Sz    string   `json:"sz"`
	Time  int64    `json:"time"`
	Hash  string   `json:"hash"`
	TID   int64    `json:"tid"`
	Users []string `json:"users"`
}

// Coin is the side-0 asset name of an outcome
func Coin(outcome int) string { return "#" + strconv.Itoa(coinFactor*outcome) }

func (o Outcome) Market() venue.Market {
	f := fields(o.Description)
	m := venue.Market{
		Venue:  venue.Hyperliquid,
		ID:     Coin(o.ID),
		Title:  title(f, o.Description),
		Kind:   strings.TrimPrefix(o.Name, "template:"),
		Status: "open",
	}
	for _, k := range []string{"resolutionDeadline", "time"} {
		if t, err := time.Parse(deadline, f[k]); err == nil {
			m.Close = t.UTC()
			break
		}
	}
	return m
}

func fields(desc string) map[string]string {
	f := map[string]string{}
	for part := range strings.SplitSeq(desc, "|") {
		if k, v, ok := strings.Cut(part, ":"); ok {
			f[k] = v
		}
	}
	return f
}

func title(f map[string]string, desc string) string {
	switch {
	case f["participantA"] != "" && f["participantB"] != "":
		return f["participantA"] + " vs " + f["participantB"]
	case f["participant"] != "":
		return f["participant"]
	case f["perp"] != "":
		return strings.TrimSpace(fmt.Sprintf("%s %s %s", f["perp"], f["priceDescription"], cmpValue(f)))
	}
	return desc
}

func cmpValue(f map[string]string) string {
	if v := f["threshold"]; v != "" {
		return v
	}
	return f["target"]
}

// Convert turns a side-0 fill into a stored trade, and skips rows that would break the table checks
func Convert(t Trade) (venue.Trade, error) {
	px, err := strconv.ParseFloat(t.Px, 64)
	if err != nil || math.IsNaN(px) || px < 0 || px > 1 {
		return venue.Trade{}, fmt.Errorf("%w: price %q", errBadTrade, t.Px)
	}
	sz, err := strconv.ParseFloat(t.Sz, 64)
	if err != nil || math.IsNaN(sz) || math.IsInf(sz, 0) || sz <= 0 || sz > math.MaxInt64/venue.Micro/2 {
		return venue.Trade{}, fmt.Errorf("%w: size %q", errBadTrade, t.Sz)
	}
	if len(t.Users) != 2 {
		return venue.Trade{}, fmt.Errorf("%w: %d users", errBadTrade, len(t.Users))
	}
	buyer, err := wallet(t.Users[0])
	if err != nil {
		return venue.Trade{}, err
	}
	seller, err := wallet(t.Users[1])
	if err != nil {
		return venue.Trade{}, err
	}
	out := venue.Trade{
		Venue:    venue.Hyperliquid,
		ID:       strconv.FormatInt(t.TID, 10),
		MarketID: t.Coin,
		TS:       time.UnixMilli(t.Time).UTC(),
		PricePPM: int32(math.Round(px * venue.PPM)),
		Qty:      int64(math.Round(sz * venue.Micro)),
		USD:      int64(math.Round(sz * px * venue.Micro)),
	}
	switch t.Side {
	case "B":
		out.Side, out.Taker, out.Maker = "buy", buyer, seller
	case "A":
		out.Side, out.Taker, out.Maker = "sell", seller, buyer
	default:
		return venue.Trade{}, fmt.Errorf("%w: side %q", errBadTrade, t.Side)
	}
	return out, nil
}

func wallet(s string) ([]byte, error) {
	b, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
	if err != nil || len(b) != 20 {
		return nil, fmt.Errorf("%w: wallet %q", errBadTrade, s)
	}
	return b, nil
}
