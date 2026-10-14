//go:build ignore

// Records the venue fixtures from the live APIs, run from the engine root as go run testdata/venues/record.go [hip4] [polyus] [kalshi]
package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/coder/websocket"
)

const (
	hlBase     = "https://api.hyperliquid.xyz"
	pmusBase   = "https://gateway.polymarket.us/v1"
	kalshiBase = "https://api.elections.kalshi.com/trade-api/v2"
	outDir     = "testdata/venues"
	// tradeRows is how many whole trades a trades fixture keeps
	tradeRows = 6
)

type trade struct {
	Coin string `json:"coin"`
	Hash string `json:"hash"`
	TID  int64  `json:"tid"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	which := os.Args[1:]
	if len(which) == 0 {
		which = []string{"hip4", "polyus", "kalshi"}
	}
	for _, w := range which {
		var err error
		switch w {
		case "hip4":
			err = recordHIP4(ctx)
		case "polyus":
			err = recordPolyUS(ctx)
		case "kalshi":
			err = recordKalshi(ctx)
		default:
			err = fmt.Errorf("unknown target %q", w)
		}
		if err != nil {
			log.Fatal(err)
		}
	}
}

func save(name string, v any) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outDir, name), append(b, '\n'), 0o644); err != nil {
		log.Fatal(err)
	}
	log.Printf("wrote %s (%d bytes)", name, len(b))
}

func do(ctx context.Context, method, url string, body any, out any) (int, error) {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rd)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "sona-intel-pm-recorder/1.0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, fmt.Errorf("%s %s: %d %s", method, url, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return resp.StatusCode, json.Unmarshal(b, out)
}

func info(ctx context.Context, body map[string]string, out any) error {
	_, err := do(ctx, http.MethodPost, hlBase+"/info", body, out)
	return err
}

// recordHIP4 keeps three outcomes of outcomeMeta, one of them with a busy tape, plus that tape from both sides and from the WebSocket
func recordHIP4(ctx context.Context) error {
	var meta struct {
		Outcomes  []json.RawMessage `json:"outcomes"`
		Questions []json.RawMessage `json:"questions"`
		Deployers []json.RawMessage `json:"deployers"`
		FeeScale  json.RawMessage   `json:"feeScale"`
	}
	if err := info(ctx, map[string]string{"type": "outcomeMeta"}, &meta); err != nil {
		return err
	}
	type pick struct {
		raw    json.RawMessage
		id     int
		trades []json.RawMessage
	}
	var picks []pick
	for _, raw := range meta.Outcomes {
		var o struct {
			Outcome int    `json:"outcome"`
			Name    string `json:"name"`
		}
		if err := json.Unmarshal(raw, &o); err != nil {
			return err
		}
		var rows []json.RawMessage
		if err := info(ctx, map[string]string{"type": "recentTrades", "coin": fmt.Sprintf("#%d", 10*o.Outcome)}, &rows); err != nil {
			return err
		}
		time.Sleep(time.Second)
		if len(rows) >= tradeRows || (len(picks) > 0 && len(rows) > 0 && len(picks) < 3) {
			picks = append(picks, pick{raw: raw, id: o.Outcome, trades: rows})
		}
		if len(picks) == 3 && len(picks[0].trades) >= tradeRows {
			break
		}
	}
	if len(picks) < 3 || len(picks[0].trades) < tradeRows {
		return fmt.Errorf("found %d busy outcomes", len(picks))
	}
	meta.Outcomes = nil
	for _, p := range picks {
		meta.Outcomes = append(meta.Outcomes, p.raw)
	}
	meta.Questions, meta.Deployers = []json.RawMessage{}, []json.RawMessage{}
	save("hl_outcomemeta.json", meta)

	busy := picks[0]
	save("hl_recenttrades.json", busy.trades[:tradeRows])
	var mirror []json.RawMessage
	if err := info(ctx, map[string]string{"type": "recentTrades", "coin": fmt.Sprintf("#%d", 10*busy.id+1)}, &mirror); err != nil {
		return err
	}
	save("hl_recenttrades_side1.json", firstN(mirror, tradeRows))

	c, _, err := websocket.Dial(ctx, "wss://api.hyperliquid.xyz/ws", nil)
	if err != nil {
		return err
	}
	defer c.CloseNow()
	sub := fmt.Sprintf(`{"method":"subscribe","subscription":{"type":"trades","coin":"#%d"}}`, 10*busy.id)
	if err := c.Write(ctx, websocket.MessageText, []byte(sub)); err != nil {
		return err
	}
	for {
		_, b, err := c.Read(ctx)
		if err != nil {
			return err
		}
		var m struct {
			Channel string            `json:"channel"`
			Data    []json.RawMessage `json:"data"`
		}
		if json.Unmarshal(b, &m) == nil && m.Channel == "trades" {
			m.Data = firstN(m.Data, tradeRows)
			save("hl_ws_trades.json", m)
			return nil
		}
	}
}

func firstN[T any](s []T, n int) []T {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// recordPolyUS keeps two open markets of the volume ranking with their quotes, plus one settled market
func recordPolyUS(ctx context.Context) error {
	var list struct {
		Markets []json.RawMessage `json:"markets"`
	}
	if _, err := do(ctx, http.MethodGet, pmusBase+"/markets?active=true&closed=false&orderBy=volume&orderDirection=desc&limit=10", nil, &list); err != nil {
		return err
	}
	var kept []json.RawMessage
	for _, raw := range list.Markets {
		var m struct {
			Slug string `json:"slug"`
		}
		if err := json.Unmarshal(raw, &m); err != nil {
			return err
		}
		var bbo json.RawMessage
		_, err := do(ctx, http.MethodGet, pmusBase+"/markets/"+m.Slug+"/bbo", nil, &bbo)
		time.Sleep(3 * time.Second)
		if err != nil {
			log.Printf("skip %s: %v", m.Slug, err)
			continue
		}
		if len(kept) == 0 {
			save("polyus_bbo.json", bbo)
		}
		kept = append(kept, raw)
		if len(kept) == 2 {
			break
		}
	}
	if len(kept) < 2 {
		return fmt.Errorf("only %d markets answered bbo", len(kept))
	}
	save("polyus_markets.json", map[string]any{"markets": kept})

	var closed struct {
		Markets []json.RawMessage `json:"markets"`
	}
	if _, err := do(ctx, http.MethodGet, pmusBase+"/markets?closed=true&orderBy=volume&orderDirection=desc&limit=1", nil, &closed); err != nil {
		return err
	}
	var m struct {
		Slug string `json:"slug"`
	}
	if len(closed.Markets) == 0 || json.Unmarshal(closed.Markets[0], &m) != nil {
		return fmt.Errorf("no closed market")
	}
	save("polyus_markets_closed.json", map[string]any{"markets": closed.Markets})
	var settle, bbo json.RawMessage
	time.Sleep(3 * time.Second)
	if _, err := do(ctx, http.MethodGet, pmusBase+"/markets/"+m.Slug+"/settlement", nil, &settle); err != nil {
		return err
	}
	save("polyus_settlement.json", settle)
	time.Sleep(3 * time.Second)
	if _, err := do(ctx, http.MethodGet, pmusBase+"/markets/"+m.Slug+"/bbo", nil, &bbo); err != nil {
		return err
	}
	save("polyus_bbo_expired.json", bbo)
	return nil
}

// recordKalshi keeps three open markets (busy, quiet-but-priced, unpriced), five large prints and two settled markets
func recordKalshi(ctx context.Context) error {
	var page struct {
		Markets []json.RawMessage `json:"markets"`
	}
	if _, err := do(ctx, http.MethodGet, kalshiBase+"/markets?status=open&mve_filter=exclude&limit=1000", nil, &page); err != nil {
		return err
	}
	type row struct {
		raw    json.RawMessage
		vol24  float64
		volume float64
		last   float64
	}
	var rows []row
	for _, raw := range page.Markets {
		var m struct {
			Vol24 string `json:"volume_24h_fp"`
			Vol   string `json:"volume_fp"`
			Last  string `json:"last_price_dollars"`
		}
		if err := json.Unmarshal(raw, &m); err != nil {
			return err
		}
		r := row{raw: raw}
		fmt.Sscan(m.Vol24, &r.vol24)
		fmt.Sscan(m.Vol, &r.volume)
		fmt.Sscan(m.Last, &r.last)
		rows = append(rows, r)
	}
	var busy, priced, unpriced json.RawMessage
	var best float64
	for _, r := range rows {
		switch {
		case r.vol24 > best:
			busy, best = r.raw, r.vol24
		case r.volume > 0 && r.last > 0 && priced == nil:
			priced = r.raw
		case r.volume == 0 && r.last == 0 && unpriced == nil:
			unpriced = r.raw
		}
	}
	if busy == nil || priced == nil || unpriced == nil {
		return fmt.Errorf("first page lacks a busy, a priced and an unpriced market")
	}
	save("kalshi_markets.json", map[string]any{"markets": []json.RawMessage{busy, priced, unpriced}})

	type tape struct {
		Trades []json.RawMessage `json:"trades"`
		Cursor string            `json:"cursor"`
	}
	var all []json.RawMessage
	cursor := ""
	since := time.Now().Add(-time.Minute).Unix()
	for range 4 {
		var tapes tape
		if _, err := do(ctx, http.MethodGet, fmt.Sprintf("%s/markets/trades?limit=1000&min_ts=%d&cursor=%s", kalshiBase, since, cursor), nil, &tapes); err != nil {
			return err
		}
		all = append(all, tapes.Trades...)
		if cursor = tapes.Cursor; cursor == "" {
			break
		}
	}
	type print struct {
		raw    json.RawMessage
		cost   float64
		side   string
		ticker string
	}
	var prints []print
	for _, raw := range all {
		var t struct {
			Count string `json:"count_fp"`
			Yes   string `json:"yes_price_dollars"`
			No    string `json:"no_price_dollars"`
			Side  string `json:"taker_side"`
			Tick  string `json:"ticker"`
		}
		if err := json.Unmarshal(raw, &t); err != nil {
			return err
		}
		var n, y, no float64
		fmt.Sscan(t.Count, &n)
		fmt.Sscan(t.Yes, &y)
		fmt.Sscan(t.No, &no)
		cost := n * y
		if t.Side == "no" {
			cost = n * no
		}
		prints = append(prints, print{raw, cost, t.Side, t.Tick})
	}
	slices.SortFunc(prints, func(a, b print) int { return cmp.Compare(b.cost, a.cost) })
	// one combo print to show the filter, the biggest plain prints, one taker on the no side and the smallest print
	var kept []json.RawMessage
	var combo, haveNo bool
	for _, p := range prints {
		isCombo := strings.HasPrefix(p.ticker, "KXMVE")
		switch {
		case isCombo && !combo:
			combo = true
		case !isCombo && (len(kept) < 3 || (p.side == "no" && !haveNo)):
			haveNo = haveNo || p.side == "no"
		default:
			continue
		}
		kept = append(kept, p.raw)
	}
	kept = append(kept, prints[len(prints)-1].raw)
	save("kalshi_trades.json", map[string]any{"trades": kept})

	var settled struct {
		Markets []json.RawMessage `json:"markets"`
	}
	if _, err := do(ctx, http.MethodGet, fmt.Sprintf("%s/markets?status=settled&mve_filter=exclude&limit=200&min_settled_ts=%d", kalshiBase, time.Now().Add(-time.Hour).Unix()), nil, &settled); err != nil {
		return err
	}
	var yes, no json.RawMessage
	for _, raw := range settled.Markets {
		var m struct {
			Result string `json:"result"`
		}
		if err := json.Unmarshal(raw, &m); err != nil {
			return err
		}
		if m.Result == "yes" && yes == nil {
			yes = raw
		}
		if m.Result == "no" && no == nil {
			no = raw
		}
	}
	if yes == nil || no == nil {
		return fmt.Errorf("no settled market with each result")
	}
	save("kalshi_settled.json", map[string]any{"markets": []json.RawMessage{yes, no}})
	return nil
}
