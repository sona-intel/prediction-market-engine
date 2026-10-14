// Package venuetest gives source tests an in-memory venue.Store
package venuetest

import (
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	"pm/internal/venue"
)

type Mem struct {
	mu      sync.Mutex
	markets map[string]venue.Market
	snaps   map[string]venue.Snap
	trades  map[string]venue.Trade
	cursors map[string]string
}

var _ venue.Store = (*Mem)(nil)

func New() *Mem {
	return &Mem{markets: map[string]venue.Market{}, snaps: map[string]venue.Snap{}, trades: map[string]venue.Trade{}, cursors: map[string]string{}}
}

func (m *Mem) UpsertMarkets(_ context.Context, ms []venue.Market) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, n := range ms {
		k := n.Venue + "/" + n.ID
		if old, ok := m.markets[k]; ok && old.Result != "" {
			n.Status = old.Status
		}
		if old, ok := m.markets[k]; ok && n.Result == "" {
			n.Result = old.Result
		}
		m.markets[k] = n
	}
	return nil
}

func (m *Mem) InsertSnaps(_ context.Context, ss []venue.Snap) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, s := range ss {
		k := s.Venue + "/" + s.MarketID + "/" + s.TS.String()
		if _, ok := m.snaps[k]; !ok {
			m.snaps[k] = s
			n++
		}
	}
	return n, nil
}

func (m *Mem) InsertTrades(_ context.Context, ts []venue.Trade) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, t := range ts {
		k := t.Venue + "/" + t.ID
		if _, ok := m.trades[k]; !ok {
			m.trades[k] = t
			n++
		}
	}
	return n, nil
}

func (m *Mem) Cursor(_ context.Context, source string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cursors[source], nil
}

func (m *Mem) SetCursor(_ context.Context, source, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cursors[source] = value
	return nil
}

func (m *Mem) LastTradeTS(_ context.Context, code, marketID string) (time.Time, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var last time.Time
	for _, t := range m.trades {
		if t.Venue == code && t.MarketID == marketID && t.TS.After(last) {
			last = t.TS
		}
	}
	return last, !last.IsZero(), nil
}

func (m *Mem) ids(code string, keep func(venue.Market) bool) []string {
	var ids []string
	for _, v := range m.markets {
		if v.Venue == code && keep(v) {
			ids = append(ids, v.ID)
		}
	}
	slices.Sort(ids)
	return ids
}

func (m *Mem) OpenMarketIDs(_ context.Context, code string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ids(code, func(v venue.Market) bool { return v.Result == "" }), nil
}

func (m *Mem) MarkClosed(_ context.Context, code string, ids []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range ids {
		if v, ok := m.markets[code+"/"+id]; ok && v.Status == "open" {
			v.Status = "closed"
			m.markets[code+"/"+id] = v
		}
	}
	return nil
}

func (m *Mem) ExpiredMarketIDs(_ context.Context, code string, before time.Time, limit int) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := m.ids(code, func(v venue.Market) bool { return v.Result == "" && !v.Close.IsZero() && v.Close.Before(before) })
	slices.SortStableFunc(ids, func(a, b string) int { return m.markets[code+"/"+b].Close.Compare(m.markets[code+"/"+a].Close) })
	return ids[:min(limit, len(ids))], nil
}

func (m *Mem) SetResult(_ context.Context, code, id, status, result string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := m.markets[code+"/"+id]
	v.Status, v.Result = status, result
	m.markets[code+"/"+id] = v
	return nil
}

func (m *Mem) Market(code, id string) (venue.Market, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.markets[code+"/"+id]
	return v, ok
}

// Trades returns the stored trades of a venue ordered by id
func (m *Mem) Trades(code string) []venue.Trade {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []venue.Trade
	for _, k := range slices.Sorted(maps.Keys(m.trades)) {
		if m.trades[k].Venue == code {
			out = append(out, m.trades[k])
		}
	}
	return out
}

// Snaps returns the stored snapshots of a venue ordered by market and time
func (m *Mem) Snaps(code string) []venue.Snap {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []venue.Snap
	for _, k := range slices.Sorted(maps.Keys(m.snaps)) {
		if m.snaps[k].Venue == code {
			out = append(out, m.snaps[k])
		}
	}
	return out
}
