package hip4

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"pm/internal/httpx"
	"pm/internal/venue"
)

const (
	// settleWallets is how many recent traders of a market are asked for the settlement fill, since a wallet that closed out holds none
	settleWallets = 4
	retryAfter    = time.Hour
	skipDepth     = 5
)

// Fill is one row of a wallet's fills, and a settlement is a fill whose direction is Settlement
type Fill struct {
	Coin string `json:"coin"`
	Px   string `json:"px"`
	Dir  string `json:"dir"`
	Time int64  `json:"time"`
}

type SettleMetrics struct {
	settled  prometheus.Counter
	failures prometheus.Counter
}

func NewSettleMetrics(reg prometheus.Registerer) *SettleMetrics {
	m := &SettleMetrics{
		settled:  prometheus.NewCounter(prometheus.CounterOpts{Name: "pm_hip4_settled_total", Help: "HIP-4 outcomes whose settlement was recorded"}),
		failures: prometheus.NewCounter(prometheus.CounterOpts{Name: "pm_hip4_settle_failures_total", Help: "HIP-4 fill lookups that failed while settling"}),
	}
	reg.MustRegister(m.settled, m.failures)
	return m
}

func (m *SettleMetrics) Settled() prometheus.Counter { return m.settled }

func (m *SettleMetrics) Failures() prometheus.Counter { return m.failures }

// Settlements records how outcomes that left the live list ended, which the venue shows only as Settlement fills of the wallets that held them
type Settlements struct {
	HTTP    *httpx.Client
	InfoURL string
	Store   SettleStore
	Batch   int
	Metrics *SettleMetrics
	Now     func() time.Time

	tried map[string]time.Time
}

func (s *Settlements) Name() string { return venue.Hyperliquid + "/settlement" }

func (s *Settlements) Poll(ctx context.Context) error {
	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	if s.tried == nil {
		s.tried = map[string]time.Time{}
	}
	// outcomes tried lately are skipped, so the list is read deeper than one batch
	ids, err := s.Store.UnsettledClosed(ctx, venue.Hyperliquid, s.Batch*skipDepth)
	if err != nil {
		return err
	}
	attempts := 0
	for _, id := range ids {
		if at, ok := s.tried[id]; ok && now().Sub(at) < retryAfter {
			continue
		}
		if attempts++; attempts > s.Batch {
			break
		}
		s.tried[id] = now()
		yes, at, ok, err := s.find(ctx, id)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			s.Metrics.failures.Inc()
			slog.Warn("settlement lookup failed", "market", id, "error", err)
			continue
		}
		if !ok {
			continue
		}
		if err := s.Store.SettleMarket(ctx, venue.Hyperliquid, id, Result(yes), at); err != nil {
			return err
		}
		delete(s.tried, id)
		s.Metrics.settled.Inc()
	}
	return nil
}

// find asks the newest traders of a market for their fills since its last trade and reads the settlement price of the Yes side
func (s *Settlements) find(ctx context.Context, coin string) (yesPPM int32, at time.Time, ok bool, err error) {
	last, has, err := s.Store.LastTradeTS(ctx, venue.Hyperliquid, coin)
	if err != nil || !has {
		return 0, time.Time{}, false, err
	}
	traders, err := s.Store.LastTraders(ctx, venue.Hyperliquid, coin, settleWallets)
	if err != nil {
		return 0, time.Time{}, false, err
	}
	var firstErr error
	for _, w := range traders {
		var fills []Fill
		body := map[string]any{"type": "userFillsByTime", "user": "0x" + hex.EncodeToString(w), "startTime": last.UnixMilli(), "aggregateByTime": false}
		if err := s.HTTP.Post(ctx, s.InfoURL, body, &fills); err != nil {
			firstErr = err
			continue
		}
		if yes, at, ok := Settlement(fills, coin); ok {
			return yes, at, true, nil
		}
	}
	return 0, time.Time{}, false, firstErr
}

// Settlement reads the Yes payout of a coin from a wallet's fills, taking a fill of the No coin as the complement
func Settlement(fills []Fill, coin string) (yesPPM int32, at time.Time, ok bool) {
	base := strings.TrimSuffix(coin, "0")
	for _, f := range fills {
		if f.Dir != "Settlement" || (f.Coin != coin && f.Coin != base+"1") {
			continue
		}
		px, err := strconv.ParseFloat(f.Px, 64)
		if err != nil || math.IsNaN(px) || px < 0 || px > 1 {
			continue
		}
		yes := int32(math.Round(px * venue.PPM))
		if f.Coin != coin {
			yes = venue.PPM - yes
		}
		return yes, time.UnixMilli(f.Time).UTC(), true
	}
	return 0, time.Time{}, false
}

// Result names a Yes payout, with a fraction written out when the deployer settled between the two sides
func Result(yesPPM int32) string {
	switch yesPPM {
	case venue.PPM:
		return "yes"
	case 0:
		return "no"
	}
	return fmt.Sprintf("%g", float64(yesPPM)/venue.PPM)
}

// Payout converts a stored result back to the payout ppm of Yes and No
func Payout(result string) (yes, no int64, ok bool) {
	switch result {
	case "yes":
		return venue.PPM, 0, true
	case "no":
		return 0, venue.PPM, true
	case "":
		return 0, 0, false
	}
	f, err := strconv.ParseFloat(result, 64)
	if err != nil || f < 0 || f > 1 {
		return 0, 0, false
	}
	yes = int64(math.Round(f * venue.PPM))
	return yes, venue.PPM - yes, true
}
