package collect

import (
	"cmp"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"pm/internal/httpx"
	"pm/internal/live"
	"pm/internal/venue"
	"pm/internal/venue/kalshi"
)

const (
	kalshiURL       = "https://api.elections.kalshi.com/trade-api/v2"
	kalshiUserAgent = "sona-intel-pm-collect/1.0"
	// kalshiRPS keeps a sweep of 60 pages under what the keyless API tolerated in a live test
	kalshiRPS = 2
	kalshiTop = 500
)

func kalshiJobs(get func(string) string, st venue.Store, reg prometheus.Registerer) ([]Job, error) {
	tradesEvery, err := Duration(get, "PM_KALSHI_EVERY", 10*time.Second)
	if err != nil {
		return nil, err
	}
	marketsEvery, err := Duration(get, "PM_KALSHI_MARKETS_EVERY", 10*time.Minute)
	if err != nil {
		return nil, err
	}
	settleEvery, err := Duration(get, "PM_KALSHI_SETTLE_EVERY", 5*time.Minute)
	if err != nil {
		return nil, err
	}
	minUSD, err := Float(get, "PM_KALSHI_MIN_USD", live.MinTradeUSD)
	if err != nil {
		return nil, err
	}
	top, err := Int(get, "PM_KALSHI_TOP", kalshiTop)
	if err != nil {
		return nil, err
	}
	rps, err := Float(get, "PM_KALSHI_RPS", kalshiRPS)
	if err != nil {
		return nil, err
	}
	cli := &kalshi.Client{
		HTTP: &httpx.Client{
			HTTP: &http.Client{Timeout: 30 * time.Second}, UserAgent: kalshiUserAgent, Limiter: httpx.NewLimiter(rps), Retries: 4,
		},
		BaseURL: strings.TrimSuffix(cmp.Or(get("PM_KALSHI_URL"), kalshiURL), "/"),
	}
	m := kalshi.NewMetrics(reg)
	return []Job{
		{Source: &kalshi.Trades{Client: cli, Store: st, MinUSD: minUSD, Metrics: m}, Every: tradesEvery},
		{Source: &kalshi.Catalogue{Client: cli, Store: st, Top: top, Metrics: m}, Every: marketsEvery},
		{Source: &kalshi.Settlements{Client: cli, Store: st, Metrics: m}, Every: settleEvery},
	}, nil
}
