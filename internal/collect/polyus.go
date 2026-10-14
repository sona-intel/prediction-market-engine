package collect

import (
	"cmp"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"pm/internal/httpx"
	"pm/internal/venue"
	"pm/internal/venue/polyus"
)

const (
	pmusURL       = "https://gateway.polymarket.us/v1"
	pmusUserAgent = "sona-intel-pm-collect/1.0"
	// pmusRPS is the sustained rate the gateway allowed in a live test, as it answers 429 with Retry-After above about half a request a second
	pmusRPS         = 0.4
	pmusTop         = 100
	pmusSettleBatch = 100
)

func polymarketUS(get func(string) string, st venue.Store, reg prometheus.Registerer) ([]Job, error) {
	quotesEvery, err := Duration(get, "PM_PMUS_EVERY", 10*time.Minute)
	if err != nil {
		return nil, err
	}
	sweepEvery, err := Duration(get, "PM_PMUS_SWEEP_EVERY", time.Hour)
	if err != nil {
		return nil, err
	}
	settleEvery, err := Duration(get, "PM_PMUS_SETTLE_EVERY", 10*time.Minute)
	if err != nil {
		return nil, err
	}
	top, err := Int(get, "PM_PMUS_TOP", pmusTop)
	if err != nil {
		return nil, err
	}
	rps, err := Float(get, "PM_PMUS_RPS", pmusRPS)
	if err != nil {
		return nil, err
	}
	cli := &polyus.Client{
		HTTP: &httpx.Client{
			HTTP: &http.Client{Timeout: 30 * time.Second}, UserAgent: pmusUserAgent, Limiter: httpx.NewLimiter(rps), Retries: 4,
		},
		BaseURL: strings.TrimSuffix(cmp.Or(get("PM_PMUS_URL"), pmusURL), "/"),
	}
	m := polyus.NewMetrics(reg)
	return []Job{
		{Source: &polyus.Quotes{Client: cli, Store: st, Top: top, Metrics: m}, Every: quotesEvery},
		{Source: &polyus.Catalogue{Client: cli, Store: st}, Every: sweepEvery},
		{Source: &polyus.Settlements{Client: cli, Store: st, Batch: pmusSettleBatch, Metrics: m}, Every: settleEvery},
	}, nil
}
