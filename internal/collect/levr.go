package collect

import (
	"cmp"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"pm/internal/httpx"
	"pm/internal/levr"
	"pm/internal/venue"
	vl "pm/internal/venue/levr"
)

const (
	levrURL       = "https://levrbet.squids.live/v1-levr-prod-portal@v1/api/graphql"
	levrUserAgent = "sona-intel-pm-collect/1.0"
	levrRPS       = 2
)

// levrStore is the part of the Postgres store the Levr jobs need beyond the shared one
type levrStore interface {
	vl.Store
	levr.WalletStore
}

func levrJobs(get func(string) string, st venue.Store, reg prometheus.Registerer) ([]Job, error) {
	store, ok := st.(levrStore)
	if !ok {
		return nil, fmt.Errorf("%w: the store cannot hold Levr positions", ErrConfig)
	}
	every, err := Duration(get, "PM_LEVR_EVERY", time.Minute)
	if err != nil {
		return nil, err
	}
	walletsEvery, err := Duration(get, "PM_LEVR_WALLETS_EVERY", 10*time.Minute)
	if err != nil {
		return nil, err
	}
	lookback, err := Duration(get, "PM_LEVR_LOOKBACK", 24*time.Hour)
	if err != nil {
		return nil, err
	}
	rps, err := Float(get, "PM_LEVR_RPS", levrRPS)
	if err != nil {
		return nil, err
	}
	src := &vl.Source{
		Client: &vl.Client{
			HTTP: &httpx.Client{HTTP: &http.Client{Timeout: 60 * time.Second}, UserAgent: levrUserAgent, Limiter: httpx.NewLimiter(rps), Retries: 3},
			URL:  cmp.Or(get("PM_LEVR_URL"), levrURL),
		},
		Store:    store,
		Metrics:  vl.NewMetrics(reg),
		Backfill: get("PM_LEVR_BACKFILL") == "1",
		Lookback: lookback,
	}
	return []Job{{Source: src, Every: every}, {Source: &levr.Wallets{Store: store}, Every: walletsEvery}}, nil
}
