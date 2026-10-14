package collect

import (
	"cmp"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"pm/internal/hip4"
	"pm/internal/httpx"
	"pm/internal/venue"
	venuehip4 "pm/internal/venue/hip4"
)

const (
	hlURL       = "https://api.hyperliquid.xyz"
	hlUserAgent = "sona-intel-pm-collect/1.0"
	hlCheckEach = 30 * time.Second
)

// hip4Store is the part of the Postgres store the settlement and wallet jobs need beyond the shared one
type hip4Store interface {
	venue.Store
	hip4.SettleStore
	hip4.WalletStore
}

func hyperliquid(get func(string) string, st venue.Store, reg prometheus.Registerer) ([]Job, error) {
	store, ok := st.(hip4Store)
	if !ok {
		return nil, fmt.Errorf("%w: the store cannot settle or rate HIP-4 outcomes", ErrConfig)
	}
	settleEvery, err := Duration(get, "PM_HL_SETTLE_EVERY", 5*time.Minute)
	if err != nil {
		return nil, err
	}
	settleBatch, err := Int(get, "PM_HL_SETTLE_BATCH", 10)
	if err != nil {
		return nil, err
	}
	walletsEvery, err := Duration(get, "PM_HL_WALLETS_EVERY", 10*time.Minute)
	if err != nil {
		return nil, err
	}
	meta, err := Duration(get, "PM_HL_META_EVERY", 5*time.Minute)
	if err != nil {
		return nil, err
	}
	weight, err := Int(get, "PM_HL_GAP_WEIGHT", 300)
	if err != nil {
		return nil, err
	}
	base := strings.TrimSuffix(cmp.Or(get("PM_HL_URL"), hlURL), "/")
	ws := "ws" + strings.TrimPrefix(base, "http") + "/ws"
	s := &venuehip4.Stream{
		Store:     st,
		HTTP:      &httpx.Client{HTTP: &http.Client{Timeout: 20 * time.Second}, UserAgent: hlUserAgent, Limiter: httpx.NewLimiter(1), Retries: 3},
		InfoURL:   base + "/info",
		WSURL:     ws,
		Metrics:   venuehip4.NewMetrics(reg),
		MetaEvery: meta,
		PingEvery: 30 * time.Second,
		SendEvery: 50 * time.Millisecond,
		GapWeight: weight,
	}
	settle := &hip4.Settlements{HTTP: s.HTTP, InfoURL: s.InfoURL, Store: store, Batch: settleBatch, Metrics: hip4.NewSettleMetrics(reg)}
	return []Job{
		{Source: s, Every: hlCheckEach},
		{Source: settle, Every: settleEvery},
		{Source: &hip4.Wallets{Store: store}, Every: walletsEvery},
	}, nil
}
