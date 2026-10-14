package collect

import (
	"cmp"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"pm/internal/httpx"
	"pm/internal/venue"
	"pm/internal/venue/hip4"
)

const (
	hlURL       = "https://api.hyperliquid.xyz"
	hlUserAgent = "sona-intel-pm-collect/1.0"
	hlCheckEach = 30 * time.Second
)

func hyperliquid(get func(string) string, st venue.Store, reg prometheus.Registerer) ([]Job, error) {
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
	s := &hip4.Stream{
		Store:     st,
		HTTP:      &httpx.Client{HTTP: &http.Client{Timeout: 20 * time.Second}, UserAgent: hlUserAgent, Limiter: httpx.NewLimiter(1), Retries: 3},
		InfoURL:   base + "/info",
		WSURL:     ws,
		Metrics:   hip4.NewMetrics(reg),
		MetaEvery: meta,
		PingEvery: 30 * time.Second,
		SendEvery: 50 * time.Millisecond,
		GapWeight: weight,
	}
	return []Job{{Source: s, Every: hlCheckEach}}, nil
}
