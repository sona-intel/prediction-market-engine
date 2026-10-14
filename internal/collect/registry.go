package collect

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"pm/internal/venue"
)

const housekeepEvery = time.Hour

// Factory builds the jobs of one venue from its settings
type Factory func(get func(string) string, st venue.Store, reg prometheus.Registerer) ([]Job, error)

// factories grows as each venue source lands, and a venue without one is skipped with a warning
func factories() map[string]Factory {
	return map[string]Factory{venue.Hyperliquid: hyperliquid, venue.PolymarketUS: polymarketUS, venue.Kalshi: kalshiJobs}
}

// Pruner drops snapshots and the tape of walletless venues past retention
type Pruner interface {
	PruneSnaps(ctx context.Context, before time.Time) (int64, error)
	PruneTrades(ctx context.Context, code string, before time.Time) (int64, error)
}

// Build returns the jobs for every enabled venue, plus the housekeeping job
func Build(cfg Config, get func(string) string, st venue.Store, pruner Pruner, reg prometheus.Registerer) ([]Job, error) {
	var jobs []Job
	fs := factories()
	for _, code := range cfg.Venues {
		f, ok := fs[code]
		if !ok {
			slog.Warn("venue has no source yet", "venue", code)
			continue
		}
		js, err := f(get, st, reg)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, js...)
	}
	hk := &housekeeping{pruner: pruner, keep: time.Duration(cfg.KeepDays) * 24 * time.Hour, now: time.Now}
	for _, code := range cfg.Venues {
		if !venue.Catalog[code].Wallets {
			hk.tapes = append(hk.tapes, code)
		}
	}
	return append(jobs, Job{Source: hk, Every: housekeepEvery}), nil
}

type housekeeping struct {
	pruner Pruner
	keep   time.Duration
	tapes  []string
	now    func() time.Time
}

func (h *housekeeping) Name() string { return "housekeeping" }

func (h *housekeeping) Poll(ctx context.Context) error {
	before := h.now().Add(-h.keep)
	n, err := h.pruner.PruneSnaps(ctx, before)
	if err != nil {
		return err
	}
	if n > 0 {
		slog.Info("pruned snapshots", "rows", n)
	}
	for _, code := range h.tapes {
		n, err := h.pruner.PruneTrades(ctx, code, before)
		if err != nil {
			return err
		}
		if n > 0 {
			slog.Info("pruned trades", "venue", code, "rows", n)
		}
	}
	return nil
}
