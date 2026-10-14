package collect

import (
	"context"
	"log/slog"
	"time"

	"pm/internal/venue"
)

const housekeepEvery = time.Hour

// Factory builds the jobs of one venue from its settings
type Factory func(get func(string) string, sink venue.Sink) ([]Job, error)

// factories grows as each venue source lands, and a venue without one is skipped with a warning
func factories() map[string]Factory { return map[string]Factory{} }

// Pruner drops snapshots past retention
type Pruner interface {
	PruneSnaps(ctx context.Context, before time.Time) (int64, error)
}

// Build returns the jobs for every enabled venue, plus the housekeeping job
func Build(cfg Config, get func(string) string, sink venue.Sink, pruner Pruner) ([]Job, error) {
	var jobs []Job
	fs := factories()
	for _, code := range cfg.Venues {
		f, ok := fs[code]
		if !ok {
			slog.Warn("venue has no source yet", "venue", code)
			continue
		}
		js, err := f(get, sink)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, js...)
	}
	return append(jobs, Job{Source: &housekeeping{pruner: pruner, keep: time.Duration(cfg.KeepDays) * 24 * time.Hour, now: time.Now}, Every: housekeepEvery}), nil
}

type housekeeping struct {
	pruner Pruner
	keep   time.Duration
	now    func() time.Time
}

func (h *housekeeping) Name() string { return "housekeeping" }

func (h *housekeeping) Poll(ctx context.Context) error {
	n, err := h.pruner.PruneSnaps(ctx, h.now().Add(-h.keep))
	if err == nil && n > 0 {
		slog.Info("pruned snapshots", "rows", n)
	}
	return err
}
