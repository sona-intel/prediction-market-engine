// Package collect runs the venue sources on their own tickers, so one failing source never stops another
package collect

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"pm/internal/venue"
)

// Job polls one source every Every, and sources named venue/feed mark their venue as healthy on success
type Job struct {
	Source venue.Source
	Every  time.Duration
}

// Health records the last good poll of a venue
type Health interface {
	MarkOK(ctx context.Context, code string, at time.Time) error
}

type Metrics struct {
	polls    *prometheus.CounterVec
	duration *prometheus.GaugeVec
	lastOK   *prometheus.GaugeVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		polls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pm_collect_polls_total", Help: "Polls by source and result",
		}, []string{"source", "result"}),
		duration: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pm_collect_poll_seconds", Help: "Duration of the last poll by source",
		}, []string{"source"}),
		lastOK: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pm_collect_last_ok_timestamp_seconds", Help: "Unix time of the last good poll by source",
		}, []string{"source"}),
	}
	reg.MustRegister(m.polls, m.duration, m.lastOK)
	return m
}

// PollsFor returns the counter of one source and result, for tests
func (m *Metrics) PollsFor(source, result string) prometheus.Counter {
	return m.polls.WithLabelValues(source, result)
}

type Scheduler struct {
	Jobs    []Job
	Health  Health
	Metrics *Metrics
	Now     func() time.Time
}

// Run blocks until ctx ends and returns once every job has stopped
func (s *Scheduler) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, j := range s.Jobs {
		wg.Go(func() { s.loop(ctx, j) })
	}
	wg.Wait()
}

func (s *Scheduler) loop(ctx context.Context, j Job) {
	s.poll(ctx, j)
	t := time.NewTicker(j.Every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.poll(ctx, j)
		}
	}
}

func (s *Scheduler) poll(ctx context.Context, j Job) {
	name := j.Source.Name()
	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	start := now()
	err := j.Source.Poll(ctx)
	if ctx.Err() != nil {
		return
	}
	took := now().Sub(start)
	if s.Metrics != nil {
		s.Metrics.duration.WithLabelValues(name).Set(took.Seconds())
	}
	if err != nil {
		slog.Warn("poll failed", "source", name, "error", err)
		if s.Metrics != nil {
			s.Metrics.polls.WithLabelValues(name, "error").Inc()
		}
		return
	}
	if s.Metrics != nil {
		s.Metrics.polls.WithLabelValues(name, "ok").Inc()
		s.Metrics.lastOK.WithLabelValues(name).Set(float64(now().Unix()))
	}
	if code, _, feed := strings.Cut(name, "/"); feed && s.Health != nil {
		if err := s.Health.MarkOK(ctx, code, now()); err != nil {
			slog.Warn("mark venue ok failed", "venue", code, "error", err)
		}
	}
}
