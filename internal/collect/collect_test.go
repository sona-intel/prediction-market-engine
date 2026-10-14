package collect_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"pm/internal/collect"
)

type fake struct {
	name  string
	polls atomic.Int32
	err   error
}

func (f *fake) Name() string { return f.name }

func (f *fake) Poll(context.Context) error {
	f.polls.Add(1)
	return f.err
}

type health struct {
	mu sync.Mutex
	ok map[string]int
}

func (h *health) MarkOK(_ context.Context, code string, _ time.Time) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ok[code]++
	return nil
}

func (h *health) count(code string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ok[code]
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestFailingSourceDoesNotStopTheOthers(t *testing.T) {
	good := &fake{name: "kalshi/trades"}
	bad := &fake{name: "levr/positions", err: errors.New("down")}
	h := &health{ok: map[string]int{}}
	reg := prometheus.NewRegistry()
	m := collect.NewMetrics(reg)
	s := &collect.Scheduler{
		Jobs:    []collect.Job{{Source: good, Every: 5 * time.Millisecond}, {Source: bad, Every: 5 * time.Millisecond}},
		Health:  h,
		Metrics: m,
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	waitFor(t, func() bool { return good.polls.Load() >= 3 && bad.polls.Load() >= 3 })
	cancel()
	<-done
	if h.count("kalshi") == 0 || h.count("levr") != 0 {
		t.Fatalf("health marks: kalshi %d, levr %d", h.count("kalshi"), h.count("levr"))
	}
	if testutil.ToFloat64(m.PollsFor("levr/positions", "error")) == 0 || testutil.ToFloat64(m.PollsFor("kalshi/trades", "ok")) == 0 {
		t.Fatal("poll counters did not move")
	}
}

func TestPollsRunOnceImmediatelyAndStopOnCancel(t *testing.T) {
	f := &fake{name: "housekeeping"}
	s := &collect.Scheduler{Jobs: []collect.Job{{Source: f, Every: time.Hour}}}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	waitFor(t, func() bool { return f.polls.Load() == 1 })
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("scheduler did not stop")
	}
	if f.polls.Load() != 1 {
		t.Fatalf("polled %d times", f.polls.Load())
	}
}

func TestConfig(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	if _, err := collect.ConfigFrom(env(nil)); !errors.Is(err, collect.ErrConfig) {
		t.Fatalf("missing url: %v", err)
	}
	c, err := collect.ConfigFrom(env(map[string]string{"PM_PG_URL": "x"}))
	if err != nil || len(c.Venues) != 4 || c.KeepDays != 60 {
		t.Fatalf("defaults %+v, %v", c, err)
	}
	c, err = collect.ConfigFrom(env(map[string]string{"PM_PG_URL": "x", "PM_COLLECT_VENUES": "kalshi, kalshi,levr", "PM_SNAP_KEEP_DAYS": "7"}))
	if err != nil || len(c.Venues) != 2 || c.KeepDays != 7 {
		t.Fatalf("custom %+v, %v", c, err)
	}
	for _, bad := range []map[string]string{
		{"PM_PG_URL": "x", "PM_COLLECT_VENUES": "nope"},
		{"PM_PG_URL": "x", "PM_SNAP_KEEP_DAYS": "0"},
	} {
		if _, err := collect.ConfigFrom(env(bad)); !errors.Is(err, collect.ErrConfig) {
			t.Fatalf("%v: %v", bad, err)
		}
	}
	if d, err := collect.Duration(env(map[string]string{"K": "3s"}), "K", time.Minute); err != nil || d != 3*time.Second {
		t.Fatalf("duration %v, %v", d, err)
	}
	if _, err := collect.Duration(env(map[string]string{"K": "-1s"}), "K", time.Minute); !errors.Is(err, collect.ErrConfig) {
		t.Fatalf("negative: %v", err)
	}
}

type pruner struct {
	before time.Time
	tapes  []string
}

func (p *pruner) PruneTrades(_ context.Context, code string, _ time.Time) (int64, error) {
	p.tapes = append(p.tapes, code)
	return 0, nil
}

func (p *pruner) PruneSnaps(_ context.Context, before time.Time) (int64, error) {
	p.before = before
	return 0, nil
}

func TestBuildAddsHousekeepingAndSkipsVenuesWithoutSources(t *testing.T) {
	p := &pruner{}
	jobs, err := collect.Build(collect.Config{Venues: []string{"kalshi", "levr"}, KeepDays: 2}, func(string) string { return "" }, nil, p, prometheus.NewRegistry())
	if err != nil || len(jobs) != 1 || jobs[0].Source.Name() != "housekeeping" {
		t.Fatalf("jobs %v, %v", jobs, err)
	}
	if err := jobs[0].Source.Poll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if age := time.Since(p.before); age < 47*time.Hour || age > 49*time.Hour {
		t.Fatalf("pruned before %v ago", age)
	}
	if len(p.tapes) != 1 || p.tapes[0] != "kalshi" {
		t.Fatalf("pruned tapes %v", p.tapes)
	}
}

func TestBuildMakesTheJobsOfHyperliquidAndPolymarketUS(t *testing.T) {
	env := map[string]string{"PM_PMUS_TOP": "7", "PM_PMUS_EVERY": "3m"}
	cfg := collect.Config{Venues: []string{"hyperliquid", "polymarket-us"}, KeepDays: 2}
	jobs, err := collect.Build(cfg, func(k string) string { return env[k] }, nil, &pruner{}, prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, j := range jobs {
		names = append(names, j.Source.Name())
	}
	want := []string{"hyperliquid/stream", "polymarket-us/quotes", "polymarket-us/catalogue", "polymarket-us/settlement", "housekeeping"}
	if !slices.Equal(names, want) || jobs[1].Every != 3*time.Minute {
		t.Fatalf("jobs %v", names)
	}
	env["PM_HL_GAP_WEIGHT"] = "0"
	if _, err := collect.Build(cfg, func(k string) string { return env[k] }, nil, &pruner{}, prometheus.NewRegistry()); !errors.Is(err, collect.ErrConfig) {
		t.Fatalf("zero weight gave %v", err)
	}
	delete(env, "PM_HL_GAP_WEIGHT")
	env["PM_PMUS_RPS"] = "0"
	if _, err := collect.Build(cfg, func(k string) string { return env[k] }, nil, &pruner{}, prometheus.NewRegistry()); !errors.Is(err, collect.ErrConfig) {
		t.Fatalf("zero rate gave %v", err)
	}
}
