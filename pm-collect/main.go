package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"pm/internal/collect"
	"pm/internal/pgstore"
	"pm/internal/venue"
)

const (
	addr          = ":8080"
	healthURL     = "http://127.0.0.1" + addr + "/healthz"
	healthTimeout = 3 * time.Second
	stopTimeout   = 10 * time.Second
)

var errUsage = errors.New("usage: pm-collect [serve | migrate | healthcheck]")

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	slog.SetDefault(logger())
	if err := run(ctx, os.Args[1:], os.Getenv); err != nil {
		slog.Error("pm-collect stopped", "error", err)
		stop()
		os.Exit(1)
	}
}

func logger() *slog.Logger {
	level := slog.LevelInfo
	_ = level.UnmarshalText([]byte(os.Getenv("LOG_LEVEL")))
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}

func run(ctx context.Context, args []string, get func(string) string) error {
	cmd := "serve"
	if len(args) > 0 {
		cmd = args[0]
	}
	if len(args) > 1 {
		return errUsage
	}
	switch cmd {
	case "serve":
		return serve(ctx, get)
	case "migrate":
		return migrate(ctx, get)
	case "healthcheck":
		return healthcheck(ctx, healthURL)
	}
	return errUsage
}

func connect(ctx context.Context, get func(string) string) (*pgstore.Store, collect.Config, error) {
	cfg, err := collect.ConfigFrom(get)
	if err != nil {
		return nil, cfg, err
	}
	st, err := pgstore.Connect(ctx, cfg.PGURL, "")
	if err != nil {
		return nil, cfg, err
	}
	return st, cfg, nil
}

func migrate(ctx context.Context, get func(string) string) error {
	st, _, err := connect(ctx, get)
	if err != nil {
		return err
	}
	defer st.Close()
	return st.Migrate(ctx)
}

func serve(ctx context.Context, get func(string) string) error {
	st, cfg, err := connect(ctx, get)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		return err
	}
	for _, code := range cfg.Venues {
		info := venue.Catalog[code]
		info.Since = time.Now().UTC()
		if err := st.UpsertVenue(ctx, info); err != nil {
			return err
		}
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	jobs, err := collect.Build(cfg, get, st, st, reg)
	if err != nil {
		return err
	}
	sched := &collect.Scheduler{Jobs: jobs, Health: st, Metrics: collect.NewMetrics(reg)}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := st.Ping(r.Context()); err != nil {
			http.Error(w, "postgres is down", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	srv := &http.Server{Addr: cmp.Or(get("PM_COLLECT_ADDR"), addr), Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	srvErr := make(chan error, 1)
	go func() { srvErr <- srv.ListenAndServe() }()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() {
		sched.Run(ctx)
		close(done)
	}()
	var first error
	select {
	case <-ctx.Done():
	case first = <-srvErr:
		cancel()
	}
	<-done
	stopCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), stopTimeout)
	defer stop()
	_ = srv.Shutdown(stopCtx)
	if first != nil && !errors.Is(first, http.ErrServerClosed) {
		return first
	}
	return nil
}

var errUnhealthy = errors.New("healthz did not answer 200")

func healthcheck(ctx context.Context, url string) error {
	ctx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: %d", errUnhealthy, resp.StatusCode)
	}
	return nil
}
