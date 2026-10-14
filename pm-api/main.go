package main

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"pm/internal/live"
	"pm/internal/pgstore"
	"pm/internal/server"
	"pm/internal/store"
)

var errUsage = errors.New("usage: pm-api [serve | healthcheck]")

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("pm-api stopped", "error", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cmd := "serve"
	if len(args) > 0 {
		cmd = args[0]
	}
	switch {
	case cmd == "serve" && len(args) <= 1:
		return serve()
	case cmd == "healthcheck" && len(args) <= 1:
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return server.Healthcheck(ctx)
	}
	return errUsage
}

func logger() *slog.Logger {
	level := slog.LevelInfo
	_ = level.UnmarshalText([]byte(os.Getenv("LOG_LEVEL")))
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}

func serve() error {
	slog.SetDefault(logger())
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	s := store.New(cmp.Or(os.Getenv("PM_DB"), "/data/pm.duckdb"), store.Options{
		MemoryLimit: "160MB", Threads: 2, TempDir: "/tmp", Every: 10 * time.Second,
	})
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := server.NewMetrics(reg)
	go s.Run(ctx)
	feed := live.New(live.Options{})
	go feed.Run(ctx)
	// the pool connects on first use, so the service starts and answers 503 on the venue routes while Postgres is down
	var venues server.Venues
	if dsn := os.Getenv("PM_PG_URL"); dsn != "" {
		pg, err := pgstore.Connect(ctx, dsn, "")
		if err != nil {
			return errors.Join(err, s.Close())
		}
		defer pg.Close()
		venues = pg
	}
	err := server.Run(ctx,
		server.NewServer(server.Addr, server.NewFull(s, feed, venues, m)),
		server.Debug(server.DebugAddr, promhttp.HandlerFor(reg, promhttp.HandlerOpts{})))
	return errors.Join(err, s.Close())
}
