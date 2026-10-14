package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"pm/internal/pgstore"
	"pm/internal/pgstore/pgtest"
)

func TestUsage(t *testing.T) {
	if err := run(t.Context(), []string{"nope"}, func(string) string { return "" }); !errors.Is(err, errUsage) {
		t.Fatalf("got %v", err)
	}
	if err := run(t.Context(), []string{"serve", "x"}, func(string) string { return "" }); !errors.Is(err, errUsage) {
		t.Fatalf("got %v", err)
	}
	if err := run(t.Context(), nil, func(string) string { return "" }); err == nil {
		t.Fatal("serve without PM_PG_URL started")
	}
}

func TestHealthcheck(t *testing.T) {
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
	defer srv.Close()
	if err := healthcheck(t.Context(), srv.URL); err != nil {
		t.Fatal(err)
	}
	status = http.StatusServiceUnavailable
	if err := healthcheck(t.Context(), srv.URL); !errors.Is(err, errUnhealthy) {
		t.Fatalf("got %v", err)
	}
}

func TestServeRunsAndExitsCleanly(t *testing.T) {
	pgtest.New(t)
	dsn := os.Getenv(pgtest.EnvDSN)
	schema := "t_serve"
	root, err := pgstore.Connect(t.Context(), dsn, "")
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if _, err := root.Pool.Exec(t.Context(), "drop schema if exists "+schema+" cascade; create schema "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = root.Pool.Exec(context.Background(), "drop schema if exists "+schema+" cascade") })
	env := map[string]string{
		"PM_PG_URL":         dsn + "?search_path=" + schema,
		"PM_COLLECT_ADDR":   "127.0.0.1:0",
		"PM_COLLECT_VENUES": "kalshi,levr",
	}
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	if err := run(ctx, []string{"serve"}, func(k string) string { return env[k] }); err != nil {
		t.Fatalf("serve: %v", err)
	}
	st, err := pgstore.Connect(t.Context(), env["PM_PG_URL"], "")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	vs, err := st.Venues(t.Context())
	if err != nil || len(vs) != 2 {
		t.Fatalf("venues %+v, %v", vs, err)
	}
}
