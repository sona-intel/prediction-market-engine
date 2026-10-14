package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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
		"PM_PG_URL":         withSchema(dsn, schema),
		"PM_COLLECT_ADDR":   "127.0.0.1:0",
		"PM_COLLECT_VENUES": "kalshi,levr",
		// nothing listens here, so the test never reaches a live venue
		"PM_KALSHI_URL": "http://127.0.0.1:1",
		"PM_LEVR_URL":   "http://127.0.0.1:1",
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

func TestOncePollsOneNamedJob(t *testing.T) {
	pgtest.New(t)
	dsn := os.Getenv(pgtest.EnvDSN)
	schema := "t_once"
	root, err := pgstore.Connect(t.Context(), dsn, "")
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if _, err := root.Pool.Exec(t.Context(), "drop schema if exists "+schema+" cascade; create schema "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = root.Pool.Exec(context.Background(), "drop schema if exists "+schema+" cascade") })
	env := map[string]string{"PM_PG_URL": withSchema(dsn, schema), "PM_COLLECT_VENUES": "hyperliquid,levr"}
	get := func(k string) string { return env[k] }
	if err := run(t.Context(), []string{"once", "hyperliquid/wallets"}, get); err != nil {
		t.Fatalf("wallet job: %v", err)
	}
	if err := run(t.Context(), []string{"once", "levr/wallets"}, get); err != nil {
		t.Fatalf("levr wallet job: %v", err)
	}
	if err := run(t.Context(), []string{"once", "nope/job"}, get); !errors.Is(err, errNoSuchJob) {
		t.Fatalf("got %v", err)
	}
	if err := run(t.Context(), []string{"once"}, get); !errors.Is(err, errUsage) {
		t.Fatalf("got %v", err)
	}
}

// a DSN from the environment may already carry query parameters such as sslmode
func withSchema(dsn, schema string) string {
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "search_path=" + schema
}
