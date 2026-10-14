package httpx_test

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"pm/internal/httpx"
)

func client(retries int, slept *[]time.Duration) *httpx.Client {
	return &httpx.Client{
		HTTP: http.DefaultClient, UserAgent: "test", Retries: retries,
		Sleep: func(_ context.Context, d time.Duration) error { *slept = append(*slept, d); return nil },
	}
}

func TestGetDecodesAndSendsHeaders(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") != "test" || r.URL.Query().Get("a") != "1" {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"n":7}`))
	}))
	defer srv.Close()
	var out struct {
		N int `json:"n"`
	}
	var slept []time.Duration
	if err := client(0, &slept).Get(t.Context(), srv.URL, url.Values{"a": {"1"}}, &out); err != nil || out.N != 7 {
		t.Fatalf("got %v, %v", out, err)
	}
}

func TestGetRetriesOn429(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) < 3 {
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"n":1}`))
	}))
	defer srv.Close()
	var out struct {
		N int `json:"n"`
	}
	var slept []time.Duration
	if err := client(3, &slept).Get(t.Context(), srv.URL, nil, &out); err != nil || out.N != 1 {
		t.Fatalf("got %v, %v", out, err)
	}
	if len(slept) != 2 || slept[0] != 2*time.Second {
		t.Fatalf("slept %v", slept)
	}
}

func TestGetGivesUpAndDoesNotRetryOtherStatuses(t *testing.T) {
	var calls atomic.Int32
	status := http.StatusTooManyRequests
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(status)
	}))
	defer srv.Close()
	var out any
	var slept []time.Duration
	if err := client(2, &slept).Get(t.Context(), srv.URL, nil, &out); !errors.Is(err, httpx.ErrStatus) || calls.Load() != 3 {
		t.Fatalf("429: %v after %d calls", err, calls.Load())
	}
	calls.Store(0)
	status = http.StatusInternalServerError
	if err := client(2, &slept).Get(t.Context(), srv.URL, nil, &out); !errors.Is(err, httpx.ErrStatus) || calls.Load() != 1 {
		t.Fatalf("500: %v after %d calls", err, calls.Load())
	}
}

func TestBackoffGrowsAndCaps(t *testing.T) {
	for attempt, base := range []time.Duration{1, 2, 4, 8, 16, 30, 30} {
		d := httpx.Backoff(attempt)
		lo := base * time.Second
		if d < lo || d > lo+lo/2 {
			t.Fatalf("attempt %d: %v outside [%v, %v]", attempt, d, lo, lo+lo/2)
		}
	}
}

func TestLimiterSpacesCalls(t *testing.T) {
	l := httpx.NewLimiter(50)
	start := time.Now()
	for range 5 {
		if err := l.Wait(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if d := time.Since(start); d < 70*time.Millisecond {
		t.Fatalf("five calls took %v", d)
	}
	if httpx.NewLimiter(0).Wait(t.Context()) != nil {
		t.Fatal("nil limiter waited")
	}
}

func TestLimiterStopsOnCancel(t *testing.T) {
	l := httpx.NewLimiter(0.5)
	_ = l.Wait(t.Context())
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := l.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
}

func TestFinite(t *testing.T) {
	if httpx.Finite(1.5) != 1.5 || httpx.Finite(math.NaN()) != 0 {
		t.Fatal("finite")
	}
}

func TestRetriesBackOffFurtherWithoutRetryAfter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	var out any
	var slept []time.Duration
	_ = client(3, &slept).Get(t.Context(), srv.URL, nil, &out)
	if len(slept) != 3 || slept[0] > 1500*time.Millisecond || slept[2] < 4*time.Second {
		t.Fatalf("slept %v", slept)
	}
}
