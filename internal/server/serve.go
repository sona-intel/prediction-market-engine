package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	Addr      = ":8080"
	DebugAddr = ":9464"
)

func NewServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}
}

func Debug(addr string, metrics http.Handler) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return NewServer(addr, mux)
}

func Run(ctx context.Context, servers ...*http.Server) error {
	errs := make(chan error, len(servers))
	for _, s := range servers {
		go func() { errs <- s.ListenAndServe() }()
	}
	var first error
	record := func(err error) {
		if first == nil && !errors.Is(err, http.ErrServerClosed) {
			first = err
		}
	}
	pending := len(servers)
	select {
	case <-ctx.Done():
	case err := <-errs:
		record(err)
		pending--
	}
	stop, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for _, s := range servers {
		wg.Go(func() { _ = s.Shutdown(stop) })
	}
	wg.Wait()
	for ; pending > 0; pending-- {
		record(<-errs)
	}
	return first
}

var errUnhealthy = errors.New("healthz did not answer 200")

func Healthcheck(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1"+Addr+"/healthz", nil)
	if err != nil {
		return err
	}
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: %d", errUnhealthy, resp.StatusCode)
	}
	return nil
}

/* Metrics */

type Metrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	labels := []string{"route", "code"}
	return &Metrics{
		requests: f.NewCounterVec(prometheus.CounterOpts{Name: "pm_api_requests_total", Help: "Served requests"}, labels),
		duration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "pm_api_request_duration_seconds",
			Help:    "Duration of served requests",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		}, labels),
	}
}

func (m *Metrics) observe(route string, status int, elapsed time.Duration) {
	code := strconv.Itoa(status)
	m.requests.WithLabelValues(route, code).Inc()
	m.duration.WithLabelValues(route, code).Observe(elapsed.Seconds())
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *statusWriter) code() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}
