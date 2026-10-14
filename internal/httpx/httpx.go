// Package httpx holds the JSON GET helper shared by the live pollers and the venue sources, with a rate limiter and a 429 backoff
package httpx

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

const (
	bodyLimit  = 32 << 20
	errLimit   = 1 << 16
	backoffMin = time.Second
	backoffMax = 30 * time.Second
)

var ErrStatus = errors.New("unexpected status")

// StatusError carries the code of an answer that was not 200 and matches ErrStatus
type StatusError struct{ Code int }

func (e StatusError) Error() string { return fmt.Sprintf("%s: %d", ErrStatus, e.Code) }

func (e StatusError) Is(target error) bool { return target == ErrStatus }

// Client sends requests through the limiter and retries 429 answers up to Retries times, so a zero Retries never retries
type Client struct {
	HTTP      *http.Client
	UserAgent string
	Limiter   *Limiter
	Retries   int
	// Sleep waits between retries and is replaced in tests
	Sleep func(ctx context.Context, d time.Duration) error
}

func (c *Client) Get(ctx context.Context, base string, q url.Values, out any) error {
	target := base
	if len(q) > 0 {
		target += "?" + q.Encode()
	}
	return c.do(ctx, http.MethodGet, target, nil, out)
}

// Post sends body as JSON and decodes the answer, with the same limiter and retries as Get
func (c *Client) Post(ctx context.Context, target string, body, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	return c.do(ctx, http.MethodPost, target, b, out)
}

func (c *Client) do(ctx context.Context, method, target string, body []byte, out any) error {
	for attempt := 0; ; attempt++ {
		if err := c.Limiter.Wait(ctx); err != nil {
			return err
		}
		wait, err := c.once(ctx, method, target, body, attempt, out)
		if wait == 0 || attempt >= c.Retries {
			return err
		}
		sleep := c.Sleep
		if sleep == nil {
			sleep = Sleep
		}
		if err := sleep(ctx, wait); err != nil {
			return err
		}
	}
}

// once returns a nonzero wait when the answer was a 429
func (c *Client) once(ctx context.Context, method, target string, body []byte, attempt int, out any) (time.Duration, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, rd)
	if err != nil {
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("User-Agent", c.UserAgent)
	req.Header.Set("Accept", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, errLimit))
		return RetryAfter(resp.Header.Get("Retry-After"), attempt), StatusError{Code: resp.StatusCode}
	}
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, errLimit))
		return 0, StatusError{Code: resp.StatusCode}
	}
	return 0, json.UnmarshalRead(io.LimitReader(resp.Body, bodyLimit), out)
}

// RetryAfter reads whole seconds from the header and falls back to the backoff of the attempt, as some venues send none
func RetryAfter(h string, attempt int) time.Duration {
	if s, err := strconv.Atoi(h); err == nil && s > 0 {
		return min(time.Duration(s)*time.Second, backoffMax)
	}
	return Backoff(attempt)
}

// Backoff doubles from one second up to thirty with up to half of it added as jitter
func Backoff(attempt int) time.Duration {
	d := min(backoffMin<<min(max(attempt, 0), 6), backoffMax)
	return d + rand.N(d/2+1)
}

func Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Limiter spaces calls at least one interval apart, and a nil Limiter never waits
type Limiter struct {
	mu    sync.Mutex
	every time.Duration
	next  time.Time
}

// NewLimiter allows rps calls a second and returns nil for a rate of zero or less
func NewLimiter(rps float64) *Limiter {
	if rps <= 0 {
		return nil
	}
	return &Limiter{every: time.Duration(float64(time.Second) / rps)}
}

func (l *Limiter) Wait(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	now := time.Now()
	at := now
	if l.next.After(now) {
		at = l.next
	}
	l.next = at.Add(l.every)
	l.mu.Unlock()
	if d := at.Sub(now); d > 0 {
		return Sleep(ctx, d)
	}
	return nil
}

func Finite(f float64) float64 {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0
	}
	return f
}
