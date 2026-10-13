package dataset

import (
	"context"
	"sync"
	"time"
)

type Pacer struct {
	mu    sync.Mutex
	start time.Time
	rate  float64
	total int
}

func NewPacer(rate float64) *Pacer {
	return &Pacer{start: time.Now(), rate: rate}
}

func (p *Pacer) Done(ctx context.Context, requests int) error {
	p.mu.Lock()
	p.total += requests
	wait := time.Until(p.start.Add(time.Duration(float64(p.total) / p.rate * float64(time.Second))))
	p.mu.Unlock()
	if wait <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
