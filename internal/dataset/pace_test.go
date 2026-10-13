package dataset_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"pm/internal/dataset"
)

func TestPacerAbsorbsSlowWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		p := dataset.NewPacer(8)
		if err := p.Done(t.Context(), 16); err != nil {
			t.Fatal(err)
		}
		if got := time.Since(start); got != 2*time.Second {
			t.Fatalf("after 16 requests clock at %s, want 2s", got)
		}
		time.Sleep(time.Second)
		if err := p.Done(t.Context(), 8); err != nil {
			t.Fatal(err)
		}
		if got := time.Since(start); got != 3*time.Second {
			t.Fatalf("after 24 requests clock at %s, want 3s", got)
		}
	})
}

func TestPacerStopsOnCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		time.AfterFunc(time.Second, cancel)
		start := time.Now()
		if err := dataset.NewPacer(1).Done(ctx, 100); err == nil {
			t.Fatal("want the context error")
		}
		if got := time.Since(start); got != time.Second {
			t.Fatalf("returned at %s, want 1s", got)
		}
	})
}
