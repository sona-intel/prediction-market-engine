package main

import (
	"errors"
	"testing"
)

func TestRunRejectsUnknownCommand(t *testing.T) {
	for _, args := range [][]string{{"bogus"}, {"serve", "extra"}, {"healthcheck", "extra"}} {
		if err := run(args); !errors.Is(err, errUsage) {
			t.Errorf("run(%v) returned %v, want the usage error", args, err)
		}
	}
}
