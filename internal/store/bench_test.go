package store_test

import (
	"os"
	"testing"
	"time"

	"pm/internal/store"
)

func benchStore(b *testing.B) *store.Store {
	b.Helper()
	path := os.Getenv("PM_BENCH_DB")
	if path == "" {
		b.Skip("PM_BENCH_DB names no database")
	}
	s := store.New(path, store.Options{MemoryLimit: "160MB", Threads: 2, TempDir: b.TempDir(), Every: time.Hour})
	b.Cleanup(func() { _ = s.Close() })
	if !s.Ready() {
		b.Fatalf("%s did not open", path)
	}
	return s
}

func BenchmarkTrader(b *testing.B) {
	s := benchStore(b)
	db := readOnly(b, os.Getenv("PM_BENCH_DB"))
	rows, err := db.QueryContext(b.Context(), "SELECT address FROM wallet WHERE fills > 5 ORDER BY hash(wallet_id) LIMIT 1000")
	if err != nil {
		b.Fatal(err)
	}
	var addrs [][20]byte
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			b.Fatal(err)
		}
		addrs = append(addrs, [20]byte(raw))
	}
	if err := rows.Close(); err != nil || len(addrs) == 0 {
		b.Fatalf("sampled %d wallets (%v)", len(addrs), err)
	}
	b.ResetTimer()
	for i := range b.N {
		if _, err := s.Trader(b.Context(), addrs[i%len(addrs)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLeaderboard(b *testing.B) {
	s := benchStore(b)
	boards := store.Boards
	b.ResetTimer()
	for i := range b.N {
		if _, err := s.Board(b.Context(), boards[i%len(boards)], 50, 0); err != nil {
			b.Fatal(err)
		}
	}
}
