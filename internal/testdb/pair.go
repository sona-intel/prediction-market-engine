package testdb

import (
	"testing"

	"pm/internal/dstest"
)

const (
	Alice = "0xA11cE00000000000000000000000000000000001"
	Bob   = "0xB0b0000000000000000000000000000000000002"
	Carol = "0xCa70100000000000000000000000000000000003"
)

func Pair(t testing.TB, dir string) (dbPath string, err error) {
	t.Helper()
	w := dstest.NewWorld(t)
	tok, other := w.Toks[0][0], w.Toks[1][0]
	w.Rows("order_filled", "01",
		dstest.Buy("137_100_1", 1700000100, Alice, Bob, tok, 100),
		dstest.Sell("137_100_2", 1700000100, Bob, dstest.CTFExchange, tok, 100),
		dstest.Sell("137_101_1", 1700000200, Alice, Bob, tok, 100),
		dstest.Buy("137_101_2", 1700000200, Bob, dstest.CTFExchange, tok, 100),
		dstest.Buy("137_102_1", 1700000300, Alice, Carol, other, 40),
		dstest.Sell("137_102_2", 1700000300, Carol, dstest.CTFExchange, other, 40),
	)
	return BuildFrom(t.Context(), w.Root, dir)
}
