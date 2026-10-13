package dstest

import (
	"strconv"
	"strings"
	"testing"
)

const (
	CTFExchange = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
	NegExchange = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
)

var Columns = map[string][]string{
	"order_filled":        {"id", "timestamp", "maker", "taker", "makerAssetId", "takerAssetId", "makerAmountFilled", "takerAmountFilled"},
	"redemption":          {"id", "redeemer", "condition", "timestamp"},
	"split":               {"id", "timestamp", "stakeholder", "condition", "amount"},
	"merge":               {"id", "timestamp", "stakeholder", "condition", "amount"},
	"neg_risk_conversion": {"id", "timestamp", "stakeholder", "negRiskMarketId", "amount", "indexSet", "questionCount"},
	"fee_refunded":        {"id", "refundee", "tokenId", "feeCharged"},
}

type World struct {
	*Set
	IDs  []string
	Toks [][2]string
}

func NewWorld(t testing.TB) *World {
	t.Helper()
	w := &World{Set: New(t)}
	var conditions, positions, names [][]any
	for i := range 3 {
		id := "0x" + strings.Repeat(strconv.Itoa(i+1), 64)
		tok := [2]string{strings.Repeat("9", 70) + strconv.Itoa(100+2*i), strings.Repeat("9", 70) + strconv.Itoa(101+2*i)}
		w.IDs = append(w.IDs, id)
		w.Toks = append(w.Toks, tok)
		numerators, den := []string{}, "0"
		if i == 0 {
			numerators, den = []string{"1", "0"}, "1"
		}
		conditions = append(conditions, []any{id, []string{tok[0], tok[1]}, numerators, den})
		positions = append(positions, []any{tok[0], id, "0"}, []any{tok[1], id, "1"})
		names = append(names, []any{tok[0], "2024-09-10T00:00:00Z", id, "Will " + string(rune('A'+i)) + " happen?", "q" + strconv.Itoa(i)})
	}
	w.Write("condition.parquet", []string{"id", "positionIds", "payoutNumerators", "payoutDenominator"}, conditions...)
	w.Write("position.parquet", []string{"id", "condition", "outcomeIndex"}, positions...)
	w.Write("market_data.parquet", []string{"id", "endDate", "condition", "marketName", "marketSlug"}, names...)
	w.Write("neg_risk_event.parquet", []string{"id", "feeBps", "questionCount"})
	w.Write("wallet.parquet", []string{"id", "signer", "type"})
	w.Write("fpmm_transaction/year=2022/month=06.parquet", []string{"id", "type", "user"})
	for table, cols := range Columns {
		w.Write(table+"/year=2024/month=01.parquet", cols)
	}
	return w
}

func (w *World) Rows(table, month string, rows ...[]any) {
	w.t.Helper()
	w.Write(table+"/year=2025/month="+month+".parquet", Columns[table], rows...)
}

func Buy(id string, ts int, maker, taker, tok string, shares int) []any {
	return []any{id, strconv.Itoa(ts), maker, taker, "0", tok, strconv.Itoa(shares * 500_000), strconv.Itoa(shares * 1_000_000)}
}

func Sell(id string, ts int, maker, taker, tok string, shares int) []any {
	return []any{id, strconv.Itoa(ts), maker, taker, tok, "0", strconv.Itoa(shares * 1_000_000), strconv.Itoa(shares * 500_000)}
}
