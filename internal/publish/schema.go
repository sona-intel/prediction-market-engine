package publish

import "pm/internal/dataset"

const (
	CutoffBlock  = 85948288
	CutoffTS     = 1777016620
	RulesVersion = "pm-rules-1"

	evidenceTrades = 20
	evidencePeers  = 5
)

var horizons = [...]int{4, 12, 24, 168}

type table struct{ name, columns string }

var schema = []table{
	{"meta", "(key VARCHAR PRIMARY KEY, value VARCHAR)"},
	{"market", `(market_id UINTEGER PRIMARY KEY, condition BLOB UNIQUE, question VARCHAR, slug VARCHAR, end_date DATE,
		kind UTINYINT, neg_risk BOOLEAN, event_key BLOB, resolved BOOLEAN, payout0 UBIGINT, payout1 UBIGINT, payout_den UBIGINT,
		resolved_ts UINTEGER, trades UBIGINT, volume_shares UBIGINT, volume_usdc UBIGINT, p0_4h UINTEGER, p0_12h UINTEGER,
		p0_24h UINTEGER, p0_168h UINTEGER, paired_shares UBIGINT, wash_shares UBIGINT, wash_theta UINTEGER)`},
	{"wallet", `(wallet_id UINTEGER UNIQUE, address BLOB PRIMARY KEY, owner BLOB, owner_kind UTINYINT,
		fills UINTEGER, volume_usdc UBIGINT, buy_volume_usdc UBIGINT, taker_rows UINTEGER, taker_volume_usdc UBIGINT,
		first_ts UINTEGER, last_ts UINTEGER, markets UINTEGER, amm_trades UINTEGER, gross_pnl BIGINT, fees UBIGINT,
		biggest_win BIGINT, resolved_markets UINTEGER, won_markets UINTEGER, open_unresolved UINTEGER,
		rated_markets UINTEGER, rated_events UINTEGER, brier DOUBLE, edge DOUBLE, edge_se DOUBLE, rated BOOLEAN,
		wash_score DOUBLE, wash_x0 DOUBLE, paired_shares UBIGINT, wash_shares UBIGINT)`},
	{"wallet_market", `(wallet_id UINTEGER, market_id UINTEGER, volume_usdc UBIGINT, gross_pnl BIGINT, fees UBIGINT,
		f_ppm UINTEGER, o_ppm UINTEGER)`},
	{"wallet_calibration", "(wallet_id UINTEGER, bucket UTINYINT, n UINTEGER, sum_f UBIGINT, sum_o UBIGINT)"},
	{"calibration", "(horizon_h USMALLINT, kind UTINYINT, bucket UTINYINT, markets UBIGINT, sum_p UBIGINT, sum_o UBIGINT, sum_sq DOUBLE)"},
	{"wash_counterparty", "(wallet_id UINTEGER, rank UTINYINT, counterparty_id UINTEGER, markets UINTEGER, trades UINTEGER, shares UBIGINT)"},
	{"wash_trade", `(wallet_id UINTEGER, rank UTINYINT, block UINTEGER, log_index UINTEGER, market_id UINTEGER,
		counterparty_id UINTEGER, is_buy BOOLEAN, outcome UTINYINT, shares UBIGINT, usdc UBIGINT, ts UINTEGER)`},
}

var coverage = [][2]string{
	{"note", "Polymarket history through 2026-04-24 07:43:40 UTC (Polygon block 85,948,288). Order-book trades start on 2022-11-21. Later trades are not included. Trades on the old automated market maker are not part of profit and loss."},
	{"attribution", "Polymarket history: Envio, CC-BY-4.0"},
	{"license_url", "https://creativecommons.org/licenses/by/4.0/"},
	{"source_url", "https://huggingface.co/datasets/moose-code/polymarket-onchain-v1"},
	{"revision", dataset.Revision},
	{"modified", "Modified by Sona: source rows were aggregated into per-wallet and per-market figures. Not affiliated with or endorsed by Polymarket or Envio."},
}

var requiredMeta = []string{
	"note", "attribution", "license_url", "source_url", "revision", "modified", "cutoff_block", "cutoff_ts", "rules_version",
	"built_at", "total_wallets", "total_rated", "total_fills", "total_markets", "total_resolved_markets",
}
