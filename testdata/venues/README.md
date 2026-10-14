# Venue fixtures

Real answers from the live venue APIs, recorded on 2026-10-14 with `record.go` and trimmed only by dropping whole rows. Nothing is edited or made up. Re-record with:

```sh
go run testdata/venues/record.go          # every venue
go run testdata/venues/record.go hip4     # one venue (hip4 or polyus)
```

The Polymarket US part waits 3 seconds between requests because the gateway answers 429 above about half a request a second.

## Hyperliquid HIP-4

| File | How it was recorded |
|---|---|
| `hl_outcomemeta.json` | `POST api.hyperliquid.xyz/info {"type":"outcomeMeta"}`. The first three outcomes with a tape are kept, and `questions` and `deployers` are emptied. |
| `hl_recenttrades.json` | `{"type":"recentTrades","coin":"#111210"}`, the side-0 coin of the first outcome with at least 6 trades. The newest 6 rows are kept. |
| `hl_recenttrades_side1.json` | The same for the mirror coin `#111211`. Every row has a twin in the side-0 file with the same hash, time and size, the other side, a price that sums to 1 and the two wallets swapped. |
| `hl_ws_trades.json` | The first `trades` message after subscribing to `#111210` on `wss://api.hyperliquid.xyz/ws`, which is the snapshot of recent trades. The newest 6 rows are kept. |

## Polymarket US

| File | How it was recorded |
|---|---|
| `polyus_markets.json` | `GET gateway.polymarket.us/v1/markets?active=true&closed=false&orderBy=volume&orderDirection=desc&limit=10`. The first two markets whose `bbo` answered are kept. |
| `polyus_bbo.json` | `GET /markets/{slug}/bbo` for the first of those. |
| `polyus_markets_closed.json` | `GET /markets?closed=true&orderBy=volume&orderDirection=desc&limit=1`, a resolved market. |
| `polyus_settlement.json` | `GET /markets/{slug}/settlement` for that market. |
| `polyus_bbo_expired.json` | `GET /markets/{slug}/bbo` for that market, which has no price. |
