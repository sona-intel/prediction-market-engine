# Venue fixtures

Real answers from the live venue APIs, recorded on 2026-10-14 with `record.go` and trimmed only by dropping whole rows. Nothing is edited or made up. Re-record with:

```sh
go run testdata/venues/record.go          # every venue
go run testdata/venues/record.go hip4     # one venue (hip4, polyus, kalshi or levr)
```

The Polymarket US part waits 3 seconds between requests because the gateway answers 429 above about half a request a second.

## Hyperliquid HIP-4

| File | How it was recorded |
|---|---|
| `hl_outcomemeta.json` | `POST api.hyperliquid.xyz/info {"type":"outcomeMeta"}`. The first three outcomes with a tape are kept, and `questions` and `deployers` are emptied. |
| `hl_recenttrades.json` | `{"type":"recentTrades","coin":"#111210"}`, the side-0 coin of the first outcome with at least 6 trades. The newest 6 rows are kept. |
| `hl_recenttrades_side1.json` | The same for the mirror coin `#111211`. Every row has a twin in the side-0 file with the same hash, time and size, the other side, a price that sums to 1 and the two wallets swapped. |
| `hl_userfills.json` | `{"type":"userFills","user":<wallet>}` for the first trader of the busiest outcome who held a Yes coin and a No coin at settlement. Four whole fills are kept, one Settlement on a Yes coin, one on a No coin, a Buy and a Sell. Running the whole hip4 target again rewrites every HIP-4 file with the tape of that day. |
| `hl_ws_trades.json` | The first `trades` message after subscribing to `#111210` on `wss://api.hyperliquid.xyz/ws`, which is the snapshot of recent trades. The newest 6 rows are kept. |

## Polymarket US

| File | How it was recorded |
|---|---|
| `polyus_markets.json` | `GET gateway.polymarket.us/v1/markets?active=true&closed=false&orderBy=volume&orderDirection=desc&limit=10`. The first two markets whose `bbo` answered are kept. |
| `polyus_bbo.json` | `GET /markets/{slug}/bbo` for the first of those. |
| `polyus_markets_closed.json` | `GET /markets?closed=true&orderBy=volume&orderDirection=desc&limit=1`, a resolved market. |
| `polyus_settlement.json` | `GET /markets/{slug}/settlement` for that market. |
| `polyus_bbo_expired.json` | `GET /markets/{slug}/bbo` for that market, which has no price. |

## Kalshi

| File | How it was recorded |
|---|---|
| `kalshi_markets.json` | `GET api.elections.kalshi.com/trade-api/v2/markets?status=open&mve_filter=exclude&limit=1000`. Three whole markets from the first page are kept, the one with the most 24 hour volume, one that has traded and one with no trade and a book only. |
| `kalshi_trades.json` | `GET /markets/trades?limit=1000&min_ts=<a minute ago>` over up to 4 pages. Five whole prints are kept, the biggest combo print (`KXMVE`, which the collector drops), the three biggest plain prints, one with a no-side taker, and the smallest print. |
| `kalshi_settled.json` | `GET /markets?status=settled&mve_filter=exclude&limit=200&min_settled_ts=<an hour ago>`. One market that settled yes and one that settled no. |

## Levr Bet

Each file is the verbatim answer of one query to the Subsquid GraphQL (`levrbet.squids.live/v1-levr-prod-portal@v1/api/graphql`), asking by `positionId` or `gameMarketId` so that only whole rows are kept. The fields are the constants the poller itself uses.

| File | What it holds |
|---|---|
| `levr_opened.json` | `PositionOpened` rows of three positions. One later won, one was in a cancelled game and one was liquidated. |
| `levr_redeemed.json` | `PositionRedeemed` rows of the first two. The win has `profit` 10,909,113, and the cancelled game has `marketWinner` 0 and no profit. |
| `levr_liquidated.json` | The `PositionLiquidated` row of the third position. |
| `levr_settled.json` | `MarketSettled` rows of the games of the first two. They carry the winner and no payout. |
