# Sona Intel — prediction-market engine

How Sona analyses prediction markets. It reads market and trade data (Polymarket's full history from Envio's public dataset, through block 85,948,288 on 2026-04-24) and works out:

- trader profiles and lifetime profit and loss;
- skill ratings and calibration;
- wash-trading and unusual-activity checks, with the evidence shown.

Settings come from environment variables (see `.env.example` once added). Never commit real keys.

## Run

All commands run from `services/`. Copy `.env.example` to `.env` in this folder first and set `HF_TOKEN` to raise the download rate (optional).

```sh
# build the images
docker compose --profile build build pm-api pm-build

# full build, hours long and resumable (rerun the same command after a failure)
docker compose --profile build run -d --name pm-build-full pm-build build
docker logs --tail 20 pm-build-full

# check the published file (works while pm-api holds it)
docker compose --profile build run --rm pm-build verify

# serve it, pm-api reopens the file within 10 s of a new publish
docker compose up -d pm-api
curl -s http://127.0.0.1:8191/internal/v1/status
```

The build needs 35 GB free in `data/` (git-ignored, on the host disk). After a verified publish it deletes `stage/`, `tmp/` and `work.duckdb`, so `data/` holds only `pm.duckdb`. `build --keep-work` keeps them. The API contract is `pm-api/openapi.yaml`, served at `http://127.0.0.1:8090/specs/pm-api/openapi.yaml`.

The wash solver runs in SQL over an edge table on disk (about 10 bytes a pair, 666 million directed pairs on the full history), and `PM_WASH_MAX_PAIRS` stops it at 1,000,000,000 pairs by default. `pm-build` runs under a 3g container cap (GOMEMLIMIT 1000MiB, DuckDB 700MB) and its anon memory reached 2.4 to 2.7 GiB on the full history, so a busier VM can kill it again, and a rerun resumes from the last finished bucket. A work file records its dataset root, revision and rules version and refuses a resume that does not match.

## Collector

`pm-collect` records the venues beyond Polymarket (Hyperliquid HIP-4, Kalshi, Polymarket US, Levr Bet) into `pm-postgres`, schema `pm`, and is the only writer. It applies the embedded migrations at start, runs one ticker per source, and keeps the cursor of each source in the `cursor` table. Metrics and `/healthz` are on `127.0.0.1:8192`. Postgres listens on `127.0.0.1:5437`.

```sh
# the passwords PM_PG_PASSWORD and PM_PG_ENGINE_PASSWORD live in services/.env
docker compose up -d --no-deps pm-postgres pm-collect

# run the Postgres tests against it with the owner role, they make and drop their own schema
PM_TEST_PG=postgres://pm:...@127.0.0.1:5437/pm go test -race ./...
```

Tests that need Postgres skip when `PM_TEST_PG` is unset. Settings are `PM_PG_URL`, `PM_COLLECT_VENUES` and `PM_SNAP_KEEP_DAYS`, and every backfill switch stays off by default.

### Hyperliquid HIP-4

One WebSocket connection follows every outcome and subscribes to side 0 only, because the two sides of an outcome carry the same fills mirrored (same hash, time and size, prices that sum to 1, wallets swapped). On the live feed 5,135 of 5,135 side-0 fills had exactly one twin on side 1. The outcome list is re-read every `PM_HL_META_EVERY` (5m), and outcomes that leave it are unsubscribed and marked closed. After a restart or reconnect, `recentTrades` is called once per coin that had a stored tape, paced to `PM_HL_GAP_WEIGHT` weight a minute (300), and a gap is counted when that page starts after the newest stored trade. The count is `pm_hip4_gaps_total` and the `hyperliquid/gaps` cursor. Rows with a price outside 0 to 1, a bad wallet or an odd size are skipped and counted in `pm_hip4_trades_skipped_total`. `PM_HL_URL` overrides the host.

### Polymarket US

The gateway has no trade tape, so three jobs share one request limiter (`PM_PMUS_RPS`, 0.4), since it answers 429 with Retry-After above about half a request a second across all paths.

- `catalogue` sweeps every open market each `PM_PMUS_SWEEP_EVERY` (1h), about 152 pages.
- `quotes` snapshots the top `PM_PMUS_TOP` (100) markets by volume each `PM_PMUS_EVERY` (10m). Volume and open interest are share counts, so they are stored as notional dollars.
- `settlement` checks markets past their end date each `PM_PMUS_SETTLE_EVERY` (10m) and records the `settlement` flag, with `yes` meaning the long side won. The quote's `settlementPx` also shows on open markets, so it is not used.

Fixtures for both venues are in `testdata/venues` with the recorder that wrote them.

### Kalshi

The keyless API publishes no trader ids, so Kalshi gets market data and large prints only, and its tape is pruned after `PM_SNAP_KEEP_DAYS`. Three jobs share one limiter (`PM_KALSHI_RPS`, 2) and the 429 backoff, since Kalshi sends no Retry-After.

- `markets` sweeps every open market with `mve_filter=exclude` each `PM_KALSHI_MARKETS_EVERY` (10m), about 60 pages of 1,000, and snapshots the `PM_KALSHI_TOP` (500) with the most 24 hour volume. Price is the last trade, or the middle of the book when nothing has traded. Volume and open interest are contract counts, which equal notional dollars. Markets missing from a full sweep are marked closed.
- `trades` reads `/markets/trades` each `PM_KALSHI_EVERY` (10s), about 165 prints a second, rewinds the cursor 5 seconds so late prints are caught, and keeps prints that cost the taker at least `PM_KALSHI_MIN_USD` (10,000, the same constant as the Polymarket live feed). The side column is the taker side (yes or no), and combo tickers (`KXMVE`) are dropped as the market sweep excludes them. A poll that hits its 30 page cap counts a gap in `pm_kalshi_gaps_total`.
- `settled` copies results of markets settled since the last poll (`PM_KALSHI_SETTLE_EVERY`, 5m) onto markets already recorded.

The `/historical` stitch is not built, so history starts at deploy.
