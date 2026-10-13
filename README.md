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
