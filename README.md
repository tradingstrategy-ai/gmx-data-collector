# GMX Historical Data Collection

Collect historical price data for every listed GMX V2 perp market with smart incremental updates.

## Features

- **Smart Incremental Updates** — Only fetches data you don't have (10-30x faster for daily updates)
- **Dual Data Sources** — GMX API (recent ~6 months) + Chainlink (historical)
- **GMX V2 Markets** — Chainlink-feed markets plus non-Chainlink markets; the set is whatever GMX lists (see `delisted_markets.json` and the per-release `data_report.txt` for the current counts)
- **6 Timeframes** — 1min, 5min, 15min, 1h, 4h, 1d
- **Freqtrade Compatible** — Direct export to Feather format (OHLCV + funding rate + mark price)
- **Concurrent Collection** — Parallel symbol processing and RPC batch requests
- **Historical OI & Pool Liquidity** — Full on-chain history of open interest and LP pool depth via HyperSync
- **Analysis Notebooks** — Interactive Plotly notebooks for OI trends, pool utilisation, cross-exchange validation, and CEX/GMX correlation

## Changelog

### 2026-06-16 — incremental updates actually update (append by default)

`collect --update` previously skipped any symbol that had a checkpoint *wholesale*
(in three separate code paths), so once a token was collected it was never
refreshed again — "smart incremental" silently did nothing on the second run, and
the only way to update was `--force`, which re-walked each Chainlink feed from
genesis (hours). Fixed:

- **Append by default.** In `--update` mode every symbol now flows through the gap
  detector, which checks existing coverage and fetches **only the missing slice**
  (recent gap via GMX API; older history via Chainlink/oracle only when genuinely
  missing), then merges/appends. No more genesis re-walks. Verified: a current
  symbol fetches ~0 rounds; `--symbol BTC` finished in ~37s with full history
  preserved.
- **`--force` = rewrite.** `--force` re-fetches from genesis **and** overwrites
  stored files (`overwrite=True`), bypassing the coverage check and the
  checkpoint skip. Without it, writes always append.
- The wholesale checkpoint skip now applies only to `--full`-mode resume.

### 2026-04-27 — GitHub Releases migration

Daily data collection now ships as **GitHub Releases** instead of the `data/daily-collection` git branch.

**What changed:**

| Area | Before | After |
|------|--------|-------|
| Daily data storage | `data/daily-collection` git branch (rewrote feather files every day → 8.7 GB repo) | `data-YYYY-MM-DD` GitHub Release tags (14-day rolling window) |
| Consumer download | `git clone --depth 1 --branch data/daily-collection` | `scripts/download_gmx_data.sh` or `gh release download` |
| Quickstart (`-q` flag) | `seed_from_branch()` — shallow git clone | `seed_from_release()` — `gh release download` + tar extract |
| CI workflows | `collect-gmx-data.yml` + `collect-volume.yml` (two separate scheduled jobs) | `release-data.yml` (single job, 02:00 UTC) |
| Release assets | — | `gmx-full.tar.gz`, `gmx-light.tar.gz` (no OHLCV; 1h funding only), `gmx-funding-state-<timestamp>.tar.gz`, `data_report.txt` |

**`data/daily-collection` branch** is frozen as of 2026-04-27 — no further updates. Historical data remains accessible for existing consumers. New consumers should use the Releases path (see [Downloading GMX historical data](#downloading-gmx-historical-data) below).

---

## ⚠️ Data depth: where full history lives, and two residual limits

**Status.** `wstETH` is excluded (ETH-denominated feed only, not tradeable). The
**daily release ships feather only** (`gmx/futures/{PAIR}-{tf}-futures.feather`); parquet
copies (`.parquet`) exist only in locally exported trees (`make export-candles-both`).
Depth is per timeframe and per source, not "genesis" — see
[What the daily release contains](#what-the-daily-release-contains). Two limitations
remain — see below.

> **The Freqtrade feather export is NOT broken.** Its history-preservation merge
> guard (`FreqtradeExporter._write` → `_assert_history_preserved`) is what kept the
> deep history intact while the *source* candle parquets shrank — that is why the
> feathers were deeper than the source parquets, not a bug. **Never** run the
> exporter with `--unsafe-overwrite` against these tokens: it bypasses the guard and
> would overwrite the deep feathers/parquets with shallow source data, destroying
> history irrecoverably.

### The two stores (don't confuse them)

| Store | Path | Schema | Role |
|-------|------|--------|------|
| Source candles | `candles/arbitrum/{TOKEN}/{tf}.parquet` | `timestamp,…,symbol` | collector output |
| FreqTrade exports | `gmx/futures/{PAIR}-{tf}-futures.feather` (`.parquet` too in locally exported trees; the release ships feather only) | `date,o,h,l,c,volume` | what FreqTrade reads |

The **FreqTrade exports are the deepest copy** for non-Chainlink tokens — the source
candles are *not*. Treat `gmx/futures/` as the source of truth for those.

### Limit 1 — non-Chainlink source candle parquets stay shallow

The GMX API only serves a shallow rolling window (`limit=10000`, ~7 days of 1m).
Deep history needs a backfill source:

- **Chainlink-backed tokens** rebuild full history from Chainlink RPC on a
  `--force` collect of a local tree (all timeframes, to genesis). This works — verified. It does not change what the daily release ships; see [Candle depth](#candle-depth).
- **Non-Chainlink tokens cannot be rebuilt by the collector.**
  `collect_non_chainlink_markets()` only layers a thin oracle-event pass on top of
  the GMX window (≈10–11k 1m rows), **not** a genesis backfill. Their deep history
  exists **only in the feathers** (captured by an earlier, more complete run).
  So the source `candles/` parquets for these tokens remain shallow by design; the
  deep data is preserved in `gmx/futures/` (feather; parquet too in locally exported trees).

### Limit 2 — intraday (1m/5m/15m/1h) truncated for many non-Chainlink tokens

For a subset of non-Chainlink pairs (e.g. `ADA, DOT, BCH, TRX, FIL, INJ, XLM, ICP, SUI`)
in the local deep-history trees (as of the 2026-06 recovery pass; the daily release's 1d/4h
depth is the API contract in [Candle depth](#candle-depth)),
**1d / 4h reached 2022 but 1m/5m/15m/1h only start ~2024-11** (their deep daily came
from a coarse CEX backfill that never pulled fine-resolution data). Not yet fixed.
The only fix is the **CEX gap-fill at fine resolution** (`make fill-gaps-cex`,
Binance/Bybit have deep 1m) followed by a re-export.

### Recovery procedure (what actually works)

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
DATA=/Volumes/WD\ Blue\ 1tb/VMs/data/gmx   # external-drive data root

# Chainlink-backed tokens — genesis re-backfill (ignores checkpoints), all timeframes:
poetry run python -m gmx_historical_data.cli collect --full --force \
  --symbol ETH,BNB,DOGE --output-dir "$DATA" --concurrency 4

# Non-Chainlink tokens — the collector CANNOT deepen these.  The deep history lives
# in the feathers, so derive the FreqTrade parquet directly from them instead of a
# fresh export (a fresh export reads the shallow source candles):
freqtrade convert-data --datadir user_data/data/gmx --trading-mode futures \
  --format-from feather --format-to parquet -t 1m 5m 15m 1h 4h 1d

# Then (re)export — the merge guard only ever ADDS, never truncates.  FORMAT=parquet
# emits FreqTrade-format parquet so FT/CCXT work WITHOUT feather files:
make export-candles-both DATA_DIR="$DATA" FEATHER_DIR=./user_data/data OVERWRITE=--overwrite
```

Note: tokens listed on GMX only recently (e.g. `MEGA`, `SPCX`) have no deeper data
at any source — the GMX window already covers their entire life, so a short history
for them is complete, not truncated.

---

## Quick Start

```bash
# Install
poetry install

# Set environment variables
export JSON_RPC_ARBITRUM="https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY"
export HYPERSYNC_API_TOKEN="your_token_here"  # Free from https://envio.dev

# Collect full historical data for the Chainlink-feed tokens (no HyperSync needed)
make collect-candles ARGS="--chainlink-only"

# Initial collection: all tokens (~4-5 hours)
make full-data

# Daily incremental update (~10-30 minutes for all tokens)
make collect-update
```

All data is written to `./user_data` by default. Override with `DATA_DIR=./my_path`.

## Downloading GMX historical data

Daily snapshots are published as **GitHub Releases**. Use the helper script:

```bash
# Latest, full snapshot (apy, snapshots, tickers, volumes, futures feathers incl. funding/mark)
./scripts/download_gmx_data.sh

# Latest, light snapshot (no OHLCV candles, no mark — keeps 1h funding + coverage json; faster download)
./scripts/download_gmx_data.sh --asset light

# Specific historical release
./scripts/download_gmx_data.sh --release data-2026-04-27
```

Requires `gh` CLI authenticated (`gh auth login`). Releases are kept for 14 days, except that the newest release carrying a funding-state asset is retained beyond that until a newer one replaces it.

> **Note (2026-04-27):** The previous `data/daily-collection` branch is deprecated.
> It will not receive further updates. New consumers should use the Releases path above.

### Quickstart from a release

If you want to skip waiting for the first full collection, the `-q` flag on
`collect_daily_snapshot.py` seeds `user_data/` from the latest GitHub Release,
then runs today's snapshot on top:

```bash
# Seed + collect today in one shot
poetry run python scripts/collect_daily_snapshot.py -q

# Only seed, don't collect today
poetry run python scripts/collect_daily_snapshot.py -q --seed-only
```

The seed is merge-only — existing local files are never overwritten, only
missing ones are copied, so `-q` is safe to re-run and easy to undo
(`rm -rf user_data/`).

## What the daily release contains

Each `data-YYYY-MM-DD` release carries `gmx-full.tar.gz`, `gmx-light.tar.gz` and
`data_report.txt` (the report is the authority for current symbol counts, per-timeframe
date ranges and freshness), plus `gmx-funding-state-<YYYYMMDDTHHMMSSZ>.tar.gz` when funding state exists.
Candles ship as **feather only** — there are no `.parquet` candle files in the release.

| Asset | Contents |
|---|---|
| `gmx-full.tar.gz` | Everything under `user_data/data/gmx/` except the raw funding state: `apy/`, `snapshots/`, `tickers/`, `volumes/`, `tick_volume/`, `ticks/`, `checkpoints/`, `futures/` (OHLCV `-futures` feathers for 1m/5m/15m/1h/4h/1d, plus `1h` `-funding_rate` and `1h` `-mark` feathers and `_cadence_manifest.json`), `funding_coverage.json` and `delisted_markets.json`. The backtest bundle. |
| `gmx-light.tar.gz` | `apy/`, `snapshots/`, `tickers/`, `volumes/`, `tick_volume/`, the `futures/*-1h-funding_rate.feather` files and `funding_coverage.json`. **No OHLCV candles and no mark** (mark is derived from the candles). |
| `gmx-funding-state-<YYYYMMDDTHHMMSSZ>.tar.gz` | Raw funding state the next daily run resumes from. Stamped name, never replaced in place; the newest stamp in the newest release that carries one is restored. Shipped only when funding state exists on the runner. Not a consumer artifact. |
| `gmx-ticks.tar.gz`, `gmx-ticks.manifest.json` | Trade-tick intake assets; present only when the tick intake publishes them. |

The release notes list a per-asset/timeframe **Coverage** table inside a collapsed
`<details>` block (one row per candle file).

### Candle depth

Depth is set by what the GMX candles API serves per timeframe, plus whatever earlier
daily runs accumulated; it is not "genesis" history.

* **1m / 5m / 15m:** the API window plus history accumulated since each file was seeded.
* **1h:** the API caps a request at **4320 bars** (~180 days), so 1h depth beyond that exists
  only where earlier runs already stored it.
* **4h / 1d:** the contract is *API depth*. After a `release-data.yml` dispatch with
  `repair_ohlcv=true` and `repair_timeframes=1d,4h` (#72), each file reaches as far back as the
  API serves for that timeframe — for 1d, 2021-10 for the longest-listed majors (BTC, ETH,
  SOL), shorter for newer markets. Releases cut before that dispatch (e.g. `data-2026-10-18`)
  still start 1d at 2026-03-08 for most pairs.
* The newest candle in each candle file is a **closed** bar; in-progress candles are no longer
  stored (#72). The newest funding hour is provisional: the exporter extends the latest funding
  event through the hour end, and the next run revises that hour as needed.

### Market-specific caveats

* **Delisted pairs are frozen history.** Markets named in `delisted_markets.json` keep their
  `futures/` files but stop growing at delisting; a relisted market resumes after one
  legitimate seam. The cadence gate and the funding staleness grading exempt them.
* **Thin markets (#59).** Several live markets trade only a handful of fills per day (e.g.
  OKB, OP, ORDI, 0G, CHZ, LINEA when audited), so ≥90% of their 1h candles legitimately have
  `volume == 0`. The report grades such pairs `expected (thin market)` and flags
  `suspicious (source mismatch)` only when the tick-volume sidecar shows volume the candles
  lack. Open interest is context, not evidence of a collector gap.
* **`APE` vs `APE_deprecated`.** GMX metadata carries both, so `tickers/`, `snapshots/` and
  `apy/` have rows for each. `APE_deprecated` is excluded from candle collection
  (`EXCLUDED_SYMBOLS`); use `APE`.
* **Un-backfillable days.** `snapshots/`, `tickers/`, `apy/` and `volumes/` are point-in-time
  reads taken on the run date, so a day on which no run succeeded cannot be reconstructed
  later. `data_report.txt` lists them under *Missing Days (daily-stamped sources)* (as of
  `data-2026-10-18`: 8 days for snapshots/tickers/apy, 7 for volumes). Candle gaps are tracked
  separately in `futures/_cadence_manifest.json`.
* **`volumes/{date}.parquet`** is a rolling 24h window ending at the ~02:30 UTC run, i.e.
  mostly the previous UTC day (see [Volume Data](#volume-data)).

## Installation

**Prerequisites:** Python 3.11 or 3.12, Rust toolchain (for hypersync)

```bash
# Install Rust (if needed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install dependencies
poetry install
```

## Makefile Reference

All targets write to `./user_data` by default. Configuration variables can be overridden on the command line.

### Candle Collection

```bash
make collect-candles                       # Full candle backfill (all symbols)
make collect-candles SYMBOL=ETH            # Single symbol only
make collect-candles ARGS="--chainlink-only" # Chainlink-feed markets only (no HyperSync)
make collect-candles CONCURRENCY=15        # Tune parallelism (default: 5)
make collect-candles DATA_DIR=./data       # Override output directory

make collect-update                        # Incremental update (only missing data)
make collect-update SYMBOL=BTC             # Incremental for one symbol
```

### Combined Updates

```bash
make refresh-data                          # Candles + funding (recommended for daily runs)
make extract-all                           # OI + pool liquidity (full history)
make extract-all-resume                    # OI + pool liquidity (incremental)
```

### Funding Rates

```bash
make funding-unified                       # Full extraction (HyperSync only, fast)
make funding-unified-resume               # Incremental update (resume from checkpoints)
make funding-unified-merge                # Merge only (skip re-extraction)
make funding-full                         # Include DataStore phase (pre-V2.2, slow)
make funding-feather                      # Export to FreqTrade feather format
```

### Open Interest & Pool Liquidity

```bash
make oi                                   # Full OI backfill from genesis
make oi-resume                            # Incremental OI update
make pool-liquidity                       # Full pool liquidity backfill
make pool-liquidity-resume               # Incremental pool liquidity update
```

### Utilities

```bash
make show-config                          # Print all current configuration values
make install                              # Install dependencies with Poetry
make export-freqtrade                     # Export candles + funding to FreqTrade format
```

### Configuration Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_DIR` | `./user_data` | Root output directory |
| `SYMBOL` | *(all)* | Limit candle collection to one symbol |
| `ARGS` | *(none)* | Extra CLI flags passed to the collect script |
| `CONCURRENCY` | `4` | Parallel workers |
| `NETWORK` | `arbitrum` | Chain name |
| `FROM_BLOCK` | *(auto)* | Start block for extraction scripts |
| `TO_BLOCK` | *(latest)* | End block |
| `MARKET` | *(all)* | Filter to a single market address |
| `INCLUDE_DATASTORE` | *(off)* | Set to `1` to include archive RPC DataStore phase |
| `NICE` | `nice -n 10` | OS process priority prefix — keeps the server responsive |

### Resource Limiting

All `make` targets run under `nice -n 10` by default, giving the OS permission to preempt the collector in favour of other services. The range is -20 (highest priority) to +19 (lowest); 0 is the system default.

```bash
# Disable (run at normal priority)
make collect-candles NICE=""

# Run at even lower priority (background-only)
make collect-candles NICE="nice -n 19"
```

Tip: create a `.env` file — it is auto-loaded by the Makefile:

```bash
HYPERSYNC_API_TOKEN=your_token
JSON_RPC_ARBITRUM=https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY
CONCURRENCY=8
```

## CLI Reference

| Command | Description |
|---------|-------------|
| `collect --full` | Full historical collection |
| `collect --full --chainlink-only` | Collect only Chainlink-feed markets (no HyperSync needed) |
| `collect --update` | Incremental update — only fetches new data |
| `verify` | Verify data quality |
| `export-freqtrade` | Export OHLCV + funding rate + mark price to Freqtrade feather format |
| `debug-oracle` | Debug oracle events |

### Common Options

| Option | Description | Default |
|--------|-------------|---------|
| `--output-dir PATH` | Output directory | `./user_data` |
| `--symbol TEXT` | Token(s) — comma-separated for `collect`, repeatable for `export-freqtrade` | All tokens |
| `--chainlink-only` | Only collect Chainlink markets (no HyperSync needed) | Off |
| `--concurrency INT` | Parallelism level | `4` |

### Concurrency

| `--concurrency` | Symbols parallel | RPC batch workers | Use case |
|-----------------|------------------|-------------------|----------|
| 1 | 1 | 1 | Slow/rate-limited RPC |
| 4 (default) | 4 | 4 | Balanced |
| 8 | 8 | 8 | Fast RPC endpoint |
| 15 | 15 | 8 (capped) | Maximum speed, fast RPC + HyperSync |

### Estimated Times (with `--concurrency 10`)

**Initial collection (`--full`):**
- Single Chainlink token: ~2-3 minutes
- All tokens: ~4-5 hours

**Incremental updates (`--update`):**
- Single token (daily): ~10-30 seconds
- All tokens (daily): ~10-30 minutes

## Automated Updates

```bash
# Cron job: update daily at 2 AM
0 2 * * * cd /path/to/gmx_historical_data && \
  make refresh-data >> /var/log/gmx-update.log 2>&1
```

## Export for Freqtrade

Exports OHLCV candles, funding rates, and mark prices in FreqTrade's feather format (CCXT-compatible).

```bash
# Export all data types
make export-freqtrade

# Or via CLI directly
gmx_historical_data export-freqtrade --data-dir ./user_data --output-dir ./user_data

# Export specific symbols/timeframes
gmx_historical_data export-freqtrade --data-dir ./user_data --symbol ETH --symbol BTC --timeframe 1h
```

Output structure:

```
user_data/gmx/futures/
├── ETH_USDC_USDC-1h-futures.feather
├── ETH_USDC_USDC-1h-funding_rate.feather
├── ETH_USDC_USDC-1h-mark.feather
└── ...
```

### Cadence manifest

Every `export-freqtrade` run publishes `_cadence_manifest.json` beside the exported
feathers, at `user_data/data/gmx/futures/_cadence_manifest.json`. It ships inside
`gmx-full.tar.gz`.

GMX's oracle and this collector both have real outage windows, so a `-futures.feather`
can be missing an interior bar. The series is still strictly monotonic, so a consumer
checking only row counts or ordering cannot see the hole. The manifest records every one
of them, per file:

```json
{
  "generated_at": "2026-09-06T12:00:00+00:00",
  "files": {
    "BTC_USDC_USDC-4h-futures.feather": {
      "timeframe": "4h",
      "expected_interval_seconds": 14400,
      "rows": 6862,
      "first": "2023-07-20T08:00:00+00:00",
      "last": "2026-09-06T00:00:00+00:00",
      "breaks_total": 1,
      "missing_bars_total": 1,
      "truncated": false,
      "breaks": [
        {
          "before": "2025-06-02T16:00:00+00:00",
          "after": "2025-06-03T00:00:00+00:00",
          "missing_bars": 1
        }
      ]
    }
  }
}
```

Contract notes:

- **Every exported `-futures` file gets an entry**, including contiguous ones
  (`breaks_total: 0`). Absence from the manifest means "not checked", not "clean".
- A file whose feather could not be read on a given run is **removed** from the manifest
  for that run rather than keeping its previous entry. The manifest merges across partial
  runs, so a carried-forward entry would sit under a fresh `generated_at` and vouch for a
  verdict nobody recomputed. Dropping it keeps `generated_at` honest: every entry present
  was checked by the run that stamped it.
- `breaks_total` and `missing_bars_total` are always exact. When `truncated` is `true`
  the `breaks` list is capped at 200 entries (1m series can carry thousands).
- Only candle (`-futures`) files appear in this cadence manifest. Funding holes are checked
  separately by series coverage: recent holes block release (B3, #76), while older holes can
  remain when the raw source cannot backfill them.
- Timestamps are ISO-8601 UTC. `before` is the last bar present before the hole, `after`
  the next one present.
- A gap listed here is a known upstream outage, not corruption. GMX retains raw data for
  only ~5 weeks, so most listed gaps cannot be backfilled from source.
- An optional `"regressed_from": "<previous release tag>"` field means *this release
  introduced a break this file did not carry in the named release* — most often an
  overnight collector or oracle outage in the newest 24h of data. The data still shipped
  — withholding a whole day of candles because one symbol regressed is worse than
  publishing it labelled — so a consumer should quarantine just that file. The release
  workflow files a deduplicated tracking issue and ends red whenever it stamps one.
- Delisted markets are exempt from that check. Their history freezes at delisting and
  resumes at relisting, leaving one large legitimate seam; the roster in
  `user_data/data/gmx/delisted_markets.json` names them, and the release gate skips the
  same files the futures-integrity gate already skips.

## Backtesting

GMX requires [gmx-ccxt-freqtrade](https://github.com/tradingstrategy-ai/gmx-ccxt-freqtrade) monkeypatch. Use the included `freqtrade-gmx` wrapper.

> **Important:** Install `freqtrade` in a separate isolated environment — it requires `pandas<3.0` which conflicts with this project's `pandas>=3.0`.

```bash
python -m venv freqtrade-venv && source freqtrade-venv/bin/activate
pip install "freqtrade>=2025.11" "web3-ethereum-defi[web3v7,ccxt]>=0.38" plotly
```

```bash
# Export data to freqtrade format
make export-freqtrade

# Run backtest
./freqtrade-gmx backtesting --config configs/adxmomentum_gmx.json \
    --strategy ADXMomentum --timerange 20210713-
```

See `examples/strategies/ADXMomentum.py` for an example strategy.

### Plot Results

```bash
./freqtrade-gmx plot-profit --config configs/adxmomentum_gmx.example.json --auto-open
./freqtrade-gmx plot-dataframe --config configs/adxmomentum_gmx.example.json \
    --strategy ADXMomentum -p BTC/USDC:USDC --auto-open
```

## Data Structure

```
user_data/
├── candles/arbitrum/{SYMBOL}/
│   ├── 1min.parquet
│   ├── 1h.parquet
│   └── 1d.parquet
├── data/gmx/
│   ├── open_interest/arbitrum/
│   │   ├── raw/{SYMBOL}/data.parquet         # Raw OI events (every position change)
│   │   ├── snapshots/{SYMBOL}/daily.parquet  # End-of-day OI snapshots
│   │   └── checkpoints/
│   ├── pool_liquidity/arbitrum/
│   │   ├── pool_liquidity_raw.parquet        # Raw PoolAmountUpdated events
│   │   ├── pool_liquidity_daily.parquet      # Daily pool depth snapshots
│   │   └── checkpoints/
│   └── volumes/
│       └── {YYYY-MM-DD}.parquet              # Rolling 24h volume per market (see below)
├── funding/arbitrum/
│   ├── raw/
│   │   ├── funding/{SYMBOL}/partition=0/data.parquet
│   │   └── fee_per_size/{SYMBOL}/data.parquet
│   ├── rates/{SYMBOL}/
│   │   ├── 1h.feather                        # Unified hourly rates (feather)
│   │   ├── 1h_factor.parquet                 # HyperSync-only rates (V2.2+)
│   │   └── 1h_datastore.parquet              # DataStore-only rates (pre-V2.2)
│   ├── direction/{SYMBOL}/1h.parquet
│   └── checkpoints/
└── borrowing/arbitrum/
    ├── raw/borrowing/{SYMBOL}/partition=0/data.parquet
    ├── rates/{SYMBOL}/1h.parquet
    └── checkpoints/
```

### Data Note: OHLC vs OHLCV

Candle data is **OHLC only** (no per-candle volume). Both data sources — GMX API candlesticks and Chainlink oracle price events — provide price data without trade volume. The "V" in OHLCV would require processing on-chain trade events (PositionIncrease/PositionDecrease) at per-candle granularity, which is not currently implemented.

The daily volume snapshots collected from Subsquid (stored in `data/gmx/volumes/`) are **aggregate 24h trading volume per market**, useful for market filtering and reporting but not for per-candle volume. The file dated `D` is Subsquid's *rolling* 24h window ending at the ~02:30 UTC run that wrote it, so it covers mostly UTC day `D-1`, not calendar day `D`.

### Volume Data

Daily 24h trading volume per market is collected from the Subsquid GraphQL indexer. To view current volume data:

```bash
# Show 24h volume for all markets (with symbol names)
poetry run python scripts/fetch_volume.py

# Include 7-day aggregate volume history
poetry run python scripts/fetch_volume.py --history 7

# Avalanche chain
poetry run python scripts/fetch_volume.py --chain avalanche
```

## Funding Rate Extraction

Extract exact per-second `fundingFactorPerSecond` from on-chain GMX V2 events (HyperSync). GMX API provides only 8-hour epoch snapshots; this gives sub-second resolution since protocol launch.

```bash
# Full extraction
make funding-unified

# Incremental update (resumes from checkpoints)
make funding-unified-resume

# Include pre-V2.2 DataStore phase (requires archive RPC, slow)
make funding-full

# Export to FreqTrade feather format
make funding-feather
```

### How It Works

1. **Phase 2 — Funding Factor** (HyperSync): Streams `Funding` events for `fundingFactorPerSecond` magnitude (V2.2+, Aug 2025+)
2. **Phase 3 — Direction** (HyperSync): Streams `FundingFeeAmountPerSizeUpdated`, compares long/short delta sums to determine who pays
3. **Phase 1 — DataStore** (opt-in): Reads signed `savedFundingFactorPerSecond` via batched `eth_call` at hourly intervals (pre-V2.2, requires archive RPC)
4. **Merge**: Combines sources, applies direction correction, deduplicates, writes unified `1h.feather`

The DataStore phase uses fully-batched JSON-RPC — all network I/O completes before any record is written (~203 HTTP requests total vs ~17,000 sequential).

### Unified Hourly Rates Schema (`rates/{SYMBOL}/1h.feather`)

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | datetime[ms, UTC] | Hour start |
| `funding_rate` | float64 | Mean per-second rate |
| `funding_rate_hourly` | float64 | `rate * 3600` |
| `funding_rate_annualized` | float64 | `rate * 3600 * 8760` |
| `longs_pay_shorts` | bool | True = longs pay |
| `funding_fee_long` | float64 | Signed hourly rate for longs |
| `funding_fee_short` | float64 | Signed hourly rate for shorts |
| `update_count` | uint32 | Events in hour |
| `source` | string | `"datastore"` or `"hypersync"` |

### Daily Cronjob

```bash
0 2 * * * cd /path/to/gmx_historical_data && \
  make funding-unified-resume >> logs/unified_funding.log 2>&1
```

### Funding and mark in the daily release

The daily `data-YYYY-MM-DD` release ships hourly `futures/{PAIR}-1h-funding_rate.feather`
and `futures/{PAIR}-1h-mark.feather` (#56). Which bundle carries what is in
[What the daily release contains](#what-the-daily-release-contains); the freqtrade settings
and the exported-value semantics are in [Funding data contract](#funding-data-contract).

How it runs (`release-data.yml`): restore the newest stamped state asset (from the newest retained release that has one), `extract_unified_funding.py
--resume` (HyperSync only), export through the shared signed exporter into a **staging
directory**, grade every pair (`gmx_historical_data.series_coverage`), and copy the
staged files over `futures/` only if the gate passes. If it does not, `futures/` keeps
the previous release's companions unchanged, `funding_coverage.json` says `WITHHELD`
with the last good bar per pair, the release notes say funding was **not refreshed**, and
the job ends red -- the rest of the release still publishes. The first-ever run with
nothing previous ships no companions.

* **Coverage record.** `funding_coverage.json` (also in the release notes and
  `data_report.txt`) lists every `LAGGING` (>6h) and `STALE` (>48h) market with its last
  bar and age. Tails are never forward-filled; a quiet market's tail is reported, not
  blocking (`--stale-tail-blocks` flips that).
* **History starts 2025-08-19** (see the contract below). Earlier funding needs archive-RPC
  DataStore reads (#47) and is not in the daily release.
* **One-off rollout.** After merging, dispatch `release-data.yml` once with
  `seed_funding_from=data/full-history`. It copies the branch's raw funding sources,
  derives the checkpoints the branch never stored, catches up to now (step timeout 30
  min) and publishes the state asset. It never replaces an existing state. Without a
  seed every run withholds the companions.
* **Catch-up.** When the daily 15-minute extraction cannot close a long gap (HyperSync
  throttling), dispatch `funding-catchup.yml`: the same `--resume` extraction for up to
  330 minutes, then the advanced state is uploaded as a new stamped asset to the release
  it was restored from (even after a timeout). It shares the `gmx-funding-state`
  concurrency group with `release-data.yml`, leaves the bundles and notes alone, and its
  step summary says whether head was reached. Re-dispatch until it is; the next daily
  release then exports and gates as usual.

### Funding data contract

What a freqtrade backtest reads from `futures/{PAIR}-1h-funding_rate.feather` (#57):

* **Value:** the rate is in the `open` column (other OHLCV columns are 0), as an **hourly
  fraction of position notional** (`0.0001` = 0.01% per hour), not annualised and not
  per-second.
* **Sign:** positive = **longs pay shorts**, negative = shorts pay longs (the Freqtrade
  convention, #53). Only unsigned positive magnitudes with unknown direction are excluded rather than
  exported positive; zero, already-negative and DataStore (signed) rows pass through
  (`FreqtradeExporter._transform_funding_rate`).
* **Paying side only, symmetric approximation.** The exported number is the *paying* side's
  rate, and Freqtrade applies that one signed rate to both sides (a short receives what a long
  pays). GMX actually pays the receiving side `payer_rate × OI_payer / OI_receiver`, so for the
  receiving side the export is an approximation that is wrong whenever long and short open
  interest are imbalanced.
* **Borrowing fees are not included** anywhere in the export; they are a separate cost GMX
  charges on top of funding (`scripts/extract_borrowing_factor.py` is a standalone tool).
  `funding_rate + borrowing_rate` is therefore not what the backtest charges.
* **Freqtrade settings:** set `funding_fee_timeframe` and `mark_ohlcv_timeframe` to `1h`
  (the default `8h` has no file). History starts 2025-08-19.
* **Daily-snapshot API fields differ.** `snapshots/{date}.parquet` columns
  `funding_rate_long/short`, `borrowing_rate_long/short` and `net_rate_long/short` are
  GMX's live per-side values at snapshot time, stored as the API returns them: raw strings in
  1e30 fixed point, **annualised** (÷1e30 for the annual fraction, ÷8766 for an hourly one).
  They are a point-in-time sample, not an hourly series, and are not what the feather exports.

**Future work (not implemented):** exact per-side funding (receiver rate) and borrowing costs
would need side-aware historical schema, extraction and downstream Freqtrade support; a single
signed rate cannot represent two positive borrowing costs. Until then treat funding backtests
as an approximation.

## Borrowing Rate Extraction

Extract `borrowingFactorPerSecond` from GMX V2 `Borrowing` events — the actual on-chain rate charged to the dominant OI side, paid to LPs.

```bash
# Full historical extraction
poetry run python scripts/extract_borrowing_factor.py --from-block 120000000

# Incremental
poetry run python scripts/extract_borrowing_factor.py --resume

# Filter by market
poetry run python scripts/extract_borrowing_factor.py --resume --market "ETH/USD"
```

**Hourly rates schema** (`rates/{SYMBOL}/1h.parquet`):

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | datetime[ms, UTC] | Hour start |
| `borrowing_rate` | float64 | Mean per-second rate |
| `borrowing_rate_hourly` | float64 | `rate * 3600` |
| `borrowing_rate_annualized` | float64 | `rate * 3600 * 8760` |
| `update_count` | uint32 | Events in hour |
| `symbol` | string | Human-readable symbol |
| `market` | string | Market contract address |

To compute net position cost: `net_rate = funding_rate + borrowing_rate`. Both outputs use the same hourly format and can be joined on `(timestamp, symbol)`.

## Open Interest Extraction

Extract full historical `OpenInterestUpdated` and `OpenInterestInTokensUpdated` events from GMX V2 via HyperSync. Records every position change with USD and token-denominated OI.

```bash
make oi                                    # Full backfill from genesis (~20 min)
make oi-resume                             # Incremental update (resume from checkpoint)
make oi MARKET="ETH/USD"                   # Single market
make oi FROM_BLOCK=290000000 TO_BLOCK=290100000  # Block range
```

**Daily snapshot schema** (`snapshots/{SYMBOL}/daily.parquet`):

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | string | Market symbol (e.g. `ETH/USD`) |
| `date` | string | UTC date |
| `longOiUsd` | string | Long OI in USD (30-decimal string) |
| `shortOiUsd` | string | Short OI in USD (30-decimal string) |
| `totalOiUsd` | string | Total OI in USD |
| `longShortRatio` | string | Long/short ratio |
| `eventCount` | int | Position changes that day |

## Pool Liquidity Extraction

Extract full historical `PoolAmountUpdated` events — LP pool token changes on every deposit, withdrawal, and position-triggered rebalance.

```bash
make pool-liquidity                        # Full backfill (~20 min via HyperSync)
make pool-liquidity-resume                # Incremental update
make pool-liquidity MARKET="ETH/USD"      # Single market
```

**Daily snapshot schema** (`pool_liquidity_daily.parquet`):

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | string | Market symbol |
| `date` | datetime[UTC] | UTC date |
| `token` | string | Pool token address |
| `pool_tokens` | float64 | End-of-day pool token amount (raw units) |

## Analysis Notebooks

Interactive Plotly notebooks for exploring OI, liquidity, and cross-exchange data.

JupyterLab, Plotly and statsmodels live in an optional `notebooks` dependency
group, so that the daily collector's runner does not install them. Install the
group once, then launch:

```bash
poetry install --with notebooks
poetry run jupyter lab notebooks/
```

| Notebook | Description |
|----------|-------------|
| `notebooks/01_oi_analysis.ipynb` | OI time series, long/short breakdown, market rankings, monthly heatmap |
| `notebooks/02_liquidity_analysis.ipynb` | Pool depth over time, OI vs pool dual-axis, utilisation scatter, trading universe filter |
| `notebooks/03_cross_exchange_validation.ipynb` | GMX OI vs Binance/Hyperliquid volume correlation, cross-exchange funding rate comparison |
| `notebooks/04_cex_gmx_correlation.ipynb` | CEX vs GMX price correlation, spread analysis, and arbitrage signal identification |

**Usage example:**

```python
# Load OI snapshots for all markets
import pandas as pd
from pathlib import Path

SCALE_30 = 10 ** 30
snapshots_dir = Path("user_data/data/gmx/open_interest/arbitrum/snapshots")
frames = [pd.read_parquet(f) for f in snapshots_dir.rglob("daily.parquet")]
oi = pd.concat(frames)
oi["total_oi_usd"] = oi["totalOiUsd"].astype(float) / SCALE_30
```

## Troubleshooting

### Poetry Install Failing

```bash
poetry cache clear pypi --all
poetry install --no-cache
```

Ensure Python 3.11 or 3.12 is active (3.13 requires workarounds):

```bash
pyenv install 3.11 && pyenv local 3.11
poetry env use python3.11 && poetry install
```

### HyperSync Build Errors

HyperSync requires Rust and Cap'n Proto:

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh && source $HOME/.cargo/env

# Install Cap'n Proto
sudo apt-get install build-essential capnproto libcapnp-dev  # Ubuntu/Debian
brew install capnp                                            # macOS
```

Common errors:
- `linker 'cc' not found` → Install build-essential/gcc
- `capnp/capnp.h: No such file` → Install libcapnp-dev
- `cargo not found` → `source $HOME/.cargo/env`
- `PyO3's maximum supported version` → Use Python 3.11/3.12 or set `PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1`

> **Note:** hypersync 0.8.x has a build issue. Use 0.7.x until resolved.

### Rate Limits

Add multiple HyperSync tokens (comma-separated) for automatic rotation:

```bash
export HYPERSYNC_API_TOKEN="token1,token2,token3"
```

Get free tokens at https://envio.dev

### Funding Rate Issues

- **HyperSync 500 errors**: Both scripts retry up to 15 times with exponential backoff. The unified script retries each phase up to 3 times.
- **Unknown markets**: Market info is cached for 24h in `~/.cache/gmx_historical_data/markets_arbitrum.json`. Run with `--refresh-markets` to force refresh.
- **Rates look wrong**: Divide raw `funding_factor_per_second` by `10^30` for per-second decimal rate. Multiply by `3600 * 8760` for annualized. Typical ETH/USD annual rate: ~2-5%.

## Development

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
pytest tests/ -v
```

### Changing dependencies

The collector workflows do not install from `pyproject.toml` — Poetry is slow
on a runner and the daily release is on a clock. They install
`requirements-collector.txt`, a pinned file generated from `poetry.lock`.

After adding, removing or bumping any dependency, regenerate it:

```bash
poetry lock
python scripts/export_requirements.py
```

Commit `pyproject.toml`, `poetry.lock` and `requirements-collector.txt`
together. `tests/test_workflow_dependencies.py` fails if the generated file is
stale, or if an entry point imports a package the file does not pin — that
check exists because a hand-maintained package list once drifted from the code
and killed two nightly releases at import time.
