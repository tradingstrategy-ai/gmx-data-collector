# CEX Gap-Fill for GMX Feather Export — Design

**Date:** 2026-04-24

**Status:** Draft — awaiting implementation-plan approval

**Branch (to be created by user):** `feat/cex-gap-fill`

## Problem

GMX on-chain OHLCV data is derived from oracle events. Because events only fire on
state changes, the resulting candles are discontinuous in two ways:

1. **Missing price information.** Long periods without oracle updates produce bars
   that are either absent, forward-filled from a stale price, or followed by a
   large percentage jump (commonly 100% or more) when oracle reporting resumes.
   These jumps do not reflect a real market move. They reflect a data gap.
2. **Zero-volume bars.** Many intervals have valid prices but no on-chain trade
   flow, so volume is recorded as zero. Strategies that rely on volume (volume
   pair lists, VWAP calculations, liquidity filters) receive misleading signals.

CEX venues (Binance, Bybit) trade these same underlying assets continuously and do
not have either problem. We can use CEX OHLCV to close the gaps, producing a
corrected dataset while still treating GMX as the canonical source whenever it
actually has market information.

The feature is additive. Current `collect` / `collect-update` / `collect-update-nn`
behaviour and the `export-freqtrade` step must remain byte-identical when the new
pipeline stage is not invoked.

## Goals

- Introduce a new pipeline stage, *CEX gap-fill*, that runs after GMX parquet
  collection and before feather export.
- Detect price-jump gaps and zero-volume bars in GMX OHLCV parquets.
- Download matching CEX OHLCV via the existing `./freqtrade-gmx` wrapper
  (`freqtrade download-data`), not a custom HTTP client.
- Replace bar ranges where GMX data is missing or discontinuous with CEX data,
  applying documented 1000× price scaling where symbol conventions differ.
- Replace volume on zero-volume bars where GMX price is plausible.
- Leave untouched any GMX symbol that has no CEX listing on either venue.
- Produce a human-readable log and a JSON summary for every run.
- Guarantee that no change is made to the current flow when the new stage is off.

## Non-Goals

- Replacing funding-rate feathers with CEX funding data. GMX funding has
  different semantics; the existing funding pipeline is untouched.
- Building an HTTP client for Binance or Bybit. The freqtrade wrapper already
  handles authentication, retries, rate limiting, and pair discovery via ccxt.
- Real-time streaming. This runs as a batch step.
- Cross-venue price arbitration beyond the single heuristic described below.
- Repairing already-corrupted parquet datasets. The fill is applied to the
  output of a normal collect run, not to legacy on-disk artefacts.

## Architecture

The new stage sits between GMX parquet write and feather export. It reads and
writes to the same parquet paths that `collect` already produces, so the feather
exporter picks up corrected data naturally with no exporter changes.

```text
┌──────────────────────────┐
│ GMX collect (unchanged)  │
│   HyperSync / RPC events │
│   → data/candles/arbitrum│
│     /{SYMBOL}/{tf}.parquet│
└─────────────┬────────────┘
              │
              ▼
┌──────────────────────────────────────────────────────┐
│ NEW: CEX gap-fill stage                              │
│  1. detect gaps in GMX parquet                       │
│  2. resolve symbol → (exchange, pair) via router     │
│  3. call ./freqtrade-gmx download-data (subprocess)  │
│  4. load CEX feathers, align timestamps, scale price │
│  5. merge: full OHLCV replace on price-jump,         │
│     volume-only replace on zero-vol-price-sane       │
│  6. write corrected parquet (same path)              │
│  7. emit run log + JSON summary                      │
└─────────────┬────────────────────────────────────────┘
              │
              ▼
┌───────────────────────────────┐
│ feather export (unchanged)    │
│  freqtrade_exporter._write()  │
│  → data/gmx/futures/*.feather │
│  mark/index derive from close │
│  and inherit the correction.  │
└───────────────────────────────┘
```

### Module layout

```
src/gmx_historical_data/cex_gap_fill/
  __init__.py          # fill_gaps_from_cex(...) entry point
  detector.py          # gap range detection
  router.py            # symbol → {exchange, pair} resolution
  symbols.py           # static GMX → CEX symbol constants
  freqtrade_runner.py  # subprocess wrapper for ./freqtrade-gmx
  reconciler.py        # timestamp alignment + merge
  logging_utils.py     # log file + JSON summary writer
```

All current behaviour is preserved because:

- `cex_gap_fill` is never imported from any existing code path.
- The new CLI command, the new Makefile targets, and any future chain invocation
  are the only entry points that import the module.
- The new stage writes to the same parquet paths the old flow uses, but only
  when invoked. If not invoked, those parquets are untouched.

## Gap detection

`detector.py` consumes a polars DataFrame with schema
`(timestamp, open, high, low, close, volume)` for a single `(symbol, timeframe)`.

Two independent per-bar checks:

- `price_bad`: `abs(pct_change(close)) > GAP_PCT_THRESHOLD`
  (default `0.20`, configurable).
- `vol_bad`: `volume == 0`.

A missing-row check reindexes onto the expected timeframe grid between the first
and last observed timestamps. Missing rows are marked both `price_bad` and
`vol_bad` and are treated as full-replace candidates.

Contiguous runs of `price_bad` form `FullReplaceRange(start_ts, end_ts)`.
Contiguous runs of `vol_bad AND NOT price_bad` form `VolumeReplaceRange`. Runs
separated by `≤ MERGE_GAP_BARS` (default 2) bars are coalesced to avoid stitching
seams inside a single logical outage.

### Confirmation pass

A single 200% price jump may be a real market event rather than a data gap. To
guard against false-positive replacement, the reconciler runs a confirmation
pass: CEX pct-change across the same window is compared against GMX pct-change.
If `|cex_pct − gmx_pct| < GAP_PCT_THRESHOLD / 2`, both sources agree on the move
and the GMX bars are retained. Otherwise the bars are replaced.

### Tunables

All available via CLI flag and environment variable:

| Name                   | Default | Purpose                                       |
|------------------------|---------|-----------------------------------------------|
| `GAP_PCT_THRESHOLD`    | 0.20    | Price-jump cutoff                             |
| `MERGE_GAP_BARS`       | 2       | Coalesce near-contiguous ranges               |
| `MIN_RANGE_BARS`       | 1       | Minimum range size to consider                |
| `CEX_GAP_FILL_LOG_LEVEL` | INFO  | Python logging level                          |

## Routing

### Static mappings (in code)

Copied verbatim from `gmx-strategies/scripts/merge_gmx_binance.py` and
`gmx-strategies/plugins/pairlist/HistoricalVolumePairList.py`. These are stable
and maintained in code rather than config. They live in
`cex_gap_fill/symbols.py`.

```python
GMX_TO_BINANCE_NAME: dict[str, str] = {
    "BONK":  "1000BONK",
    "FLOKI": "1000FLOKI",
    "PEPE":  "1000PEPE",
    "SHIB":  "1000SHIB",
    "SATS":  "1000SATS",
}

PRICE_DIVISORS: dict[str, int] = {
    "BONK":  1000,
    "FLOKI": 1000,
    "PEPE":  1000,
    "SHIB":  1000,
    "SATS":  1000,
}

def normalize_k_prefix(ticker: str) -> str:
    # Hyperliquid convention: kPEPE -> KPEPE. Applied before further lookups.
    if ticker.startswith("k") and len(ticker) > 1 and ticker[1].isupper():
        return "K" + ticker[1:]
    return ticker
```

### Dynamic state (on disk)

`configs/cex_routing.json`:

```json
{
  "version": 1,
  "defaults": {
    "primary": "binance",
    "fallback": "bybit",
    "skip_unresolved": true
  },
  "overrides": {
    "BTC":  { "exchange": "binance", "pair": "BTC/USDT:USDT" },
    "FART": { "exchange": "skip" }
  },
  "auto": {
    "ETH": { "exchange": "binance", "pair": "ETH/USDT:USDT", "resolved_at": "2026-04-24" }
  }
}
```

Resolution order for each GMX symbol:

1. If in `overrides`, use it (including `{"exchange": "skip"}`).
2. If in `auto`, use the cached mapping.
3. Probe `defaults.primary` (Binance). If unavailable, probe `defaults.fallback`
   (Bybit).
4. If both probes fail, write `{"exchange": "skip"}` to `auto` and log a warning.
5. Persist newly-resolved entries back to `auto` after the run.

### Pair conventions

- GMX side: `{SYMBOL}/USDC:USDC`, feather suffix `_USDC_USDC`.
- Binance and Bybit side: `{SYMBOL}/USDT:USDT`, feather suffix `_USDT_USDT`,
  linear perps.
- 1000×-prefix symbols route to their prefixed CEX pair name and carry a price
  scale factor applied by the reconciler.

## Freqtrade runner

`freqtrade_runner.py` builds and invokes a subprocess:

```python
subprocess.run([
    "./freqtrade-gmx", "download-data",
    "--exchange", exchange,
    "--pairs", *pairs,
    "--timeframes", *timeframes,
    "--timerange", f"{start_yyyymmdd}-",   # open-ended: fetch to latest
    "--data-format-ohlcv", "feather",
    "--trading-mode", "futures",
    # "--datadir" passed only if user overrides via --cex-datadir / CEX_DATADIR
], check=True, capture_output=True, cwd=REPO_ROOT, timeout=CEX_DOWNLOAD_TIMEOUT)
```

Batching rules:

- One subprocess per exchange. All pairs and timeframes for that exchange are
  passed in a single call; freqtrade fans out internally.
- `timerange_start` is the earliest gap-start across all `(pair, tf)` for that
  exchange, floored to day boundary.
- Pre-check: if all required `(pair, tf)` feathers exist and cover the needed
  range, skip the subprocess entirely.

Error handling:

- Non-zero exit raises `CEXDownloadError` with a stderr tail. The current
  exchange is aborted; the other exchange is still attempted. GMX parquet is not
  modified.
- Partial failures parsed from freqtrade stdout are routed to `auto` as `skip`
  and logged.
- Timeout default 30 minutes per exchange call (`CEX_DOWNLOAD_TIMEOUT`).

Output discovery uses explicit path construction:

```
{datadir}/{exchange}/futures/{BASE}_USDT_USDT-{tf}-futures.feather
```

## Reconciler

Input:

- `gmx_df`: polars DataFrame, indexed by timestamp, for one `(symbol, timeframe)`.
- `cex_df`: freqtrade feather loaded into polars.
- `ranges`: detector output.

Algorithm:

1. Apply price scaling to `cex_df` before any use. For a symbol with divisor
   `d`, OHLC columns are divided by `d` and volume is multiplied by `d`
   (mirroring `merge_gmx_binance.py` lines 164–166).
2. Align timestamps to UTC nanoseconds; inner-join on timestamp for overlap.
3. For each `FullReplaceRange`:
   - If `|cex_pct_change − gmx_pct_change| < GAP_PCT_THRESHOLD / 2`, keep GMX.
   - Otherwise, replace the GMX OHLCV rows in that range with the CEX rows.
   - If CEX has no data for the range, log and leave GMX untouched.
4. For each `VolumeReplaceRange`:
   - Copy CEX volume into GMX volume where GMX volume is zero.
   - Preserve GMX OHLC unchanged.
5. Seam validation: after all replacements, recompute `pct_change`. Any new bar
   whose `|pct_change| > GAP_PCT_THRESHOLD` at a seam is logged as a warning.
   The reconciler never raises. Raising would break the invariant that the new
   stage must not poison the GMX parquet.
6. Dedupe on timestamp, sort, and run a history-preservation assertion: the
   earliest row in the corrected frame must equal the earliest row in the
   original GMX frame.

Output: a polars DataFrame with the same schema as the input. The existing
`_assert_history_preserved` contract in `storage.py` / `freqtrade_exporter.py`
continues to guard the downstream write.

## CLI and Makefile

### New CLI command

```
poetry run python -m gmx_historical_data.cli fill-gaps-cex [OPTIONS]
```

Flags:

| Flag                  | Default                        | Purpose                                |
|-----------------------|--------------------------------|----------------------------------------|
| `--data-dir`          | `./user_data`                  | Root data dir (same as `collect`)      |
| `--symbol`            | all on disk                    | Comma-separated whitelist              |
| `--timeframe`         | all six                        | Comma-separated whitelist              |
| `--gap-threshold`     | `0.20`                         | Price-jump cutoff                      |
| `--merge-gap-bars`    | `2`                            | Range coalesce window                  |
| `--cex-datadir`       | freqtrade default              | Override CEX output dir                |
| `--exchanges`         | `binance,bybit`                | Which CEX venues to try                |
| `--routing-file`      | `configs/cex_routing.json`     | Override state file                    |
| `--skip-download`     | off                            | Reuse on-disk CEX feathers             |
| `--dry-run`           | off                            | Detect and log without writing parquet |
| `--log-file`          | `logs/cex_gap_fill_{run}.log`  | Override log path                      |

Exit codes:

- `0` all requested symbols processed
- `2` one or more symbols failed; pipeline continued
- `1` fatal configuration error

The `collect` command is not modified.

### Makefile targets

New variables with safe defaults:

```makefile
GAP_THRESHOLD     ?= 0.20
MERGE_GAP_BARS    ?= 2
CEX_DATADIR       ?=
CEX_EXCHANGES     ?= binance,bybit
CEX_ROUTING_FILE  ?= configs/cex_routing.json
SKIP_DOWNLOAD     ?=
```

New phony targets, added to the `.PHONY` line at the top of the Makefile and to
the `help:` text:

| Target             | Chain                                                                               |
|--------------------|-------------------------------------------------------------------------------------|
| `fill-gaps-cex`    | Standalone: run the gap-fill stage against existing parquet                         |
| `refresh-data-cex` | `collect-update` → `funding-unified-resume` → `extract-all-resume` → `fill-gaps-cex` → `export-freqtrade` |
| `full-data-cex`    | `collect-full` → `funding-unified` → `extract-all` → `fill-gaps-cex` → `export-freqtrade` |
| `full-data-nn-cex` | `collect-full-nn` → `funding-unified` → `extract-all` → `fill-gaps-cex` → `export-freqtrade` |

Existing `refresh-data`, `full-data`, and `full-data-nn` are untouched.

## Logging

One log file per run:
`logs/cex_gap_fill_{YYYYMMDD_HHMMSS}.log`

Each `(symbol, timeframe)` writes a block:

```
[2026-04-24T10:15:03Z] SYMBOL=ETH  TF=1h  exchange=binance  pair=ETH/USDT:USDT
  gaps_detected: 4 full-replace ranges, 12 volume-only ranges
  ranges:
    FULL   2025-11-03T04:00 -> 2025-11-03T07:00  (4 bars)  gmx_pct=+2.13  cex_pct=+0.04  -> REPLACE
    FULL   2026-01-17T22:00 -> 2026-01-18T02:00  (5 bars)  gmx_pct=+0.38  cex_pct=+0.35  -> KEEP (CEX confirms)
    VOL    2025-12-05T11:00 -> 2025-12-05T13:00  (3 bars)  gmx_vol=0      cex_vol=47250  -> REPLACE
  summary: replaced=27 bars, volume_only=38 bars, kept=9 bars, cex_missing=0 bars
```

A single JSON summary per run:
`logs/cex_gap_fill_{run_id}.summary.json`

```json
{
  "run_id": "20260424_101503",
  "started_at": "...",
  "finished_at": "...",
  "symbols_processed": 118,
  "symbols_skipped_no_cex": ["FART", "GMXONLY1"],
  "totals": { "full_replaced": 1847, "volume_replaced": 5392, "kept": 923 },
  "errors": []
}
```

No sidecar parquet audit files. No extra columns on the feather output; the
downstream feather schema must remain exactly freqtrade-compatible.

## Dependencies

No new Python dependencies in `pyproject.toml`. The subprocess to
`./freqtrade-gmx` runs in freqtrade's own venv. In-process code uses polars
(already present) and stdlib (`json`, `subprocess`, `logging`, `pathlib`).

## Testing

Test tree: `tests/cex_gap_fill/`.

Unit:

- `test_detector.py` — synthetic frames with known price-jumps, zero-vol runs,
  missing rows, first/last bar edge cases. Assert range clustering is correct.
- `test_symbols.py` — K-prefix normalization, 1000× mapping round-trip,
  unknown-symbol pass-through.
- `test_router.py` — routing file load/save, override precedence, auto-cache
  persistence, unknown symbol ending up as `skip`.
- `test_reconciler.py` — synthetic GMX + CEX frames, including 1000BONK
  scaling, seam warning, history-preserve assertion, cex-missing path.
- `test_freqtrade_runner.py` — mock `subprocess.run`; assert CLI args,
  open-ended timerange, skip-if-cached logic, timeout wiring.
- `test_cli_fill_gaps_cex.py` — end-to-end with fake parquet + mocked
  subprocess. Idempotent re-run.

Regression:

- `test_legacy_flow_unchanged.py` — run `collect-update` → `export-freqtrade`
  without the fill stage against a committed fixture. Diff output feather bytes
  against a golden file.

Manual integration, documented in this spec:

- `make refresh-data-cex SYMBOL=ETH` against a small fixture; inspect
  `data/gmx/futures/ETH_USDC_USDC-1h-futures.feather` and the run log.

## Risks and mitigations

- **False positive replacement** on a legitimate large move (e.g. a news pump).
  Mitigated by the CEX confirmation pass.
- **Price-scale mismatch** on 1000×-prefixed symbols silently producing a 1000×
  error. Mitigated by explicit per-symbol `PRICE_DIVISORS`, a reconciler unit
  test, and a seam-validation warning.
- **Freqtrade wrapper drift** (the patched entrypoint changes its CLI surface).
  Mitigated by keeping the subprocess call to published `download-data` flags
  only and pinning a runner test that asserts the command-line shape.
- **Current flow regression.** Mitigated by the golden-file regression test and
  by the module being imported only from the new CLI command and the new
  Makefile targets.

## Rollout

1. Create branch `feat/cex-gap-fill` (user initiates).
2. Land the module skeleton, detector, symbols, and unit tests.
3. Land router and freqtrade runner with mocked-subprocess tests.
4. Land reconciler and the CLI command.
5. Land the Makefile targets and the legacy-flow regression test.
6. Manual validation: run `make full-data-cex SYMBOL=ETH` end-to-end; inspect
   log and feather output; compare against `full-data SYMBOL=ETH` on the same
   parquet to quantify replacement counts.
7. Broader run on a larger symbol set; iterate on threshold defaults.
