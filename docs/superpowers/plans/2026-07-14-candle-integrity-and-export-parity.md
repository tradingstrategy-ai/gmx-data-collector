# Candle Integrity and Export Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent invalid GMX OHLCV data from being stored or exported, keep Feather and Parquet futures exports equivalent, and make the collector respect currently disabled GMX markets.

**Architecture:** Add one shared, pure OHLCV validation module used at source persistence and export boundaries. The exporter will validate incoming and existing data before merging, write paired formats from one canonical merged frame through temporary files, and assert parity before publication. Market discovery will retain the raw GMX `isDisabled` state and the collection workflow will skip disabled markets while preserving their historical files as archives.

**Tech Stack:** Python 3.11/3.12, pandas, Polars, PyArrow, pytest, Typer, GMX REST API.

---

## Scope and confirmed audit baseline

- The external-drive authoritative futures exports are `/Volumes/WD Blue 1tb/VMs/data/gmx/futures`.
- `BONK` is value-valid on all six timeframes and both formats (4h/1d assessed under the same bounded ordering tolerance as the majors below, not a zero-tolerance standard — see next point).
- Aggregated 4h/1d candles for ~14 core markets (BTC, ETH, SOL, ARB, AVAX, BNB, DOGE, LINK, LTC, NEAR, OP, UNI, XRP, wstETH, ATOM, AAVE) carry small, benign OHLC-ordering overshoots — `low` slightly above `min(open, close)`, or `high` slightly below `max(open, close)` — a tiny oracle-aggregation artifact of how GMX derives these candles, not corruption. Worst observed overshoot is 0.84% (NEAR/4h). A zero-tolerance ordering gate would have broken export, incremental collection, and the release CI for these majors, so the gate below tolerates this specific, bounded band at 4h/1d only.
- `SATS` has one all-null 1h candle at `2025-09-27 13:00 UTC` in source and both exports.
- `OM` has stale/corrupt exports (1d, 4h, 15m, and 5m); `XAUT` has a corrupt 1h Parquet mirror.
- Existing code preserves date coverage but does not reject invalid OHLC values or require Feather/Parquet parity. Do not delete or rewrite drive data until the gates and tests below pass.

## File structure

- Create: `src/gmx_historical_data/ohlcv_validation.py` — format-neutral OHLCV invariants (with a bounded, timeframe-aware ordering tolerance for 4h/1d aggregation artifacts), structured validation result, and parity comparison.
- Modify: `src/gmx_historical_data/storage.py` — validate source candles before any write or merge.
- Modify: `src/gmx_historical_data/freqtrade_exporter.py` — validate every export boundary; add `output_format="both"` to atomically publish Feather/Parquet from one canonical merged frame.
- Modify: `src/gmx_historical_data/market_registry.py` — retain GMX `isDisabled` state from `/markets/info`.
- Modify: `src/gmx_historical_data/daemon/config.py` and the collector symbol-selection path in `src/gmx_historical_data/cli.py` — skip disabled markets and emit an explicit archival status.
- Modify: `.github/workflows/release-data.yml` — run integrity and parity validation before publishing a release.
- Modify: `scripts/validate_price_continuity.py` — reuse the shared validation module and report OHLC violations, not only close continuity, including tolerated 4h/1d ordering overshoots reported separately from failures.
- Create: `tests/test_ohlcv_validation.py` — unit coverage for null, nonpositive, nonfinite, inverted OHLC, timestamps, and parity.
- Modify: `tests/test_freqtrade_exporter.py` and `tests/test_freqtrade_exporter_isolation.py` — exporter rejection, atomic paired publication, and mirror parity.
- Create: `tests/test_market_registry.py` and modify `tests/test_cli_refactor.py` — disabled-market behavior.
- Create: `tests/test_drive_integrity_regressions.py` — synthetic SATS/OM/XAUT regressions; no external-drive dependency.

### Task 1: Define the shared OHLCV contract

**Files:**

- Create: `src/gmx_historical_data/ohlcv_validation.py`
- Create: `tests/test_ohlcv_validation.py`

- [ ] **Step 1: Write failing tests for invalid source and Freqtrade frames**

```python
import polars as pl
import pytest

from gmx_historical_data.ohlcv_validation import validate_ohlcv


@pytest.mark.parametrize(
    "column,value",
    [("close", None), ("close", 0.0), ("close", float("inf"))],
)
def test_validate_ohlcv_rejects_nonfinite_or_nonpositive_prices(column, value):
    frame = pl.DataFrame({
        "date": ["2025-09-27T13:00:00Z"],
        "open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0],
    }).with_columns(pl.col("date").str.to_datetime())
    frame = frame.with_columns(pl.lit(value, dtype=pl.Float64).alias(column))

    with pytest.raises(ValueError, match="invalid OHLCV"):
        validate_ohlcv(frame, timestamp_column="date", location="SATS/1h")


def test_validate_ohlcv_rejects_inverted_high_low():
    frame = pl.DataFrame({
        "date": ["2026-01-23T06:00:00Z"],
        "open": [4957.67], "high": [1.0], "low": [4942.54], "close": [4952.28],
    }).with_columns(pl.col("date").str.to_datetime())

    with pytest.raises(ValueError, match="OHLC ordering"):
        validate_ohlcv(frame, timestamp_column="date", location="XAUT/1h")
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `poetry run pytest tests/test_ohlcv_validation.py -q`

Expected: FAIL because `gmx_historical_data.ohlcv_validation` does not exist.

- [ ] **Step 3: Implement `validate_ohlcv()` and `assert_export_parity()`**

Implement a pure module that accepts a Polars frame and explicit timestamp column. It must reject missing required columns, null/nonfinite/nonpositive OHLC values, duplicate timestamps, non-monotonic timestamps, and `low > min(open, close)` / `high < max(open, close)`. Include the location, count, and first offending timestamp in every `ValueError`.

`validate_ohlcv()` takes an `ordering_tolerance` parameter (default `0.0`, strict) that bounds the ordering check alone. A shared `ordering_tolerance_for_timeframe()` helper returns `0.01` (1.0%) for `4h`/`1d` and `0.0` for `1m`/`5m`/`15m`/`1h`; every caller (storage, exporter, release audit) passes the timeframe-derived tolerance through. This only widens the acceptance band for the benign 4h/1d overshoots recorded in the baseline above — ordering violations beyond the tolerance are still rejected at every timeframe.

```python
def assert_export_parity(left: pl.DataFrame, right: pl.DataFrame, *, location: str) -> None:
    columns = ["date", "open", "high", "low", "close", "volume"]
    if left.select(columns).sort("date").equals(right.select(columns).sort("date")):
        return
    raise ValueError(f"{location}: Feather/Parquet export parity mismatch")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_ohlcv_validation.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/ohlcv_validation.py tests/test_ohlcv_validation.py
git commit -m "feat: validate OHLCV integrity at data boundaries"
```

### Task 2: Block invalid source candles

**Files:**

- Modify: `src/gmx_historical_data/storage.py:save_candles`
- Modify: `tests/test_ohlcv_validation.py`

- [ ] **Step 1: Write a failing persistence regression test**

```python
def test_save_candles_rejects_sats_style_all_null_bar(tmp_path):
    storage = ParquetStorage(tmp_path)
    bad = _candle_frame("SATS", ["2025-09-27 13:00:00+00:00"])
    bad.loc[:, ["open", "high", "low", "close"]] = float("nan")

    with pytest.raises(ValueError, match="save_candles\(SATS/1h\).*invalid OHLCV"):
        storage.save_candles(bad, timeframe="1h", symbol="SATS")
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `poetry run pytest tests/test_ohlcv_validation.py::test_save_candles_rejects_sats_style_all_null_bar -q`

Expected: FAIL because `save_candles()` currently accepts the null row.

- [ ] **Step 3: Validate incoming and merged frames in `save_candles()`**

Call `validate_ohlcv()` after normalizing the incoming timestamp type and again after deduplicating the merged frame. Preserve the existing history-coverage guard; validation must run in addition to it, not instead of it.

- [ ] **Step 4: Run storage and regression tests**

Run: `poetry run pytest tests/test_ohlcv_validation.py tests/test_freqtrade_exporter.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/storage.py tests/test_ohlcv_validation.py
git commit -m "fix: reject invalid OHLCV source candles"
```

### Task 3: Make paired exports valid, atomic, and equivalent

**Files:**

- Modify: `src/gmx_historical_data/freqtrade_exporter.py:export_candles,_write,export_funding`
- Modify: `tests/test_freqtrade_exporter.py`
- Modify: `tests/test_freqtrade_exporter_isolation.py`

- [ ] **Step 1: Write failing exporter tests**

Cover three cases: an existing invalid file must abort before replacement; a clean source exported with `output_format="both"` must produce equivalent Feather and Parquet frames; a failed second temporary write must leave both prior files intact.

```python
def test_export_candles_both_rejects_existing_xaut_style_corruption(tmp_path):
    exporter = _exporter_with_clean_xaut_source(tmp_path)
    corrupt = _freqtrade_frame("2026-01-23 06:00:00+00:00", high=4.953592e15, close=4.952280e15)
    corrupt.write_parquet(exporter.futures_dir / "XAUT_USDC_USDC-1h-futures.parquet")

    with pytest.raises(ValueError, match="XAUT_USDC_USDC-1h-futures.parquet.*invalid OHLCV"):
        exporter.export_candles(symbols=["XAUT"], timeframes=["1h"], output_format="parquet")
```

- [ ] **Step 2: Run focused exporter tests to verify failure**

Run: `poetry run pytest tests/test_freqtrade_exporter.py tests/test_freqtrade_exporter_isolation.py -q`

Expected: FAIL because `_write()` validates only date coverage.

- [ ] **Step 3: Refactor export preparation around one canonical merged frame**

Add a private helper that transforms source candles, validates the transformed frame, reads and validates an existing destination, merges once, validates the merged result, and returns the canonical frame. Extend `FreqtradeExporter.export_candles()` and the Typer `--format` option with `both`; `make export-candles-both` must invoke that single call rather than two independent exports. In `both` mode, write the returned frame to temporary Feather and Parquet paths in the same directory. Validate both temporary reads with `assert_export_parity()` before `Path.replace()` publishes either file. On any exception, remove temporary files and leave both destination files untouched.

The helper checks `unsafe_overwrite` (or destination non-existence) *before* reading and validating any existing destination, not after — so a corrupt pre-existing file, such as XAUT's corrupt 1h Parquet mirror, can actually be regenerated instead of tripping the validator on the very file it's meant to replace (see Task 6, Step 4).

- [ ] **Step 4: Preserve current single-format behavior**

Single-format exports still use the canonical merge helper and validity gate. They may update only their selected format, but must never silently accept a corrupt pre-existing destination. Keep `unsafe_overwrite` as the only explicit bypass and make it log a high-severity warning.

The separate `export_funding()` path validates the funding-rate frame — rate stored in `open`, other OHLCV columns zeroed — with `allow_nonpositive_prices=True`. This flag was renamed from an earlier `allow_zero_prices`: hourly funding rates legitimately go negative (observed in 89 of 131 markets), so the check now only requires finite values and skips the OHLC ordering check for these frames, without weakening validation anywhere else.

- [ ] **Step 5: Run exporter test suite**

Run: `poetry run pytest tests/test_freqtrade_exporter.py tests/test_freqtrade_exporter_isolation.py tests/test_freqtrade_exporter_compression.py tests/test_freqtrade_exporter_timestamps.py -q`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/gmx_historical_data/freqtrade_exporter.py tests/test_freqtrade_exporter.py tests/test_freqtrade_exporter_isolation.py
git commit -m "fix: validate and atomically publish paired candle exports"
```

### Task 4: Respect current GMX disabled-market state

**Files:**

- Modify: `src/gmx_historical_data/market_registry.py`
- Modify: `src/gmx_historical_data/cli.py`
- Modify: `src/gmx_historical_data/daemon/config.py`
- Create: `tests/test_market_registry.py`
- Modify: the existing CLI symbol-selection test module

- [ ] **Step 1: Write failing live-market status tests**

```python
def test_build_registry_preserves_is_disabled_from_market_info():
    registry = _build_registry([{
        "marketToken": "0x123", "name": "OM/USD [OM-USDC]",
        "indexToken": "0x456", "listingDate": "2025-01-01",
        "isListed": True, "isDisabled": True,
    }])
    assert registry["0x123"]["isDisabled"] is True


def test_symbol_selection_skips_disabled_market_by_default():
    assert _filter_and_categorize_symbols(["OM", "BONK"], disabled_symbols={"OM"}) == ["BONK"]
```

- [ ] **Step 2: Run the tests to verify failure**

Run: `poetry run pytest tests/test_market_registry.py tests/test_cli_refactor.py -q`

Expected: FAIL because the registry reads `/markets`, does not preserve `isDisabled`, and static configuration still includes OM.

- [ ] **Step 3: Fetch `/markets/info` for collection eligibility**

Extend the API adapter/registry to use GMX market info for live eligibility, preserving both `isListed` and `isDisabled`. Treat cache/network failure conservatively: use the last cached status and never delete historical data. The CLI and daemon should log `archived disabled market: OM` and skip candle collection/export unless a new explicit `--include-disabled` override is supplied.

- [ ] **Step 4: Run market and collector-selection tests**

Run: `poetry run pytest tests/test_market_registry.py tests/test_cli_refactor.py tests/test_daily_snapshot.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/market_registry.py src/gmx_historical_data/cli.py src/gmx_historical_data/daemon/config.py tests/test_market_registry.py tests/test_cli_refactor.py
git commit -m "fix: skip disabled GMX markets during collection"
```

### Task 5: Make release validation block regressions

**Files:**

- Modify: `scripts/validate_price_continuity.py`
- Modify: `.github/workflows/release-data.yml`
- Create: `tests/test_drive_integrity_regressions.py`

- [ ] **Step 1: Write regression fixtures for SATS, OM, and XAUT**

Create synthetic frames matching the observed defects: SATS all-null 1h bar, OM 0.06685 → 0.007513 → 0.06685 price regime, and XAUT 4,952 → 4.95e15 scale jump. Assert the CLI/library reports each as failure and accepts a clean BONK-style frame.

- [ ] **Step 2: Run regression tests to verify failure**

Run: `poetry run pytest tests/test_drive_integrity_regressions.py -q`

Expected: FAIL until the shared validator is wired into the script.

- [ ] **Step 3: Extend the release workflow**

After collection and before release packaging, validate all `*-futures.feather` and `*-futures.parquet` candle files. Fail on malformed OHLCV, scale jumps, duplicate timestamps, or Feather/Parquet mismatch where both files exist. Report coverage gaps separately; do not fail the release solely for documented non-Chainlink depth limitations.

Also report tolerated 4h/1d OHLC-ordering overshoots — the audit script's `tolerated` column and its `tolerated_ordering` report field — separately from failures; do not fail the release on overshoots within `ordering_tolerance_for_timeframe()`'s band, only on violations beyond it.

- [ ] **Step 4: Run all relevant tests**

Run: `poetry run pytest tests/test_ohlcv_validation.py tests/test_drive_integrity_regressions.py tests/test_freqtrade_exporter.py tests/test_freqtrade_exporter_isolation.py tests/test_market_registry.py tests/test_release_workflow.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/validate_price_continuity.py .github/workflows/release-data.yml tests/test_drive_integrity_regressions.py
git commit -m "ci: block malformed candle exports from releases"
```

### Task 6: Repair only affected active data after safeguards land

**Files:**

- No repository source changes.
- External data: `/Volumes/WD Blue 1tb/VMs/data/gmx/candles/arbitrum/{SATS,OM,XAUT}/`
- External exports: `/Volumes/WD Blue 1tb/VMs/data/gmx/futures/`

- [ ] **Step 1: Snapshot the affected files and record checksums**

Run: `shasum -a 256 /Volumes/WD\ Blue\ 1tb/VMs/data/gmx/{candles/arbitrum/SATS/1h.parquet,futures/SATS_USDC_USDC-1h-futures.feather,futures/SATS_USDC_USDC-1h-futures.parquet}`

Expected: checksums captured before any repair.

- [ ] **Step 2: Repair the SATS source gap from a verified oracle/CEX interval**

Use the project’s CEX gap-fill route for `SATS` and only the one invalid 1h interval. Verify the replacement has finite positive OHLC values and preserves its neighboring timestamps.

- [ ] **Step 3: Archive OM instead of refetching it**

After live `isDisabled` confirmation, move OM exports to a dated archive directory or exclude them from published active exports. Do not manufacture a repair from an inactive source.

- [ ] **Step 4: Regenerate XAUT Parquet from the validated canonical Feather/source frame**

Run the paired exporter for `XAUT` after its source has passed validation; verify the 1h Parquet no longer contains the January 23 scale artifacts.

- [ ] **Step 5: Regenerate both formats for SATS and active affected markets**

Run: `make export-candles-both DATA_DIR='/Volumes/WD Blue 1tb/VMs/data/gmx' FEATHER_DIR='/Volumes/WD Blue 1tb/VMs/data/gmx' SYMBOL='SATS,XAUT'`

Expected: paired exports have identical rows, timestamps, and OHLCV values.

- [ ] **Step 6: Run the read-only audit again**

Run: `poetry run python scripts/validate_price_continuity.py /Volumes/WD\ Blue\ 1tb/VMs/data/gmx/futures/{SATS,XAUT}_USDC_USDC-*-futures.{feather,parquet}`

Expected: zero malformed rows or scale jumps; OM absent from active publish set; any documented coverage limitation reported separately.

## Plan self-review

- Spec coverage: source validation, export validation/parity, disabled-market handling, CI enforcement, tests, and data remediation are all represented.
- Ambiguity resolved: coverage gaps and tolerated 4h/1d ordering overshoots are reported but do not become value-corruption failures; invalid prices, over-tolerance ordering, and format divergence do.
- Safety: no external-drive modification happens until automated safeguards pass and a pre-repair checksum is captured.
