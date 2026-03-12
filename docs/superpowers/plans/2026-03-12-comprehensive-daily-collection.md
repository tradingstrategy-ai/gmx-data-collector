# Comprehensive GMX Daily Data Collection — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the daily snapshot collector to capture ALL GMX API data (6 OHLCV timeframes, tickers, APY) with proper dedup and historical accumulation.

**Architecture:** Enhance `scripts/collect_daily_snapshot.py` with new collection phases. Extract a reusable `_merge_feather()` helper for the dedup-and-append pattern used across all timeframes. Add `tickers/` and `apy/` parquet directories alongside existing `futures/` and `snapshots/`. Update the GitHub Actions workflow to commit all new data directories.

**Tech Stack:** Python 3.12, pandas, pyarrow, eth_defi.gmx.api.GMXAPI, rich, GitHub Actions

---

## File Structure

| File | Action | Purpose |
|------|--------|---------|
| `scripts/collect_daily_snapshot.py` | **Modify** | Add multi-TF OHLCV, tickers, APY collection + reusable merge helper |
| `.github/workflows/collect-gmx-data.yml` | **Modify** | Add new data dirs to `git add`, increase timeout |
| `tests/test_daily_snapshot.py` | **Create** | Unit tests for helpers and collection functions |

## Output Directory Layout (after changes)

```
user_data/data/gmx/
├── futures/                                    # Freqtrade-compatible feather (EXISTS + NEW TFs)
│   ├── {SYM}_USDC_USDC-1m-futures.feather      # NEW
│   ├── {SYM}_USDC_USDC-5m-futures.feather      # NEW
│   ├── {SYM}_USDC_USDC-15m-futures.feather     # NEW
│   ├── {SYM}_USDC_USDC-1h-futures.feather      # NEW
│   ├── {SYM}_USDC_USDC-4h-futures.feather      # NEW
│   └── {SYM}_USDC_USDC-1d-futures.feather      # EXISTS
├── snapshots/                                  # EXISTS - daily market state (OI, liquidity, rates)
│   └── {date}.parquet
├── tickers/                                    # NEW - bid/ask/volume point-in-time
│   └── {date}.parquet
└── apy/                                        # NEW - yield data all periods
    └── {date}.parquet
```

---

## Chunk 1: Core helpers and multi-timeframe OHLCV

### Task 1: Extract `_merge_feather()` helper and `_extract_symbols()` helper

The merge-and-save pattern is duplicated and will be needed 6× for OHLCV timeframes. Extract it once.

**Files:**
- Modify: `scripts/collect_daily_snapshot.py:174-204` (inline merge logic)
- Test: `tests/test_daily_snapshot.py`

- [ ] **Step 1: Write failing tests for `_merge_feather()`**

Create `tests/test_daily_snapshot.py`:

```python
"""Tests for daily snapshot collection helpers."""

import pandas as pd
import pyarrow.feather as feather
import pytest


def _make_ohlcv(dates, close_values):
    """Build a minimal OHLCV DataFrame for testing."""
    return pd.DataFrame({
        "date": pd.to_datetime(dates, utc=True).as_unit("ns"),
        "open": close_values,
        "high": close_values,
        "low": close_values,
        "close": close_values,
        "volume": 0.0,
    })


class TestMergeFeather:
    """Tests for the _merge_feather helper."""

    def test_new_file_created(self, tmp_path):
        """First run creates the file from scratch."""
        from scripts.collect_daily_snapshot import _merge_feather

        filepath = tmp_path / "TEST_USDC_USDC-1d-futures.feather"
        new_df = _make_ohlcv(["2026-03-10", "2026-03-11"], [100.0, 110.0])

        _merge_feather(new_df, filepath)

        result = pd.read_feather(filepath)
        assert len(result) == 2
        assert list(result["close"]) == [100.0, 110.0]

    def test_append_no_overlap(self, tmp_path):
        """New rows are appended when no overlap exists."""
        from scripts.collect_daily_snapshot import _merge_feather

        filepath = tmp_path / "TEST_USDC_USDC-1d-futures.feather"
        existing = _make_ohlcv(["2026-03-10"], [100.0])
        feather.write_feather(existing, filepath)

        new_df = _make_ohlcv(["2026-03-11", "2026-03-12"], [110.0, 120.0])
        _merge_feather(new_df, filepath)

        result = pd.read_feather(filepath)
        assert len(result) == 3
        assert list(result["close"]) == [100.0, 110.0, 120.0]

    def test_overlap_keeps_new(self, tmp_path):
        """Overlapping timestamps use new data, existing data preserved."""
        from scripts.collect_daily_snapshot import _merge_feather

        filepath = tmp_path / "TEST_USDC_USDC-1d-futures.feather"
        existing = _make_ohlcv(["2026-03-10", "2026-03-11"], [100.0, 110.0])
        feather.write_feather(existing, filepath)

        # New data overlaps on 03-11 with updated close, adds 03-12
        new_df = _make_ohlcv(["2026-03-11", "2026-03-12"], [115.0, 120.0])
        _merge_feather(new_df, filepath)

        result = pd.read_feather(filepath)
        assert len(result) == 3
        assert list(result["close"]) == [100.0, 115.0, 120.0]

    def test_existing_data_never_lost(self, tmp_path):
        """Historical data not in new fetch is always preserved."""
        from scripts.collect_daily_snapshot import _merge_feather

        filepath = tmp_path / "TEST_USDC_USDC-1d-futures.feather"
        existing = _make_ohlcv(
            ["2026-03-08", "2026-03-09", "2026-03-10"], [80.0, 90.0, 100.0]
        )
        feather.write_feather(existing, filepath)

        # New data only has 03-11 (no overlap at all)
        new_df = _make_ohlcv(["2026-03-11"], [110.0])
        _merge_feather(new_df, filepath)

        result = pd.read_feather(filepath)
        assert len(result) == 4
        assert list(result["close"]) == [80.0, 90.0, 100.0, 110.0]

    def test_sorted_output(self, tmp_path):
        """Output is always sorted by date regardless of input order."""
        from scripts.collect_daily_snapshot import _merge_feather

        filepath = tmp_path / "TEST_USDC_USDC-1d-futures.feather"
        # Provide data out of order
        new_df = _make_ohlcv(["2026-03-12", "2026-03-10", "2026-03-11"], [120.0, 100.0, 110.0])

        _merge_feather(new_df, filepath)

        result = pd.read_feather(filepath)
        assert list(result["close"]) == [100.0, 110.0, 120.0]


class TestExtractSymbols:
    """Tests for the _extract_symbols helper."""

    def test_extracts_unique_listed_perp_symbols(self):
        from scripts.collect_daily_snapshot import _extract_symbols

        markets = [
            {"name": "ETH/USD [ETH-USDC]", "isListed": True},
            {"name": "ETH/USD [ETH-ETH]", "isListed": True},
            {"name": "BTC/USD [WBTC.b-USDC]", "isListed": True},
            {"name": "USDC-USDT", "isListed": True},  # swap-only, no "/"
            {"name": "DOGE/USD [ETH-USDC]", "isListed": False},  # unlisted
        ]
        symbols = _extract_symbols(markets)
        assert symbols == ["BTC", "ETH"]

    def test_empty_markets(self):
        from scripts.collect_daily_snapshot import _extract_symbols

        assert _extract_symbols([]) == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC && poetry run python -m pytest tests/test_daily_snapshot.py -v`
Expected: FAIL with `ImportError` (functions don't exist yet)

- [ ] **Step 3: Implement `_merge_feather()` and `_extract_symbols()`**

Add to `scripts/collect_daily_snapshot.py` after the `_GMX_PRECISION` constant (after line 48):

```python
# All OHLCV timeframes to collect from the GMX API.
TIMEFRAMES = ["1m", "5m", "15m", "1h", "4h", "1d"]

# APY periods available from the GMX API.
APY_PERIODS = ["1d", "7d", "30d", "90d", "180d", "1y", "total"]


def _merge_feather(new_df: pd.DataFrame, filepath: Path) -> None:
    """Merge new OHLCV rows into an existing feather file (or create it).

    Existing historical data is never deleted. Overlapping timestamps are
    replaced with the newer values (``keep='last'``). Output is always
    sorted by date.

    :param new_df: New rows with columns ``[date, open, high, low, close, volume]``.
    :param filepath: Path to the feather file (created if missing).
    """
    if new_df.empty:
        return

    if new_df["date"].dt.tz is None:
        new_df["date"] = new_df["date"].dt.tz_localize("UTC")
    new_df["date"] = new_df["date"].dt.as_unit("ns")

    if filepath.exists():
        existing = pd.read_feather(filepath)
        if existing["date"].dt.tz is None:
            existing["date"] = existing["date"].dt.tz_localize("UTC")
        existing["date"] = existing["date"].dt.as_unit("ns")
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date"], keep="last")
    else:
        combined = new_df

    combined = combined.sort_values("date").reset_index(drop=True)
    if combined["date"].dtype == "object":
        combined["date"] = pd.to_datetime(combined["date"], utc=True)
    combined["date"] = combined["date"].dt.as_unit("ns")
    feather.write_feather(combined, filepath)


def _extract_symbols(markets: list[dict]) -> list[str]:
    """Extract sorted unique symbols from listed perpetual markets.

    Skips swap-only pools (no ``/`` in name) and unlisted markets.

    :param markets: Raw market dicts from ``get_markets_info()``.
    :returns: Sorted list of unique symbol strings.
    """
    symbols = set()
    for market in markets:
        if not market.get("isListed", True):
            continue
        name = market.get("name", "")
        if "/" not in name:
            continue
        symbol = name.split("/")[0].strip()
        if symbol:
            symbols.add(symbol)
    return sorted(symbols)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC && poetry run python -m pytest tests/test_daily_snapshot.py -v`
Expected: All 7 tests PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_daily_snapshot.py scripts/collect_daily_snapshot.py
git commit -m "feat: extract _merge_feather and _extract_symbols helpers for daily collection"
```

---

### Task 2: Refactor OHLCV collection to support all 6 timeframes

Replace the 1d-only `collect_and_save_ohlcv` with a multi-timeframe version.

**Files:**
- Modify: `scripts/collect_daily_snapshot.py:121-223` (replace `collect_and_save_ohlcv`)
- Test: `tests/test_daily_snapshot.py`

- [ ] **Step 1: Write failing test for multi-timeframe filename generation**

Add to `tests/test_daily_snapshot.py`:

```python
class TestCollectOhlcvFilenames:
    """Verify correct Freqtrade-format filenames for each timeframe."""

    def test_filename_per_timeframe(self, tmp_path):
        """Each timeframe produces the correct filename pattern."""
        from scripts.collect_daily_snapshot import TIMEFRAMES

        for tf in TIMEFRAMES:
            expected = f"ETH_USDC_USDC-{tf}-futures.feather"
            assert expected.endswith(".feather")
            assert tf in expected
```

- [ ] **Step 2: Rewrite `collect_and_save_ohlcv` to iterate timeframes**

Replace the existing `collect_and_save_ohlcv` function (lines 121-223) with:

```python
def collect_and_save_ohlcv(
    api: GMXAPI,
    markets: list[dict],
    futures_dir: Path,
    timeframes: list[str] | None = None,
) -> tuple[int, list[str]]:
    """Fetch OHLCV candles for all timeframes and append to feather files.

    For each unique listed perpetual symbol, fetches candles across all
    requested timeframes and merges into per-symbol feather files using
    the Freqtrade naming convention.

    On first run (file doesn't exist): fetches max history (``limit=10000``).
    On subsequent runs (file exists): fetches recent candles (``limit=5``
    for 1d, proportionally more for smaller timeframes).

    :param api: Initialised GMXAPI client.
    :param markets: Raw market dicts from ``get_markets_info()``.
    :param futures_dir: Directory for CCXT feather files.
    :param timeframes: List of timeframes to collect (default: all).
    :returns: Tuple of (total files saved, list of failed symbol-timeframe pairs).
    """
    tfs = timeframes or TIMEFRAMES
    symbols = _extract_symbols(markets)
    console.print(f"  Fetching candles for {len(symbols)} symbols × {len(tfs)} timeframes...")

    futures_dir.mkdir(parents=True, exist_ok=True)
    saved = 0
    failed = []

    # Recent-fetch limits per timeframe (how many candles to fetch on incremental runs)
    # 1m: ~24h = 1440, 5m: ~2d = 576, 15m: ~3d = 288, 1h: ~5d = 120, 4h: ~10d = 60, 1d: 5
    incremental_limits = {
        "1m": 1440,
        "5m": 576,
        "15m": 288,
        "1h": 120,
        "4h": 60,
        "1d": 5,
    }

    for symbol in symbols:
        for tf in tfs:
            try:
                filepath = futures_dir / f"{symbol}_USDC_USDC-{tf}-futures.feather"

                if filepath.exists():
                    limit = incremental_limits.get(tf, 100)
                else:
                    limit = 10000  # Max available history from API

                df = api.get_candlesticks_dataframe(symbol, period=tf, limit=limit)
                if df.empty:
                    failed.append(f"{symbol}/{tf}")
                    continue

                new_rows = pd.DataFrame({
                    "date": df["timestamp"],
                    "open": df["open"].astype(float),
                    "high": df["high"].astype(float),
                    "low": df["low"].astype(float),
                    "close": df["close"].astype(float),
                    "volume": 0.0,
                })

                _merge_feather(new_rows, filepath)
                saved += 1

            except Exception as e:
                failed.append(f"{symbol}/{tf}")
                console.print(f"    [yellow]Warning: {symbol}/{tf} — {e}[/yellow]")

            time.sleep(0.05)  # Polite delay (reduced since more calls now)

    console.print(
        f"  [green]Saved {saved} candle files[/green]"
        + (
            f" [yellow]({len(failed)} failed: {', '.join(failed[:5])}"
            f"{'...' if len(failed) > 5 else ''})[/yellow]"
            if failed
            else ""
        )
    )
    return saved, failed
```

- [ ] **Step 3: Update `main()` to pass timeframes and remove `date_str` arg from OHLCV call**

In `main()`, update the Phase 2 call (around line 393-396):

```python
    # --- 2. OHLCV candles for ALL timeframes (CCXT feather format) ---
    console.print("\n[bold]Phase 2: OHLCV candles (all timeframes)[/bold]")
    candle_count, failed_symbols = collect_and_save_ohlcv(
        api, all_markets, futures_dir
    )
    console.print()
```

- [ ] **Step 4: Run tests + verify lint passes**

Run: `export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC && poetry run python -m pytest tests/test_daily_snapshot.py -v && poetry run ruff check scripts/collect_daily_snapshot.py`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/collect_daily_snapshot.py tests/test_daily_snapshot.py
git commit -m "feat: collect OHLCV candles across all 6 timeframes (1m-1d)"
```

---

## Chunk 2: Tickers, APY, report, and workflow

### Task 3: Add ticker collection

Tickers provide bid/ask spread and volume data. One API call, saved as daily parquet.

**Files:**
- Modify: `scripts/collect_daily_snapshot.py`
- Test: `tests/test_daily_snapshot.py`

- [ ] **Step 1: Write failing test**

Add to `tests/test_daily_snapshot.py`:

```python
class TestCollectTickers:
    """Tests for the ticker collection function."""

    def test_flatten_ticker_data(self):
        """Ticker list is flattened to a DataFrame with correct columns."""
        from scripts.collect_daily_snapshot import _flatten_tickers

        raw_tickers = [
            {
                "tokenAddress": "0xabc",
                "tokenSymbol": "ETH",
                "minPrice": "330000000000",
                "maxPrice": "331000000000",
                "updatedAt": 1773308683207,
                "timestamp": 1773308682,
            },
            {
                "tokenAddress": "0xdef",
                "tokenSymbol": "BTC",
                "minPrice": "8300000000000",
                "maxPrice": "8310000000000",
                "updatedAt": 1773308683207,
                "timestamp": 1773308682,
            },
        ]
        df = _flatten_tickers(raw_tickers, "2026-03-12")
        assert len(df) == 2
        assert "token_symbol" in df.columns
        assert "min_price" in df.columns
        assert "max_price" in df.columns
        assert "date" in df.columns
        assert list(df["token_symbol"]) == ["ETH", "BTC"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC && poetry run python -m pytest tests/test_daily_snapshot.py::TestCollectTickers -v`
Expected: FAIL

- [ ] **Step 3: Implement `_flatten_tickers()` and `collect_and_save_tickers()`**

Add to `scripts/collect_daily_snapshot.py`:

```python
def _flatten_tickers(raw_tickers: list[dict], date_str: str) -> pd.DataFrame:
    """Flatten raw ticker API response into a tabular DataFrame.

    :param raw_tickers: List of ticker dicts from ``get_tickers()``.
    :param date_str: ISO date string to tag each row.
    :returns: DataFrame with one row per token.
    """
    rows = []
    for ticker in raw_tickers:
        rows.append({
            "date": date_str,
            "token_symbol": ticker.get("tokenSymbol", ""),
            "token_address": ticker.get("tokenAddress", ""),
            "min_price": ticker.get("minPrice", "0"),
            "max_price": ticker.get("maxPrice", "0"),
            "updated_at": ticker.get("updatedAt", 0),
            "timestamp": ticker.get("timestamp", 0),
        })
    return pd.DataFrame(rows)


def collect_and_save_tickers(
    api: GMXAPI,
    date_str: str,
    tickers_dir: Path,
) -> int:
    """Fetch current ticker data and save as daily parquet snapshot.

    :param api: Initialised GMXAPI client.
    :param date_str: ISO date string for the snapshot.
    :param tickers_dir: Directory for ticker parquet files.
    :returns: Number of tickers saved.
    """
    tickers_dir.mkdir(parents=True, exist_ok=True)

    raw_tickers = api.get_tickers(use_cache=False)
    if not raw_tickers:
        console.print("  [yellow]Warning: No ticker data returned[/yellow]")
        return 0

    df = _flatten_tickers(raw_tickers, date_str)
    ticker_path = tickers_dir / f"{date_str}.parquet"
    df.to_parquet(ticker_path, index=False)
    console.print(f"  [green]Saved {len(df)} tickers[/green] → {ticker_path}")
    return len(df)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC && poetry run python -m pytest tests/test_daily_snapshot.py::TestCollectTickers -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/collect_daily_snapshot.py tests/test_daily_snapshot.py
git commit -m "feat: add ticker data collection (bid/ask/volume daily parquet)"
```

---

### Task 4: Add APY collection

APY provides yield data across 7 time periods. Needs to be joined with market names for readability.

**Files:**
- Modify: `scripts/collect_daily_snapshot.py`
- Test: `tests/test_daily_snapshot.py`

- [ ] **Step 1: Write failing test**

Add to `tests/test_daily_snapshot.py`:

```python
class TestCollectApy:
    """Tests for the APY collection function."""

    def test_flatten_apy_data(self):
        """APY response is flattened with market_token and period columns."""
        from scripts.collect_daily_snapshot import _flatten_apy

        raw_apy = {
            "markets": {
                "0xabc": {"apy": 0.05, "baseApy": 0.04, "bonusApr": 0.01},
                "0xdef": {"apy": 0.10, "baseApy": 0.10, "bonusApr": 0.0},
            },
            "glvs": {
                "0x111": {"apy": 0.03, "baseApy": 0.03, "bonusApr": 0.0},
            },
        }
        df = _flatten_apy(raw_apy, "30d", "2026-03-12")
        assert len(df) == 3  # 2 markets + 1 glv
        assert "market_token" in df.columns
        assert "period" in df.columns
        assert "apy" in df.columns
        assert "type" in df.columns
        assert set(df["type"]) == {"market", "glv"}
        assert all(df["period"] == "30d")

    def test_flatten_apy_empty(self):
        """Empty APY response returns empty DataFrame."""
        from scripts.collect_daily_snapshot import _flatten_apy

        df = _flatten_apy({}, "30d", "2026-03-12")
        assert len(df) == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC && poetry run python -m pytest tests/test_daily_snapshot.py::TestCollectApy -v`
Expected: FAIL

- [ ] **Step 3: Implement `_flatten_apy()` and `collect_and_save_apy()`**

Add to `scripts/collect_daily_snapshot.py`:

```python
def _flatten_apy(
    raw_apy: dict,
    period: str,
    date_str: str,
) -> pd.DataFrame:
    """Flatten raw APY API response into a tabular DataFrame.

    Combines both ``markets`` and ``glvs`` entries with a ``type`` column
    to distinguish them.

    :param raw_apy: Dict from ``get_apy()`` with ``markets`` and ``glvs`` keys.
    :param period: APY period string (e.g., ``'30d'``).
    :param date_str: ISO date string to tag each row.
    :returns: DataFrame with one row per market/glv token.
    """
    rows = []
    for market_token, data in raw_apy.get("markets", {}).items():
        rows.append({
            "date": date_str,
            "period": period,
            "type": "market",
            "market_token": market_token,
            "apy": data.get("apy", 0.0),
            "base_apy": data.get("baseApy", 0.0),
            "bonus_apr": data.get("bonusApr", 0.0),
        })
    for glv_token, data in raw_apy.get("glvs", {}).items():
        rows.append({
            "date": date_str,
            "period": period,
            "type": "glv",
            "market_token": glv_token,
            "apy": data.get("apy", 0.0),
            "base_apy": data.get("baseApy", 0.0),
            "bonus_apr": data.get("bonusApr", 0.0),
        })
    return pd.DataFrame(rows)


def collect_and_save_apy(
    api: GMXAPI,
    date_str: str,
    apy_dir: Path,
) -> int:
    """Fetch APY data for all periods and save as daily parquet snapshot.

    Fetches APY for each period in :data:`APY_PERIODS` and combines into
    a single parquet file for the day.

    :param api: Initialised GMXAPI client.
    :param date_str: ISO date string for the snapshot.
    :param apy_dir: Directory for APY parquet files.
    :returns: Total number of APY entries saved.
    """
    apy_dir.mkdir(parents=True, exist_ok=True)

    all_frames = []
    for period in APY_PERIODS:
        try:
            raw_apy = api.get_apy(period=period, use_cache=False)
            df = _flatten_apy(raw_apy, period, date_str)
            if not df.empty:
                all_frames.append(df)
        except Exception as e:
            console.print(f"    [yellow]Warning: APY {period} — {e}[/yellow]")
        time.sleep(0.1)

    if not all_frames:
        console.print("  [yellow]Warning: No APY data collected[/yellow]")
        return 0

    combined = pd.concat(all_frames, ignore_index=True)
    apy_path = apy_dir / f"{date_str}.parquet"
    combined.to_parquet(apy_path, index=False)
    console.print(f"  [green]Saved {len(combined)} APY entries ({len(APY_PERIODS)} periods)[/green] → {apy_path}")
    return len(combined)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC && poetry run python -m pytest tests/test_daily_snapshot.py::TestCollectApy -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/collect_daily_snapshot.py tests/test_daily_snapshot.py
git commit -m "feat: add APY data collection (7 periods, daily parquet)"
```

---

### Task 5: Update `main()` to orchestrate all phases and update report

Wire up the new collection functions into `main()` and enhance the report.

**Files:**
- Modify: `scripts/collect_daily_snapshot.py:326-421` (main function)
- Modify: `scripts/collect_daily_snapshot.py:226-323` (generate_report function)

- [ ] **Step 1: Update `main()` to add new phases**

Replace the `main()` function body (from directory setup through summary) with:

```python
def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Collect daily GMX V2 market snapshot (all data types, all timeframes)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Today's snapshot (all data)
  poetry run python scripts/collect_daily_snapshot.py

  # Specific date
  poetry run python scripts/collect_daily_snapshot.py --date 2026-03-10

  # Custom output root
  poetry run python scripts/collect_daily_snapshot.py --output-dir ./my_data
        """,
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./user_data"),
        help="Root output directory (default: ./user_data)",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Snapshot date in YYYY-MM-DD format (default: today UTC)",
    )
    parser.add_argument(
        "--network",
        choices=["arbitrum", "avalanche"],
        default="arbitrum",
        help="GMX network (default: arbitrum)",
    )

    args = parser.parse_args()
    date_str = args.date or datetime.now(UTC).strftime("%Y-%m-%d")

    futures_dir = args.output_dir / "data" / "gmx" / "futures"
    snapshots_dir = args.output_dir / "data" / "gmx" / "snapshots"
    tickers_dir = args.output_dir / "data" / "gmx" / "tickers"
    apy_dir = args.output_dir / "data" / "gmx" / "apy"
    report_path = args.output_dir.parent / "data_report.txt"

    console.print(f"\n[bold]GMX Daily Snapshot — {date_str}[/bold]")
    console.print(f"  Network:    {args.network}")
    console.print(f"  Futures:    {futures_dir}")
    console.print(f"  Snapshots:  {snapshots_dir}")
    console.print(f"  Tickers:    {tickers_dir}")
    console.print(f"  APY:        {apy_dir}\n")

    api = GMXAPI(chain=args.network)

    # --- Fetch all markets once ---
    console.print("[bold]Fetching markets from GMX API...[/bold]")
    all_markets = _fetch_all_markets(api)
    if not all_markets:
        console.print("[red]Error: No market data returned from GMX API[/red]")
        sys.exit(1)

    # --- Phase 1: Markets snapshot (ALL markets: perp + swap-only + unlisted) ---
    console.print("\n[bold]Phase 1: Markets snapshot (OI, liquidity, rates)[/bold]")
    markets_df = collect_markets_snapshot(all_markets, date_str)
    markets_path = snapshots_dir / f"{date_str}.parquet"
    markets_path.parent.mkdir(parents=True, exist_ok=True)
    markets_df.to_parquet(markets_path, index=False)
    console.print(f"  Saved → {markets_path}\n")

    # --- Phase 2: OHLCV candles for ALL timeframes ---
    console.print("[bold]Phase 2: OHLCV candles (all timeframes)[/bold]")
    candle_count, failed_symbols = collect_and_save_ohlcv(
        api, all_markets, futures_dir
    )
    console.print()

    # --- Phase 3: Tickers (bid/ask/volume) ---
    console.print("[bold]Phase 3: Tickers (bid/ask prices)[/bold]")
    ticker_count = collect_and_save_tickers(api, date_str, tickers_dir)
    console.print()

    # --- Phase 4: APY (all periods) ---
    console.print("[bold]Phase 4: APY (yield data)[/bold]")
    apy_count = collect_and_save_apy(api, date_str, apy_dir)
    console.print()

    # --- Phase 5: Generate report ---
    console.print("[bold]Phase 5: Data report[/bold]")
    generate_report(
        date_str=date_str,
        markets_df=markets_df,
        candle_count=candle_count,
        failed_symbols=failed_symbols,
        ticker_count=ticker_count,
        apy_count=apy_count,
        futures_dir=futures_dir,
        snapshots_dir=snapshots_dir,
        tickers_dir=tickers_dir,
        apy_dir=apy_dir,
        report_path=report_path,
    )
    console.print()

    # --- Summary ---
    console.print("[bold]Summary[/bold]")
    console.print(f"  Date:      {date_str}")
    console.print(f"  Markets:   {len(markets_df)} (all)")
    console.print(f"  Candles:   {candle_count} files ({len(TIMEFRAMES)} timeframes)")
    console.print(f"  Tickers:   {ticker_count}")
    console.print(f"  APY:       {apy_count} entries")
    console.print("[green]Done.[/green]")
```

- [ ] **Step 2: Update `generate_report()` to include all data types**

Replace `generate_report` signature and body:

```python
def generate_report(
    date_str: str,
    markets_df: pd.DataFrame,
    candle_count: int,
    failed_symbols: list[str],
    ticker_count: int,
    apy_count: int,
    futures_dir: Path,
    snapshots_dir: Path,
    tickers_dir: Path,
    apy_dir: Path,
    report_path: Path,
) -> None:
    """Write a human-readable data report after each collection run.

    :param date_str: Snapshot date.
    :param markets_df: Full markets snapshot DataFrame.
    :param candle_count: Number of OHLCV feather files written.
    :param failed_symbols: Symbols/timeframes that failed candle fetch.
    :param ticker_count: Number of ticker entries saved.
    :param apy_count: Number of APY entries saved.
    :param futures_dir: Path to CCXT feather directory.
    :param snapshots_dir: Path to snapshots directory.
    :param tickers_dir: Path to tickers directory.
    :param apy_dir: Path to APY directory.
    :param report_path: Output path for the report file.
    """
    now_utc = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")

    perp_df = markets_df[~markets_df["is_swap_only"]]
    swap_df = markets_df[markets_df["is_swap_only"]]
    listed_df = markets_df[markets_df["is_listed"]]

    # Count files per data type
    snapshot_files = list(snapshots_dir.glob("*.parquet"))
    ticker_files = list(tickers_dir.glob("*.parquet")) if tickers_dir.exists() else []
    apy_files = list(apy_dir.glob("*.parquet")) if apy_dir.exists() else []

    # Count feather files per timeframe
    tf_counts = {}
    for tf in TIMEFRAMES:
        tf_files = list(futures_dir.glob(f"*-{tf}-futures.feather"))
        tf_counts[tf] = len(tf_files)

    # Compute total OI
    total_oi = 0
    for _, row in perp_df.iterrows():
        try:
            oi_long = int(row["open_interest_long"])
            oi_short = int(row["open_interest_short"])
            total_oi += oi_long + oi_short
        except (ValueError, TypeError):
            pass
    total_oi_usd = total_oi / _GMX_PRECISION

    # Top 10 markets by OI
    oi_rows = []
    for _, row in perp_df.iterrows():
        try:
            oi = (int(row["open_interest_long"]) + int(row["open_interest_short"])) / _GMX_PRECISION
            oi_rows.append((row["name"], oi))
        except (ValueError, TypeError):
            pass
    oi_rows.sort(key=lambda x: x[1], reverse=True)

    lines = [
        f"# GMX Data Report — {date_str}",
        f"Generated: {now_utc}",
        "",
        "## Collection Summary",
        f"- Snapshot date: {date_str}",
        f"- Total markets from API: {len(markets_df)}",
        f"  - Perpetual markets: {len(perp_df)}",
        f"  - Swap-only pools: {len(swap_df)}",
        f"  - Listed: {len(listed_df)}, Unlisted: {len(markets_df) - len(listed_df)}",
        f"- OHLCV candle files saved: {candle_count}"
        + (f", {len(failed_symbols)} failed" if failed_symbols else ""),
        f"- Ticker entries: {ticker_count}",
        f"- APY entries: {apy_count}",
        f"- Total OI (all markets): ${total_oi_usd:,.0f}",
        "",
        "## Data Files",
        f"- Snapshot parquet files: {len(snapshot_files)} days",
        f"- Ticker parquet files: {len(ticker_files)} days",
        f"- APY parquet files: {len(apy_files)} days",
        "- OHLCV feather files by timeframe:",
    ]
    for tf in TIMEFRAMES:
        lines.append(f"    {tf}: {tf_counts[tf]} symbols")

    lines.extend([
        "",
        "## Top 10 Markets by Open Interest",
    ])
    for i, (name, oi) in enumerate(oi_rows[:10], 1):
        lines.append(f"  {i:2d}. {name:<40s} ${oi:>14,.0f}")

    if failed_symbols:
        lines.append("")
        lines.append("## Failed OHLCV Fetches")
        for f in failed_symbols[:20]:
            lines.append(f"  {f}")
        if len(failed_symbols) > 20:
            lines.append(f"  ... and {len(failed_symbols) - 20} more")

    # Per-symbol candle counts for ALL timeframes
    for tf in TIMEFRAMES:
        lines.append("")
        tf_label = {"1m": "minutes", "5m": "5-min bars", "15m": "15-min bars",
                     "1h": "hours", "4h": "4-hour bars", "1d": "days"}
        lines.append(f"## OHLCV Coverage — {tf} (rows per symbol)")
        tf_feather_files = sorted(futures_dir.glob(f"*-{tf}-futures.feather"))
        for f in tf_feather_files:
            try:
                df = pd.read_feather(f)
                sym = f.stem.replace(f"_USDC_USDC-{tf}-futures", "")
                lines.append(f"  {sym}: {len(df)} {tf_label.get(tf, 'rows')}")
            except Exception:
                pass

    lines.append("")

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines), encoding="utf-8")
    console.print(f"  Report → {report_path}")
```

- [ ] **Step 3: Update module docstring**

Replace lines 1-32 with updated docstring:

```python
"""Daily GMX V2 comprehensive data collector.

Fetches a point-in-time snapshot of **all** GMX V2 markets (perpetual +
swap-only) via the public REST API. No HyperSync, no RPC, no API keys.

Captures:

- OHLCV candles across all timeframes (1m, 5m, 15m, 1h, 4h, 1d) → CCXT feather format
- Open Interest (long/short per market, including alt-collateral variants)
- Pool Liquidity (pool amounts, available liquidity)
- Funding & Borrowing rates
- Swap-only pool data
- Ticker data (bid/ask prices, volume)
- APY data (yield across 7 periods: 1d, 7d, 30d, 90d, 180d, 1y, total)

Output follows the existing ``user_data/`` layout::

    user_data/data/gmx/
    ├── futures/{SYM}_USDC_USDC-{tf}-futures.feather  # OHLCV (appended)
    ├── snapshots/{date}.parquet                       # All markets snapshot
    ├── tickers/{date}.parquet                         # Bid/ask/volume
    └── apy/{date}.parquet                             # Yield data

A ``data_report.txt`` file is generated in the output root after each run.

Usage::

    # Today's snapshot
    poetry run python scripts/collect_daily_snapshot.py

    # Specific date
    poetry run python scripts/collect_daily_snapshot.py --date 2026-03-10

    # Custom output root
    poetry run python scripts/collect_daily_snapshot.py --output-dir ./my_data
"""
```

- [ ] **Step 4: Run all tests + lint**

Run: `export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC && poetry run python -m pytest tests/test_daily_snapshot.py -v && poetry run ruff check scripts/collect_daily_snapshot.py`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/collect_daily_snapshot.py tests/test_daily_snapshot.py
git commit -m "feat: wire up all collection phases (markets, OHLCV, tickers, APY) + updated report"
```

---

### Task 6: Update GitHub Actions workflow

Add new data directories to git add and increase timeout for the larger collection.

**Files:**
- Modify: `.github/workflows/collect-gmx-data.yml`

- [ ] **Step 1: Update workflow**

Changes to `.github/workflows/collect-gmx-data.yml`:

1. Increase `timeout-minutes` from `30` to `45` (more API calls now).

2. Update the `git add` line (line 86) to include new directories:

```yaml
          git add --force \
            user_data/data/gmx/futures/ \
            user_data/data/gmx/snapshots/ \
            user_data/data/gmx/tickers/ \
            user_data/data/gmx/apy/ \
            data_report.txt
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/collect-gmx-data.yml
git commit -m "ci: add tickers and apy directories to daily data commit"
```

---

### Task 7: Run full test suite and verify

- [ ] **Step 1: Run all tests**

Run: `export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC && poetry run python -m pytest tests/test_daily_snapshot.py tests/test_cli_refactor.py -v`
Expected: All tests PASS

- [ ] **Step 2: Lint entire scripts directory**

Run: `poetry run ruff check scripts/ && poetry run ruff format --check scripts/`
Expected: PASS

- [ ] **Step 3: Dry-run the script locally (optional manual verification)**

Run: `poetry run python scripts/collect_daily_snapshot.py --output-dir ./test_output`
Expected: Creates files under `test_output/data/gmx/{futures,snapshots,tickers,apy}/`

Verify:
- `ls test_output/data/gmx/futures/ | head -20` — should show `*-1m-futures.feather`, `*-5m-futures.feather`, etc.
- `ls test_output/data/gmx/tickers/` — should show `{date}.parquet`
- `ls test_output/data/gmx/apy/` — should show `{date}.parquet`
- `cat test_output/../data_report.txt` — should show all timeframe counts

Clean up: `rm -rf test_output/`
