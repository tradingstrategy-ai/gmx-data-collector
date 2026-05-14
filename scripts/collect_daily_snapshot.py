"""Daily GMX V2 comprehensive data collector.

Fetches a point-in-time snapshot of **all** GMX V2 markets (perpetual +
swap-only) via the public REST API. No HyperSync, no RPC, no API keys.

Captures:

- OHLCV candles across all timeframes (1m, 5m, 15m, 1h, 4h, 1d) → CCXT feather format
- Open Interest (long/short per market, including alt-collateral variants)
- Pool Liquidity (pool amounts, available liquidity)
- Funding & Borrowing rates
- Swap-only pool data
- Ticker data (bid/ask prices)
- APY data (yield across 7 periods: 1d, 7d, 30d, 90d, 180d, 1y, total)
- 24h trading volume per market (from Subsquid GraphQL)

Output follows the existing ``user_data/`` layout::

    user_data/data/gmx/
    ├── futures/{SYM}_USDC_USDC-{tf}-futures.feather  # OHLCV (appended)
    ├── snapshots/{date}.parquet                       # All markets snapshot
    ├── tickers/{date}.parquet                         # Bid/ask prices
    ├── apy/{date}.parquet                             # Yield data
    └── volumes/{date}.parquet                         # 24h volume per market

A ``data_report.txt`` file is generated in the output root after each run.

Usage::

    # Today's snapshot
    poetry run python scripts/collect_daily_snapshot.py

    # Specific date
    poetry run python scripts/collect_daily_snapshot.py --date 2026-03-10

    # Custom output root
    poetry run python scripts/collect_daily_snapshot.py --output-dir ./my_data
"""

import argparse
import sys
import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pyarrow.feather as feather
import pyarrow.parquet as pq
from eth_defi.gmx.api import GMXAPI
from rich.console import Console

from gmx_historical_data.coverage_gate import (
    SkipDecision,
    has_ohlcv_through,
    is_current,
)
from gmx_historical_data.quickstart import (
    DEFAULT_RELEASE_TAG,
    print_coverage_summary,
    seed_from_release,
)

console = Console()

# GMX 30-decimal precision for OI/rate values.
_GMX_PRECISION = 1e30

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


def _expected_last_bar(
    tf: str,
    target_date: str,
    *,
    now: pd.Timestamp | None = None,
) -> pd.Timestamp:
    """Latest fully-closed bar we expect on disk for ``target_date``.

    For *today's* date, returns the most recent closed bar of the timeframe
    (e.g. ``1h`` at 17:42 UTC → ``today 17:00``).  For *past* dates,
    returns the last bar of that calendar day (e.g. ``1h`` and
    ``--date 2026-03-10`` → ``2026-03-10 23:00``).

    :param tf: Timeframe (``1m``, ``5m``, ``15m``, ``1h``, ``4h``, ``1d``).
    :param target_date: ISO date string (``YYYY-MM-DD``).
    :param now: Override for the current UTC wall-clock; defaults to
        ``pd.Timestamp.now(tz='UTC')``.
    """
    target = pd.Timestamp(target_date, tz="UTC").normalize()
    now = now if now is not None else pd.Timestamp.now(tz="UTC")
    if target.date() < now.date():
        anchor = target + pd.Timedelta(hours=23, minutes=59)
    else:
        anchor = now
    floor_freq = {
        "1m": "1min",
        "5m": "5min",
        "15m": "15min",
        "1h": "1h",
        "4h": "4h",
        "1d": "1D",
    }[tf]
    return anchor.floor(floor_freq)


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


def _fetch_candles_with_retry(
    api: GMXAPI,
    symbol: str,
    tf: str,
    limit: int,
    *,
    max_retries: int = 5,
    initial_backoff: float = 2.0,
    max_backoff: float = 60.0,
) -> pd.DataFrame:
    """Fetch one candle slice with retry and exponential backoff.

    Retries both transport/API exceptions and empty responses. Empty payloads
    are treated as transient here because a listed market should not normally
    return no candles for a populated timeframe.
    """
    backoff = initial_backoff
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            df = api.get_candlesticks_dataframe(symbol, period=tf, limit=limit)
            if df.empty:
                raise RuntimeError("empty candle response")
            return df
        except Exception as e:
            last_error = e
            if attempt >= max_retries:
                break

            wait_time = min(backoff, max_backoff)
            console.print(
                f"    [yellow]Retry {attempt}/{max_retries} for {symbol}/{tf} "
                f"in {wait_time:.1f}s after error: {e}[/yellow]"
            )
            time.sleep(wait_time)
            backoff *= 2

    raise RuntimeError(
        f"Failed to fetch candles for {symbol}/{tf} after {max_retries} attempts"
    ) from last_error


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
        rows.append(
            {
                "date": date_str,
                "period": period,
                "type": "market",
                "market_token": market_token,
                "apy": data.get("apy", 0.0),
                "base_apy": data.get("baseApy", 0.0),
                "bonus_apr": data.get("bonusApr", 0.0),
            }
        )
    for glv_token, data in raw_apy.get("glvs", {}).items():
        rows.append(
            {
                "date": date_str,
                "period": period,
                "type": "glv",
                "market_token": glv_token,
                "apy": data.get("apy", 0.0),
                "base_apy": data.get("baseApy", 0.0),
                "bonus_apr": data.get("bonusApr", 0.0),
            }
        )
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
    console.print(
        f"  [green]Saved {len(combined)} APY entries ({len(APY_PERIODS)} periods)[/green] → {apy_path}"
    )
    return len(combined)


def _flatten_tickers(raw_tickers: list[dict], date_str: str) -> pd.DataFrame:
    """Flatten raw ticker API response into a tabular DataFrame.

    :param raw_tickers: List of ticker dicts from ``get_tickers()``.
    :param date_str: ISO date string to tag each row.
    :returns: DataFrame with one row per token.
    """
    rows = []
    for ticker in raw_tickers:
        rows.append(
            {
                "date": date_str,
                "token_symbol": ticker.get("tokenSymbol", ""),
                "token_address": ticker.get("tokenAddress", ""),
                "min_price": ticker.get("minPrice", "0"),
                "max_price": ticker.get("maxPrice", "0"),
                "updated_at": ticker.get("updatedAt", 0),
                "timestamp": ticker.get("timestamp", 0),
            }
        )
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


def collect_and_save_volumes(
    date_str: str,
    volumes_dir: Path,
    chain: str = "arbitrum",
) -> tuple[int, dict[str, "Decimal"]]:
    """Fetch per-market 24h trading volume from Subsquid and save as daily parquet.

    :param date_str: ISO date string (e.g. ``"2026-04-01"``).
    :param volumes_dir: Output directory for volume parquet files.
    :param chain: GMX chain name.
    :returns: Tuple of (number of markets saved, raw volumes dict).
    """
    from gmx_historical_data.subsquid_volume import fetch_daily_volumes

    volumes_dir.mkdir(parents=True, exist_ok=True)

    try:
        volumes = fetch_daily_volumes(chain=chain)
    except Exception as e:
        console.print(f"  [yellow]Warning: Volume fetch failed — {e}[/yellow]")
        return 0, {}

    if not volumes:
        console.print("  [yellow]Warning: No volume data returned[/yellow]")
        return 0, {}

    rows = [
        {"date": date_str, "market_address": addr, "volume_usd": str(vol)}
        for addr, vol in sorted(volumes.items())
    ]
    df = pd.DataFrame(rows)
    path = volumes_dir / f"{date_str}.parquet"
    df.to_parquet(path, index=False)

    total = sum(volumes.values())
    nonzero = sum(1 for v in volumes.values() if v > 0)
    console.print(f"  [green]Saved {len(df)} market volumes ({nonzero} active)[/green] → {path}")
    console.print(f"  Total 24h volume: ${total:,.0f}")
    return len(df), volumes


def _fetch_all_markets(api: GMXAPI) -> list[dict]:
    """Fetch the full market list from the GMX API.

    :param api: Initialised GMXAPI client.
    :returns: List of market dicts (all markets, no filtering).
    """
    data = api.get_markets_info()
    return data.get("markets", [])


def collect_markets_snapshot(
    markets: list[dict],
    date_str: str,
) -> pd.DataFrame:
    """Flatten all markets into a tabular snapshot DataFrame.

    Includes every market returned by the API: perpetual markets,
    alt-collateral variants, and swap-only pools. Deprecated (unlisted)
    markets are tagged but still included.

    :param markets: Raw market dicts from ``get_markets_info()``.
    :param date_str: ISO date string to tag the snapshot.
    :returns: DataFrame with one row per market.
    """
    rows = []
    for market in markets:
        name = market.get("name", "")
        is_listed = market.get("isListed", True)
        is_swap_only = "/" not in name
        symbol = "" if is_swap_only else name.split("/")[0].strip()

        rows.append(
            {
                "date": date_str,
                "name": name,
                "symbol": symbol,
                "market_token": market.get("marketToken", ""),
                "index_token": market.get("indexToken", ""),
                "long_token": market.get("longToken", ""),
                "short_token": market.get("shortToken", ""),
                "listing_date": market.get("listingDate", ""),
                "is_listed": is_listed,
                "is_swap_only": is_swap_only,
                "open_interest_long": market.get("openInterestLong", "0"),
                "open_interest_short": market.get("openInterestShort", "0"),
                "pool_amount_long": market.get("poolAmountLong", "0"),
                "pool_amount_short": market.get("poolAmountShort", "0"),
                "available_liquidity_long": market.get("availableLiquidityLong", "0"),
                "available_liquidity_short": market.get("availableLiquidityShort", "0"),
                "funding_rate_long": market.get("fundingRateLong", "0"),
                "funding_rate_short": market.get("fundingRateShort", "0"),
                "borrowing_rate_long": market.get("borrowingRateLong", "0"),
                "borrowing_rate_short": market.get("borrowingRateShort", "0"),
                "net_rate_long": market.get("netRateLong", "0"),
                "net_rate_short": market.get("netRateShort", "0"),
            }
        )

    df = pd.DataFrame(rows)
    perp = df[~df["is_swap_only"]]
    swap = df[df["is_swap_only"]]
    listed = df[df["is_listed"]]
    console.print(
        f"  [green]Fetched {len(df)} markets[/green] "
        f"({len(perp)} perpetual, {len(swap)} swap-only, "
        f"{len(listed)} listed)"
    )
    return df


def _date_stats(dates: pd.Series) -> dict | None:
    """Compute ``{rows, min_date, max_date}`` from a Series of timestamps.

    :param dates: Series of timestamp-like values (naive or tz-aware).
    :returns: Dict with ``rows`` (int), ``min_date`` / ``max_date``
        (``pd.Timestamp`` UTC), or ``None`` if input is empty.
    """
    if dates is None or len(dates) == 0:
        return None
    if not pd.api.types.is_datetime64_any_dtype(dates):
        dates = pd.to_datetime(dates, utc=True)
    elif dates.dt.tz is None:
        dates = dates.dt.tz_localize("UTC")
    return {
        "rows": int(len(dates)),
        "min_date": dates.min(),
        "max_date": dates.max(),
    }


def _feather_date_stats(filepath: Path) -> dict | None:
    """Read ``date`` column from a feather file and summarise it.

    :param filepath: Feather file to inspect.
    :returns: ``_date_stats`` dict or ``None`` if file missing/empty/unreadable.
    """
    if not filepath.exists():
        return None
    try:
        df = pd.read_feather(filepath, columns=["date"])
    except Exception:
        return None
    if df.empty:
        return None
    return _date_stats(df["date"])


def _row_count(path: Path) -> int:
    """Return parquet row count via metadata; ``0`` if missing/corrupt.

    Used by the gate-skip code path to report "existing N rows" without
    reopening the file twice.

    :param path: Path to the parquet file.
    :returns: Row count, or ``0`` if the file is missing or unreadable.
    """
    if not path.exists():
        return 0
    try:
        return pq.read_metadata(str(path)).num_rows
    except Exception:
        return 0


def collect_and_save_ohlcv(
    api: GMXAPI,
    markets: list[dict],
    futures_dir: Path,
    timeframes: list[str] | None = None,
    *,
    force_refresh: bool = False,
    target_date: str | None = None,
) -> tuple[int, list[str], dict[tuple[str, str], dict]]:
    """Fetch OHLCV candles for all timeframes and append to feather files.

    For each unique listed perpetual symbol, fetches candles across all
    requested timeframes and merges into per-symbol feather files using
    the Freqtrade naming convention.

    On first run (file doesn't exist): fetches max history (``limit=10000``).
    On subsequent runs (file exists): fetches recent candles proportional
    to the timeframe resolution.

    :param api: Initialised GMXAPI client.
    :param markets: Raw market dicts from ``get_markets_info()``.
    :param futures_dir: Directory for CCXT feather files.
    :param timeframes: List of timeframes to collect (default: all from TIMEFRAMES).
    :returns: Tuple of ``(total files saved, failed symbol/timeframe pairs,
        coverage_map)``. ``coverage_map`` is keyed by ``(symbol, tf)`` and
        each value is a dict with ``pre_merge``, ``api_slice``, ``post_merge``
        sub-dicts (each ``None`` or ``{rows, min_date, max_date}``) plus a
        ``status`` string from ``{"OK", "NEW", "REGRESSION", "FLAT", "FAILED"}``.
    """
    tfs = timeframes or TIMEFRAMES
    symbols = _extract_symbols(markets)
    console.print(f"  Fetching candles for {len(symbols)} symbols × {len(tfs)} timeframes...")

    futures_dir.mkdir(parents=True, exist_ok=True)
    saved = 0
    failed = []
    coverage: dict[tuple[str, str], dict] = {}

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
            filepath = futures_dir / f"{symbol}_USDC_USDC-{tf}-futures.feather"
            pre_stats = _feather_date_stats(filepath)

            # Coverage gate — skip fetch entirely if on-disk feather already
            # covers today's last expected bar. ``has_ohlcv_through`` is
            # imported at the top of this module alongside ``is_current``.
            if target_date is not None:
                expected_last = _expected_last_bar(tf, target_date=target_date)
                gate = has_ohlcv_through(filepath, expected_last, force=force_refresh)
                if gate.skip:
                    coverage[(symbol, tf)] = {
                        "pre_merge": pre_stats,
                        "api_slice": None,
                        "post_merge": pre_stats,  # no write happened
                        "status": "SKIPPED",
                    }
                    continue

            entry: dict = {
                "pre_merge": pre_stats,
                "api_slice": None,
                "post_merge": None,
                "status": "FAILED",
            }
            coverage[(symbol, tf)] = entry

            try:
                limit = incremental_limits.get(tf, 100) if filepath.exists() else 10000
                df = _fetch_candles_with_retry(api, symbol, tf, limit)

                api_stats = _date_stats(df["timestamp"])
                entry["api_slice"] = api_stats

                new_rows = pd.DataFrame(
                    {
                        "date": df["timestamp"],
                        "open": df["open"].astype(float),
                        "high": df["high"].astype(float),
                        "low": df["low"].astype(float),
                        "close": df["close"].astype(float),
                        "volume": 0.0,
                    }
                )

                _merge_feather(new_rows, filepath)
                post_stats = _feather_date_stats(filepath)
                entry["post_merge"] = post_stats

                if pre_stats is None:
                    entry["status"] = "NEW"
                elif post_stats is not None and post_stats["min_date"] > pre_stats["min_date"]:
                    # Historical earliest moved forward — should never happen.
                    entry["status"] = "REGRESSION"
                elif (
                    post_stats is not None
                    and api_stats is not None
                    and post_stats["min_date"] >= api_stats["min_date"]
                ):
                    # No historical depth beyond what API already returned.
                    entry["status"] = "FLAT"
                else:
                    entry["status"] = "OK"

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
    return saved, failed, coverage


def _summarise_daily_files(dir_path: Path) -> dict:
    """Inspect a directory of ``YYYY-MM-DD.parquet`` daily files.

    :param dir_path: Directory holding daily-stamped parquet files.
    :returns: Dict with ``count`` (int), ``first`` / ``last`` (ISO date strings
        or ``None``) and ``missing`` (sorted list of ISO date strings missing
        between first and last). ``count`` is 0 when the directory is empty
        or missing.
    """
    if not dir_path.exists():
        return {"count": 0, "first": None, "last": None, "missing": []}

    dates: list[datetime] = []
    for f in dir_path.glob("*.parquet"):
        try:
            dates.append(datetime.strptime(f.stem, "%Y-%m-%d"))
        except ValueError:
            continue

    if not dates:
        return {"count": 0, "first": None, "last": None, "missing": []}

    dates.sort()
    first, last = dates[0], dates[-1]
    expected = pd.date_range(first, last, freq="D").to_pydatetime().tolist()
    present = {d.date() for d in dates}
    missing = [d.date().isoformat() for d in expected if d.date() not in present]
    return {
        "count": len(dates),
        "first": first.date().isoformat(),
        "last": last.date().isoformat(),
        "missing": missing,
    }


def generate_report(
    date_str: str,
    markets_df: pd.DataFrame,
    candle_count: int,
    failed_symbols: list[str],
    ticker_count: int,
    apy_count: int,
    volume_count: int,
    volume_data: dict[str, Decimal],
    futures_dir: Path,
    snapshots_dir: Path,
    tickers_dir: Path,
    apy_dir: Path,
    volumes_dir: Path,
    report_path: Path,
    ohlcv_coverage: dict[tuple[str, str], dict] | None = None,
) -> None:
    """Write a human-readable data report after each collection run.

    :param date_str: Snapshot date.
    :param markets_df: Full markets snapshot DataFrame.
    :param candle_count: Number of OHLCV feather files written.
    :param failed_symbols: Symbols/timeframes that failed candle fetch.
    :param ticker_count: Number of ticker entries saved.
    :param apy_count: Number of APY entries saved.
    :param volume_count: Number of market volumes saved.
    :param volume_data: Dict mapping market address to 24h volume in USD.
    :param futures_dir: Path to CCXT feather directory.
    :param snapshots_dir: Path to snapshots directory.
    :param tickers_dir: Path to tickers directory.
    :param apy_dir: Path to APY directory.
    :param volumes_dir: Path to volumes directory.
    :param report_path: Output path for the report file.
    :param ohlcv_coverage: Per-``(symbol, tf)`` coverage map from
        :func:`collect_and_save_ohlcv`. When provided, the report includes a
        per-symbol API-vs-Combined date range table and a leading
        ``Suspected Seed Regressions`` block listing entries where historical
        depth is suspicious.
    """
    now_utc = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")

    perp_df = markets_df[~markets_df["is_swap_only"]]
    swap_df = markets_df[markets_df["is_swap_only"]]
    listed_df = markets_df[markets_df["is_listed"]]

    # Count files per data type
    snapshot_files = list(snapshots_dir.glob("*.parquet"))
    ticker_files = list(tickers_dir.glob("*.parquet")) if tickers_dir.exists() else []
    apy_files = list(apy_dir.glob("*.parquet")) if apy_dir.exists() else []
    volume_files = list(volumes_dir.glob("*.parquet")) if volumes_dir.exists() else []

    # Count feather files per timeframe
    tf_counts = {}
    for tf in TIMEFRAMES:
        tf_files = list(futures_dir.glob(f"*-{tf}-futures.feather"))
        tf_counts[tf] = len(tf_files)

    # Compute total OI across all perpetual markets
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
        f"- Volume entries: {volume_count} markets",
        f"- Total OI (all markets): ${total_oi_usd:,.0f}",
        f"- Total 24h Volume: ${sum(volume_data.values()):,.0f}"
        if volume_data
        else "- Total 24h Volume: N/A",
        "",
        "## Date Range Summary",
    ]

    # Daily-stamped data types — inspected by filename (no parquet reads).
    daily_sources = (
        ("Markets snapshots", snapshots_dir),
        ("Tickers", tickers_dir),
        ("APY", apy_dir),
        ("Volumes", volumes_dir),
    )
    daily_summaries = {label: _summarise_daily_files(d) for label, d in daily_sources}
    for label, info in daily_summaries.items():
        if info["count"] == 0:
            lines.append(f"- {label}: No data available")
            continue
        gap_note = f", {len(info['missing'])} missing day(s)" if info["missing"] else ""
        lines.append(
            f"- {label}: {info['first']} to {info['last']} ({info['count']} days{gap_note})"
        )

    # OHLCV per-timeframe aggregate range — prefer the in-memory coverage map
    # over re-reading every feather, but fall back to feather reads when the
    # report is regenerated standalone (no coverage map passed in).
    for tf in TIMEFRAMES:
        agg_min: pd.Timestamp | None = None
        agg_max: pd.Timestamp | None = None
        sym_count = 0

        if ohlcv_coverage:
            for (_, ctf), entry in ohlcv_coverage.items():
                if ctf != tf:
                    continue
                post = entry.get("post_merge")
                if post is None:
                    continue
                sym_count += 1
                agg_min = post["min_date"] if agg_min is None else min(agg_min, post["min_date"])
                agg_max = post["max_date"] if agg_max is None else max(agg_max, post["max_date"])
        else:
            for f in futures_dir.glob(f"*-{tf}-futures.feather"):
                stats = _feather_date_stats(f)
                if stats is None:
                    continue
                sym_count += 1
                agg_min = stats["min_date"] if agg_min is None else min(agg_min, stats["min_date"])
                agg_max = stats["max_date"] if agg_max is None else max(agg_max, stats["max_date"])

        if sym_count == 0:
            lines.append(f"- OHLCV {tf}: No data available")
            continue
        lines.append(
            f"- OHLCV {tf}: {agg_min.strftime('%Y-%m-%d')} to "
            f"{agg_max.strftime('%Y-%m-%d')} ({sym_count} symbols)"
        )

    # Missing-day detail for daily-stamped sources (only when gaps exist).
    gap_lines = []
    for label, info in daily_summaries.items():
        if info["missing"]:
            preview = ", ".join(info["missing"][:10])
            tail = "" if len(info["missing"]) <= 10 else f" (+{len(info['missing']) - 10} more)"
            gap_lines.append(f"- {label}: {preview}{tail}")
    if gap_lines:
        lines.append("")
        lines.append("## Missing Days (daily-stamped sources)")
        lines.extend(gap_lines)

    lines.extend([
        "",
        "## Data Files",
        f"- Snapshot parquet files: {len(snapshot_files)} days",
        f"- Ticker parquet files: {len(ticker_files)} days",
        f"- APY parquet files: {len(apy_files)} days",
        f"- Volume parquet files: {len(volume_files)} days",
        "- OHLCV feather files by timeframe:",
    ])
    for tf in TIMEFRAMES:
        lines.append(f"    {tf}: {tf_counts[tf]} symbols")

    # ------------------------------------------------------------------
    # Suspected seed regressions — surfaces the cases where the historical
    # archive failed to extend coverage beyond what the GMX API serves on
    # its own. Empty when every feather either grew or was newly seeded.
    # ------------------------------------------------------------------
    if ohlcv_coverage:
        regressions = [
            (sym, tf, entry)
            for (sym, tf), entry in sorted(ohlcv_coverage.items())
            if entry["status"] in ("REGRESSION", "FLAT")
        ]
        if regressions:
            lines.append("")
            lines.append("## Suspected Seed Regressions")
            lines.append(
                "  Entries where post-merge history matches the API window "
                "(no historical depth) or moved forward (lost rows)."
            )
            for sym, tf, entry in regressions:
                pre = entry["pre_merge"]
                api = entry["api_slice"]
                post = entry["post_merge"]
                pre_str = (
                    f"{pre['min_date'].strftime('%Y-%m-%d')} ({pre['rows']} rows)"
                    if pre
                    else "n/a"
                )
                api_str = (
                    f"{api['min_date'].strftime('%Y-%m-%d')} ({api['rows']} rows)"
                    if api
                    else "n/a"
                )
                post_str = (
                    f"{post['min_date'].strftime('%Y-%m-%d')} ({post['rows']} rows)"
                    if post
                    else "n/a"
                )
                lines.append(
                    f"  [{entry['status']:11s}] {sym}/{tf}: "
                    f"pre={pre_str}  api={api_str}  post={post_str}"
                )

    lines.extend(
        [
            "",
            "## Top 10 Markets by Open Interest",
        ]
    )
    for i, (name, oi) in enumerate(oi_rows[:10], 1):
        lines.append(f"  {i:2d}. {name:<40s} ${oi:>14,.0f}")

    # Top 10 markets by 24h volume
    if volume_data:
        # Build address → name lookup from markets DataFrame
        addr_to_name: dict[str, str] = {}
        for _, row in markets_df.iterrows():
            addr = row.get("market_token", "")
            name = row.get("name", addr[:10] + "...")
            if addr:
                addr_to_name[addr.lower()] = name

        vol_rows = []
        for addr, vol in volume_data.items():
            name = addr_to_name.get(addr.lower(), addr[:10] + "...")
            vol_rows.append((name, float(vol)))
        vol_rows.sort(key=lambda x: x[1], reverse=True)

        lines.extend(["", "## Top 10 Markets by 24h Volume"])
        for i, (name, vol) in enumerate(vol_rows[:10], 1):
            lines.append(f"  {i:2d}. {name:<40s} ${vol:>14,.0f}")

    if failed_symbols:
        lines.append("")
        lines.append("## Failed OHLCV Fetches")
        for f in failed_symbols[:20]:
            lines.append(f"  {f}")
        if len(failed_symbols) > 20:
            lines.append(f"  ... and {len(failed_symbols) - 20} more")

    # ------------------------------------------------------------------
    # Per-symbol OHLCV coverage — for each timeframe, list every symbol
    # with its API-slice range (what GMX served this run) alongside the
    # combined range after merge. The "historical depth" column makes it
    # obvious how much pre-API data the seed contributes.
    # ------------------------------------------------------------------
    # Minutes per bar — used to convert row counts into approximate days.
    tf_minutes = {
        "1m": 1,
        "5m": 5,
        "15m": 15,
        "1h": 60,
        "4h": 240,
        "1d": 1440,
    }

    def _fmt_range(s: dict | None) -> str:
        if s is None:
            return f"{'n/a':<24s} {'-':>7s}r"
        return (
            f"{s['min_date'].strftime('%Y-%m-%d')}→{s['max_date'].strftime('%Y-%m-%d')}"
            f" {s['rows']:>7d}r"
        )

    for tf in TIMEFRAMES:
        lines.append("")
        lines.append(f"## OHLCV Coverage — {tf} (API slice vs Combined feather)")
        lines.append(
            f"  {'symbol':<10s}  {'status':<11s}  {'API slice':<32s}  "
            f"{'Combined':<32s}  hist depth"
        )

        if ohlcv_coverage:
            rows_for_tf = sorted(
                ((sym, entry) for (sym, ctf), entry in ohlcv_coverage.items() if ctf == tf),
                key=lambda kv: kv[0],
            )
            if not rows_for_tf:
                lines.append("  (no symbols collected)")
            else:
                mpb = tf_minutes.get(tf, 1)
                for sym, entry in rows_for_tf:
                    api = entry["api_slice"]
                    post = entry["post_merge"]
                    pre = entry["pre_merge"]
                    if post and api:
                        depth_rows = post["rows"] - api["rows"]
                        depth_days = round(depth_rows * mpb / 1440, 1)
                        if pre and post["min_date"] < api["min_date"]:
                            depth_str = f"+{depth_rows} rows (~{depth_days}d pre-API)"
                        else:
                            depth_str = "API-only window"
                    elif post and not api:
                        depth_str = "post-only (no API slice)"
                    else:
                        depth_str = "—"
                    lines.append(
                        f"  {sym:<10s}  {entry['status']:<11s}  "
                        f"{_fmt_range(api):<32s}  {_fmt_range(post):<32s}  {depth_str}"
                    )
        else:
            # Fallback: no coverage map — fall back to feather reads.
            for f in sorted(futures_dir.glob(f"*-{tf}-futures.feather")):
                stats = _feather_date_stats(f)
                if stats is None:
                    continue
                sym = f.stem.replace(f"_USDC_USDC-{tf}-futures", "")
                lines.append(
                    f"  {sym:<10s}  {'n/a':<11s}  {'(no API stats)':<32s}  "
                    f"{_fmt_range(stats):<32s}  —"
                )

    lines.append("")

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines), encoding="utf-8")
    console.print(f"  Report → {report_path}")


def _abort_on_failed_ohlcv_fetches(failed_symbols: list[str]) -> None:
    """Stop the run if any OHLCV fetches failed.

    The release workflow must never publish a partial candle history. If one
    symbol/timeframe cannot be refreshed, the run fails so it can be retried
    instead of shipping a hole in the historical archive.
    """
    if not failed_symbols:
        return

    console.print(
        "[red]Aborting release: OHLCV fetch failures would produce a partial snapshot.[/red]"
    )
    console.print(
        f"  Failed pairs: {len(failed_symbols)}"
        + (f" (first: {failed_symbols[0]})" if failed_symbols else "")
    )
    sys.exit(1)


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

  # Quickstart: seed user_data/ from the data/daily-collection branch,
  # then collect today's snapshot in one shot
  poetry run python scripts/collect_daily_snapshot.py -q

  # Only seed, don't collect today
  poetry run python scripts/collect_daily_snapshot.py -q --seed-only
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
    parser.add_argument(
        "-q", "--quickstart",
        action="store_true",
        help="Seed user_data/ from the data/daily-collection branch before "
             "running today's snapshot collection. Existing local files are "
             "never overwritten — only missing ones are copied.",
    )
    parser.add_argument(
        "--seed-only",
        action="store_true",
        help="With --quickstart, seed and exit without collecting today's snapshot.",
    )
    parser.add_argument(
        "--quickstart-ref",
        default=DEFAULT_RELEASE_TAG,
        help=f"Release tag to seed from (default: {DEFAULT_RELEASE_TAG} = most recent).",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help=(
            "Ignore the coverage gate and re-fetch all data types even if "
            "the on-disk file is already current.  Default off; matches "
            "the conservative behaviour expected by the daily release "
            "workflow."
        ),
    )

    args = parser.parse_args()

    if args.quickstart:
        console.print("\n[bold]Quickstart: seeding from GitHub Release[/bold]")
        summary = seed_from_release(args.output_dir, args.quickstart_ref, console)
        if "error" not in summary:
            console.print(
                f"  Copied {summary['copied']} new files "
                f"({summary['bytes'] / 1e6:.1f} MB), "
                f"skipped {summary['skipped']} existing."
            )
            print_coverage_summary(args.output_dir, console)
        else:
            console.print(
                "  [yellow]Proceeding without seed — the collector will "
                "still run.[/yellow]"
            )
        if args.seed_only:
            console.print("\n[green]--seed-only set, exiting.[/green]")
            return
        console.print()
    elif args.seed_only:
        console.print("[red]--seed-only requires --quickstart.[/red]")
        sys.exit(2)

    date_str = args.date or datetime.now(UTC).strftime("%Y-%m-%d")

    futures_dir = args.output_dir / "data" / "gmx" / "futures"
    snapshots_dir = args.output_dir / "data" / "gmx" / "snapshots"
    tickers_dir = args.output_dir / "data" / "gmx" / "tickers"
    apy_dir = args.output_dir / "data" / "gmx" / "apy"
    volumes_dir = args.output_dir / "data" / "gmx" / "volumes"
    report_path = args.output_dir.parent / "data_report.txt"

    console.print(f"\n[bold]GMX Daily Snapshot — {date_str}[/bold]")
    console.print(f"  Network:    {args.network}")
    console.print(f"  Futures:    {futures_dir}")
    console.print(f"  Snapshots:  {snapshots_dir}")
    console.print(f"  Tickers:    {tickers_dir}")
    console.print(f"  APY:        {apy_dir}")
    console.print(f"  Volumes:    {volumes_dir}\n")

    api = GMXAPI(chain=args.network)

    # --- Fetch all markets once ---
    console.print("[bold]Fetching markets from GMX API...[/bold]")
    all_markets = _fetch_all_markets(api)
    if not all_markets:
        console.print("[red]Error: No market data returned from GMX API[/red]")
        sys.exit(1)

    # --- Phase 1: Markets snapshot (ALL markets: perp + swap-only + unlisted) ---
    console.print("\n[bold]Phase 1: Markets snapshot (OI, liquidity, rates)[/bold]")
    markets_path = snapshots_dir / f"{date_str}.parquet"
    markets_path.parent.mkdir(parents=True, exist_ok=True)

    skipped: dict[str, SkipDecision] = {}
    markets_decision = is_current(
        markets_path, expected_min_rows=100, force=args.force_refresh
    )
    if markets_decision.skip:
        skipped["markets"] = markets_decision
        markets_df = pd.read_parquet(markets_path)
        console.print(
            f"  [yellow]Skipped — existing {markets_decision.existing_rows} rows ≥ "
            f"{markets_decision.expected_min_rows} required[/yellow]"
        )
    else:
        markets_df = collect_markets_snapshot(all_markets, date_str)
        markets_df.to_parquet(markets_path, index=False)
        console.print(f"  Saved → {markets_path}")
    console.print()

    # --- Phase 2: OHLCV candles for ALL timeframes ---
    console.print("[bold]Phase 2: OHLCV candles (all timeframes)[/bold]")
    candle_count, failed_symbols, ohlcv_coverage = collect_and_save_ohlcv(
        api,
        all_markets,
        futures_dir,
        force_refresh=args.force_refresh,
        target_date=date_str,
    )
    console.print()

    # Phase 3: Volume collection runs in a separate workflow (collect-volume.yml)
    volume_count, volume_data = 0, {}

    # --- Phase 4: Tickers (bid/ask prices) ---
    console.print("[bold]Phase 4: Tickers (bid/ask prices)[/bold]")
    ticker_path = tickers_dir / f"{date_str}.parquet"
    ticker_decision = is_current(
        ticker_path, expected_min_rows=100, force=args.force_refresh
    )
    if ticker_decision.skip:
        skipped["tickers"] = ticker_decision
        ticker_count = _row_count(ticker_path)
        console.print(
            f"  [yellow]Skipped — existing {ticker_decision.existing_rows} rows ≥ "
            f"{ticker_decision.expected_min_rows} required[/yellow]"
        )
    else:
        ticker_count = collect_and_save_tickers(api, date_str, tickers_dir)
    console.print()

    # --- Phase 5: APY (all periods) ---
    console.print("[bold]Phase 5: APY (yield data)[/bold]")
    apy_path = apy_dir / f"{date_str}.parquet"
    apy_decision = is_current(
        apy_path, expected_min_rows=7 * 100, force=args.force_refresh
    )
    if apy_decision.skip:
        skipped["apy"] = apy_decision
        apy_count = _row_count(apy_path)
        console.print(
            f"  [yellow]Skipped — existing {apy_decision.existing_rows} rows ≥ "
            f"{apy_decision.expected_min_rows} required[/yellow]"
        )
    else:
        apy_count = collect_and_save_apy(api, date_str, apy_dir)
    console.print()

    # --- Phase 6: Generate report ---
    console.print("[bold]Phase 6: Data report[/bold]")
    generate_report(
        date_str=date_str,
        markets_df=markets_df,
        candle_count=candle_count,
        failed_symbols=failed_symbols,
        ticker_count=ticker_count,
        apy_count=apy_count,
        volume_count=volume_count,
        volume_data=volume_data,
        futures_dir=futures_dir,
        snapshots_dir=snapshots_dir,
        tickers_dir=tickers_dir,
        apy_dir=apy_dir,
        volumes_dir=volumes_dir,
        report_path=report_path,
        ohlcv_coverage=ohlcv_coverage,
    )
    _abort_on_failed_ohlcv_fetches(failed_symbols)
    console.print()

    # --- Summary ---
    console.print("[bold]Summary[/bold]")
    console.print(f"  Date:      {date_str}")
    console.print(f"  Markets:   {len(markets_df)} (all)")
    console.print(f"  Candles:   {candle_count} files ({len(TIMEFRAMES)} timeframes)")
    console.print(f"  Tickers:   {ticker_count}")
    console.print(f"  APY:       {apy_count} entries")
    console.print("[green]Done.[/green]")


if __name__ == "__main__":
    main()
