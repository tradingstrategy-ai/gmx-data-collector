"""One-time reconciliation: merge dense Freqtrade-export candles back into
the source ``candles/arbitrum/{TOKEN}/{tf}.parquet`` store.

Root cause this script exists to fix
===================================

``candles/arbitrum/{TOKEN}/{tf}.parquet`` (the source store, built by
:class:`~gmx_historical_data.chainlink_rpc_collector.ChainlinkRPCCollector`
walking Chainlink's classic on-chain feed) and
``user_data/data/gmx/futures/{TOKEN}_USDC_USDC-{tf}-futures.feather`` (the
Freqtrade export) are populated by two independent pipelines:

* The source store is kept current by ``gmx_historical_data.cli collect
  --update``, gated by :class:`~gmx_historical_data.daemon.gap_detector.
  AdaptiveGapDetector`. That detector used to treat "we already have a row
  for every recent timestamp" as "up to date" -- true even when those rows
  are Chainlink's forward-filled flat (``high == low``) placeholders, so it
  latched on ``NO_GAP`` and never refetched GMX's own denser API data once
  the initial Chainlink walk reached "now" (fixed by the ``STALE_DENSITY``
  status in ``gap_detector.py`` -- see :mod:`gmx_historical_data.
  ohlcv_density` for the full writeup). That fix stops the *regression from
  recurring*, but does not retroactively fix data already on disk.
* The Freqtrade export's OHLCV feathers are kept current by
  ``scripts/collect_daily_snapshot.py``, run daily by the
  ``release-data.yml`` workflow. It fetches directly from GMX's own
  ``/prices/candles`` API on every run and has been doing so for months,
  so the feather files already hold genuinely dense (0% flat) data for the
  window GMX's API can serve -- accumulated day by day, which a single
  fresh API pull cannot replicate (GMX's live 1-minute window is only a
  few hours; the feather's ~6 months of density came from daily
  accumulation, not one query).

This script closes the gap for data already on disk: for each
Chainlink-backed symbol, read the existing Freqtrade export feathers and
merge them into the source parquet store via
:meth:`~gmx_historical_data.storage.ParquetStorage.save_candles`, whose
merge (as of the same fix) prefers the denser row on any timestamp
collision. Older history the feathers don't cover is untouched.

Usage::

    poetry run python scripts/reconcile_dense_from_futures.py
    poetry run python scripts/reconcile_dense_from_futures.py --symbol BTC ETH
    poetry run python scripts/reconcile_dense_from_futures.py --dry-run
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from rich.console import Console
from rich.table import Table

from gmx_historical_data.config import FILENAME_TO_TIMEFRAME
from gmx_historical_data.daemon.config import get_gmx_markets_with_chainlink_feeds
from gmx_historical_data.ohlcv_density import flat_fraction_pandas
from gmx_historical_data.storage import ParquetStorage

console = Console()

# Timeframes as named in the Freqtrade export filenames (matches
# scripts/collect_daily_snapshot.py's TIMEFRAMES).
FUTURES_TIMEFRAMES = ["1m", "5m", "15m", "1h", "4h", "1d"]


def _futures_path(futures_dir: Path, symbol: str, tf: str) -> Path:
    """Build the Freqtrade export feather path for a symbol/timeframe.

    :param futures_dir: Directory containing the CCXT feather exports.
    :param symbol: Token symbol (e.g. ``'BTC'``).
    :param tf: Freqtrade-style timeframe string (e.g. ``'1m'``).
    :return: Path to the feather file (may not exist).
    """
    return futures_dir / f"{symbol}_USDC_USDC-{tf}-futures.feather"


def _feather_to_candles(feather_df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Convert a Freqtrade-export feather frame to the candles-store schema.

    :param feather_df: Frame with ``date, open, high, low, close, volume``.
    :param symbol: Token symbol to stamp on every row.
    :return: Frame with ``timestamp, open, high, low, close, symbol``,
        timezone-aware UTC timestamps.
    """
    out = pd.DataFrame(
        {
            "timestamp": feather_df["date"],
            "open": feather_df["open"].astype(float),
            "high": feather_df["high"].astype(float),
            "low": feather_df["low"].astype(float),
            "close": feather_df["close"].astype(float),
            "symbol": symbol,
        }
    )
    if out["timestamp"].dt.tz is None:
        out["timestamp"] = out["timestamp"].dt.tz_localize("UTC")
    return out


def reconcile_symbol(
    storage: ParquetStorage,
    futures_dir: Path,
    symbol: str,
    timeframes: list[str] = FUTURES_TIMEFRAMES,
    dry_run: bool = False,
) -> dict[str, dict]:
    """Merge one symbol's Freqtrade-export feathers into the candles store.

    :param storage: ParquetStorage pointed at the same ``data`` root as
        ``futures_dir``'s parent.
    :param futures_dir: Directory containing the CCXT feather exports.
    :param symbol: Token symbol (e.g. ``'BTC'``).
    :param timeframes: Freqtrade-style timeframes to reconcile.
    :param dry_run: If True, compute before/after flat fractions without
        writing anything to the candles store.
    :return: Dict keyed by timeframe with ``rows_in``, ``before_flat``,
        ``after_flat`` (``after_flat`` is a projection under ``--dry-run``).
    """
    results: dict[str, dict] = {}

    for tf in timeframes:
        fpath = _futures_path(futures_dir, symbol, tf)
        if not fpath.exists():
            continue

        feather_df = pd.read_feather(fpath)
        if feather_df.empty:
            continue

        candle_tf = FILENAME_TO_TIMEFRAME.get(tf, tf)
        existing = storage.read_candles(candle_tf, symbol)
        before_flat = flat_fraction_pandas(existing) if not existing.empty else None

        incoming = _feather_to_candles(feather_df, symbol)

        if dry_run:
            # Project the merge without writing: a timestamp already dense
            # on disk stays dense; a flat/missing timestamp within the
            # feather's coverage becomes dense.
            if existing.empty:
                after_flat = flat_fraction_pandas(incoming)
            else:
                merged_preview = pd.concat([existing, incoming], ignore_index=True)
                existing_dense_ts = set(
                    existing.loc[existing["high"] != existing["low"], "timestamp"]
                )
                merged_preview["__dense"] = (
                    merged_preview["high"] != merged_preview["low"]
                ) | merged_preview["timestamp"].isin(existing_dense_ts)
                merged_preview = merged_preview.sort_values(
                    ["timestamp", "__dense"]
                ).drop_duplicates(subset=["timestamp"], keep="last")
                after_flat = 1.0 - merged_preview["__dense"].mean()
        else:
            storage.save_candles(incoming, candle_tf, symbol)
            after = storage.read_candles(candle_tf, symbol)
            after_flat = flat_fraction_pandas(after)

        results[tf] = {
            "rows_in": len(incoming),
            "before_flat": before_flat,
            "after_flat": after_flat,
        }

    return results


def main() -> int:
    """CLI entrypoint.

    :return: Process exit code (0 on success).
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("user_data"),
        help="Output root containing data/gmx/{candles,futures} (default: user_data)",
    )
    parser.add_argument(
        "--symbol",
        nargs="+",
        default=None,
        help="Symbols to reconcile (default: all Chainlink-backed markets)",
    )
    parser.add_argument(
        "--timeframe",
        nargs="+",
        default=None,
        choices=FUTURES_TIMEFRAMES,
        help="Timeframes to reconcile (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report projected before/after flat fractions without writing",
    )
    args = parser.parse_args()

    data_dir = args.output_dir / "data" / "gmx"
    futures_dir = data_dir / "futures"
    storage = ParquetStorage(data_dir)

    symbols = args.symbol or get_gmx_markets_with_chainlink_feeds()
    timeframes = args.timeframe or FUTURES_TIMEFRAMES

    console.print(
        f"[bold]Reconciling {len(symbols)} symbol(s) x {len(timeframes)} timeframe(s) "
        f"from {futures_dir} into {data_dir / 'candles' / 'arbitrum'}[/bold]"
        + (" [yellow](dry-run)[/yellow]" if args.dry_run else "")
    )

    table = Table(title="Dense reconciliation results")
    table.add_column("Symbol")
    table.add_column("TF")
    table.add_column("Rows merged", justify="right")
    table.add_column("Flat % before", justify="right")
    table.add_column("Flat % after", justify="right")

    any_rows = False
    for symbol in symbols:
        try:
            results = reconcile_symbol(
                storage, futures_dir, symbol, timeframes=timeframes, dry_run=args.dry_run
            )
        except Exception as e:  # noqa: BLE001 - report and continue with other symbols
            console.print(f"  [red]✗ {symbol}: {e}[/red]")
            continue

        for tf, stats in results.items():
            any_rows = True
            before = "n/a" if stats["before_flat"] is None else f"{stats['before_flat']:.1%}"
            after = f"{stats['after_flat']:.1%}"
            table.add_row(symbol, tf, str(stats["rows_in"]), before, after)

    if any_rows:
        console.print(table)
    else:
        console.print("[yellow]No matching feather files found -- nothing to reconcile.[/yellow]")

    return 0


if __name__ == "__main__":
    sys.exit(main())
