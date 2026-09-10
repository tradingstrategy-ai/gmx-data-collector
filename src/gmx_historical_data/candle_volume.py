"""Write traded volume onto the candle feathers, and track how far we have read.

Volume is derived from on-chain fills (see
:mod:`gmx_historical_data.gmx_trade_ticks`) by both the daily snapshot and
the historical backfill, so the write path, the per-UTC-day tick tape and the
checkpoint bookkeeping all live here rather than in either script. Both
callers must file a fill under the same day and compute a bar the same way,
and the only way to guarantee that is a single implementation.

The feather schema is locked to
:data:`~gmx_historical_data.ohlcv_validation.EXPORT_COLUMNS` because
Freqtrade reads those six columns positionally. USD notional therefore never
enters the feather -- it goes to the ``tick_volume/`` sidecar instead.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from pathlib import Path

import pandas as pd
import pyarrow.feather as feather

from gmx_historical_data.atomic_parquet import atomic_write_parquet_pandas
from gmx_historical_data.gmx_trade_ticks import (
    PERP_KIND,
    SWAP_KIND,
    TradeTick,
    aggregate_tick_volume,
    ticks_to_frame,
)

logger = logging.getLogger(__name__)

#: Timeframes that have candle files, in filename form.
CANDLE_TIMEFRAMES = ["1m", "5m", "15m", "1h", "4h", "1d"]

# Which on-chain flows count toward a candle's ``volume``.
#
# Perp fills only. A candle for BTC describes the BTC perpetual, so its
# volume is the perp size traded on BTC markets (``PositionIncrease`` +
# ``PositionDecrease``) -- what GMX itself reports as ``marginVolumeUsd``.
# GM-pool swaps are collateral movement rather than perp trading, so folding
# them in would inflate the number a strategy reads as perp liquidity.
#
# Swap fills are still decoded and stored in the tick tape and broken out in
# the sidecar, so switching to ``(PERP_KIND, SWAP_KIND)`` changes the
# published definition without needing a re-collection.
CANDLE_VOLUME_KINDS: tuple[str, ...] = (PERP_KIND,)


def restore_cleared_volume(combined: pd.DataFrame, existing: pd.DataFrame) -> pd.DataFrame:
    """Put back any non-zero volume that a merge replaced with a zero.

    :param combined: Merged frame after ``drop_duplicates(keep='last')``.
    :param existing: Frame as it was on disk before the merge.
    :returns: ``combined`` with previously-known volume restored.
    """
    if "volume" not in combined.columns or "volume" not in existing.columns:
        return combined

    prior = (
        existing.dropna(subset=["date"])
        .drop_duplicates(subset=["date"], keep="last")
        .set_index("date")["volume"]
    )
    if prior.empty:
        return combined

    cleared = combined["volume"].fillna(0) == 0
    if not cleared.any():
        return combined

    recovered = combined.loc[cleared, "date"].map(prior)
    combined.loc[cleared, "volume"] = recovered.fillna(0.0).to_numpy()
    return combined


def apply_volume_to_candles(
    volume_df: pd.DataFrame,
    futures_dir: Path,
    timeframe: str,
) -> int:
    """Write per-symbol traded volume onto the bars of one timeframe.

    Only the ``volume`` column is touched -- OHLC values and every bar
    without a matching volume row are left exactly as they were, so this is
    safe to re-run.

    :param volume_df: Frame from
        :func:`~gmx_historical_data.gmx_trade_ticks.aggregate_tick_volume`,
        with ``symbol``, ``date`` and ``volume`` columns.
    :param futures_dir: Directory holding the candle feathers.
    :param timeframe: Timeframe suffix of the files to update, e.g. ``1h``.
    :returns: Number of feather files actually rewritten.
    """
    if volume_df is None or volume_df.empty:
        return 0

    updated = 0
    for symbol, group in volume_df.groupby("symbol", sort=True):
        filepath = Path(futures_dir) / f"{symbol}_USDC_USDC-{timeframe}-futures.feather"
        if not filepath.exists():
            continue

        candles = pd.read_feather(filepath)
        if candles.empty or "date" not in candles.columns:
            continue
        if candles["date"].dt.tz is None:
            candles["date"] = candles["date"].dt.tz_localize("UTC")
        candles["date"] = candles["date"].dt.as_unit("ns")

        wanted = group.copy()
        wanted["date"] = pd.to_datetime(wanted["date"], utc=True).dt.as_unit("ns")
        lookup = wanted.drop_duplicates(subset=["date"], keep="last").set_index("date")["volume"]

        matched = candles["date"].map(lookup)
        if matched.notna().sum() == 0:
            continue

        candles["volume"] = matched.fillna(candles["volume"]).astype(float)
        feather.write_feather(candles, filepath, compression="zstd", compression_level=3)
        updated += 1

    return updated


def write_tick_tapes(ticks: Sequence[TradeTick], ticks_dir: Path) -> set[str]:
    """Merge ticks into their per-UTC-day tape files.

    Keyed by the day each fill *happened*, never by the day the run happened:
    a scan window straddles midnight, so one run routinely produces fills for
    two days, and the next run produces more fills for one of those same days.
    Files are merged rather than overwritten for the same reason.

    Rows are deduplicated on ``(transaction_hash, log_index, symbol, kind)``.
    The symbol is part of the key because one ``SwapInfo`` log yields two
    ticks sharing a transaction hash *and* a log index, differing only by
    token -- keying on the pair alone drops one side of every swap.

    :param ticks: Ticks to store.
    :param ticks_dir: Directory of ``{date}.parquet`` tape files.
    :returns: The set of ISO dates whose files were touched.
    """
    if not ticks:
        return set()

    frame = ticks_to_frame(ticks)
    frame["_date"] = frame["date"].dt.strftime("%Y-%m-%d")
    ticks_dir = Path(ticks_dir)
    ticks_dir.mkdir(parents=True, exist_ok=True)

    touched: set[str] = set()
    for date_str, group in frame.groupby("_date"):
        group = group.drop(columns=["_date"])
        path = ticks_dir / f"{date_str}.parquet"
        if path.exists():
            group = pd.concat([pd.read_parquet(path), group], ignore_index=True)
        group = group.drop_duplicates(
            subset=["transaction_hash", "log_index", "symbol", "kind"], keep="last"
        ).sort_values(["date", "block_number", "log_index"])
        atomic_write_parquet_pandas(group.reset_index(drop=True), path)
        touched.add(str(date_str))
    return touched


def load_ticks_from_tape(path: Path) -> list[TradeTick]:
    """Rebuild ticks from a stored tape file.

    :param path: Tape parquet path.
    :returns: Ticks, or an empty list when the file is missing or empty.
    """
    path = Path(path)
    if not path.exists():
        return []
    frame = pd.read_parquet(path)
    if frame.empty:
        return []

    return [
        TradeTick(
            timestamp=int(row.date.timestamp()),
            block_number=int(row.block_number),
            transaction_hash=row.transaction_hash,
            log_index=int(row.log_index),
            event_name=row.event_name,
            kind=row.kind,
            symbol=row.symbol,
            market=row.market,
            price_usd=float(row.price_usd),
            size_tokens=float(row.size_tokens),
            size_usd=float(row.size_usd),
            is_long=row.is_long,
            price_impact_usd=float(row.price_impact_usd),
            side=getattr(row, "side", None),
        )
        for row in frame.itertuples()
    ]


def apply_volume_from_tapes(
    ticks_dir: Path,
    tick_volume_dir: Path,
    futures_dir: Path,
    timeframes: Sequence[str],
    dates: Sequence[str] | None = None,
    volume_kinds: Sequence[str] = CANDLE_VOLUME_KINDS,
) -> int:
    """Recompute volume for whole UTC days and write it onto the candles.

    Always aggregates a day from its *complete* tape, never from the slice a
    single scan happened to see. Writing a partial sum would be wrong twice
    over: :func:`apply_volume_to_candles` replaces a bar rather than adding to
    it, so the last run of a day would win, and the 02:00 UTC cron's window
    straddles midnight, so a bar is routinely filled by two different runs.

    Re-running is safe: the tape is deduplicated and the bar is recomputed
    from scratch, so a repeated scan cannot inflate a day.

    :param ticks_dir: Directory of per-day tape files.
    :param tick_volume_dir: Directory for the per-symbol volume sidecar.
    :param futures_dir: Directory of candle feathers to update.
    :param timeframes: Candle timeframes to fill.
    :param dates: ISO dates to process; ``None`` processes every tape on disk.
    :param volume_kinds: Which tick kinds count toward candle ``volume``.
    :returns: Number of candle files written.
    """
    ticks_dir = Path(ticks_dir)
    paths = (
        sorted(ticks_dir.glob("*.parquet"))
        if dates is None
        else [ticks_dir / f"{d}.parquet" for d in sorted(dates)]
    )
    paths = [p for p in paths if p.exists()]
    if not paths:
        return 0

    tick_volume_dir = Path(tick_volume_dir)
    tick_volume_dir.mkdir(parents=True, exist_ok=True)
    updated = 0

    for path in paths:
        ticks = load_ticks_from_tape(path)
        if not ticks:
            continue

        sidecar = []
        for timeframe in timeframes:
            # The sidecar breaks volume out per flow so a consumer can rebuild
            # either definition (perp-only or perp+swap) without re-collecting.
            for kind in (PERP_KIND, SWAP_KIND):
                per_kind = aggregate_tick_volume(ticks, timeframe, kinds=(kind,))
                if not per_kind.empty:
                    sidecar.append(per_kind.assign(timeframe=timeframe, kind=kind))

            volume = aggregate_tick_volume(ticks, timeframe, kinds=volume_kinds)
            if not volume.empty:
                updated += apply_volume_to_candles(volume, futures_dir, timeframe)

        if sidecar:
            atomic_write_parquet_pandas(
                pd.concat(sidecar, ignore_index=True), tick_volume_dir / f"{path.stem}.parquet"
            )

    return updated


def read_tick_checkpoint(path: Path) -> int | None:
    """Read the highest block a previous tick run completed.

    :param path: Checkpoint file path.
    :returns: Block number, or ``None`` when absent or unreadable.
    """
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        block = payload.get("last_scanned_block")
        return int(block) if block is not None else None
    except (OSError, ValueError, TypeError, AttributeError):
        return None


def write_tick_checkpoint(path: Path, block: int) -> None:
    """Record the highest block scanned, never moving backwards.

    A stale run finishing after a newer one must not rewind the checkpoint,
    or the overlapping range would be scanned again and its volume counted
    twice.

    :param path: Checkpoint file path.
    :param block: Highest block scanned by this run.
    """
    path = Path(path)
    existing = read_tick_checkpoint(path)
    if existing is not None and existing >= block:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"last_scanned_block": int(block)}), encoding="utf-8")
