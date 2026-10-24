"""Retain trade tapes/aggregates separately from zero-volume oracle candles.

The API has no candle volume. Legacy overlay entry points now enforce literal
zero in candle exports; actual token/USD traded amounts remain in tick_volume.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from pathlib import Path

import pandas as pd

from gmx_historical_data.atomic_parquet import atomic_write_ipc_pandas, atomic_write_parquet_pandas
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

# Legacy flow-filter name retained for existing callers. Oracle candle volume
# is always zero; decoded perp/swap volumes remain in separate sidecars.
CANDLE_VOLUME_KINDS: tuple[str, ...] = (PERP_KIND,)


def restore_cleared_volume(combined: pd.DataFrame, existing: pd.DataFrame) -> pd.DataFrame:
    """Enforce zero oracle-candle volume at the legacy merge boundary.

    :param combined: Merged candle frame.
    :param existing: Prior frame, retained for caller compatibility.
    :returns: Frame with literal zero candle volume.
    """
    if "volume" in combined.columns:
        combined = combined.assign(volume=0.0)
    return combined


def apply_volume_to_candles(
    volume_df: pd.DataFrame,
    futures_dir: Path,
    timeframe: str,
) -> int:
    """Clear candle volume for symbols covered by the separate trade-volume input.

    The legacy function name is retained for existing collector callers.
    Oracle candle volume is always zero; traded amounts stay in the sidecar.
    OHLC and candle timestamps are preserved.

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

        candles["volume"] = 0.0
        atomic_write_ipc_pandas(candles, filepath, compression="zstd", compression_level=3)
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
    """Recompute separate trade-volume sidecars and clear touched candle volume.

    Always aggregates a day from its *complete* tape, never from the slice a
    single scan happened to see. A scan can straddle midnight and two runs can
    contribute fills to the same UTC day. The sidecar therefore replaces its
    daily aggregate from the deduplicated tape; candle volume stays zero.

    Re-running is safe: the tape is deduplicated and the bar is recomputed
    from scratch, so a repeated scan cannot inflate a day.

    :param ticks_dir: Directory of per-day tape files.
    :param tick_volume_dir: Directory for the per-symbol volume sidecar.
    :param futures_dir: Directory of candle feathers to update.
    :param timeframes: Candle timeframes to fill.
    :param dates: ISO dates to process; ``None`` processes every tape on disk.
    :param volume_kinds: Legacy flow selection for identifying touched candle symbols; their volume stays zero.
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
