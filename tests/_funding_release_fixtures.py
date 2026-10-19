"""Shared builders for the daily-release funding/mark tests (no network)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

HOUR = timedelta(hours=1)


def hours(start: datetime, count: int) -> list[datetime]:
    """``count`` consecutive hourly timestamps from ``start``."""
    return [start + i * HOUR for i in range(count)]


def write_futures(
    futures_dir: Path, symbol: str, start: datetime, count: int, tf: str = "1h"
) -> list[datetime]:
    """Write ``{symbol}_USDC_USDC-{tf}-futures.feather`` with ``count`` hourly bars."""
    futures_dir.mkdir(parents=True, exist_ok=True)
    stamps = hours(start, count)
    frame = pl.DataFrame(
        {
            "date": stamps,
            "open": [100.0 + i * 0.01 for i in range(count)],
            "high": [101.0 + i * 0.01 for i in range(count)],
            "low": [99.0 + i * 0.01 for i in range(count)],
            "close": [100.5 + i * 0.01 for i in range(count)],
            "volume": [1.0] * count,
        }
    ).with_columns(pl.col("date").dt.replace_time_zone("UTC").dt.cast_time_unit("ns"))
    frame.write_ipc(futures_dir / f"{symbol}_USDC_USDC-{tf}-futures.feather")
    return stamps


def write_companion(
    directory: Path,
    symbol: str,
    kind: str,
    stamps: list[datetime],
    *,
    value: float = 0.00001,
    tf: str = "1h",
) -> Path:
    """Write a ``-funding_rate`` or ``-mark`` feather for the given stamps."""
    directory.mkdir(parents=True, exist_ok=True)
    count = len(stamps)
    if kind == "funding_rate":
        cols = {
            "open": [value] * count,
            "high": [0.0] * count,
            "low": [0.0] * count,
            "close": [0.0] * count,
            "volume": [0.0] * count,
        }
    else:
        cols = {
            "open": [100.0] * count,
            "high": [101.0] * count,
            "low": [99.0] * count,
            "close": [100.5] * count,
            "volume": [0.0] * count,
        }
    frame = pl.DataFrame({"date": stamps, **cols}).with_columns(
        pl.col("date").dt.replace_time_zone("UTC").dt.cast_time_unit("ns")
    )
    path = directory / f"{symbol}_USDC_USDC-{tf}-{kind}.feather"
    frame.write_ipc(path)
    return path


def write_checkpoint(directory: Path, name: str, symbol: str, last_ts: datetime) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_text(
        json.dumps(
            {
                "symbol": symbol,
                "last_block": 1000,
                "last_timestamp": int(last_ts.timestamp()),
                "total_events": 10,
                "last_updated": last_ts.isoformat(),
                "metadata": {"markets_seen": 1},
            }
        )
    )


def write_seed_source(
    root: Path,
    *,
    symbols: tuple[str, ...] = ("BTC", "ETH"),
    funding_blocks: tuple[int, int] = (1_000, 5_000_000),
    with_checkpoints: bool = False,
) -> Path:
    """Build a ``data/full-history``-shaped ``arbitrum/`` tree (raw, rates, direction)."""
    for symbol in symbols:
        raw_f = root / "raw" / "funding" / symbol / "partition=0"
        raw_f.mkdir(parents=True)
        lo, hi = funding_blocks
        blocks = [lo, (lo + hi) // 2, hi]
        pl.DataFrame(
            {
                "symbol": [symbol] * 3,
                "block_number": blocks,
                "block_timestamp": [1_700_000_000 + b for b in blocks],
                "log_index": [0, 0, 0],
            }
        ).write_parquet(raw_f / "data.parquet")
        raw_p = root / "raw" / "fee_per_size" / symbol
        raw_p.mkdir(parents=True)
        pl.DataFrame(
            {
                "symbol": [symbol] * 2,
                "block_number": [lo, hi + 10],
                "block_timestamp": [1_700_000_000 + lo, 1_700_000_000 + hi + 10],
                "log_index": [0, 1],
            }
        ).write_parquet(raw_p / "data.parquet")
        rates = root / "rates" / symbol
        rates.mkdir(parents=True)
        pl.DataFrame({"timestamp": [datetime(2026, 4, 14, tzinfo=UTC)]}).write_parquet(
            rates / "1h.parquet"
        )
        direction = root / "direction" / symbol
        direction.mkdir(parents=True)
        pl.DataFrame({"timestamp": [datetime(2026, 4, 14, tzinfo=UTC)]}).write_parquet(
            direction / "1h.parquet"
        )
    if with_checkpoints:
        cp = root / "checkpoints"
        cp.mkdir()
        for name, sym, blk in (
            ("funding_factor_checkpoint.json", "funding_factor_all", 4_000_000),
            ("fee_per_size_checkpoint.json", "fee_per_size_all", 4_100_000),
        ):
            (cp / name).write_text(
                json.dumps(
                    {
                        "symbol": sym,
                        "last_block": blk,
                        "last_timestamp": 1_700_000_000 + blk,
                        "total_events": 5,
                        "last_updated": "2026-04-14T00:00:00+00:00",
                        "metadata": {"markets_seen": 2},
                    }
                )
            )
    return root
