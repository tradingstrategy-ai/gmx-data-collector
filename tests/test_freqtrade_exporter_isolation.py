"""Tests for FreqtradeExporter isolation guarantees.

These tests lock in the post-2026-05-11 contract:

- Source candle parquet is preserved by default.
- `--overwrite` cannot shrink existing feather history; only
  `--unsafe-overwrite` may.
- `export_candles()` doesn't touch funding files (and vice versa).
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from gmx_historical_data.freqtrade_exporter import FreqtradeExporter
from gmx_historical_data.storage import ParquetStorage


def _make_candle_df(start: datetime, hours: int) -> pd.DataFrame:
    """Build a minimal OHLCV DataFrame in the on-disk schema."""
    return pd.DataFrame(
        {
            "timestamp": pd.date_range(start=start, periods=hours, freq="1h", tz="UTC"),
            "open": [100.0] * hours,
            "high": [101.0] * hours,
            "low": [99.0] * hours,
            "close": [100.5] * hours,
            "symbol": ["BTC"] * hours,
        }
    )


@pytest.fixture
def populated_data_dir(tmp_path: Path) -> Path:
    """Set up a data dir with one symbol's candle parquet."""
    data_dir = tmp_path / "data"
    storage = ParquetStorage(data_dir)
    df = _make_candle_df(datetime(2026, 1, 1, tzinfo=UTC), hours=24)
    storage.save_candles(df, timeframe="1h", symbol="BTC")
    return data_dir


def test_export_preserves_candle_parquet_by_default(populated_data_dir, tmp_path):
    """The exporter MUST NOT delete the candle parquet by default.

    This is the 2026-05-11 regression check: an export with no extra flags
    must leave the source parquet exactly where it was, so the user always
    has an on-disk fallback if the feather write goes wrong.
    """
    exporter = FreqtradeExporter(populated_data_dir, tmp_path / "feathers")
    exporter.export(symbols=["BTC"], timeframes=["1h"])

    candle_parquet = populated_data_dir / "candles" / "arbitrum" / "BTC" / "1h.parquet"
    assert candle_parquet.exists(), (
        "FreqtradeExporter deleted the candle parquet by default — "
        "this is the 2026-05-11 regression path."
    )


def test_overwrite_cannot_shrink_history(populated_data_dir, tmp_path):
    """`overwrite=True` MUST still preserve existing feather history.

    Replicates the 2026-05-11 root cause: a feather with 365 days of history,
    a candle parquet shrunk to the last 30 hours, and an overwrite export.
    The fix is for ``--overwrite`` to merge rather than replace, so the
    pre-existing history survives even if the new data is shorter.
    """
    feather_dir = tmp_path / "feathers"
    exporter = FreqtradeExporter(populated_data_dir, feather_dir)

    # Step 1: full history goes into the feather (365 * 24 hours).
    storage = ParquetStorage(populated_data_dir)
    long_df = _make_candle_df(datetime(2025, 1, 1, tzinfo=UTC), hours=365 * 24)
    storage.save_candles(long_df, timeframe="1h", symbol="BTC", overwrite=True)
    exporter.export(symbols=["BTC"], timeframes=["1h"])

    feather_path = feather_dir / "gmx" / "futures" / "BTC_USDC_USDC-1h-futures.feather"
    assert feather_path.exists()
    earliest_before = pl.read_ipc(feather_path)["date"].min()
    rows_before = pl.read_ipc(feather_path).height

    # Step 2: shrink the parquet to the last 30 hours, then export with overwrite.
    short_df = _make_candle_df(datetime(2025, 12, 31, tzinfo=UTC), hours=30)
    storage.save_candles(short_df, timeframe="1h", symbol="BTC", overwrite=True)
    exporter.export(symbols=["BTC"], timeframes=["1h"], overwrite=True)

    # Step 3: feather is untouched — overwrite no longer means "replace".
    earliest_after = pl.read_ipc(feather_path)["date"].min()
    rows_after = pl.read_ipc(feather_path).height
    assert earliest_after == earliest_before, (
        "overwrite=True silently truncated history — this is the 2026-05-11 bug."
    )
    assert rows_after >= rows_before, (
        "overwrite=True dropped existing rows when the parquet shrank."
    )


def test_unsafe_overwrite_allows_shrink(populated_data_dir, tmp_path):
    """`unsafe_overwrite=True` MUST bypass the guard (for schema migrations)."""
    feather_dir = tmp_path / "feathers"
    exporter = FreqtradeExporter(populated_data_dir, feather_dir)

    storage = ParquetStorage(populated_data_dir)
    long_df = _make_candle_df(datetime(2025, 1, 1, tzinfo=UTC), hours=365 * 24)
    storage.save_candles(long_df, timeframe="1h", symbol="BTC", overwrite=True)
    exporter.export(symbols=["BTC"], timeframes=["1h"])

    short_df = _make_candle_df(datetime(2025, 12, 31, tzinfo=UTC), hours=30)
    storage.save_candles(short_df, timeframe="1h", symbol="BTC", overwrite=True)

    # No exception — explicit opt-in.
    exporter.export(symbols=["BTC"], timeframes=["1h"], unsafe_overwrite=True)

    feather_path = feather_dir / "gmx" / "futures" / "BTC_USDC_USDC-1h-futures.feather"
    assert pl.read_ipc(feather_path).height == 30


@pytest.fixture
def data_dir_with_funding(populated_data_dir) -> Path:
    """Populated data dir + a funding parquet for BTC."""
    funding_dir = populated_data_dir / "funding" / "arbitrum" / "rates" / "BTC"
    funding_dir.mkdir(parents=True, exist_ok=True)
    funding_df = pl.DataFrame(
        {
            "timestamp": pl.datetime_range(
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 1, 1, 23, tzinfo=UTC),
                interval="1h",
                time_zone="UTC",
                eager=True,
            ),
            "funding_rate": [1e-9] * 24,
            "funding_rate_hourly": [3.6e-6] * 24,
        }
    )
    funding_df.write_parquet(funding_dir / "1h.parquet")
    return populated_data_dir


def test_export_candles_does_not_touch_funding(data_dir_with_funding, tmp_path):
    """`export_candles` must not create or modify funding_rate feathers."""
    feather_dir = tmp_path / "feathers"
    exporter = FreqtradeExporter(data_dir_with_funding, feather_dir)
    exporter.export_candles(symbols=["BTC"], timeframes=["1h"])

    gmx_dir = feather_dir / "gmx" / "futures"
    assert (gmx_dir / "BTC_USDC_USDC-1h-futures.feather").exists()
    assert (gmx_dir / "BTC_USDC_USDC-1h-mark.feather").exists()
    assert (gmx_dir / "BTC_USDC_USDC-1h-index.feather").exists()
    assert not (gmx_dir / "BTC_USDC_USDC-1h-funding_rate.feather").exists(), (
        "export_candles created a funding_rate feather — paths not isolated."
    )


def test_export_funding_does_not_touch_candles(data_dir_with_funding, tmp_path):
    """`export_funding` must not create or modify -futures/-mark/-index feathers."""
    feather_dir = tmp_path / "feathers"
    exporter = FreqtradeExporter(data_dir_with_funding, feather_dir)
    exporter.export_funding(symbols=["BTC"], timeframes=["1h"])

    gmx_dir = feather_dir / "gmx" / "futures"
    assert (gmx_dir / "BTC_USDC_USDC-1h-funding_rate.feather").exists()
    for forbidden in (
        "BTC_USDC_USDC-1h-futures.feather",
        "BTC_USDC_USDC-1h-mark.feather",
        "BTC_USDC_USDC-1h-index.feather",
    ):
        assert not (gmx_dir / forbidden).exists(), (
            f"export_funding created {forbidden} — paths not isolated."
        )


def test_export_funding_does_not_delete_funding_parquet(data_dir_with_funding, tmp_path):
    """Funding parquet is owned by the unified-funding pipeline, never deleted."""
    feather_dir = tmp_path / "feathers"
    exporter = FreqtradeExporter(data_dir_with_funding, feather_dir)
    funding_parquet = data_dir_with_funding / "funding" / "arbitrum" / "rates" / "BTC" / "1h.parquet"
    assert funding_parquet.exists()

    exporter.export_funding(symbols=["BTC"], timeframes=["1h"])

    assert funding_parquet.exists(), (
        "export_funding deleted the funding parquet — must never delete other "
        "pipelines' source data."
    )
