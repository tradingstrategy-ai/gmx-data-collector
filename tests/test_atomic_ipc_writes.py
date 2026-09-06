"""Tests for atomic Feather/IPC writes (follow-up to C1/C2).

The exported feathers (``user_data/data/gmx/futures/*.feather``) are
themselves production artifacts -- they feed the downstream regime/drawdown
panel that gates live trading -- so an interrupted write must give them the
same atomicity guarantee as the source Parquet store. ``atomic_write_ipc()``
mirrors ``atomic_write_parquet()``'s tmp+fsync+``os.replace()`` dance for
Polars' IPC writer.
"""

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from gmx_historical_data.atomic_parquet import atomic_write_ipc
from gmx_historical_data.freqtrade_exporter import FreqtradeExporter
from gmx_historical_data.storage import ParquetStorage


def _simulate_interrupted_ipc_write(monkeypatch: pytest.MonkeyPatch) -> None:
    """Monkeypatch ``pl.DataFrame.write_ipc`` to fail mid-write.

    Same technique as the Parquet crash-mid-write tests: writes garbage
    bytes to the target path and then raises, before ``atomic_write_ipc``
    ever reaches its fsync/``os.replace`` commit step.

    :param monkeypatch: pytest's monkeypatch fixture.
    """

    def _crash_mid_write(self: pl.DataFrame, path, *args, **kwargs) -> None:
        Path(path).write_bytes(b"TRUNCATED-NOT-A-REAL-FEATHER-FILE")
        raise OSError("simulated interruption mid-write")

    monkeypatch.setattr(pl.DataFrame, "write_ipc", _crash_mid_write)


def test_atomic_write_ipc_leaves_no_corrupt_target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """An interrupted re-write leaves the previous feather target intact."""
    target = tmp_path / "ETH_USDC_USDC-1h-futures.feather"
    before_df = pl.DataFrame(
        {
            "date": pl.Series([datetime(2024, 1, 1, tzinfo=UTC)], dtype=pl.Datetime("ns", "UTC")),
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [0.0],
        }
    )
    atomic_write_ipc(before_df, target)
    assert target.exists()
    before = pl.read_ipc(target)

    _simulate_interrupted_ipc_write(monkeypatch)

    new_df = pl.DataFrame(
        {
            "date": pl.Series(
                [datetime(2024, 1, 1, 1, tzinfo=UTC)], dtype=pl.Datetime("ns", "UTC")
            ),
            "open": [2.0],
            "high": [2.0],
            "low": [2.0],
            "close": [2.0],
            "volume": [0.0],
        }
    )
    with pytest.raises(OSError, match="simulated interruption"):
        atomic_write_ipc(new_df, target)

    monkeypatch.undo()

    # The previous target is untouched and still fully readable.
    assert target.exists()
    assert pl.read_ipc(target).equals(before)

    # Only a stray .tmp file was left, not a corrupt target.
    tmp_files = list(target.parent.glob("*.feather.tmp"))
    assert len(tmp_files) == 1


def test_atomic_write_ipc_leaves_no_target_when_none_existed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """An interrupted first-ever feather write leaves no target at all."""
    target = tmp_path / "ETH_USDC_USDC-1h-futures.feather"
    _simulate_interrupted_ipc_write(monkeypatch)

    df = pl.DataFrame(
        {
            "date": pl.Series([datetime(2024, 1, 1, tzinfo=UTC)], dtype=pl.Datetime("ns", "UTC")),
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [0.0],
        }
    )
    with pytest.raises(OSError, match="simulated interruption"):
        atomic_write_ipc(df, target)

    monkeypatch.undo()

    assert not target.exists()
    assert (target.parent / f"{target.name}.tmp").exists()


def _make_candles(symbol: str, hours: int = 3) -> pd.DataFrame:
    """Build a minimal valid OHLCV candle DataFrame for testing.

    :param symbol: Token symbol.
    :param hours: Number of hourly candles to generate.
    :returns: pandas DataFrame with the columns ``save_candles`` requires.
    """
    timestamps = pd.date_range("2024-01-01", periods=hours, freq="1h", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": [1.0] * hours,
            "high": [1.0] * hours,
            "low": [1.0] * hours,
            "close": [1.0] * hours,
            "symbol": [symbol] * hours,
        }
    )


def test_freqtrade_exporter_single_feather_write_is_atomic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """End-to-end: FreqtradeExporter's single-format feather branch
    (``_write_single_frame`` -> ``atomic_write_ipc``) survives an
    interrupted write with no corrupt destination feather left behind."""
    data_dir = tmp_path / "data"
    storage = ParquetStorage(data_dir)
    storage.save_candles(_make_candles("ETH"), "1h", "ETH")

    output_dir = tmp_path / "output"
    exporter = FreqtradeExporter(data_dir, output_dir)

    # First export succeeds and creates a real destination feather.
    results, failed_symbols, failures = exporter.export_candles(symbols=["ETH"], timeframes=["1h"])
    assert failed_symbols == []
    target = output_dir / "gmx" / "futures" / "ETH_USDC_USDC-1h-futures.feather"
    assert target.exists()
    before = pl.read_ipc(target)

    # Re-export with a crash injected mid-write.
    _simulate_interrupted_ipc_write(monkeypatch)
    results, failed_symbols, failures = exporter.export_candles(symbols=["ETH"], timeframes=["1h"])
    monkeypatch.undo()

    # The symbol's write failed and was skipped -- not a silent abort of the
    # whole export, and not a corrupt destination file either.
    assert failed_symbols == ["ETH"]
    assert "ETH" not in results
    assert target.exists()
    assert pl.read_ipc(target).equals(before)
