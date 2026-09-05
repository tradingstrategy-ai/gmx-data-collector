"""Tests for atomic Parquet writes in ParquetStorage (C1).

Covers the 2026-08-25 incident's root cause: a write interrupted by a
HyperSync ``429``, timeout, or kill left a truncated Parquet file with no
footer magic bytes. ``_atomic_write_parquet()`` writes to a ``.tmp`` file and
``os.replace()``s onto the target only after a complete, fsynced write, so an
interruption can only ever strand the ``.tmp`` file -- never corrupt the
target.
"""

from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from gmx_historical_data.storage import ParquetStorage


def _make_candles(symbol: str, start: str, hours: int) -> pd.DataFrame:
    """Build a minimal valid OHLCV candle DataFrame for testing.

    :param symbol: Token symbol.
    :param start: Start timestamp (parseable by :func:`pandas.date_range`).
    :param hours: Number of hourly candles to generate.
    :returns: pandas DataFrame with the columns ``save_candles`` requires.
    """
    timestamps = pd.date_range(start, periods=hours, freq="1h", tz="UTC")
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


def _simulate_interrupted_write(monkeypatch: pytest.MonkeyPatch) -> None:
    """Monkeypatch ``pl.DataFrame.write_parquet`` to fail mid-write.

    Writes garbage bytes to the target path (simulating a partially-flushed
    OS page cache) and then raises, before ``_atomic_write_parquet`` ever
    reaches its fsync/``os.replace`` step -- exactly what a HyperSync ``429``
    or a process kill would do to a real write in flight.

    :param monkeypatch: pytest's monkeypatch fixture.
    """

    def _crash_mid_write(self: pl.DataFrame, path, *args, **kwargs) -> None:
        Path(path).write_bytes(b"TRUNCATED-NOT-A-REAL-PARQUET-FILE")
        raise OSError("simulated interruption mid-write")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", _crash_mid_write)


def test_atomic_write_leaves_no_corrupt_target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """An interrupted re-write leaves the previous target intact and readable."""
    storage = ParquetStorage(tmp_path)
    storage.save_candles(_make_candles("BTC", "2024-01-01", 3), "1h", "BTC")

    target = tmp_path / "candles" / "arbitrum" / "BTC" / "1h.parquet"
    assert target.exists()
    before = pl.read_parquet(target)

    _simulate_interrupted_write(monkeypatch)

    with pytest.raises(OSError, match="simulated interruption"):
        storage.save_candles(_make_candles("BTC", "2024-01-01 03:00:00", 2), "1h", "BTC")

    monkeypatch.undo()

    # The previous target is untouched and still fully, correctly readable --
    # never a truncated file with a missing footer.
    assert target.exists()
    after = pl.read_parquet(target)
    assert after.equals(before)

    # The interrupted write left only a stray .tmp file alongside it.
    tmp_files = list(target.parent.glob("*.parquet.tmp"))
    assert len(tmp_files) == 1
    assert tmp_files[0].name == "1h.parquet.tmp"

    # The startup sweep clears the orphan without touching the real target.
    removed = storage.sweep_orphaned_tmp_files()
    assert removed == tmp_files
    assert not list(target.parent.glob("*.parquet.tmp"))
    assert pl.read_parquet(target).equals(before)


def test_atomic_write_leaves_no_target_when_none_existed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """An interrupted first-ever write leaves no target at all -- never a
    partial/truncated one."""
    storage = ParquetStorage(tmp_path)
    _simulate_interrupted_write(monkeypatch)

    with pytest.raises(OSError, match="simulated interruption"):
        storage.save_candles(_make_candles("ETH", "2024-01-01", 3), "1h", "ETH")

    monkeypatch.undo()

    target = tmp_path / "candles" / "arbitrum" / "ETH" / "1h.parquet"
    assert not target.exists()
    assert (target.parent / "1h.parquet.tmp").exists()


def test_sweep_orphaned_tmp_files_removes_stray_tmp_only(tmp_path: Path):
    """The startup sweep removes stray .tmp files without touching real ones."""
    storage = ParquetStorage(tmp_path)
    storage.save_candles(_make_candles("BTC", "2024-01-01", 3), "1h", "BTC")

    symbol_dir = tmp_path / "candles" / "arbitrum" / "BTC"
    stray = symbol_dir / "4h.parquet.tmp"
    stray.write_bytes(b"leftover from an interrupted collection")

    removed = storage.sweep_orphaned_tmp_files()

    assert removed == [stray]
    assert not stray.exists()
    assert (symbol_dir / "1h.parquet").exists()


def test_sweep_orphaned_tmp_files_on_empty_store_returns_empty(tmp_path: Path):
    """Sweeping a base_dir that doesn't exist yet must not raise."""
    storage = ParquetStorage(tmp_path / "does-not-exist-yet")
    assert storage.sweep_orphaned_tmp_files() == []


def test_save_raw_events_and_position_events_are_also_atomic(tmp_path: Path):
    """The other two write paths in storage.py (raw events, position events)
    share save_candles's atomic-write helper -- this is a smoke test that
    they still produce valid, readable Parquet output after routing through
    it (the C1 fix note: "check other write paths ... for the same flaw")."""
    from gmx_historical_data.event_decoder import AnswerUpdatedEvent

    storage = ParquetStorage(tmp_path)
    events = [
        AnswerUpdatedEvent(
            block_number=1,
            block_timestamp=1_700_000_000,
            transaction_hash="0xabc",
            log_index=0,
            round_id=1,
            price=100_000_000,
            timestamp=1_700_000_000,
            aggregator_address="0xagg",
        )
    ]
    path = storage.save_raw_events(events, "ETH")
    assert path.exists()
    assert not path.with_name(path.name + ".tmp").exists()
    df = storage.read_raw_events("ETH")
    assert len(df) == 1
