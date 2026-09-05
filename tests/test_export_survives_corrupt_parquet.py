"""Tests for the per-symbol export guard (C2).

Reproduces the 2026-08-25 incident's second half: a truncated source Parquet
(no footer magic bytes) must cost exactly the one affected symbol, not the
whole export -- and the CLI must still exit non-zero so the downstream cron
alert keeps firing.
"""

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import polars as pl
import pytest
from typer.testing import CliRunner

from gmx_historical_data.cli import app
from gmx_historical_data.freqtrade_exporter import FreqtradeExporter
from gmx_historical_data.storage import ParquetStorage


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


def _truncate_parquet_footer(path: Path) -> None:
    """Truncate a real Parquet file so it genuinely lacks footer magic bytes.

    A valid Parquet file ends with an 8-byte footer-length field followed by
    the 4-byte ``PAR1`` magic. Cutting the file in half removes both,
    reproducing the exact corruption mode of the incident's
    ``GMX/1m.parquet`` and ``OP/1h.parquet`` (interrupted writes, no footer).

    :param path: Path to a valid Parquet file to corrupt in place.
    """
    data = path.read_bytes()
    assert len(data) > 20, "fixture parquet too small to truncate meaningfully"
    path.write_bytes(data[: len(data) // 2])


def _seed_data_dir(data_dir: Path) -> None:
    """Seed a data dir with two healthy symbols and one corrupt one (BBB).

    :param data_dir: Root data directory to seed.
    """
    storage = ParquetStorage(data_dir)
    for symbol in ("AAA", "BBB", "CCC"):
        storage.save_candles(_make_candles(symbol), "1h", symbol)
    _truncate_parquet_footer(data_dir / "candles" / "arbitrum" / "BBB" / "1h.parquet")


def test_export_survives_corrupt_parquet(tmp_path: Path):
    """One corrupt symbol is skipped; every healthy symbol still exports."""
    data_dir = tmp_path / "data"
    _seed_data_dir(data_dir)

    output_dir = tmp_path / "output"
    exporter = FreqtradeExporter(data_dir, output_dir)

    results, failed_symbols, failures = exporter.export_candles(
        symbols=["AAA", "BBB", "CCC"], timeframes=["1h"]
    )

    assert failed_symbols == ["BBB"]
    assert "BBB" not in results
    assert set(results) == {"AAA", "CCC"}

    futures_dir = output_dir / "gmx" / "futures"
    assert (futures_dir / "AAA_USDC_USDC-1h-futures.feather").exists()
    assert (futures_dir / "CCC_USDC_USDC-1h-futures.feather").exists()
    assert not (futures_dir / "BBB_USDC_USDC-1h-futures.feather").exists()


def _truncate_feather_footer(path: Path) -> None:
    """Truncate a real Feather/IPC file so it lacks a valid footer.

    :param path: Path to a valid Feather file to corrupt in place.
    """
    data = path.read_bytes()
    assert len(data) > 20, "fixture feather too small to truncate meaningfully"
    path.write_bytes(data[: len(data) // 2])


def test_export_survives_corrupt_destination_feather(tmp_path: Path):
    """A corrupt DESTINATION feather (not source) is caught by the same guard.

    Reproduces the asymmetry a follow-up review found: storage.read_candles()
    (the SOURCE read, inside export_candles()'s per-tf loop) uses pandas +
    pyarrow and raises ArrowInvalid on corruption, but the merge path inside
    _write() -- reached on every re-export where the destination already
    exists -- reads that DESTINATION via Polars (pl.read_ipc/pl.read_parquet)
    and raises pl.exceptions.ComputeError on the identical corruption. Before
    the guard used the shared CORRUPT_PARQUET_ERRORS tuple, a corrupt
    destination file would escape the (ArrowInvalid, OSError) clause and
    abort the whole export -- exactly the failure C2 exists to prevent,
    just entered from the output side instead of the input side.
    """
    data_dir = tmp_path / "data"
    storage = ParquetStorage(data_dir)
    for symbol in ("AAA", "BBB", "CCC"):
        storage.save_candles(_make_candles(symbol), "1h", symbol)  # all SOURCES healthy

    output_dir = tmp_path / "output"
    futures_dir = output_dir / "gmx" / "futures"
    futures_dir.mkdir(parents=True, exist_ok=True)

    # Pre-seed a valid destination feather for BBB, then corrupt it in place
    # -- this is what a re-export onto an already-corrupt destination looks
    # like (the merge path in _write() will try to read it).
    dest = futures_dir / "BBB_USDC_USDC-1h-futures.feather"
    pl.DataFrame(
        {
            "date": pl.Series([datetime(2024, 1, 1, tzinfo=UTC)], dtype=pl.Datetime("ns", "UTC")),
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [0.0],
        }
    ).write_ipc(dest, compression="zstd")
    _truncate_feather_footer(dest)

    exporter = FreqtradeExporter(data_dir, output_dir)
    results, failed_symbols, failures = exporter.export_candles(
        symbols=["AAA", "BBB", "CCC"], timeframes=["1h"]
    )

    assert failed_symbols == ["BBB"]
    assert "BBB" not in results
    assert set(results) == {"AAA", "CCC"}
    assert (futures_dir / "AAA_USDC_USDC-1h-futures.feather").exists()
    assert (futures_dir / "CCC_USDC_USDC-1h-futures.feather").exists()


def test_export_wrapper_propagates_failed_symbols(tmp_path: Path):
    """export() (the CLI's entry point) propagates export_candles()'s
    failed_symbols list unchanged."""
    data_dir = tmp_path / "data"
    _seed_data_dir(data_dir)

    exporter = FreqtradeExporter(data_dir, tmp_path / "output")
    results, failed_symbols, failures = exporter.export(
        symbols=["AAA", "BBB", "CCC"], timeframes=["1h"]
    )

    assert failed_symbols == ["BBB"]
    assert set(results) == {"AAA", "CCC"}


def test_export_freqtrade_command_exits_nonzero_and_names_failed_symbol(tmp_path: Path):
    """The CLI names the failed symbol in its output and exits non-zero --
    required so the existing 'done WITH ERRORS' cron alert keeps firing."""
    data_dir = tmp_path / "data"
    _seed_data_dir(data_dir)

    output_dir = tmp_path / "output"
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "export-freqtrade",
            "--data-dir",
            str(data_dir),
            "--output-dir",
            str(output_dir),
            "--timeframe",
            "1h",
        ],
    )

    assert result.exit_code != 0
    assert "BBB" in result.output
    # Healthy symbols still exported despite BBB's failure.
    assert (output_dir / "gmx" / "futures" / "AAA_USDC_USDC-1h-futures.feather").exists()
    assert (output_dir / "gmx" / "futures" / "CCC_USDC_USDC-1h-futures.feather").exists()


def test_export_candles_command_exits_nonzero_and_names_failed_symbol(tmp_path: Path):
    """Same guard applies to the candles-only CLI command."""
    data_dir = tmp_path / "data"
    _seed_data_dir(data_dir)

    output_dir = tmp_path / "output"
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "export-candles",
            "--data-dir",
            str(data_dir),
            "--output-dir",
            str(output_dir),
            "--timeframe",
            "1h",
        ],
    )

    assert result.exit_code != 0
    assert "BBB" in result.output
    assert (output_dir / "gmx" / "futures" / "AAA_USDC_USDC-1h-futures.feather").exists()


def test_export_all_symbols_healthy_still_exits_zero(tmp_path: Path):
    """No regression: an export with no corrupt files still exits 0 with no
    failure summary."""
    data_dir = tmp_path / "data"
    storage = ParquetStorage(data_dir)
    for symbol in ("AAA", "CCC"):
        storage.save_candles(_make_candles(symbol), "1h", symbol)

    output_dir = tmp_path / "output"
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "export-freqtrade",
            "--data-dir",
            str(data_dir),
            "--output-dir",
            str(output_dir),
            "--timeframe",
            "1h",
        ],
    )

    assert result.exit_code == 0, result.output


def _write_funding_style_duplicate_candles(
    data_dir: Path, symbol: str, timeframe: str = "1h"
) -> None:
    """Write a candle parquet directly with a duplicate timestamp row.

    Bypasses ``ParquetStorage.save_candles()`` (which validates on write) so
    the corruption is only visible when the *exporter* reads and validates
    it -- reproducing "upstream wrote something save_candles would have
    rejected, but it's on disk anyway" rather than a byte-level truncation.

    :param data_dir: Root data directory.
    :param symbol: Token symbol to seed with a duplicate-timestamp candle file.
    :param timeframe: Timeframe filename stem to write (e.g. ``'1h'``,
        ``'4h'``). Both map to themselves under ``TIMEFRAME_TO_FILENAME``, so
        the filename is exactly ``f"{timeframe}.parquet"``.
    """
    candle_dir = data_dir / "candles" / "arbitrum" / symbol
    candle_dir.mkdir(parents=True, exist_ok=True)
    timestamps = pd.date_range("2024-01-01", periods=3, freq=timeframe, tz="UTC")
    df = pd.DataFrame(
        {
            "timestamp": [timestamps[0], timestamps[0], timestamps[1]],
            "open": [1.0, 1.0, 1.0],
            "high": [1.0, 1.0, 1.0],
            "low": [1.0, 1.0, 1.0],
            "close": [1.0, 1.0, 1.0],
        }
    )
    df.to_parquet(candle_dir / f"{timeframe}.parquet", index=False)


def test_export_candles_shares_taxonomy(tmp_path: Path):
    """A validate_ohlcv failure (not a corrupt-bytes failure) is caught by
    the same per-symbol guard as ArrowInvalid -- the exact defect this
    change fixes. Before this change, this ValueError would propagate and
    abort the whole export."""
    data_dir = tmp_path / "data"
    storage = ParquetStorage(data_dir)
    for symbol in ("AAA", "CCC"):
        storage.save_candles(_make_candles(symbol), "1h", symbol)
    _write_funding_style_duplicate_candles(data_dir, "BBB")

    output_dir = tmp_path / "output"
    exporter = FreqtradeExporter(data_dir, output_dir)
    results, failed_symbols, failures = exporter.export_candles(
        symbols=["AAA", "BBB", "CCC"], timeframes=["1h"]
    )

    assert failed_symbols == ["BBB"]
    assert "BBB" not in results
    assert set(results) == {"AAA", "CCC"}
    assert len(failures) == 1
    assert failures[0].symbol == "BBB"
    assert failures[0].timeframe == "1h"
    assert failures[0].reason == "duplicate_timestamps"


def test_export_candles_isolates_timeframes(tmp_path: Path):
    """D2: a symbol failing on one timeframe still gets its other
    timeframe's files counted in results, not discarded."""
    data_dir = tmp_path / "data"
    storage = ParquetStorage(data_dir)
    storage.save_candles(_make_candles("BBB"), "1h", "BBB")  # healthy 1h
    _write_funding_style_duplicate_candles(data_dir, "BBB", timeframe="4h")  # corrupt 4h

    output_dir = tmp_path / "output"
    exporter = FreqtradeExporter(data_dir, output_dir)
    results, failed_symbols, failures = exporter.export_candles(
        symbols=["BBB"], timeframes=["1h", "4h"]
    )

    assert failed_symbols == ["BBB"]
    assert "BBB" in results
    assert results["BBB"]["ohlcv_files"] == 1
    assert len(failures) == 1
    assert failures[0].timeframe == "4h"
    futures_dir = output_dir / "gmx" / "futures"
    assert (futures_dir / "BBB_USDC_USDC-1h-futures.feather").exists()
    assert not (futures_dir / "BBB_USDC_USDC-4h-futures.feather").exists()


def test_export_candles_aborts_on_enospc(tmp_path: Path, monkeypatch):
    """A fatal environment error (disk full) must propagate, not be
    swallowed into failed_symbols -- continuing would mark every remaining
    symbol 'failed' one at a time for a condition that isn't about their data."""
    import errno

    from gmx_historical_data import freqtrade_exporter as fe_module

    data_dir = tmp_path / "data"
    storage = ParquetStorage(data_dir)
    storage.save_candles(_make_candles("AAA"), "1h", "AAA")

    def _raise_enospc(*args, **kwargs):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(fe_module, "atomic_write_ipc", _raise_enospc)

    exporter = FreqtradeExporter(data_dir, tmp_path / "output")
    with pytest.raises(OSError) as excinfo:
        exporter.export_candles(symbols=["AAA"], timeframes=["1h"])
    assert excinfo.value.errno == errno.ENOSPC
