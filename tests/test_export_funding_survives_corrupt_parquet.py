"""Tests for the per-symbol export guard on export_funding() (C2, part 2).

Mirrors test_export_survives_corrupt_parquet.py for the funding pipeline.
export_funding() reads via ``pl.read_parquet()`` directly (not the pandas +
pyarrow path ``export_candles()`` uses), so a truncated funding Parquet
raises ``polars.exceptions.ComputeError`` rather than ``ArrowInvalid`` --
this is exercised end-to-end here rather than assumed.
"""

import errno
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest
from typer.testing import CliRunner

from gmx_historical_data import freqtrade_exporter as fe_module
from gmx_historical_data.cli import app
from gmx_historical_data.freqtrade_exporter import FreqtradeExporter
from gmx_historical_data.ohlcv_validation import ExportValidationError


def _make_funding_df(hours: int = 3) -> pl.DataFrame:
    """Build a minimal valid funding-rate DataFrame for testing.

    :param hours: Number of hourly rows to generate.
    :returns: Polars DataFrame with ``timestamp`` and ``funding_rate_hourly``.
    """
    timestamps = pl.datetime_range(
        datetime(2026, 1, 1, tzinfo=UTC),
        datetime(2026, 1, 1, hours, tzinfo=UTC),
        interval="1h",
        time_zone="UTC",
        eager=True,
        closed="left",
    )
    return pl.DataFrame(
        {
            "timestamp": timestamps,
            "funding_rate": [1e-9] * hours,
            "funding_rate_hourly": [1e-6] * hours,
        }
    )


def _truncate_parquet_footer(path: Path) -> None:
    """Truncate a real Parquet file so it genuinely lacks footer magic bytes.

    :param path: Path to a valid Parquet file to corrupt in place.
    """
    data = path.read_bytes()
    assert len(data) > 20, "fixture parquet too small to truncate meaningfully"
    path.write_bytes(data[: len(data) // 2])


def _seed_funding_dir(data_dir: Path) -> None:
    """Seed a data dir with two healthy funding symbols and one corrupt one (BBB).

    :param data_dir: Root data directory to seed.
    """
    rates_dir = data_dir / "funding" / "arbitrum" / "rates"
    for symbol in ("AAA", "BBB", "CCC"):
        symbol_dir = rates_dir / symbol
        symbol_dir.mkdir(parents=True, exist_ok=True)
        _make_funding_df().write_parquet(symbol_dir / "1h.parquet")
    _truncate_parquet_footer(rates_dir / "BBB" / "1h.parquet")


def test_export_funding_survives_corrupt_parquet(tmp_path: Path):
    """One corrupt funding symbol is skipped; every healthy symbol still exports."""
    data_dir = tmp_path / "data"
    _seed_funding_dir(data_dir)

    output_dir = tmp_path / "output"
    exporter = FreqtradeExporter(data_dir, output_dir)

    results, failed_symbols, failures = exporter.export_funding(
        symbols=["AAA", "BBB", "CCC"], timeframes=["1h"]
    )

    assert failed_symbols == ["BBB"]
    assert "BBB" not in results
    assert set(results) == {"AAA", "CCC"}

    futures_dir = output_dir / "gmx" / "futures"
    assert (futures_dir / "AAA_USDC_USDC-1h-funding_rate.feather").exists()
    assert (futures_dir / "CCC_USDC_USDC-1h-funding_rate.feather").exists()
    assert not (futures_dir / "BBB_USDC_USDC-1h-funding_rate.feather").exists()


def test_export_wrapper_propagates_funding_failed_symbols(tmp_path: Path):
    """export() (the CLI's entry point) includes a funding-only failure in
    its returned failed_symbols union, not just candle failures."""
    data_dir = tmp_path / "data"
    _seed_funding_dir(data_dir)

    exporter = FreqtradeExporter(data_dir, tmp_path / "output")
    results, failed_symbols, failures = exporter.export(
        symbols=["AAA", "BBB", "CCC"], timeframes=["1h"]
    )

    assert failed_symbols == ["BBB"]
    assert set(results) == {"AAA", "CCC"}
    assert any(f.symbol == "BBB" for f in failures)


def test_export_funding_command_exits_nonzero_and_names_failed_symbol(tmp_path: Path):
    """The dedicated export-funding CLI command names the failed symbol and
    exits non-zero -- required so the existing cron alert keeps firing."""
    data_dir = tmp_path / "data"
    _seed_funding_dir(data_dir)

    output_dir = tmp_path / "output"
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "export-funding",
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
    assert (output_dir / "gmx" / "futures" / "AAA_USDC_USDC-1h-funding_rate.feather").exists()
    assert (output_dir / "gmx" / "futures" / "CCC_USDC_USDC-1h-funding_rate.feather").exists()


def test_export_freqtrade_command_exits_nonzero_on_corrupt_funding_only(tmp_path: Path):
    """A funding-only corruption (no candle data at all) still trips the
    combined export-freqtrade command's non-zero exit."""
    data_dir = tmp_path / "data"
    _seed_funding_dir(data_dir)

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


def test_export_funding_all_symbols_healthy_still_exits_zero(tmp_path: Path):
    """No regression: a funding export with no corrupt files still exits 0."""
    data_dir = tmp_path / "data"
    rates_dir = data_dir / "funding" / "arbitrum" / "rates"
    for symbol in ("AAA", "CCC"):
        symbol_dir = rates_dir / symbol
        symbol_dir.mkdir(parents=True, exist_ok=True)
        _make_funding_df().write_parquet(symbol_dir / "1h.parquet")

    output_dir = tmp_path / "output"
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "export-funding",
            "--data-dir",
            str(data_dir),
            "--output-dir",
            str(output_dir),
            "--timeframe",
            "1h",
        ],
    )

    assert result.exit_code == 0, result.output


def _seed_funding_symbol(data_dir: Path, symbol: str, df: pl.DataFrame) -> None:
    """Write a single funding-rate symbol's parquet directly (no truncation).

    :param data_dir: Root data directory.
    :param symbol: Token symbol.
    :param df: The funding-rate DataFrame to write as-is.
    """
    symbol_dir = data_dir / "funding" / "arbitrum" / "rates" / symbol
    symbol_dir.mkdir(parents=True, exist_ok=True)
    df.write_parquet(symbol_dir / "1h.parquet")


def test_export_funding_survives_validation_error(tmp_path: Path):
    """A validate_ohlcv failure (not corrupt bytes) is caught by the same
    guard as ArrowInvalid/ComputeError -- the exact defect this change fixes.

    Unlike export_candles's equivalent test, a duplicate-timestamp row can't
    be used to trigger this: ``_transform_funding_rate`` deduplicates on
    ``date`` (``keep="first"``) *before* ``validate_ohlcv`` ever runs, so a
    duplicate never reaches the guard for funding (this dedup is pre-existing
    behavior, out of scope for this change). Instead, an all-null rate column
    survives the parquet read (non-empty on disk) but is dropped entirely by
    the transform's ``drop_nulls(subset=["open"])``, leaving an empty frame
    that ``validate_ohlcv`` rejects.
    """
    data_dir = tmp_path / "data"
    for symbol in ("AAA", "CCC"):
        _seed_funding_symbol(data_dir, symbol, _make_funding_df())

    bad = _make_funding_df(hours=2).with_columns(
        pl.lit(None, dtype=pl.Float64).alias("funding_rate_hourly")
    )
    _seed_funding_symbol(data_dir, "BBB", bad)

    output_dir = tmp_path / "output"
    exporter = FreqtradeExporter(data_dir, output_dir)
    results, failed_symbols, failures = exporter.export_funding(
        symbols=["AAA", "BBB", "CCC"], timeframes=["1h"]
    )

    assert failed_symbols == ["BBB"]
    assert "BBB" not in results
    assert set(results) == {"AAA", "CCC"}
    assert len(failures) == 1
    assert failures[0].symbol == "BBB"
    assert failures[0].reason == "empty_frame"


def test_export_funding_survives_parity_mismatch(tmp_path: Path, monkeypatch):
    """A forced assert_export_parity failure on one symbol (output_format
    'both') is caught by the guard; other symbols still export."""
    data_dir = tmp_path / "data"
    for symbol in ("AAA", "BBB", "CCC"):
        _seed_funding_symbol(data_dir, symbol, _make_funding_df())

    from gmx_historical_data import ohlcv_validation

    real_assert_export_parity = ohlcv_validation.assert_export_parity

    def fake_assert_export_parity(left, right, *, location):
        if "BBB" in location:
            raise ExportValidationError(
                location, "parity_mismatch", f"{location}: forced parity mismatch for test"
            )
        return real_assert_export_parity(left, right, location=location)

    monkeypatch.setattr(fe_module, "assert_export_parity", fake_assert_export_parity)

    output_dir = tmp_path / "output"
    exporter = FreqtradeExporter(data_dir, output_dir)
    results, failed_symbols, failures = exporter.export_funding(
        symbols=["AAA", "BBB", "CCC"], timeframes=["1h"], output_format="both"
    )

    assert failed_symbols == ["BBB"]
    assert set(results) == {"AAA", "CCC"}
    assert len(failures) == 1
    assert failures[0].reason == "parity_mismatch"


def test_export_funding_aborts_on_enospc(tmp_path: Path, monkeypatch):
    """A fatal environment error must propagate, not be recorded as a
    per-symbol failure."""
    data_dir = tmp_path / "data"
    _seed_funding_symbol(data_dir, "AAA", _make_funding_df())

    def _raise_enospc(*args, **kwargs):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(fe_module, "atomic_write_ipc", _raise_enospc)

    exporter = FreqtradeExporter(data_dir, tmp_path / "output")
    with pytest.raises(OSError) as excinfo:
        exporter.export_funding(symbols=["AAA"], timeframes=["1h"])
    assert excinfo.value.errno == errno.ENOSPC


def test_export_funding_isolates_timeframes(tmp_path: Path):
    """D2: BBB fails on 8h only -- its 1h file still exists and is counted."""
    data_dir = tmp_path / "data"
    _seed_funding_symbol(data_dir, "BBB", _make_funding_df())  # healthy 1h

    # All-null rate column -> transform drops every row -> validate_ohlcv
    # raises "empty_frame" (see test_export_funding_survives_validation_error
    # for why a duplicate timestamp can't be used here).
    bad = _make_funding_df(hours=2).with_columns(
        pl.lit(None, dtype=pl.Float64).alias("funding_rate_hourly")
    )
    symbol_dir = data_dir / "funding" / "arbitrum" / "rates" / "BBB"
    bad.write_parquet(symbol_dir / "8h.parquet")

    output_dir = tmp_path / "output"
    exporter = FreqtradeExporter(data_dir, output_dir)
    results, failed_symbols, failures = exporter.export_funding(
        symbols=["BBB"], timeframes=["1h", "8h"]
    )

    assert failed_symbols == ["BBB"]
    assert "BBB" in results
    assert results["BBB"]["funding_files"] == 1
    assert len(failures) == 1
    assert failures[0].timeframe == "8h"
    futures_dir = output_dir / "gmx" / "futures"
    assert (futures_dir / "BBB_USDC_USDC-1h-funding_rate.feather").exists()
    assert not (futures_dir / "BBB_USDC_USDC-8h-funding_rate.feather").exists()
