"""Tests for Freqtrade exporter."""

import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from gmx_historical_data.freqtrade_exporter import FreqtradeExporter
from gmx_historical_data.storage import ParquetStorage


@pytest.fixture
def sample_storage():
    """Create storage with sample data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = ParquetStorage(Path(tmpdir))

        # Create test candles for ETH
        df_eth = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    [
                        "2024-01-01 00:00:00",
                        "2024-01-01 01:00:00",
                        "2024-01-01 02:00:00",
                    ],
                    utc=True,
                ),
                "open": [2000.0, 2010.0, 2005.0],
                "high": [2050.0, 2060.0, 2055.0],
                "low": [1990.0, 2000.0, 1995.0],
                "close": [2010.0, 2005.0, 2020.0],
                "symbol": ["ETH", "ETH", "ETH"],
            }
        )
        storage.save_candles(df_eth, "1h", "ETH")
        storage.save_candles(df_eth, "4h", "ETH")

        # Create test candles for BTC
        df_btc = df_eth.assign(
            symbol="BTC",
            open=[40000.0, 40100.0, 40050.0],
            high=[40500.0, 40600.0, 40550.0],
            low=[39900.0, 40000.0, 39950.0],
            close=[40100.0, 40050.0, 40200.0],
        )
        storage.save_candles(df_btc, "1h", "BTC")

        yield Path(tmpdir)


def test_export_creates_freqtrade_format(sample_storage):
    """Test export creates files with correct freqtrade futures format."""
    with tempfile.TemporaryDirectory() as output_dir:
        exporter = FreqtradeExporter(sample_storage, Path(output_dir))
        exporter.export()

        # Check files created in gmx/futures subdirectory
        futures_dir = Path(output_dir) / "gmx" / "futures"
        assert futures_dir.exists()

        # Futures format: BASE_QUOTE_SETTLE-timeframe-futures.feather
        eth_1h = futures_dir / "ETH_USDC_USDC-1h-futures.feather"
        assert eth_1h.exists()

        # Read and verify format
        df = pd.read_feather(eth_1h)
        assert list(df.columns) == ["date", "open", "high", "low", "close", "volume"]
        assert len(df) == 3
        assert df["volume"].iloc[0] == 0.0


def test_export_specific_symbols(sample_storage):
    """Test export filters by symbol."""
    with tempfile.TemporaryDirectory() as output_dir:
        exporter = FreqtradeExporter(sample_storage, Path(output_dir))
        exporter.export(symbols=["ETH"])

        futures_dir = Path(output_dir) / "gmx" / "futures"
        assert (futures_dir / "ETH_USDC_USDC-1h-futures.feather").exists()
        assert not (futures_dir / "BTC_USDC_USDC-1h-futures.feather").exists()


def test_export_specific_timeframes(sample_storage):
    """Test export filters by timeframe."""
    with tempfile.TemporaryDirectory() as output_dir:
        exporter = FreqtradeExporter(sample_storage, Path(output_dir))
        exporter.export(timeframes=["1h"])

        futures_dir = Path(output_dir) / "gmx" / "futures"
        assert (futures_dir / "ETH_USDC_USDC-1h-futures.feather").exists()
        assert not (futures_dir / "ETH_USDC_USDC-4h-futures.feather").exists()


def test_export_returns_stats(sample_storage):
    """Test export returns statistics."""
    with tempfile.TemporaryDirectory() as output_dir:
        exporter = FreqtradeExporter(sample_storage, Path(output_dir))
        result, failed_symbols, failures = exporter.export()

        assert failed_symbols == []
        assert failures == []
        assert "ETH" in result
        assert "BTC" in result
        # ETH: 1h + 4h = 2 ohlcv + 2 mark + 2 index = 6
        assert result["ETH"]["ohlcv_files"] == 2
        assert result["ETH"]["mark_files"] == 2
        assert result["ETH"]["index_files"] == 2
        assert result["ETH"]["files"] == 6
        # BTC: 1h = 1 ohlcv + 1 mark + 1 index = 3
        assert result["BTC"]["ohlcv_files"] == 1
        assert result["BTC"]["mark_files"] == 1
        assert result["BTC"]["index_files"] == 1
        assert result["BTC"]["files"] == 3


def test_export_creates_index_files(sample_storage):
    """Test export creates index price files matching mark price data."""
    with tempfile.TemporaryDirectory() as output_dir:
        exporter = FreqtradeExporter(sample_storage, Path(output_dir))
        exporter.export()

        futures_dir = Path(output_dir) / "gmx" / "futures"
        index_file = futures_dir / "ETH_USDC_USDC-1h-index.feather"
        mark_file = futures_dir / "ETH_USDC_USDC-1h-mark.feather"

        assert index_file.exists()

        df_index = pd.read_feather(index_file)
        df_mark = pd.read_feather(mark_file)
        pd.testing.assert_frame_equal(df_index, df_mark)


def test_date_column_is_datetime(sample_storage):
    """Test date column is proper datetime type."""
    with tempfile.TemporaryDirectory() as output_dir:
        exporter = FreqtradeExporter(sample_storage, Path(output_dir))
        exporter.export()

        df = pd.read_feather(
            Path(output_dir) / "gmx" / "futures" / "ETH_USDC_USDC-1h-futures.feather"
        )
        assert pd.api.types.is_datetime64_any_dtype(df["date"])


def test_export_preserves_existing_longer_history(tmp_path):
    """Test that re-exporting with newer data merges and keeps oldest timestamp."""
    storage_dir = tmp_path / "data"
    storage_dir.mkdir()
    storage = ParquetStorage(storage_dir)

    # Save candles that start in 2024 (the "older" data)
    older_data = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01", "2024-01-02"], utc=True),
            "open": [1.0, 2.0],
            "high": [1.0, 2.0],
            "low": [1.0, 2.0],
            "close": [1.0, 2.0],
            "symbol": ["ETH", "ETH"],
        }
    )
    storage.save_candles(older_data, "1h", "ETH")

    output_dir = tmp_path / "output"
    output_dir.mkdir()
    exporter = FreqtradeExporter(storage_dir, output_dir)

    # First export — creates the feather file
    exporter.export(symbols=["ETH"], timeframes=["1h"])

    # Now add newer candles to storage
    newer_data = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-03", "2024-01-04"], utc=True),
            "open": [3.0, 4.0],
            "high": [3.0, 4.0],
            "low": [3.0, 4.0],
            "close": [3.0, 4.0],
            "symbol": ["ETH", "ETH"],
        }
    )
    storage.save_candles(newer_data, "1h", "ETH")

    # Second export — should merge with existing feather, not replace it
    exporter.export(symbols=["ETH"], timeframes=["1h"])

    # Find the feather file
    feather_files = list(output_dir.rglob("*.feather"))
    assert len(feather_files) >= 1
    # Find the OHLCV futures feather
    futures_feathers = [f for f in feather_files if "futures" in f.name]
    assert len(futures_feathers) == 1
    result = pd.read_feather(futures_feathers[0])
    assert result["date"].min() == pd.Timestamp("2024-01-01", tz="UTC")
    assert result["date"].max() == pd.Timestamp("2024-01-04", tz="UTC")


def test_export_raises_when_existing_feather_cannot_be_read(tmp_path, monkeypatch):
    """Test that export raises when the existing feather cannot be read."""
    storage_dir = tmp_path / "data"
    storage_dir.mkdir()
    storage = ParquetStorage(storage_dir)

    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01"], utc=True),
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "symbol": ["ETH"],
        }
    )
    storage.save_candles(df, "1h", "ETH")

    output_dir = tmp_path / "output"
    output_dir.mkdir()
    exporter = FreqtradeExporter(storage_dir, output_dir)
    # keep_parquet=True so the source parquet survives for the second export
    exporter.export(symbols=["ETH"], timeframes=["1h"], keep_parquet=True)

    def boom(*_args, **_kwargs):
        raise RuntimeError("cannot read existing feather")

    # Patch the IPC reader used inside freqtrade_exporter
    monkeypatch.setattr("gmx_historical_data.freqtrade_exporter.pl.read_ipc", boom)

    with pytest.raises(RuntimeError, match="cannot read existing feather"):
        exporter.export(symbols=["ETH"], timeframes=["1h"], keep_parquet=True)


def _freqtrade_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date": pl.datetime_range(
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 1, 1, 3, tzinfo=UTC),
                interval="1h",
                time_zone="UTC",
                eager=True,
                closed="left",
            ).cast(pl.Datetime("ns", "UTC")),
            "open": [1.0, 2.0, 3.0],
            "high": [1.5, 2.5, 3.5],
            "low": [0.5, 1.5, 2.5],
            "close": [1.25, 2.25, 3.25],
            "volume": [0.0, 0.0, 0.0],
        }
    )


def test_export_candles_both_writes_equivalent_files(sample_storage, tmp_path):
    exporter = FreqtradeExporter(sample_storage, tmp_path / "output")
    exporter.export_candles(symbols=["ETH"], timeframes=["1h"], output_format="both")

    gmx_dir = tmp_path / "output" / "gmx" / "futures"
    feather = pl.read_ipc(gmx_dir / "ETH_USDC_USDC-1h-futures.feather")
    parquet = pl.read_parquet(gmx_dir / "ETH_USDC_USDC-1h-futures.parquet")

    assert feather.equals(parquet)


def test_export_candles_both_aborts_when_existing_file_is_corrupt(sample_storage, tmp_path):
    exporter = FreqtradeExporter(sample_storage, tmp_path / "output")
    gmx_dir = tmp_path / "output" / "gmx" / "futures"
    gmx_dir.mkdir(parents=True, exist_ok=True)

    corrupt = gmx_dir / "ETH_USDC_USDC-1h-futures.parquet"
    pl.DataFrame(
        {
            "date": pl.Series([datetime(2026, 1, 1, tzinfo=UTC)], dtype=pl.Datetime("ns", "UTC")),
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [float("inf")],
            "volume": [0.0],
        }
    ).write_parquet(corrupt)

    with pytest.raises(ValueError, match="invalid OHLCV|parity mismatch"):
        exporter.export_candles(symbols=["ETH"], timeframes=["1h"], output_format="both")


def test_export_candles_both_leaves_existing_files_intact_on_second_write_failure(
    sample_storage, tmp_path, monkeypatch
):
    exporter = FreqtradeExporter(sample_storage, tmp_path / "output")
    gmx_dir = tmp_path / "output" / "gmx" / "futures"
    gmx_dir.mkdir(parents=True, exist_ok=True)

    seed = _freqtrade_frame()
    feather_path = gmx_dir / "ETH_USDC_USDC-1h-futures.feather"
    parquet_path = gmx_dir / "ETH_USDC_USDC-1h-futures.parquet"
    seed.write_ipc(feather_path)
    seed.write_parquet(parquet_path)

    feather_before = pl.read_ipc(feather_path)
    parquet_before = pl.read_parquet(parquet_path)

    original_write_single_frame = FreqtradeExporter._write_single_frame

    def boom(self, df, path, fmt):
        if fmt == "parquet" and path.suffix == ".tmp":
            raise RuntimeError("boom during parquet write")
        return original_write_single_frame(self, df, path, fmt)

    monkeypatch.setattr(FreqtradeExporter, "_write_single_frame", boom)

    with pytest.raises(RuntimeError, match="boom during parquet write"):
        exporter.export_candles(symbols=["ETH"], timeframes=["1h"], output_format="both")

    assert pl.read_ipc(feather_path).equals(feather_before)
    assert pl.read_parquet(parquet_path).equals(parquet_before)


def test_export_candles_both_rolls_back_when_second_publish_fails(
    sample_storage, tmp_path, monkeypatch
):
    """A publish failure is caught by the per-symbol guard (C2): the symbol is
    recorded in ``failed_symbols`` and skipped rather than raising, and the
    on-disk targets are left exactly as they were (no partial/truncated write)
    -- the same invariant the atomic-write fix (C1) gives ``storage.py``.
    """
    exporter = FreqtradeExporter(sample_storage, tmp_path / "output")
    gmx_dir = tmp_path / "output" / "gmx" / "futures"
    gmx_dir.mkdir(parents=True, exist_ok=True)

    seed = _freqtrade_frame()
    feather_path = gmx_dir / "ETH_USDC_USDC-1h-futures.feather"
    parquet_path = gmx_dir / "ETH_USDC_USDC-1h-futures.parquet"
    seed.write_ipc(feather_path)
    seed.write_parquet(parquet_path)
    feather_before = pl.read_ipc(feather_path)
    parquet_before = pl.read_parquet(parquet_path)

    original_replace = Path.replace
    failed = False

    def fail_once_on_parquet_publish(path, target):
        nonlocal failed
        if Path(target) == parquet_path and path.suffix == ".tmp" and not failed:
            failed = True
            raise OSError("publish failed")
        return original_replace(path, target)

    monkeypatch.setattr(Path, "replace", fail_once_on_parquet_publish)

    results, failed_symbols, failures = exporter.export_candles(
        symbols=["ETH"], timeframes=["1h"], output_format="both"
    )

    assert failed_symbols == ["ETH"]
    assert "ETH" not in results
    assert pl.read_ipc(feather_path).equals(feather_before)
    assert pl.read_parquet(parquet_path).equals(parquet_before)


def test_export_funding_accepts_negative_rates(tmp_path):
    """Negative funding rates (majority of markets) must export, not raise."""
    data_dir = tmp_path / "data"
    funding_dir = data_dir / "funding" / "arbitrum" / "rates" / "AAVE"
    funding_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "timestamp": pl.datetime_range(
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 1, 1, 3, tzinfo=UTC),
                interval="1h",
                time_zone="UTC",
                eager=True,
                closed="left",
            ),
            "funding_rate": [1e-9, -2e-9, 3e-9],
            "funding_rate_hourly": [1e-6, -2e-6, 3e-6],
        }
    ).write_parquet(funding_dir / "1h.parquet")

    exporter = FreqtradeExporter(data_dir, tmp_path / "output")
    result, failed_symbols, failures = exporter.export_funding(symbols=["AAVE"], timeframes=["1h"])

    out = tmp_path / "output" / "gmx" / "futures" / "AAVE_USDC_USDC-1h-funding_rate.feather"
    assert out.exists()
    written = pl.read_ipc(out)
    assert written["open"].min() < 0  # negative funding rate preserved
    assert result["AAVE"]["funding_files"] == 1
    assert failed_symbols == []


def test_export_funding_both_counts_both_files(tmp_path):
    data_dir = tmp_path / "data"
    funding_dir = data_dir / "funding" / "arbitrum" / "rates" / "AAVE"
    funding_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "timestamp": pl.Series(
                [datetime(2026, 1, 1, tzinfo=UTC)], dtype=pl.Datetime("ns", "UTC")
            ),
            "funding_rate": [1e-9],
            "funding_rate_hourly": [1e-6],
        }
    ).write_parquet(funding_dir / "1h.parquet")

    result, failed_symbols, failures = FreqtradeExporter(
        data_dir, tmp_path / "output"
    ).export_funding(symbols=["AAVE"], timeframes=["1h"], output_format="both")

    assert result["AAVE"]["funding_files"] == 2
    assert failed_symbols == []
    out = tmp_path / "output" / "gmx" / "futures"
    assert (out / "AAVE_USDC_USDC-1h-funding_rate.feather").exists()
    assert (out / "AAVE_USDC_USDC-1h-funding_rate.parquet").exists()


def test_export_funding_both_counts_two_files_per_timeframe(tmp_path):
    """output_format='both' writes a feather AND a parquet per timeframe.

    The ``funding_files`` counter must reflect both files written, matching
    the sibling ``ohlcv_files``/``mark_files``/``index_files`` counters in
    :meth:`FreqtradeExporter.export_candles`.
    """
    data_dir = tmp_path / "data"
    funding_dir = data_dir / "funding" / "arbitrum" / "rates" / "AAVE"
    funding_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "timestamp": pl.datetime_range(
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 1, 1, 3, tzinfo=UTC),
                interval="1h",
                time_zone="UTC",
                eager=True,
                closed="left",
            ),
            "funding_rate_hourly": [1e-6, -2e-6, 3e-6],
        }
    ).write_parquet(funding_dir / "1h.parquet")

    exporter = FreqtradeExporter(data_dir, tmp_path / "output")
    result, failed_symbols, failures = exporter.export_funding(
        symbols=["AAVE"], timeframes=["1h"], output_format="both"
    )

    gmx_dir = tmp_path / "output" / "gmx" / "futures"
    feather = gmx_dir / "AAVE_USDC_USDC-1h-funding_rate.feather"
    parquet = gmx_dir / "AAVE_USDC_USDC-1h-funding_rate.parquet"
    assert feather.exists()
    assert parquet.exists()
    assert result["AAVE"]["funding_files"] == 2
    assert failed_symbols == []


def test_export_candles_both_unsafe_overwrite_regenerates_corrupt_file(sample_storage, tmp_path):
    """--unsafe-overwrite must be able to replace a corrupt existing destination."""
    exporter = FreqtradeExporter(sample_storage, tmp_path / "output")
    gmx_dir = tmp_path / "output" / "gmx" / "futures"
    gmx_dir.mkdir(parents=True, exist_ok=True)

    corrupt = gmx_dir / "ETH_USDC_USDC-1h-futures.parquet"
    pl.DataFrame(
        {
            "date": pl.Series([datetime(2026, 1, 1, tzinfo=UTC)], dtype=pl.Datetime("ns", "UTC")),
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [float("inf")],
            "volume": [0.0],
        }
    ).write_parquet(corrupt)

    exporter.export_candles(
        symbols=["ETH"], timeframes=["1h"], output_format="both", unsafe_overwrite=True
    )

    feather = pl.read_ipc(gmx_dir / "ETH_USDC_USDC-1h-futures.feather")
    parquet = pl.read_parquet(corrupt)
    assert feather.equals(parquet)
    assert parquet.filter(~pl.col("close").is_finite()).height == 0
