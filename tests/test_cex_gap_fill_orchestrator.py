"""End-to-end test for fill_gaps_from_cex with mocked freqtrade subprocess."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from gmx_historical_data.cex_gap_fill import fill_gaps_from_cex


def _write_gmx_parquet(path: Path, prices: list[float], volumes: list[float]) -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    ts = [start + timedelta(hours=i) for i in range(len(prices))]
    df = pl.DataFrame({
        "timestamp": ts,
        "open": prices, "high": prices, "low": prices, "close": prices,
        "volume": volumes,
    })
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def _write_cex_feather(path: Path, prices: list[float], volumes: list[float]) -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    ts = [start + timedelta(hours=i) for i in range(len(prices))]
    df = pl.DataFrame({
        "date": ts,
        "open": prices, "high": prices, "low": prices, "close": prices,
        "volume": volumes,
    })
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_ipc(path, compression=None)


def test_fill_gaps_from_cex_end_to_end(tmp_path: Path):
    data_dir = tmp_path / "user_data"
    cex_datadir = tmp_path / "cex"

    parquet_path = data_dir / "candles" / "arbitrum" / "BTC" / "1h.parquet"
    _write_gmx_parquet(parquet_path, [100, 100, 200, 200, 200], [1, 1, 1, 1, 1])

    cex_feather = cex_datadir / "binance" / "futures" / "BTC_USDT_USDT-1h-futures.feather"
    _write_cex_feather(cex_feather, [100, 101, 102, 103, 104], [10, 10, 10, 10, 10])

    routing_file = tmp_path / "cex_routing.json"
    routing_file.write_text(
        '{"version":1,"defaults":{"primary":"binance","fallback":"bybit","skip_unresolved":true},'
        '"overrides":{"BTC":{"exchange":"binance","pair":"BTC/USDT:USDT"}},"auto":{}}'
    )

    with patch("gmx_historical_data.cex_gap_fill.freqtrade_runner.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout=b"", stderr=b"")
        summary = fill_gaps_from_cex(
            data_dir=data_dir,
            symbols=["BTC"],
            timeframes=["1h"],
            routing_file=routing_file,
            cex_datadir=cex_datadir,
            exchanges=["binance"],
            gap_threshold=0.20,
            merge_gap_bars=0,
            log_dir=tmp_path / "logs",
            dry_run=False,
        )

    out = pl.read_parquet(parquet_path)
    assert out["close"][2] == pytest.approx(102)
    assert summary.symbols_processed == 1
    assert summary.totals["full_replaced"] >= 1


def test_fill_gaps_from_cex_dry_run_does_not_write(tmp_path: Path):
    data_dir = tmp_path / "user_data"
    cex_datadir = tmp_path / "cex"

    parquet_path = data_dir / "candles" / "arbitrum" / "BTC" / "1h.parquet"
    _write_gmx_parquet(parquet_path, [100, 100, 200, 200, 200], [1, 1, 1, 1, 1])

    cex_feather = cex_datadir / "binance" / "futures" / "BTC_USDT_USDT-1h-futures.feather"
    _write_cex_feather(cex_feather, [100, 101, 102, 103, 104], [10, 10, 10, 10, 10])

    routing_file = tmp_path / "cex_routing.json"
    routing_file.write_text(
        '{"version":1,"defaults":{"primary":"binance","fallback":"bybit","skip_unresolved":true},'
        '"overrides":{"BTC":{"exchange":"binance","pair":"BTC/USDT:USDT"}},"auto":{}}'
    )

    with patch("gmx_historical_data.cex_gap_fill.freqtrade_runner.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout=b"", stderr=b"")
        fill_gaps_from_cex(
            data_dir=data_dir,
            symbols=["BTC"],
            timeframes=["1h"],
            routing_file=routing_file,
            cex_datadir=cex_datadir,
            exchanges=["binance"],
            gap_threshold=0.20,
            merge_gap_bars=0,
            log_dir=tmp_path / "logs",
            dry_run=True,
        )

    out = pl.read_parquet(parquet_path)
    assert out["close"][2] == pytest.approx(200)


def test_fill_gaps_from_cex_no_volume_column_in_parquet(tmp_path: Path):
    """GMX oracle parquets have no volume column — orchestrator must synthesise zero vol."""
    data_dir = tmp_path / "user_data"
    cex_datadir = tmp_path / "cex"

    start = datetime(2026, 1, 1, tzinfo=UTC)
    ts = [start + timedelta(hours=i) for i in range(5)]
    parquet_path = data_dir / "candles" / "arbitrum" / "BTC" / "1h.parquet"
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "timestamp": ts,
        "open": [100.0, 100.0, 200.0, 200.0, 200.0],
        "high": [100.0, 100.0, 200.0, 200.0, 200.0],
        "low":  [100.0, 100.0, 200.0, 200.0, 200.0],
        "close":[100.0, 100.0, 200.0, 200.0, 200.0],
    }).write_parquet(parquet_path)

    cex_feather = cex_datadir / "binance" / "futures" / "BTC_USDT_USDT-1h-futures.feather"
    cex_feather.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "date": ts,
        "open": [100.0, 101.0, 102.0, 103.0, 104.0],
        "high": [100.0, 101.0, 102.0, 103.0, 104.0],
        "low":  [100.0, 101.0, 102.0, 103.0, 104.0],
        "close":[100.0, 101.0, 102.0, 103.0, 104.0],
        "volume":[10.0, 10.0, 10.0, 10.0, 10.0],
    }).write_ipc(cex_feather, compression=None)

    routing_file = tmp_path / "cex_routing.json"
    routing_file.write_text(
        '{"version":1,"defaults":{"primary":"binance","fallback":"bybit","skip_unresolved":true},'
        '"overrides":{"BTC":{"exchange":"binance","pair":"BTC/USDT:USDT"}},"auto":{}}'
    )

    with patch("gmx_historical_data.cex_gap_fill.freqtrade_runner.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout=b"", stderr=b"")
        summary = fill_gaps_from_cex(
            data_dir=data_dir,
            symbols=["BTC"],
            timeframes=["1h"],
            routing_file=routing_file,
            cex_datadir=cex_datadir,
            exchanges=["binance"],
            gap_threshold=0.20,
            merge_gap_bars=0,
            log_dir=tmp_path / "logs",
            dry_run=False,
        )

    out = pl.read_parquet(parquet_path)
    assert "volume" in out.columns
    assert out["volume"][2] == pytest.approx(10.0)
    assert summary.symbols_processed == 1


def test_fill_gaps_from_cex_skip_symbol_with_no_route(tmp_path: Path):
    data_dir = tmp_path / "user_data"
    cex_datadir = tmp_path / "cex"

    parquet_path = data_dir / "candles" / "arbitrum" / "GMX" / "1h.parquet"
    _write_gmx_parquet(parquet_path, [10, 10, 20, 20], [1, 1, 1, 1])

    routing_file = tmp_path / "cex_routing.json"
    routing_file.write_text(
        '{"version":1,"defaults":{"primary":"binance","fallback":"bybit","skip_unresolved":true},'
        '"overrides":{},"auto":{}}'
    )

    with patch("gmx_historical_data.cex_gap_fill.freqtrade_runner.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout=b"", stderr=b"")
        summary = fill_gaps_from_cex(
            data_dir=data_dir,
            symbols=["GMX"],
            timeframes=["1h"],
            routing_file=routing_file,
            cex_datadir=cex_datadir,
            exchanges=["binance"],
            gap_threshold=0.20,
            merge_gap_bars=0,
            log_dir=tmp_path / "logs",
            dry_run=False,
        )

    assert "GMX" in summary.symbols_skipped_no_cex
    out = pl.read_parquet(parquet_path)
    assert out["close"][2] == pytest.approx(20)
