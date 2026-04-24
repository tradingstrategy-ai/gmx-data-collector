"""Regression test: importing and running cex_gap_fill does not corrupt legacy exporter output.

Verifies that fill_gaps_from_cex with dry_run=True leaves the source parquet
byte-for-byte unchanged, and that the FreqtradeExporter produces identical
data values before and after the cex_gap_fill module is imported.
"""

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest


def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _seed_parquet(path: Path) -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    df = pl.DataFrame({
        "timestamp": [start + timedelta(hours=i) for i in range(10)],
        "open":   [float(100 + i) for i in range(10)],
        "high":   [float(101 + i) for i in range(10)],
        "low":    [float( 99 + i) for i in range(10)],
        "close":  [float(100 + i) + 0.5 for i in range(10)],
        "volume": [float(i + 1) for i in range(10)],
    })
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def test_dry_run_leaves_parquet_byte_identical(tmp_path: Path):
    data_dir = tmp_path / "data"
    parquet_path = data_dir / "candles" / "arbitrum" / "BTC" / "1h.parquet"
    _seed_parquet(parquet_path)
    sha_before = _sha(parquet_path)

    routing_file = tmp_path / "routing.json"
    routing_file.write_text(
        '{"version":1,"defaults":{"primary":"binance","fallback":"bybit","skip_unresolved":true},'
        '"overrides":{"BTC":{"exchange":"binance","pair":"BTC/USDT:USDT"}},"auto":{}}'
    )

    from gmx_historical_data.cex_gap_fill import fill_gaps_from_cex

    with patch("gmx_historical_data.cex_gap_fill.freqtrade_runner.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout=b"", stderr=b"")
        fill_gaps_from_cex(
            data_dir=data_dir,
            symbols=["BTC"],
            timeframes=["1h"],
            routing_file=routing_file,
            cex_datadir=tmp_path / "cex",
            exchanges=["binance"],
            gap_threshold=0.20,
            merge_gap_bars=0,
            log_dir=tmp_path / "logs",
            dry_run=True,
        )

    assert _sha(parquet_path) == sha_before


def test_import_cex_gap_fill_does_not_affect_exporter_data(tmp_path: Path):
    from gmx_historical_data.freqtrade_exporter import FreqtradeExporter

    data_dir = tmp_path / "data"
    parquet_path = data_dir / "candles" / "arbitrum" / "ETH" / "1h.parquet"
    _seed_parquet(parquet_path)

    out1 = tmp_path / "out1"
    FreqtradeExporter(data_dir=data_dir, output_dir=out1).export(
        symbols=["ETH"], timeframes=["1h"], keep_parquet=True
    )
    feather1 = out1 / "gmx" / "futures" / "ETH_USDC_USDC-1h-futures.feather"
    assert feather1.exists()
    data1 = pl.read_ipc(feather1)

    import gmx_historical_data.cex_gap_fill  # noqa: F401 — verify no side effects

    out2 = tmp_path / "out2"
    FreqtradeExporter(data_dir=data_dir, output_dir=out2).export(
        symbols=["ETH"], timeframes=["1h"], keep_parquet=True
    )
    feather2 = out2 / "gmx" / "futures" / "ETH_USDC_USDC-1h-futures.feather"
    assert feather2.exists()
    data2 = pl.read_ipc(feather2)

    assert data1.shape == data2.shape
    assert data1["close"].to_list() == data2["close"].to_list()
    assert data1["volume"].to_list() == data2["volume"].to_list()
