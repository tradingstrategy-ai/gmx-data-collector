"""Tests for cex_gap_fill.freqtrade_runner."""

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from gmx_historical_data.cex_gap_fill.freqtrade_runner import (
    CEXDownloadError,
    build_download_argv,
    resolve_feather_path,
    run_download,
)


# ── Task 9: argv builder ──────────────────────────────────────────────────────


def test_build_download_argv_open_ended_timerange():
    argv = build_download_argv(
        exchange="binance",
        pairs=["BTC/USDT:USDT", "ETH/USDT:USDT"],
        timeframes=["1h", "4h"],
        timerange_start="20230801",
        datadir=None,
    )
    assert argv[0] == "./freqtrade-gmx"
    assert argv[1] == "download-data"
    assert "--exchange" in argv
    assert argv[argv.index("--exchange") + 1] == "binance"
    assert "--timerange" in argv
    assert argv[argv.index("--timerange") + 1] == "20230801-"
    assert "--data-format-ohlcv" in argv
    assert argv[argv.index("--data-format-ohlcv") + 1] == "feather"
    assert "--trading-mode" in argv
    assert argv[argv.index("--trading-mode") + 1] == "futures"


def test_build_download_argv_passes_all_pairs_and_timeframes():
    argv = build_download_argv(
        exchange="bybit",
        pairs=["SUI/USDT:USDT", "APT/USDT:USDT"],
        timeframes=["1min", "1h"],
        timerange_start="20240101",
        datadir=None,
    )
    assert "SUI/USDT:USDT" in argv
    assert "APT/USDT:USDT" in argv
    assert "1min" in argv
    assert "1h" in argv


def test_build_download_argv_with_datadir(tmp_path: Path):
    argv = build_download_argv(
        exchange="binance",
        pairs=["BTC/USDT:USDT"],
        timeframes=["1h"],
        timerange_start="20230801",
        datadir=tmp_path,
    )
    assert "--datadir" in argv
    assert str(tmp_path) in argv


def test_build_download_argv_without_datadir_omits_flag():
    argv = build_download_argv(
        exchange="binance",
        pairs=["BTC/USDT:USDT"],
        timeframes=["1h"],
        timerange_start="20230801",
        datadir=None,
    )
    assert "--datadir" not in argv


# ── Task 10: subprocess wrapper + error handling ──────────────────────────────


def test_run_download_invokes_subprocess_with_argv():
    with patch("gmx_historical_data.cex_gap_fill.freqtrade_runner.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout=b"", stderr=b"")
        run_download(
            exchange="binance",
            pairs=["BTC/USDT:USDT"],
            timeframes=["1h"],
            timerange_start="20230801",
            datadir=None,
            cwd=Path("."),
            timeout=30,
        )
        assert mock_run.called
        args, kwargs = mock_run.call_args
        argv = args[0]
        assert argv[0] == "./freqtrade-gmx"
        assert kwargs["cwd"] == Path(".")
        assert kwargs["timeout"] == 30


def test_run_download_raises_on_nonzero_exit():
    with patch("gmx_historical_data.cex_gap_fill.freqtrade_runner.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=2, stdout=b"", stderr=b"boom")
        with pytest.raises(CEXDownloadError, match="boom"):
            run_download(
                exchange="binance",
                pairs=["BTC/USDT:USDT"],
                timeframes=["1h"],
                timerange_start="20230801",
                datadir=None,
                cwd=Path("."),
                timeout=30,
            )


def test_run_download_timeout_propagates():
    with patch("gmx_historical_data.cex_gap_fill.freqtrade_runner.subprocess.run") as mock_run:
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="x", timeout=1)
        with pytest.raises(CEXDownloadError, match="timed out"):
            run_download(
                exchange="binance",
                pairs=["BTC/USDT:USDT"],
                timeframes=["1h"],
                timerange_start="20230801",
                datadir=None,
                cwd=Path("."),
                timeout=1,
            )


# ── Task 11: feather path resolver ───────────────────────────────────────────


def test_resolve_feather_path_futures_layout(tmp_path: Path):
    path = resolve_feather_path(
        datadir=tmp_path,
        exchange="binance",
        pair="BTC/USDT:USDT",
        timeframe="1h",
    )
    expected = tmp_path / "binance" / "futures" / "BTC_USDT_USDT-1h-futures.feather"
    assert path == expected


def test_resolve_feather_path_handles_1000_prefix(tmp_path: Path):
    path = resolve_feather_path(
        datadir=tmp_path,
        exchange="bybit",
        pair="1000BONK/USDT:USDT",
        timeframe="5min",
    )
    expected = tmp_path / "bybit" / "futures" / "1000BONK_USDT_USDT-5min-futures.feather"
    assert path == expected
