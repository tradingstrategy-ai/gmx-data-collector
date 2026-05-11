"""Tests for Freqtrade exporter parquet cleanup behaviour."""

from pathlib import Path

import pandas as pd

from gmx_historical_data.freqtrade_exporter import FreqtradeExporter
from gmx_historical_data.storage import ParquetStorage


def _make_candles(symbols, dates):
    """Build a minimal candle DataFrame for testing.

    :param symbols: List of symbol strings.
    :param dates: List of date strings.
    :returns: pandas DataFrame with OHLCV columns.
    """
    rows = []
    for sym in symbols:
        for d in dates:
            rows.append(
                {
                    "timestamp": pd.Timestamp(d, tz="UTC"),
                    "open": 1.0,
                    "high": 1.0,
                    "low": 1.0,
                    "close": 1.0,
                    "symbol": sym,
                }
            )
    return pd.DataFrame(rows)


def test_export_cleanup_kept_by_default(tmp_path):
    """`keep_parquet` defaults to ``True`` post-2026-05-11 — source parquet survives."""
    storage_dir = tmp_path / "data"
    storage_dir.mkdir()
    storage = ParquetStorage(storage_dir)
    storage.save_candles(_make_candles(["ETH"], ["2024-01-01"]), "1h", "ETH")

    output_dir = tmp_path / "output"
    output_dir.mkdir()
    exporter = FreqtradeExporter(storage_dir, output_dir)
    exporter.export(symbols=["ETH"], timeframes=["1h"])

    feather_files = list(output_dir.rglob("*.feather"))
    assert len(feather_files) >= 1, "feather export must succeed before cleanup is valid"

    candle_path = storage_dir / "candles" / "arbitrum" / "ETH" / "1h.parquet"
    assert candle_path.exists(), "default export must NOT delete the candle parquet"


def test_export_cleanup_removes_parquet_when_opted_in(tmp_path):
    """Explicit ``keep_parquet=False`` retains the legacy delete-after-export behaviour."""
    storage_dir = tmp_path / "data"
    storage_dir.mkdir()
    storage = ParquetStorage(storage_dir)
    storage.save_candles(_make_candles(["ETH"], ["2024-01-01"]), "1h", "ETH")

    output_dir = tmp_path / "output"
    output_dir.mkdir()
    exporter = FreqtradeExporter(storage_dir, output_dir)
    exporter.export(symbols=["ETH"], timeframes=["1h"], keep_parquet=False)

    candle_path = storage_dir / "candles" / "arbitrum" / "ETH" / "1h.parquet"
    assert not candle_path.exists(), "keep_parquet=False must delete the source parquet"


def test_makefile_export_supports_delete_source_flag():
    """Makefile wires the new ``DELETE_SOURCE`` / ``UNSAFE_OVERWRITE`` knobs.

    The legacy ``KEEP`` knob was removed after the 2026-05-11 incident, when
    keeping the parquet became the safe default.

    :raises AssertionError: If the new Makefile contract is missing.
    """
    makefile = Path(__file__).parent.parent / "Makefile"
    content = makefile.read_text()
    assert "DELETE_SOURCE ?=" in content, (
        "Makefile must declare DELETE_SOURCE variable (replaces KEEP)"
    )
    assert "UNSAFE_OVERWRITE ?=" in content, (
        "Makefile must declare UNSAFE_OVERWRITE variable for schema migrations"
    )
    assert "$(DELETE_SOURCE)" in content, (
        "Makefile must wire $(DELETE_SOURCE) into the export-freqtrade CLI invocation"
    )
    assert "KEEP ?=" not in content, (
        "Legacy KEEP variable must be removed — DELETE_SOURCE has the inverse semantics"
    )
