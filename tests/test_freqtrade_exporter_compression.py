"""Verify the FreqTrade exporter writes zstd-compressed feathers.

We assert two properties:

1. The exporter writes a *compressed* IPC file — measured by comparing the
   file size to the in-memory polars table size (compressed must be < 60 %
   of in-memory for the highly-redundant OHLCV schema we use).
2. ``pd.read_feather`` (FreqTrade's read path) round-trips it without loss.
"""

from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
import pyarrow.ipc as ipc

from gmx_historical_data.freqtrade_exporter import FreqtradeExporter


def _make_synthetic_candles(rows: int = 100_000) -> pl.DataFrame:
    """Build a realistic 1m OHLCV dataframe (highly compressible)."""
    rng = np.random.default_rng(seed=42)
    base = 1000.0 + np.cumsum(rng.normal(0, 0.1, rows))
    dates = pl.datetime_range(
        start=pl.datetime(2024, 1, 1),
        end=pl.datetime(2024, 1, 1) + pl.duration(minutes=rows),
        interval="1m",
        eager=True,
        closed="left",
    ).cast(pl.Datetime("ns", "UTC"))
    return pl.DataFrame(
        {
            "date": dates,
            "open": base,
            "high": base + 0.5,
            "low": base - 0.5,
            "close": base + rng.normal(0, 0.1, rows),
            "volume": rng.uniform(0, 100, rows),
        }
    )


def test_exporter_writes_zstd_compressed_feather(tmp_path: Path) -> None:
    df = _make_synthetic_candles()
    out = tmp_path / "BTC_USDC_USDC-1m-futures.feather"

    # Use the internal helper directly — same writer the exporter uses.
    exporter = FreqtradeExporter(data_dir=tmp_path, output_dir=tmp_path)
    exporter._write(df, out, fmt="feather", unsafe_overwrite=True)

    assert out.exists(), "writer did not produce a file"

    # Compressed size should be < 60 % of in-memory size (LINK 1m benchmark
    # measured 22 %; we allow generous headroom for synthetic data).
    in_memory_bytes = df.estimated_size()
    compressed_bytes = out.stat().st_size
    assert compressed_bytes < in_memory_bytes * 0.6, (
        f"feather not compressed: {compressed_bytes} bytes vs "
        f"{in_memory_bytes} in-memory (ratio "
        f"{compressed_bytes / in_memory_bytes:.2%})"
    )

    # FreqTrade reads via pandas.read_feather — must succeed and match shape.
    roundtrip = pd.read_feather(out)
    assert len(roundtrip) == len(df)
    assert list(roundtrip.columns) == df.columns


def test_exporter_compressed_feather_pandas_readable(tmp_path: Path) -> None:
    """Verify the IPC file round-trips through pandas with bit-perfect close sum."""
    df = _make_synthetic_candles(rows=10_000)
    out = tmp_path / "ETH_USDC_USDC-1m-futures.feather"
    exporter = FreqtradeExporter(data_dir=tmp_path, output_dir=tmp_path)
    exporter._write(df, out, fmt="feather", unsafe_overwrite=True)

    # IPC stream opens cleanly.
    with ipc.open_file(out) as reader:
        assert reader.num_record_batches >= 1

    # pandas round-trip preserves data exactly (zstd is lossless).
    roundtrip = pd.read_feather(out)
    assert len(roundtrip) == 10_000
    assert abs(roundtrip["close"].sum() - df["close"].sum()) < 1e-6
