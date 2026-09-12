"""Regression tests for ``merge_ohlcv_preferring_dense_pandas``.

This is the pandas counterpart of ``merge_ohlcv_preferring_dense`` (polars),
added as the third call site of the same dense-preferring-merge fix, after
``ParquetStorage.save_candles`` and ``FreqtradeExporter._merge_export_frames``.
It backs ``scripts/collect_daily_snapshot.py``'s ``_merge_feather``, which
writes directly to ``gmx/futures/*.feather`` -- the file the README calls
"the deepest copy" for non-Chainlink tokens. Before this fix that merge used
plain ``drop_duplicates(keep='last')``, so a flat placeholder candle (e.g.
from a GMX API hiccup) could silently overwrite an already-dense row purely
because it was written more recently. See ``ohlcv_density.py`` for the full
writeup of the underlying bug class.
"""

from __future__ import annotations

import pandas as pd

from gmx_historical_data.ohlcv_density import merge_ohlcv_preferring_dense_pandas
from scripts.collect_daily_snapshot import _merge_feather


def _row(ts: str, o: float, h: float, low: float, c: float) -> pd.DataFrame:
    """Build a single-row OHLCV frame with a ``date`` timestamp column."""
    return pd.DataFrame(
        {
            "date": pd.to_datetime([ts], utc=True),
            "open": [o],
            "high": [h],
            "low": [low],
            "close": [c],
            "volume": [0.0],
        }
    )


def test_dense_existing_beats_flat_incoming():
    """A flat incoming row must not overwrite an already-dense existing row."""
    existing = _row("2026-01-01T00:00:00Z", 100.0, 101.5, 99.2, 100.8)
    incoming = _row("2026-01-01T00:00:00Z", 999.0, 999.0, 999.0, 999.0)

    merged = merge_ohlcv_preferring_dense_pandas(existing, incoming, ts_col="date")

    assert len(merged) == 1
    row = merged.iloc[0]
    assert row["high"] == 101.5
    assert row["low"] == 99.2
    assert row["close"] == 100.8


def test_dense_incoming_beats_flat_existing():
    """A genuinely dense incoming row still replaces a flat existing one."""
    existing = _row("2026-01-01T00:00:00Z", 100.0, 100.0, 100.0, 100.0)
    incoming = _row("2026-01-01T00:00:00Z", 100.0, 101.5, 99.2, 100.8)

    merged = merge_ohlcv_preferring_dense_pandas(existing, incoming, ts_col="date")

    assert len(merged) == 1
    row = merged.iloc[0]
    assert row["high"] == 101.5
    assert row["low"] == 99.2


def test_equal_density_prefers_incoming():
    """A true tie (both flat, or both dense) keeps 'newer write wins' semantics."""
    existing = _row("2026-01-01T00:00:00Z", 100.0, 100.0, 100.0, 100.0)
    incoming = _row("2026-01-01T00:00:00Z", 110.0, 110.0, 110.0, 110.0)

    merged = merge_ohlcv_preferring_dense_pandas(existing, incoming, ts_col="date")

    assert len(merged) == 1
    assert merged.iloc[0]["close"] == 110.0


def test_no_overlap_appends_and_sorts():
    """Disjoint timestamps are concatenated and sorted, nothing dropped."""
    existing = _row("2026-01-01T00:00:00Z", 100.0, 101.0, 99.0, 100.0)
    incoming = _row("2026-01-02T00:00:00Z", 105.0, 106.0, 104.0, 105.0)

    merged = merge_ohlcv_preferring_dense_pandas(existing, incoming, ts_col="date")

    assert len(merged) == 2
    assert list(merged["close"]) == [100.0, 105.0]


def test_empty_existing_returns_incoming():
    incoming = _row("2026-01-01T00:00:00Z", 100.0, 101.0, 99.0, 100.0)
    merged = merge_ohlcv_preferring_dense_pandas(pd.DataFrame(), incoming, ts_col="date")
    assert len(merged) == 1
    assert merged.iloc[0]["close"] == 100.0


def test_empty_incoming_returns_existing():
    existing = _row("2026-01-01T00:00:00Z", 100.0, 101.0, 99.0, 100.0)
    merged = merge_ohlcv_preferring_dense_pandas(existing, pd.DataFrame(), ts_col="date")
    assert len(merged) == 1
    assert merged.iloc[0]["close"] == 100.0


def test_nan_rows_are_not_treated_as_dense():
    """Malformed NaN OHLC must not dislodge a valid dense candle."""
    existing = _row("2026-01-01T00:00:00Z", 100.0, 101.0, 99.0, 100.0)
    incoming = _row("2026-01-01T00:00:00Z", 100.0, float("nan"), float("nan"), 100.0)

    merged = merge_ohlcv_preferring_dense_pandas(existing, incoming, ts_col="date")

    assert merged.iloc[0]["high"] == 101.0


def test_explicit_merge_can_replace_dense_row(tmp_path):
    """Repair mode must be able to apply a corrected flat API candle."""
    path = tmp_path / "candles.feather"
    existing = _row("2026-01-01T00:00:00Z", 100.0, 101.0, 99.0, 100.0)
    corrected = _row("2026-01-01T00:00:00Z", 100.0, 100.0, 100.0, 100.0)
    _merge_feather(existing, path)
    _merge_feather(corrected, path, prefer_dense=False)

    stored = pd.read_feather(path)
    assert stored.iloc[0]["high"] == 100.0
