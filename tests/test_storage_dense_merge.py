"""Regression tests for dense-preferring OHLCV merges in ``save_candles``.

Root cause: ``candles/arbitrum/BTC/1m.parquet`` was found 92% flat
(``high == low``) across its *entire* history, including the most recent
~6 months, even though GMX's own API demonstrably provides dense (0% flat)
1-minute data for that same window (see the Freqtrade export,
``futures/BTC_USDC_USDC-1m-futures.feather``). Part of the fix: whichever
candle source is concatenated *last* into a merge must not automatically
win on a timestamp collision -- a genuinely dense row must always beat a
flat placeholder, regardless of which source produced it or write order.
See :mod:`gmx_historical_data.ohlcv_density` for the full writeup.
"""

from __future__ import annotations

import pandas as pd

from gmx_historical_data.storage import ParquetStorage


def _candle(ts: str, o: float, h: float, low: float, c: float, symbol: str = "BTC") -> pd.DataFrame:
    """Build a single-row OHLCV frame for a given timestamp.

    :param ts: ISO timestamp string.
    :param o: Open.
    :param h: High.
    :param low: Low.
    :param c: Close.
    :param symbol: Token symbol.
    :return: One-row DataFrame matching ``save_candles``'s required schema.
    """
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime([ts], utc=True),
            "open": [o],
            "high": [h],
            "low": [low],
            "close": [c],
            "symbol": [symbol],
        }
    )


def test_dense_incoming_wins_over_flat_existing(tmp_path):
    """A dense (GMX-API-like) write must overwrite a flat placeholder already on disk.

    :ensures: Re-collecting a symbol/timeframe with a denser source replaces
        Chainlink-style forward-filled flat candles, not just newer flat ones.
    """
    storage = ParquetStorage(tmp_path)

    flat = _candle("2026-01-01T00:00:00Z", 100.0, 100.0, 100.0, 100.0)
    storage.save_candles(flat, "1min", "BTC")

    dense = _candle("2026-01-01T00:00:00Z", 100.0, 101.5, 99.2, 100.8)
    storage.save_candles(dense, "1min", "BTC")

    result = storage.read_candles("1min", "BTC")
    assert len(result) == 1
    row = result.iloc[0]
    assert row["high"] == 101.5
    assert row["low"] == 99.2, "Dense incoming row must win over a flat existing row."


def test_dense_existing_survives_flat_incoming(tmp_path):
    """A flat re-write must NOT clobber an already-dense candle for the same timestamp.

    :ensures: A coarser source re-fetched later (e.g. a Chainlink incremental
        pass re-touching a timestamp GMX's API already densified) cannot
        regress a good candle back to a flat placeholder.
    """
    storage = ParquetStorage(tmp_path)

    dense = _candle("2026-01-01T00:00:00Z", 100.0, 101.5, 99.2, 100.8)
    storage.save_candles(dense, "1min", "BTC")

    flat = _candle("2026-01-01T00:00:00Z", 100.0, 100.0, 100.0, 100.0)
    storage.save_candles(flat, "1min", "BTC")

    result = storage.read_candles("1min", "BTC")
    assert len(result) == 1
    row = result.iloc[0]
    assert row["high"] == 101.5
    assert row["low"] == 99.2, "Existing dense row must survive a later flat write."


def test_equal_density_ties_go_to_incoming(tmp_path):
    """When both rows are equally dense (or equally flat), the newer write wins.

    :ensures: The pre-existing "newest write wins" semantics for genuine
        price updates are unchanged by the dense-preference tiebreak.
    """
    storage = ParquetStorage(tmp_path)

    first_dense = _candle("2026-01-01T00:00:00Z", 100.0, 105.0, 99.0, 104.0)
    storage.save_candles(first_dense, "1h", "ETH")

    second_dense = _candle("2026-01-01T00:00:00Z", 200.0, 210.0, 190.0, 205.0)
    storage.save_candles(second_dense, "1h", "ETH")

    result = storage.read_candles("1h", "ETH")
    assert len(result) == 1
    assert result.iloc[0]["open"] == 200.0, "Equally-dense rows: newer write should win."


def test_mixed_batch_merge_prefers_dense_per_row(tmp_path):
    """A multi-row merge resolves density independently for each timestamp.

    :ensures: Reconciling a whole feather export against an existing store
        (scripts/reconcile_dense_from_futures.py's use case) upgrades only
        the timestamps the denser source actually covers, leaving the rest
        of the existing store untouched.
    """
    storage = ParquetStorage(tmp_path)

    existing = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2026-01-01T00:00:00Z", "2026-01-01T00:01:00Z", "2026-01-01T00:02:00Z"],
                utc=True,
            ),
            "open": [100.0, 100.0, 100.0],
            "high": [100.0, 100.0, 105.0],  # last row already dense
            "low": [100.0, 100.0, 95.0],
            "close": [100.0, 100.0, 102.0],
            "symbol": ["BTC"] * 3,
        }
    )
    storage.save_candles(existing, "1min", "BTC")

    incoming = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2026-01-01T00:00:00Z", "2026-01-01T00:01:00Z", "2026-01-01T00:02:00Z"],
                utc=True,
            ),
            "open": [100.0, 100.0, 999.0],
            "high": [101.0, 100.0, 999.0],  # dense, flat, flat(-would-be-regression)
            "low": [99.0, 100.0, 999.0],
            "close": [100.5, 100.0, 999.0],
            "symbol": ["BTC"] * 3,
        }
    )
    storage.save_candles(incoming, "1min", "BTC")

    result = storage.read_candles("1min", "BTC").sort_values("timestamp").reset_index(drop=True)
    assert len(result) == 3
    # Row 0: incoming was dense, existing flat -> incoming wins.
    assert result.iloc[0]["high"] == 101.0
    assert result.iloc[0]["low"] == 99.0
    # Row 1: both flat -> incoming (newer write) wins per prior semantics.
    assert result.iloc[1]["high"] == 100.0
    # Row 2: existing was already dense, incoming is flat -> existing survives.
    assert result.iloc[2]["high"] == 105.0
    assert result.iloc[2]["low"] == 95.0
