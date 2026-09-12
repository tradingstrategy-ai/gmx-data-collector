"""Regression test: the Freqtrade OHLCV export must not regress dense candles.

``FreqtradeExporter._merge_export_frames`` is the shared merge used by both
the candle/mark/index-price export path and the funding-rate export path.
Before this fix it always kept ``keep="last"`` (incoming wins), which is
correct for funding data but wrong for OHLCV: if the source
``candles/arbitrum/`` store ever regains a flat placeholder for a timestamp
the destination feather already has a genuinely dense row for (the
data-quality direction of the BTC ``1m`` regression -- see
:mod:`gmx_historical_data.ohlcv_density`), a plain ``export-candles`` run
would silently overwrite the good feather value with the worse candles-
store value. ``prefer_dense=True`` (wired to the OHLCV/mark/index call
sites in ``export_candles`` only, NOT ``export_funding``) closes that.
"""

from __future__ import annotations

import polars as pl

from gmx_historical_data.freqtrade_exporter import FreqtradeExporter


def _frame(dates: list[str], highs: list[float], lows: list[float]) -> pl.DataFrame:
    """Build a minimal OHLCV export frame for the given dates.

    :param dates: ISO date strings.
    :param highs: High values, one per date.
    :param lows: Low values, one per date.
    :return: Polars frame with ``date, open, high, low, close, volume``.
    """
    return pl.DataFrame(
        {
            "date": dates,
            "open": [100.0] * len(dates),
            "high": highs,
            "low": lows,
            "close": [100.0] * len(dates),
            "volume": [0.0] * len(dates),
        }
    ).with_columns(pl.col("date").str.to_datetime(time_unit="ns", time_zone="UTC"))


def test_prefer_dense_keeps_existing_dense_row_over_flat_incoming(tmp_path):
    """A flat incoming row must not overwrite an already-dense existing export row.

    :ensures: Re-running ``export-candles`` after ``candles/arbitrum/``
        temporarily regresses to a flat value for a timestamp cannot clobber
        a feather that already holds the genuinely dense value.
    """
    exporter = FreqtradeExporter(tmp_path / "data", tmp_path / "out")

    existing = _frame(["2026-01-01T00:00:00"], [101.5], [99.2])  # dense
    incoming = _frame(["2026-01-01T00:00:00"], [100.0], [100.0])  # flat

    merged = exporter._merge_export_frames(
        incoming,
        existing,
        tmp_path / "OUT-1m-futures.feather",
        file_size=0,
        allow_nonpositive_prices=False,
        prefer_dense=True,
    )

    row = merged.row(0, named=True)
    assert row["high"] == 101.5
    assert row["low"] == 99.2, "Dense existing row must survive a flat incoming write."


def test_prefer_dense_false_keeps_legacy_incoming_wins_behaviour(tmp_path):
    """Without ``prefer_dense``, behaviour is unchanged (incoming always wins).

    :ensures: The funding-rate export path (which never sets
        ``prefer_dense``) keeps its pre-existing "incoming wins" semantics,
        since ``high``/``low`` don't carry an intrabar-movement meaning there.
    """
    exporter = FreqtradeExporter(tmp_path / "data", tmp_path / "out")

    existing = _frame(["2026-01-01T00:00:00"], [101.5], [99.2])
    incoming = _frame(["2026-01-01T00:00:00"], [100.0], [100.0])

    merged = exporter._merge_export_frames(
        incoming,
        existing,
        tmp_path / "OUT-1h-funding_rate.feather",
        file_size=0,
        allow_nonpositive_prices=False,
    )

    row = merged.row(0, named=True)
    assert row["high"] == 100.0
    assert row["low"] == 100.0, "Default behaviour (prefer_dense=False) keeps incoming-wins."


def test_prefer_dense_dense_incoming_still_wins_over_flat_existing(tmp_path):
    """A denser incoming row still replaces a flat existing one, as expected.

    :ensures: The dense-preference tiebreak works both directions, not just
        "existing always wins" -- it genuinely compares density.
    """
    exporter = FreqtradeExporter(tmp_path / "data", tmp_path / "out")

    existing = _frame(["2026-01-01T00:00:00"], [100.0], [100.0])  # flat
    incoming = _frame(["2026-01-01T00:00:00"], [102.0], [98.0])  # dense

    merged = exporter._merge_export_frames(
        incoming,
        existing,
        tmp_path / "OUT-1m-futures.feather",
        file_size=0,
        allow_nonpositive_prices=False,
        prefer_dense=True,
    )

    row = merged.row(0, named=True)
    assert row["high"] == 102.0
    assert row["low"] == 98.0
