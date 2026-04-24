"""Tests for cex_gap_fill.reconciler."""

from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from gmx_historical_data.cex_gap_fill.detector import DetectorConfig, detect_gaps
from gmx_historical_data.cex_gap_fill.reconciler import (
    HistoryTruncationError,
    ReconcileStats,
    apply_price_scale,
    assert_history_preserved,
    reconcile,
    warn_seam_discontinuities,
)


def _df(prices: list[float], volumes: list[float], start_hour: int = 0) -> pl.DataFrame:
    start = datetime(2026, 1, 1, start_hour, tzinfo=UTC)
    ts = [start + timedelta(hours=i) for i in range(len(prices))]
    return pl.DataFrame({
        "timestamp": ts,
        "open": prices, "high": prices, "low": prices, "close": prices,
        "volume": volumes,
    })


# ── Task 12: price scaling ────────────────────────────────────────────────────


def test_apply_price_scale_for_btc_is_noop():
    df = _df([50000.0, 51000.0], [10.0, 12.0])
    out = apply_price_scale(df, "BTC")
    assert out["close"].to_list() == [50000.0, 51000.0]
    assert out["volume"].to_list() == [10.0, 12.0]


def test_apply_price_scale_for_bonk_divides_price_multiplies_volume():
    df = _df([0.050, 0.051], [100.0, 200.0])
    out = apply_price_scale(df, "BONK")
    assert out["close"].to_list() == pytest.approx([0.050 / 1000, 0.051 / 1000])
    assert out["volume"].to_list() == pytest.approx([100_000.0, 200_000.0])


# ── Task 13: full-range replace with CEX confirmation ────────────────────────


def test_reconcile_replaces_full_range_when_cex_disagrees():
    gmx = _df([100, 100, 200, 200], [1, 1, 1, 1])
    cex = _df([100, 101, 102, 103], [10, 10, 10, 10])
    config = DetectorConfig(gap_pct_threshold=0.20, merge_gap_bars=0, min_range_bars=1)
    det = detect_gaps(gmx, tf="1h", config=config)
    out, stats = reconcile(gmx, cex, det, gmx_symbol="BTC", config=config)
    assert out["close"][2] == pytest.approx(102)
    assert stats.full_replaced >= 1


def test_reconcile_keeps_full_range_when_cex_confirms():
    gmx = _df([100, 100, 200, 200], [1, 1, 1, 1])
    cex = _df([100, 100, 200, 200], [10, 10, 10, 10])
    config = DetectorConfig(gap_pct_threshold=0.20, merge_gap_bars=0, min_range_bars=1)
    det = detect_gaps(gmx, tf="1h", config=config)
    out, stats = reconcile(gmx, cex, det, gmx_symbol="BTC", config=config)
    assert out["close"][2] == pytest.approx(200)
    assert stats.kept >= 1


def test_reconcile_empty_cex_leaves_gmx_untouched():
    gmx = _df([100, 100, 200, 200], [1, 1, 1, 1])
    cex = pl.DataFrame(schema={"timestamp": pl.Datetime("us", "UTC"), "open": pl.Float64,
                                "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
                                "volume": pl.Float64})
    config = DetectorConfig(gap_pct_threshold=0.20, merge_gap_bars=0, min_range_bars=1)
    det = detect_gaps(gmx, tf="1h", config=config)
    out, stats = reconcile(gmx, cex, det, gmx_symbol="BTC", config=config)
    assert out["close"][2] == pytest.approx(200)
    assert stats.cex_missing >= 1


def test_reconcile_volume_only_replace():
    gmx = _df([100, 101, 102], [1, 0, 1])
    cex = _df([100, 101, 102], [10, 50, 10])
    config = DetectorConfig(gap_pct_threshold=0.20, merge_gap_bars=0, min_range_bars=1)
    det = detect_gaps(gmx, tf="1h", config=config)
    out, stats = reconcile(gmx, cex, det, gmx_symbol="BTC", config=config)
    assert out["volume"][1] == pytest.approx(50)
    assert out["close"][1] == pytest.approx(101)
    assert stats.volume_replaced >= 1


# ── Task 14: history-preservation + seam warning ─────────────────────────────


def test_assert_history_preserved_passes_when_same_start():
    a = _df([100, 101, 102], [1, 1, 1])
    b = _df([100, 102, 103], [1, 1, 1])
    assert_history_preserved(original=a, corrected=b)  # must not raise


def test_assert_history_preserved_raises_when_corrected_starts_later():
    a = _df([100, 101, 102], [1, 1, 1])
    b = _df([101, 102], [1, 1], start_hour=1)
    with pytest.raises(HistoryTruncationError):
        assert_history_preserved(original=a, corrected=b)


def test_warn_seam_discontinuities_returns_count(caplog):
    import logging
    df = _df([100, 200, 201], [1, 1, 1])
    with caplog.at_level(logging.WARNING, logger="gmx_historical_data.cex_gap_fill"):
        count = warn_seam_discontinuities(df, threshold=0.20, gmx_symbol="BTC")
    assert count == 1
    assert "seam discontinuities" in caplog.text


def test_warn_seam_discontinuities_no_warning_when_clean():
    df = _df([100, 101, 102], [1, 1, 1])
    count = warn_seam_discontinuities(df, threshold=0.20, gmx_symbol="BTC")
    assert count == 0
