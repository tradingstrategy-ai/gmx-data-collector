"""Tests for cex_gap_fill.detector."""

from datetime import UTC, datetime, timedelta

import polars as pl

from gmx_historical_data.cex_gap_fill.detector import (
    DetectorConfig,
    GapRange,
    RangeKind,
    cluster_ranges,
    detect_gaps,
    minutes_for_timeframe,
    price_jump_mask,
    reindex_and_mark_missing,
    zero_volume_mask,
)


def _build_df(
    prices: list[float], volumes: list[float] | None = None, tf_minutes: int = 60
) -> pl.DataFrame:
    assert volumes is None or len(prices) == len(volumes)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    ts = [start + timedelta(minutes=tf_minutes * i) for i in range(len(prices))]
    return pl.DataFrame(
        {
            "timestamp": ts,
            "open": prices,
            "high": prices,
            "low": prices,
            "close": prices,
            "volume": volumes if volumes is not None else [1.0] * len(prices),
        }
    )


# ── Task 4 tests ──────────────────────────────────────────────────────────────


def test_price_jump_mask_detects_100pct_jump():
    df = _build_df([100, 100, 200, 200, 200])
    mask = price_jump_mask(df, threshold=0.20)
    assert mask.to_list() == [False, False, True, False, False]


def test_price_jump_mask_first_bar_is_false():
    df = _build_df([100, 100])
    mask = price_jump_mask(df, threshold=0.20)
    assert mask.to_list()[0] is False


def test_price_jump_mask_small_move_below_threshold():
    df = _build_df([100, 110])
    mask = price_jump_mask(df, threshold=0.20)
    assert mask.to_list() == [False, False]


def test_price_jump_mask_negative_jump_detected():
    df = _build_df([200, 50])
    mask = price_jump_mask(df, threshold=0.20)
    assert mask.to_list() == [False, True]


# ── Task 5 tests ──────────────────────────────────────────────────────────────


def test_zero_volume_mask_flags_zeros():
    df = _build_df([100, 100, 100], volumes=[10, 0, 5])
    mask = zero_volume_mask(df)
    assert mask.to_list() == [False, True, False]


def test_reindex_and_mark_missing_inserts_rows_for_gaps():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    df = pl.DataFrame(
        {
            "timestamp": [start, start + timedelta(hours=1), start + timedelta(hours=3)],
            "open": [100.0, 101.0, 103.0],
            "high": [100.0, 101.0, 103.0],
            "low": [100.0, 101.0, 103.0],
            "close": [100.0, 101.0, 103.0],
            "volume": [1.0, 1.0, 1.0],
        }
    )
    result, missing_mask = reindex_and_mark_missing(df, tf_minutes=60)
    assert result.height == 4
    assert missing_mask.to_list() == [False, False, True, False]
    assert result["close"][2] is None


def test_minutes_for_timeframe_1h():
    assert minutes_for_timeframe("1h") == 60


def test_minutes_for_timeframe_1min():
    assert minutes_for_timeframe("1min") == 1


def test_minutes_for_timeframe_invalid_raises():
    import pytest

    with pytest.raises(ValueError):
        minutes_for_timeframe("2h")


# ── Task 6 tests ──────────────────────────────────────────────────────────────


def test_cluster_ranges_merges_contiguous_trues():
    mask = pl.Series("m", [False, True, True, False, True, False])
    ranges = cluster_ranges(mask, merge_gap_bars=0, min_range_bars=1)
    assert ranges == [(1, 2), (4, 4)]


def test_cluster_ranges_merges_short_false_gaps():
    mask = pl.Series("m", [True, False, True, False, False, False, True])
    ranges = cluster_ranges(mask, merge_gap_bars=1, min_range_bars=1)
    assert ranges == [(0, 2), (6, 6)]


def test_cluster_ranges_drops_below_min_size():
    mask = pl.Series("m", [True, False, True, True, True])
    ranges = cluster_ranges(mask, merge_gap_bars=0, min_range_bars=2)
    assert ranges == [(2, 4)]


def test_detect_gaps_full_pipeline():
    df = _build_df(
        prices=[100, 100, 200, 200, 200, 200],
        volumes=[1, 1, 1, 1, 0, 1],
    )
    config = DetectorConfig(gap_pct_threshold=0.20, merge_gap_bars=0, min_range_bars=1)
    result = detect_gaps(df, tf="1h", config=config)
    assert len(result.full_ranges) == 1
    assert result.full_ranges[0].kind == RangeKind.FULL
    assert len(result.volume_ranges) == 1
    assert result.volume_ranges[0].kind == RangeKind.VOLUME_ONLY


def test_detect_gaps_extends_full_range_backward_across_stale_flat_run_before_jump():
    df = _build_df(
        prices=[100, 100, 100, 200, 200],
        volumes=[1, 1, 1, 1, 1],
    )
    config = DetectorConfig(gap_pct_threshold=0.20, merge_gap_bars=0, min_range_bars=1)
    result = detect_gaps(df, tf="1h", config=config)
    assert result.full_ranges == [GapRange(1, 3, RangeKind.FULL)]


def test_detect_gaps_extends_full_range_backward_across_stale_flat_run_before_downward_jump():
    df = _build_df(
        prices=[200, 200, 200, 100, 100],
        volumes=[1, 1, 1, 1, 1],
    )
    config = DetectorConfig(gap_pct_threshold=0.20, merge_gap_bars=0, min_range_bars=1)
    result = detect_gaps(df, tf="1h", config=config)
    assert result.full_ranges == [GapRange(1, 3, RangeKind.FULL)]
