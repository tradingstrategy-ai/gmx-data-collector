"""Tests for forward_fill_hourly_grid in :mod:`scripts.extract_unified_funding`."""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl


def _load_module():
    repo = Path(__file__).resolve().parents[1]
    path = repo / "scripts" / "extract_unified_funding.py"
    spec = importlib.util.spec_from_file_location("extract_unified_funding", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["extract_unified_funding"] = module
    spec.loader.exec_module(module)
    return module


_mod = _load_module()
forward_fill_hourly_grid = _mod.forward_fill_hourly_grid


def _factor_df(rows):
    """Build a factor-shaped frame from (timestamp, rate, update_count) tuples."""
    ts, rates, counts = zip(*rows)
    return pl.DataFrame(
        {
            "timestamp": list(ts),
            "funding_rate": list(rates),
            "funding_rate_min": list(rates),
            "funding_rate_max": list(rates),
            "funding_rate_hourly": [r * 3600 for r in rates],
            "funding_rate_annualized": [r * 3600 * 8760 for r in rates],
            "update_count": list(counts),
            "symbol": ["ETH"] * len(rows),
            "market": ["0xm"] * len(rows),
        },
        schema_overrides={
            "timestamp": pl.Datetime("ns", "UTC"),
            "update_count": pl.UInt32,
        },
    )


def test_fill_internal_gap_carries_rate_forward():
    """Two events with a 5-hour gap: filled rows take the earlier rate."""
    base = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    df = _factor_df(
        [
            (base, 1e-9, 3),
            (base + timedelta(hours=6), 2e-9, 2),
        ]
    )
    out = forward_fill_hourly_grid(df).sort("timestamp")

    # Grid covers 7 hours: 12:00..18:00 inclusive.
    assert out.height == 7
    rates = out["funding_rate"].to_list()
    filled = out["is_gap_filled"].to_list()
    counts = out["update_count"].to_list()

    # Hour 0: real event at 12:00 → rate 1e-9, not filled, count=3.
    assert rates[0] == 1e-9
    assert filled[0] is False
    assert counts[0] == 3
    # Hours 1-5 (13:00..17:00): forward-filled with 1e-9, count=0, filled=True.
    for i in range(1, 6):
        assert rates[i] == 1e-9
        assert filled[i] is True
        assert counts[i] == 0
    # Hour 6 (18:00): real event → rate 2e-9, not filled, count=2.
    assert rates[6] == 2e-9
    assert filled[6] is False
    assert counts[6] == 2


def test_fill_derived_columns_also_propagate():
    """funding_rate_hourly and friends must be filled, not left null."""
    base = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    df = _factor_df([(base, 1e-9, 1), (base + timedelta(hours=2), 2e-9, 1)])
    out = forward_fill_hourly_grid(df).sort("timestamp")

    # Middle hour (01:00): filled. funding_rate_hourly = 1e-9 * 3600 = 3.6e-6.
    middle = out.filter(pl.col("timestamp") == base + timedelta(hours=1))
    assert middle["funding_rate_hourly"].item() == 1e-9 * 3600
    assert middle["funding_rate_annualized"].item() == 1e-9 * 3600 * 8760
    assert middle["funding_rate_min"].item() == 1e-9
    assert middle["funding_rate_max"].item() == 1e-9


def test_fill_preserves_symbol_and_market():
    base = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    df = _factor_df([(base, 1e-9, 1), (base + timedelta(hours=2), 1e-9, 1)])
    out = forward_fill_hourly_grid(df).sort("timestamp")
    assert out["symbol"].to_list() == ["ETH"] * 3
    assert out["market"].to_list() == ["0xm"] * 3


def test_empty_input_returns_empty():
    df = pl.DataFrame(
        {
            "timestamp": [],
            "funding_rate": [],
            "update_count": [],
        },
        schema={
            "timestamp": pl.Datetime("ns", "UTC"),
            "funding_rate": pl.Float64,
            "update_count": pl.UInt32,
        },
    )
    out = forward_fill_hourly_grid(df)
    assert out.is_empty()


def test_single_row_returns_one_hour():
    base = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    df = _factor_df([(base, 5e-9, 1)])
    out = forward_fill_hourly_grid(df)
    assert out.height == 1
    assert out["is_gap_filled"].to_list() == [False]
    assert out["funding_rate"].to_list() == [5e-9]


def test_fill_does_not_extend_beyond_last_event():
    """Grid stops at max(timestamp). No zombie-fill past the last on-chain event."""
    base = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    df = _factor_df([(base, 1e-9, 1), (base + timedelta(hours=3), 2e-9, 1)])
    out = forward_fill_hourly_grid(df).sort("timestamp")
    assert out["timestamp"].max() == base + timedelta(hours=3)
    assert out.height == 4  # hours 0,1,2,3
