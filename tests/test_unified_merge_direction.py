"""Tests for direction merge in :mod:`scripts.extract_unified_funding`.

Verifies that ``apply_direction_to_rates``:

- Forward-fills direction within (symbol, market) so the last observed
  direction carries forward until the next direction event flips it.
- Leaves ``longs_pay_shorts`` and signed-fee columns as ``null`` for the
  leading gap (hours before the first direction event).
"""

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


_unified = _load_module()
apply_direction_to_rates = _unified.apply_direction_to_rates


def _rates_df(
    timestamps, rate: float = 1e-9, symbol: str = "ETH", market: str = "0xm"
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "timestamp": timestamps,
            "symbol": [symbol] * len(timestamps),
            "market": [market] * len(timestamps),
            "funding_rate": [rate] * len(timestamps),
            "funding_rate_hourly": [rate * 3600] * len(timestamps),
        },
        schema_overrides={"timestamp": pl.Datetime("ns", "UTC")},
    )


def _dir_df(timestamps, longs_pay) -> pl.DataFrame:
    return pl.DataFrame(
        {"timestamp": timestamps, "longs_pay_shorts": longs_pay},
        schema_overrides={"timestamp": pl.Datetime("ns", "UTC")},
    )


def test_forward_fill_direction_carries_last_known():
    base = datetime(2026, 1, 1, tzinfo=UTC)
    hours = [base + timedelta(hours=i) for i in range(5)]
    rates = _rates_df(hours)
    # Direction observed only at hour 2: True
    direction = _dir_df([hours[2]], [True])
    out = apply_direction_to_rates(rates, direction).sort("timestamp")

    longs = out["longs_pay_shorts"].to_list()
    # Hours 0, 1: no prior direction → null
    assert longs[0] is None
    assert longs[1] is None
    # Hours 2, 3, 4: True forward-filled
    assert longs[2] is True
    assert longs[3] is True
    assert longs[4] is True


def test_direction_flip_propagates_forward():
    base = datetime(2026, 1, 1, tzinfo=UTC)
    hours = [base + timedelta(hours=i) for i in range(6)]
    rates = _rates_df(hours)
    # Two direction observations: hour 1 = True, hour 4 = False
    direction = _dir_df([hours[1], hours[4]], [True, False])
    out = apply_direction_to_rates(rates, direction).sort("timestamp")

    longs = out["longs_pay_shorts"].to_list()
    assert longs[0] is None
    assert longs[1] is True
    assert longs[2] is True
    assert longs[3] is True
    assert longs[4] is False
    assert longs[5] is False


def test_signed_fee_columns_are_null_in_leading_gap():
    base = datetime(2026, 1, 1, tzinfo=UTC)
    hours = [base + timedelta(hours=i) for i in range(3)]
    rates = _rates_df(hours, rate=2e-9)
    direction = _dir_df([hours[2]], [True])
    out = apply_direction_to_rates(rates, direction).sort("timestamp")

    fee_long = out["funding_fee_long"].to_list()
    # Leading two hours: no direction → null signed fees
    assert fee_long[0] is None
    assert fee_long[1] is None
    # Hour 2: longs pay → fee_long is positive (rate * 3600)
    assert fee_long[2] is not None
    assert fee_long[2] > 0


def test_signed_fees_flip_with_direction():
    base = datetime(2026, 1, 1, tzinfo=UTC)
    rates = _rates_df([base, base + timedelta(hours=1)], rate=1e-9)
    direction = _dir_df([base, base + timedelta(hours=1)], [True, False])
    out = apply_direction_to_rates(rates, direction).sort("timestamp")

    # Longs-pay row: fee_long > 0, fee_short < 0
    assert out["funding_fee_long"][0] > 0
    assert out["funding_fee_short"][0] < 0
    # Shorts-pay row: fee_long < 0, fee_short > 0
    assert out["funding_fee_long"][1] < 0
    assert out["funding_fee_short"][1] > 0


def test_no_direction_data_returns_all_null_direction():
    base = datetime(2026, 1, 1, tzinfo=UTC)
    rates = _rates_df([base, base + timedelta(hours=1)])
    out = apply_direction_to_rates(rates, None).sort("timestamp")
    assert out["longs_pay_shorts"].to_list() == [None, None]
    assert out["funding_fee_long"].to_list() == [None, None]
