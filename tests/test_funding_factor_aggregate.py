"""Tests for ``aggregate_hourly_rates`` in :mod:`scripts.extract_funding_factor`.

Covers the post-fix invariants:

- The factor extractor does not emit ``longs_pay_shorts`` or signed-fee columns
  (direction is determined later in the unified merge).
- Hourly aggregation is time-weighted (TWAP) rather than arithmetic mean.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest


def _load_factor_module():
    """Import ``scripts/extract_funding_factor.py`` as a module."""
    repo = Path(__file__).resolve().parents[1]
    path = repo / "scripts" / "extract_funding_factor.py"
    spec = importlib.util.spec_from_file_location("extract_funding_factor", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["extract_funding_factor"] = module
    spec.loader.exec_module(module)
    return module


_factor = _load_factor_module()
aggregate_hourly_rates = _factor.aggregate_hourly_rates
FundingFactorRecord = _factor.FundingFactorRecord


def _rec(block: int, ts: datetime, rate: float, symbol: str = "ETH", market: str = "0xm"):
    return FundingFactorRecord(
        symbol=symbol,
        market=market,
        funding_factor_per_second=str(int(rate * 1e30)),
        funding_rate_per_second=rate,
        block_number=block,
        block_timestamp=int(ts.timestamp()),
        block_datetime=ts.isoformat(),
        transaction_hash=f"0x{block:064x}",
        log_index=0,
    )


def test_aggregate_omits_direction_and_signed_fees():
    """Factor output must not contain direction or signed-fee columns."""
    ts = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    records = [
        _rec(1, ts, 1e-9),
        _rec(2, ts + timedelta(seconds=30), 2e-9),
    ]
    by_symbol = aggregate_hourly_rates(records)
    df = by_symbol["ETH"]
    assert "longs_pay_shorts" not in df.columns
    assert "funding_fee_long" not in df.columns
    assert "funding_fee_short" not in df.columns
    # Still produces unsigned magnitudes
    assert "funding_rate" in df.columns
    assert "funding_rate_min" in df.columns
    assert "funding_rate_max" in df.columns
    assert "funding_rate_hourly" in df.columns
    assert "funding_rate_annualized" in df.columns
    assert "update_count" in df.columns


def test_twap_three_events_in_one_hour():
    """TWAP weights each rate by the seconds it stayed in effect.

    Events at minutes 0, 30, 45 within the hour with rates 1.0, 2.0, 3.0:
    - 1.0 in effect 0..30 min = 1800 s
    - 2.0 in effect 30..45 min = 900 s
    - 3.0 in effect 45..60 min = 900 s
    TWAP = (1.0*1800 + 2.0*900 + 3.0*900) / 3600 = 1.75
    """
    base = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    records = [
        _rec(1, base, 1.0),
        _rec(2, base + timedelta(minutes=30), 2.0),
        _rec(3, base + timedelta(minutes=45), 3.0),
    ]
    df = aggregate_hourly_rates(records)["ETH"]
    assert df.height == 1
    assert df["funding_rate"].item() == pytest.approx(1.75, rel=1e-9)
    assert df["funding_rate_min"].item() == 1.0
    assert df["funding_rate_max"].item() == 3.0
    assert df["update_count"].item() == 3


def test_twap_single_event_in_hour():
    """Single event in an hour: TWAP equals that rate."""
    ts = datetime(2026, 1, 1, 12, 30, tzinfo=UTC)
    df = aggregate_hourly_rates([_rec(1, ts, 5.0)])["ETH"]
    assert df["funding_rate"].item() == pytest.approx(5.0, rel=1e-9)
    assert df["update_count"].item() == 1


def test_twap_spans_hour_boundary():
    """Event near the end of one hour, next event in the following hour.

    Event A at 12:30 (rate 1.0); event B at 13:15 (rate 2.0).
    Hour 12: A holds for 12:30..13:00 → TWAP = 1.0.
    Hour 13: B is the only event observed within the hour starting at 13:15.
    Pipeline emits one row per (symbol, hour) that has at least one event.
    """
    a = datetime(2026, 1, 1, 12, 30, tzinfo=UTC)
    b = datetime(2026, 1, 1, 13, 15, tzinfo=UTC)
    df = aggregate_hourly_rates([_rec(1, a, 1.0), _rec(2, b, 2.0)])["ETH"]
    hours = df.sort("timestamp")
    assert hours.height == 2
    assert hours["funding_rate"][0] == pytest.approx(1.0, rel=1e-9)
    assert hours["funding_rate"][1] == pytest.approx(2.0, rel=1e-9)


def test_aggregate_empty_records():
    assert aggregate_hourly_rates([]) == {}
