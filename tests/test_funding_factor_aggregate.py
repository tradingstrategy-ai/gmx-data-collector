"""Tests for ``aggregate_hourly_rates`` in :mod:`scripts.extract_funding_factor`.

Covers the post-fix invariants:

- The factor extractor does not emit ``longs_pay_shorts`` or signed-fee columns
  (direction is determined later in the unified merge).
- Hourly aggregation is time-weighted (TWAP) rather than arithmetic mean.
"""

from __future__ import annotations

import hashlib
import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
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
    """Carry-in state contributes to the full hour after a boundary."""
    a = datetime(2026, 1, 1, 12, 30, tzinfo=UTC)
    b = datetime(2026, 1, 1, 13, 15, tzinfo=UTC)
    df = aggregate_hourly_rates([_rec(1, a, 1.0), _rec(2, b, 2.0)])["ETH"]
    hours = df.sort("timestamp")
    assert hours.height == 2
    assert hours["funding_rate"][0] == pytest.approx(1.0, rel=1e-9)
    assert hours["funding_rate"][1] == pytest.approx((1.0 * 900 + 2.0 * 2700) / 3600)


def test_twap_uses_pre_hour_carry_in_and_silent_hour_uses_last_state():
    base = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    records = [_rec(1, base, 1.0), _rec(2, base + timedelta(hours=1, minutes=30), 3.0)]

    hours = aggregate_hourly_rates(records)["ETH"].sort("timestamp")
    assert hours["timestamp"].to_list() == [base, base + timedelta(hours=1)]
    assert hours["funding_rate"].to_list() == pytest.approx([1.0, 2.0])
    assert hours["update_count"].to_list() == [1, 1]
    assert hours["funding_rate_min"].to_list() == [1.0, 1.0]
    assert hours["funding_rate_max"].to_list() == [1.0, 3.0]


def test_factor_rebuild_uses_raw_history_and_never_touches_canonical(tmp_path):
    base = datetime(2026, 1, 1, tzinfo=UTC)
    canonical_path = tmp_path / "rates" / "ETH" / "1h.parquet"
    canonical_path.parent.mkdir(parents=True)
    canonical = pl.DataFrame({"marker": ["canonical"]})
    canonical.write_parquet(canonical_path)

    _factor.save_raw_per_symbol([_rec(1, base, 1.0)], tmp_path)
    _factor.rebuild_rates_from_raw(tmp_path)
    _factor.save_raw_per_symbol([_rec(2, base + timedelta(minutes=30), 3.0)], tmp_path)
    _factor.rebuild_rates_from_raw(tmp_path)

    one_shot = aggregate_hourly_rates(
        [_rec(1, base, 1.0), _rec(2, base + timedelta(minutes=30), 3.0)]
    )["ETH"]
    persisted = pl.read_parquet(tmp_path / "rates" / "ETH" / "1h_factor.parquet")
    factor_path = tmp_path / "rates" / "ETH" / "1h_factor.parquet"
    factor_hash = hashlib.sha256(factor_path.read_bytes()).digest()
    _factor.rebuild_rates_from_raw(tmp_path)
    assert hashlib.sha256(factor_path.read_bytes()).digest() == factor_hash
    assert persisted.equals(one_shot)
    assert pl.read_parquet(canonical_path).equals(canonical)


def test_aggregate_empty_records():
    assert aggregate_hourly_rates([]) == {}


def test_aggregate_rejects_missing_block_timestamps():
    with pytest.raises(ValueError, match="positive Unix timestamp"):
        aggregate_hourly_rates([_rec(1, datetime.fromtimestamp(0, tz=UTC), 1.0)])


def test_unchanged_checkpoint_is_not_rewritten(tmp_path):
    checkpoint = tmp_path / "funding_factor_checkpoint.json"
    values = {
        "last_block": 123,
        "last_timestamp": 1_700_000_000,
        "total_events": 5,
        "markets_seen": 2,
    }
    _factor.save_checkpoint(checkpoint, **values)
    before = hashlib.sha256(checkpoint.read_bytes()).digest()

    _factor.save_checkpoint(checkpoint, **values)

    assert hashlib.sha256(checkpoint.read_bytes()).digest() == before
