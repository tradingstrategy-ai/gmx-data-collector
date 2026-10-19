"""A factor rebuild + unified merge never shrinks the canonical history.

On a lake whose raw event store is truncated (raw starts long after the canonical
series does) the rebuild regenerates ``1h_factor.parquet`` from raw only and discards
a polluted companion. The canonical hours before raw coverage must survive, with their
provenance, while the hours raw covers are re-derived.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

DAY0 = datetime(2026, 1, 1, tzinfo=UTC)


def _load(name: str):
    path = Path(__file__).resolve().parents[1] / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_factor = _load("extract_funding_factor")
_unified = _load("extract_unified_funding")


def _rec(block: int, ts: datetime, rate: float):
    return _factor.FundingFactorRecord(
        symbol="ETH",
        market="0xm",
        funding_factor_per_second=str(int(rate * 1e30)),
        funding_rate_per_second=rate,
        block_number=block,
        block_timestamp=int(ts.timestamp()),
        block_datetime=ts.isoformat(),
        transaction_hash=f"0x{block:064x}",
        log_index=0,
    )


def _canonical(days: int, rate: float) -> pl.DataFrame:
    """Hourly canonical rows for ``days`` days: event-derived by day, a gap-fill hour each day."""
    hours = [DAY0 + timedelta(hours=h) for h in range(days * 24)]
    return pl.DataFrame(
        {
            "timestamp": hours,
            "funding_rate": [rate] * len(hours),
            "funding_rate_hourly": [rate * 3600] * len(hours),
            "symbol": ["ETH"] * len(hours),
            "market": ["0xm"] * len(hours),
            "source": ["hypersync"] * len(hours),
            "is_gap_filled": [h.hour == 5 for h in hours],
        },
        schema_overrides={"timestamp": pl.Datetime("ns", "UTC")},
    )


def _lake(tmp_path: Path, *, raw_from_day: int, total_days: int = 12) -> Path:
    """Canonical + polluted companion from day 1; raw from ``raw_from_day`` (1-based)."""
    rates = tmp_path / "rates" / "ETH"
    rates.mkdir(parents=True)
    _canonical(total_days, 1e-9).write_parquet(rates / "1h.parquet")
    # Polluted companion: carries a source label, so the merge never reinterprets it.
    _canonical(total_days, 7e-9).write_parquet(rates / "1h_factor.parquet")

    start = DAY0 + timedelta(days=raw_from_day - 1)
    records = [
        _rec(i + 1, start + timedelta(hours=i), 2e-9)
        for i in range((total_days - raw_from_day + 1) * 24)
    ]
    _factor.save_raw_per_symbol(records, tmp_path)
    return tmp_path


def _resume(lake: Path) -> pl.DataFrame:
    _factor.rebuild_rates_from_raw(lake)
    _unified.merge_symbol("ETH", lake / "rates", lake / "direction")
    return pl.read_parquet(lake / "rates" / "ETH" / "1h.parquet").sort("timestamp")


def test_truncated_raw_keeps_pre_coverage_days_and_rederives_covered_days(tmp_path, capsys):
    lake = _lake(tmp_path, raw_from_day=10)
    before = pl.read_parquet(lake / "rates" / "ETH" / "1h.parquet").sort("timestamp")

    out = _resume(lake)

    cutoff = DAY0 + timedelta(days=9)
    assert out.height == before.height
    old = out.filter(pl.col("timestamp") < cutoff)
    new = out.filter(pl.col("timestamp") >= cutoff)
    # Days 1-9 are the pre-existing canonical rows, provenance (source, gap flag) intact.
    assert old.select(before.columns).equals(before.filter(pl.col("timestamp") < cutoff))
    assert old["is_gap_filled"].sum() == 9
    # Days 10+ are re-derived from raw, not the canonical or polluted companion value.
    assert new["funding_rate"].to_list() == [2e-9] * new.height
    assert "raw coverage starts" in capsys.readouterr().out


def test_truncated_raw_rerun_is_idempotent(tmp_path):
    lake = _lake(tmp_path, raw_from_day=10)
    first = _resume(lake)
    canonical = lake / "rates" / "ETH" / "1h.parquet"
    factor = lake / "rates" / "ETH" / "1h_factor.parquet"
    snapshot = (canonical.read_bytes(), factor.read_bytes())

    second = _resume(lake)

    assert second.equals(first)
    assert (canonical.read_bytes(), factor.read_bytes()) == snapshot


def test_full_raw_is_unchanged_from_current_behaviour(tmp_path):
    lake = _lake(tmp_path, raw_from_day=1)

    out = _resume(lake)

    assert out.height == 12 * 24
    assert out["funding_rate"].to_list() == [2e-9] * out.height
    assert out["source"].to_list() == ["hypersync"] * out.height


def test_history_never_shrinks_when_a_source_disappears(tmp_path):
    """Hours with no source at all stay; they are never dropped silently."""
    rates = tmp_path / "rates" / "ETH"
    rates.mkdir(parents=True)
    canonical = _canonical(3, 1e-9)
    canonical.write_parquet(rates / "1h.parquet")
    pl.DataFrame(
        {
            "timestamp": [DAY0],
            "funding_rate": [4e-9],
            "funding_rate_hourly": [4e-9 * 3600],
            "symbol": ["ETH"],
            "market": ["0xm"],
        },
        schema_overrides={"timestamp": pl.Datetime("ns", "UTC")},
    ).write_parquet(rates / "1h_datastore.parquet")

    _unified.merge_symbol("ETH", tmp_path / "rates", tmp_path / "direction")
    out = pl.read_parquet(rates / "1h.parquet")

    assert out.height == canonical.height
