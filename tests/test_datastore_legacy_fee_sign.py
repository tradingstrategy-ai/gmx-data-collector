"""Legacy ``1h_datastore.parquet`` rows carry an inverted ``funding_fee_long``.

The pre-#69 extractor multiplied a negative rate by another negative sign, so every
negative-rate row stored ``funding_fee_long`` with the wrong sign.
``funding_rate_hourly`` was always correctly signed, so the merge (and every other
reader) must derive ``funding_fee_long``/``funding_fee_short``/``longs_pay_shorts``
from it and never trust the stored columns.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

from gmx_historical_data.datastore_signs import repair_datastore_file, resign_from_rate
from gmx_historical_data.freqtrade_exporter import FreqtradeExporter

START = datetime(2026, 1, 1, tzinfo=UTC)
RATES = [3e-6, -2e-6, 0.0, -5e-7, 1e-6]


def _load_unified():
    path = Path(__file__).resolve().parents[1] / "scripts" / "extract_unified_funding.py"
    spec = importlib.util.spec_from_file_location("extract_unified_funding", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["extract_unified_funding"] = module
    spec.loader.exec_module(module)
    return module


_unified = _load_unified()


def _legacy_frame() -> pl.DataFrame:
    """A pre-#69 DataStore frame: negative rows have the fee sign (and direction) inverted."""
    n = len(RATES)
    return pl.DataFrame(
        {
            "timestamp": [START + timedelta(hours=i) for i in range(n)],
            "funding_rate": [r / 3600 for r in RATES],
            "funding_rate_hourly": RATES,
            # Legacy bug: the negative-rate rows are flipped.
            "longs_pay_shorts": [True, True, False, True, True],
            "funding_fee_long": [3e-6, 2e-6, 0.0, 5e-7, 1e-6],
            "funding_fee_short": [-3e-6, -2e-6, 0.0, -5e-7, -1e-6],
            "symbol": ["BTC"] * n,
            "market": ["0xm"] * n,
            "source": ["datastore"] * n,
            "is_gap_filled": [False] * n,
        },
        schema_overrides={"timestamp": pl.Datetime("ns", "UTC")},
    )


def _write_legacy(tmp_path: Path) -> tuple[Path, Path]:
    rates_dir = tmp_path / "rates"
    (rates_dir / "BTC").mkdir(parents=True)
    path = rates_dir / "BTC" / "1h_datastore.parquet"
    _legacy_frame().write_parquet(path)
    return rates_dir, path


def _assert_correct(df: pl.DataFrame) -> None:
    df = df.sort("timestamp")
    assert df["funding_fee_long"].to_list() == RATES
    assert df["funding_fee_short"].to_list() == [-r for r in RATES]
    # Strictly positive -> longs pay; zero and negative -> not.
    assert df["longs_pay_shorts"].to_list() == [True, False, False, False, True]


def test_resign_from_rate_covers_positive_zero_and_negative() -> None:
    _assert_correct(resign_from_rate(_legacy_frame()))


def test_resign_keeps_null_rates_null() -> None:
    df = _legacy_frame().with_columns(
        pl.when(pl.col("timestamp") == START)
        .then(None)
        .otherwise(pl.col("funding_rate_hourly"))
        .alias("funding_rate_hourly")
    )
    out = resign_from_rate(df).sort("timestamp")
    assert out["funding_fee_long"][0] is None
    assert out["funding_fee_short"][0] is None
    assert out["longs_pay_shorts"][0] is None


def test_merge_emits_corrected_canonical_fees_and_direction(tmp_path: Path) -> None:
    rates_dir, _ = _write_legacy(tmp_path)

    assert _unified.merge_symbol("BTC", rates_dir, tmp_path / "direction") == len(RATES)

    _assert_correct(pl.read_parquet(rates_dir / "BTC" / "1h.parquet"))


def test_merge_exports_the_correct_sign(tmp_path: Path) -> None:
    rates_dir, _ = _write_legacy(tmp_path)
    _unified.merge_symbol("BTC", rates_dir, tmp_path / "direction")

    canonical = pl.read_parquet(rates_dir / "BTC" / "1h.parquet")
    out = FreqtradeExporter(tmp_path, tmp_path)._transform_funding_rate(canonical)

    assert out.sort("date")["open"].to_list() == [3e-6, -2e-6, 0.0, -5e-7, 1e-6]


def test_merge_rewrites_the_stale_datastore_file_once(tmp_path: Path) -> None:
    rates_dir, path = _write_legacy(tmp_path)

    _unified.merge_symbol("BTC", rates_dir, tmp_path / "direction")

    _assert_correct(pl.read_parquet(path))
    assert not list(path.parent.glob("*.tmp"))


def test_repair_is_idempotent_and_does_not_touch_a_correct_file(tmp_path: Path) -> None:
    _, path = _write_legacy(tmp_path)

    assert repair_datastore_file(path) is True
    stat = path.stat()
    content = path.read_bytes()

    assert repair_datastore_file(path) is False
    assert path.read_bytes() == content
    assert path.stat().st_mtime_ns == stat.st_mtime_ns


def test_second_merge_leaves_every_file_byte_identical(tmp_path: Path) -> None:
    rates_dir, path = _write_legacy(tmp_path)
    canonical = rates_dir / "BTC" / "1h.parquet"

    _unified.merge_symbol("BTC", rates_dir, tmp_path / "direction")
    first = (path.read_bytes(), canonical.read_bytes())
    _unified.merge_symbol("BTC", rates_dir, tmp_path / "direction")

    assert (path.read_bytes(), canonical.read_bytes()) == first


def test_stale_canonical_is_corrected_on_the_next_merge(tmp_path: Path) -> None:
    """A canonical file produced by the buggy merge is replaced, not preserved."""
    rates_dir, path = _write_legacy(tmp_path)
    canonical = rates_dir / "BTC" / "1h.parquet"
    pl.read_parquet(path).write_parquet(canonical)

    _unified.merge_symbol("BTC", rates_dir, tmp_path / "direction")

    _assert_correct(pl.read_parquet(canonical))


def test_datastore_variant_export_reads_corrected_rows(tmp_path: Path) -> None:
    rates_dir, _ = _write_legacy(tmp_path)

    df = FreqtradeExporter(tmp_path, tmp_path, funding_dir=rates_dir)._read_funding_rate(
        "BTC", "1h_datastore"
    )

    _assert_correct(df)
