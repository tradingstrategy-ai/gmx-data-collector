"""Compatibility tests for raw funding parquet files from older extractors."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import polars as pl
import pytest

from gmx_historical_data.raw_schema import report_legacy_raw_columns


def _load_script(name: str):
    path = Path(__file__).resolve().parents[1] / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


factor = _load_script("extract_funding_factor")
fee = _load_script("extract_funding_fee_per_size")


def _factor_frame(legacy: bool) -> pl.DataFrame:
    data = {
        "symbol": ["ETH"],
        "market": ["0xm"],
        "funding_factor_per_second": ["1000000000000000000000000000000"],
        "funding_rate_per_second": [1.0],
        "block_number": [1],
        "block_timestamp": [1767225600],
        "block_datetime": ["2026-01-01T00:00:00+00:00"],
        "transaction_hash": ["0x01"],
        "log_index": [0],
    }
    if legacy:
        data["longs_pay_shorts"] = [True]
    return pl.DataFrame(data)


def _fee_frame(legacy: bool) -> pl.DataFrame:
    data = {
        "symbol": ["ETH"],
        "market": ["0xm"],
        "collateral_token": ["0xc"],
        "is_long": [True],
        "delta": ["100"],
        "value": ["100"],
        "block_number": [1],
        "block_timestamp": [1767225600],
        "block_datetime": ["2026-01-01T00:00:00+00:00"],
        "transaction_hash": ["0x01"],
        "log_index": [0],
    }
    if legacy:
        # Synthetic extra; the seeded fee_per_size schema already matches current.
        data["longs_pay_shorts"] = [True]
    return pl.DataFrame(data)


def test_factor_repair_scan_drops_legacy_columns_and_matches_clean_schema(tmp_path, capsys):
    report_legacy_raw_columns()
    capsys.readouterr()
    clean_dir = tmp_path / "clean"
    legacy_dir = tmp_path / "legacy"
    for root, legacy in ((clean_dir, False), (legacy_dir, True)):
        raw = root / "raw" / "funding" / "ETH" / "partition=0" / "data.parquet"
        raw.parent.mkdir(parents=True)
        _factor_frame(legacy).write_parquet(raw)

    factor.rebuild_rates_from_raw(clean_dir, repair_only=True)
    factor.rebuild_rates_from_raw(legacy_dir, repair_only=True)

    clean = pl.read_parquet(clean_dir / "rates/ETH/1h_factor.parquet")
    repaired = pl.read_parquet(legacy_dir / "rates/ETH/1h_factor.parquet")
    assert repaired.equals(clean)
    assert "longs_pay_shorts" not in repaired.columns
    report_legacy_raw_columns()
    assert "1 file(s): longs_pay_shorts" in capsys.readouterr().out


def test_direction_recompute_drops_legacy_columns_and_matches_clean_schema(tmp_path, capsys):
    report_legacy_raw_columns()
    capsys.readouterr()
    clean_dir = tmp_path / "clean"
    legacy_dir = tmp_path / "legacy"
    touched = fee.FundingFeePerSizeRecord(
        symbol="ETH",
        market="0xm",
        collateral_token="0xc",
        is_long=True,
        delta="100",
        value="100",
        block_number=1,
        block_timestamp=1767225600,
        block_datetime="2026-01-01T00:00:00+00:00",
        transaction_hash="0x01",
        log_index=0,
    )
    for root, legacy in ((clean_dir, False), (legacy_dir, True)):
        raw = root / "raw" / "fee_per_size" / "ETH" / "data.parquet"
        raw.parent.mkdir(parents=True)
        _fee_frame(legacy).write_parquet(raw)

    fee.rebuild_direction_from_raw(clean_dir, touched_records=[touched])
    fee.rebuild_direction_from_raw(legacy_dir, touched_records=[touched])

    clean = pl.read_parquet(clean_dir / "direction/ETH/1h.parquet")
    repaired = pl.read_parquet(legacy_dir / "direction/ETH/1h.parquet")
    assert repaired.equals(clean)
    assert repaired["longs_pay_shorts"].to_list() == [True]
    report_legacy_raw_columns()
    assert "1 file(s): longs_pay_shorts" in capsys.readouterr().out


def test_raw_append_rewrites_legacy_columns_for_both_extractors(tmp_path):
    factor_raw = tmp_path / "raw/funding/ETH/partition=0/data.parquet"
    fee_raw = tmp_path / "raw/fee_per_size/ETH/data.parquet"
    factor_raw.parent.mkdir(parents=True)
    fee_raw.parent.mkdir(parents=True)
    _factor_frame(True).write_parquet(factor_raw)
    _fee_frame(True).write_parquet(fee_raw)

    factor.save_raw_per_symbol(
        [
            factor.FundingFactorRecord(
                symbol="ETH",
                market="0xm",
                funding_factor_per_second="2",
                funding_rate_per_second=2.0,
                block_number=2,
                block_timestamp=1767225660,
                block_datetime="2026-01-01T00:01:00+00:00",
                transaction_hash="0x02",
                log_index=0,
            )
        ],
        tmp_path,
    )
    fee.save_raw_per_symbol(
        [
            fee.FundingFeePerSizeRecord(
                symbol="ETH",
                market="0xm",
                collateral_token="0xc",
                is_long=False,
                delta="2",
                value="102",
                block_number=2,
                block_timestamp=1767225660,
                block_datetime="2026-01-01T00:01:00+00:00",
                transaction_hash="0x02",
                log_index=0,
            )
        ],
        tmp_path,
    )

    assert "longs_pay_shorts" not in pl.read_parquet(factor_raw).columns
    assert "longs_pay_shorts" not in pl.read_parquet(fee_raw).columns
    assert pl.read_parquet(factor_raw).height == 2
    assert pl.read_parquet(fee_raw).height == 2


@pytest.mark.parametrize("module", [factor, fee])
def test_raw_append_is_idempotent_without_rewriting_parquet(tmp_path, module):
    if module is factor:
        record = factor.FundingFactorRecord(
            symbol="ETH",
            market="0xm",
            funding_factor_per_second="1",
            funding_rate_per_second=1.0,
            block_number=1,
            block_timestamp=1767225600,
            block_datetime="2026-01-01T00:00:00+00:00",
            transaction_hash="0x01",
            log_index=0,
        )
        filepath = tmp_path / "raw/funding/ETH/partition=0/data.parquet"
    else:
        record = fee.FundingFeePerSizeRecord(
            symbol="ETH",
            market="0xm",
            collateral_token="0xc",
            is_long=True,
            delta="1",
            value="1",
            block_number=1,
            block_timestamp=1767225600,
            block_datetime="2026-01-01T00:00:00+00:00",
            transaction_hash="0x01",
            log_index=0,
        )
        filepath = tmp_path / "raw/fee_per_size/ETH/data.parquet"

    module.save_raw_per_symbol([record], tmp_path)
    original = filepath.read_bytes()
    original_mtime = filepath.stat().st_mtime_ns

    module.save_raw_per_symbol([record], tmp_path)

    assert filepath.read_bytes() == original
    assert filepath.stat().st_mtime_ns == original_mtime


@pytest.mark.parametrize("module", [factor, fee])
def test_saving_no_raw_rows_does_not_write(tmp_path, module):
    module.save_raw_per_symbol([], tmp_path)

    assert not (tmp_path / "raw").exists()


@pytest.mark.parametrize(
    ("module", "frame", "relative_path", "rebuild"),
    [
        (factor, _factor_frame(False), "raw/funding/ETH/partition=0/data.parquet", "factor"),
        (fee, _fee_frame(False), "raw/fee_per_size/ETH/data.parquet", "fee"),
    ],
)
def test_raw_rebuild_rejects_nonpositive_timestamp(tmp_path, module, frame, relative_path, rebuild):
    raw = tmp_path / relative_path
    raw.parent.mkdir(parents=True)
    frame.with_columns(pl.lit(0).alias("block_timestamp")).write_parquet(raw)

    with pytest.raises(ValueError, match=rf"{raw}.*null or non-positive block_timestamp"):
        if rebuild == "factor":
            module.rebuild_rates_from_raw(tmp_path)
        else:
            record = module.FundingFeePerSizeRecord(
                symbol="ETH",
                market="0xm",
                collateral_token="0xc",
                is_long=True,
                delta="1",
                value="1",
                block_number=2,
                block_timestamp=1767225600,
                block_datetime="2026-01-01T00:00:00+00:00",
                transaction_hash="0x02",
                log_index=0,
            )
            module.rebuild_direction_from_raw(tmp_path, touched_records=[record])


@pytest.mark.parametrize(
    ("module", "frame", "relative_path", "rebuild"),
    [
        (factor, _factor_frame(False), "raw/funding/ETH/partition=0/data.parquet", "factor"),
        (fee, _fee_frame(False), "raw/fee_per_size/ETH/data.parquet", "fee"),
    ],
)
def test_raw_rebuild_rejects_null_timestamp(tmp_path, module, frame, relative_path, rebuild):
    raw = tmp_path / relative_path
    raw.parent.mkdir(parents=True)
    frame.with_columns(pl.lit(None, dtype=pl.Int64).alias("block_timestamp")).write_parquet(raw)

    with pytest.raises(ValueError, match=rf"{raw}.*null or non-positive block_timestamp"):
        if rebuild == "factor":
            module.rebuild_rates_from_raw(tmp_path)
        else:
            record = module.FundingFeePerSizeRecord(
                symbol="ETH",
                market="0xm",
                collateral_token="0xc",
                is_long=True,
                delta="1",
                value="1",
                block_number=2,
                block_timestamp=1767225600,
                block_datetime="2026-01-01T00:00:00+00:00",
                transaction_hash="0x02",
                log_index=0,
            )
            module.rebuild_direction_from_raw(tmp_path, touched_records=[record])


@pytest.mark.parametrize(
    ("module", "frame", "relative_path", "rebuild"),
    [
        (
            factor,
            _factor_frame(False).drop("market"),
            "raw/funding/ETH/partition=0/data.parquet",
            "factor",
        ),
        (fee, _fee_frame(False).drop("market"), "raw/fee_per_size/ETH/data.parquet", "fee"),
    ],
)
def test_raw_rebuild_reports_missing_required_fields(
    tmp_path, module, frame, relative_path, rebuild
):
    raw = tmp_path / relative_path
    raw.parent.mkdir(parents=True)
    frame.write_parquet(raw)

    with pytest.raises(ValueError, match=rf"{raw}.*market"):
        if rebuild == "factor":
            module.rebuild_rates_from_raw(tmp_path)
        else:
            record = module.FundingFeePerSizeRecord(
                symbol="ETH",
                market="0xm",
                collateral_token="0xc",
                is_long=True,
                delta="1",
                value="1",
                block_number=2,
                block_timestamp=1767225600,
                block_datetime="2026-01-01T00:00:00+00:00",
                transaction_hash="0x02",
                log_index=0,
            )
            module.rebuild_direction_from_raw(tmp_path, touched_records=[record])
