"""Tests for direction merge in :mod:`scripts.extract_unified_funding`.

Verifies that ``apply_direction_to_rates``:

- Forward-fills direction within (symbol, market) so the last observed
  direction carries forward until the next direction event flips it.
- Leaves ``longs_pay_shorts`` and signed-fee columns as ``null`` for the
  leading gap (hours before the first direction event).
"""

from __future__ import annotations

import hashlib
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


def _load_fee_module():
    repo = Path(__file__).resolve().parents[1]
    path = repo / "scripts" / "extract_funding_fee_per_size.py"
    spec = importlib.util.spec_from_file_location("extract_funding_fee_per_size", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["extract_funding_fee_per_size"] = module
    spec.loader.exec_module(module)
    return module


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


def test_datastore_only_merge_adds_missing_provenance_columns(tmp_path):
    rates_dir = tmp_path / "rates"
    direction_dir = tmp_path / "direction"
    sym_dir = rates_dir / "ETH"
    sym_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "timestamp": [datetime(2026, 1, 1, tzinfo=UTC)],
            "funding_rate": [-1e-9],
            "funding_rate_hourly": [-3.6e-6],
            "symbol": ["ETH"],
            "market": ["0xm"],
        },
        schema_overrides={"timestamp": pl.Datetime("ns", "UTC")},
    ).write_parquet(sym_dir / "1h_datastore.parquet")

    rows = _unified.merge_symbol("ETH", rates_dir, direction_dir)
    out = pl.read_parquet(sym_dir / "1h.parquet")

    assert rows == 1
    assert out["source"].to_list() == ["datastore"]
    assert out["is_gap_filled"].to_list() == [False]


def test_merge_ignores_legacy_factor_companion_without_raw_backing(tmp_path):
    rates_dir = tmp_path / "rates"
    direction_dir = tmp_path / "direction"
    sym_dir = rates_dir / "LTC"
    sym_dir.mkdir(parents=True)
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    pl.DataFrame(
        {
            "timestamp": [timestamp],
            "funding_rate": [-1e-9],
            "funding_rate_hourly": [-3.6e-6],
            "funding_fee_long": [0.01],
            "funding_fee_short": [-0.01],
            "longs_pay_shorts": [False],
            "symbol": ["LTC"],
            "market": ["0xltc"],
        },
        schema_overrides={"timestamp": pl.Datetime("ns", "UTC")},
    ).write_parquet(sym_dir / "1h_datastore.parquet")
    pl.DataFrame(
        {
            "timestamp": [timestamp],
            "funding_rate": [2e-9],
            "funding_rate_hourly": [7.2e-6],
            "funding_fee_long": [None],
            "source": ["hypersync"],
            "symbol": ["LTC"],
            "market": ["0xltc"],
        },
        schema_overrides={"timestamp": pl.Datetime("ns", "UTC")},
    ).write_parquet(sym_dir / "1h_factor.parquet")

    _unified.merge_symbol("LTC", rates_dir, direction_dir)
    out = pl.read_parquet(sym_dir / "1h.parquet")

    assert out["source"].to_list() == ["datastore"]
    # The stored fee columns are never trusted: they are re-derived from the signed rate.
    assert out["funding_fee_long"].to_list() == [-3.6e-6]
    assert out["funding_fee_short"].to_list() == [3.6e-6]
    assert out["longs_pay_shorts"].to_list() == [False]


def test_merge_cli_accepts_makefile_block_range_flags():
    args = _unified.build_parser().parse_args(["--from-block", "10", "--to-block", "20"])
    assert args.from_block == 10
    assert args.to_block == 20


def test_repeated_merge_is_byte_identical_and_filtered_merge_leaves_other_symbols(tmp_path):
    rates_dir = tmp_path / "rates"
    for symbol in ("ETH", "BTC"):
        sym_dir = rates_dir / symbol
        sym_dir.mkdir(parents=True)
        pl.DataFrame(
            {
                "timestamp": [datetime(2026, 1, 1, tzinfo=UTC)],
                "funding_rate": [-1e-9],
                "funding_rate_hourly": [-3.6e-6],
                "symbol": [symbol],
                "market": [f"0x{symbol.lower()}"],
            },
            schema_overrides={"timestamp": pl.Datetime("ns", "UTC")},
        ).write_parquet(sym_dir / "1h_datastore.parquet")

    _unified.merge_unified_rates(tmp_path)
    eth_path = rates_dir / "ETH" / "1h.parquet"
    btc_path = rates_dir / "BTC" / "1h.parquet"
    before_eth = hashlib.sha256(eth_path.read_bytes()).digest()
    before_btc = hashlib.sha256(btc_path.read_bytes()).digest()

    _unified.merge_unified_rates(tmp_path, "ETH/USD")

    assert hashlib.sha256(eth_path.read_bytes()).digest() == before_eth
    assert hashlib.sha256(btc_path.read_bytes()).digest() == before_btc


def test_direction_flush_recomputes_touched_hour_from_all_persisted_events(tmp_path):
    fee = _load_fee_module()
    base = datetime(2026, 1, 1, 1, 0, tzinfo=UTC)

    def record(block, is_long, delta):
        return fee.FundingFeePerSizeRecord(
            symbol="ETH",
            market="0xm",
            collateral_token="0xc",
            is_long=is_long,
            delta=str(delta),
            value=str(delta),
            block_number=block,
            block_timestamp=int((base + timedelta(minutes=block)).timestamp()),
            block_datetime=(base + timedelta(minutes=block)).isoformat(),
            transaction_hash=f"0x{block:064x}",
            log_index=0,
        )

    fee.save_raw_per_symbol([record(1, True, 100)], tmp_path)
    first = [record(1, True, 100)]
    fee.rebuild_direction_from_raw(tmp_path, touched_records=first)
    second = [record(2, False, 1)]
    fee.save_raw_per_symbol(second, tmp_path)
    fee.rebuild_direction_from_raw(tmp_path, touched_records=second)

    out = pl.read_parquet(tmp_path / "direction" / "ETH" / "1h.parquet")
    assert out["longs_pay_shorts"].to_list() == [True]
    assert out["update_count"].to_list() == [2]
