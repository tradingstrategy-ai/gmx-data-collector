"""CLI regression tests for explicit USD flow completeness accounting."""

import sys
from io import StringIO

import polars as pl
import pytest


def _write_input(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)


def _inputs(network_dir, *, missing_price=False, missing_receiving=False):
    base = 1_691_000_000
    hashes = ["a", "b", "c", "d"]
    common = {
        "market": ["0xmarket"] * 4,
        "collateral_token": ["0xtoken"] * 4,
        "is_long": [True] * 4,
        "block_timestamp": [base + i * 100 for i in range(4)],
        "block_number": [1, 2, 3, 4],
        "log_index": [0, 0, 0, 0],
        "transaction_hash": hashes,
        "delta": ["100"] * 4,
        "symbol": ["ETH"] * 4,
    }
    _write_input(network_dir / "raw/fee_per_size/ETH/data.parquet", common)
    receiving = common
    if missing_receiving:
        receiving = {key: values[:3] for key, values in common.items()}
    _write_input(network_dir / "raw/claimable_fee_per_size/ETH/data.parquet", receiving)

    priced_hashes = ["a", "c", "d"] if missing_price else hashes
    _write_input(
        network_dir / "raw/oracle_prices/data.parquet",
        {
            "transaction_hash": priced_hashes,
            "token": ["0xtoken"] * len(priced_hashes),
            "max_price": ["1000000000000000000000000000000"] * len(priced_hashes),
            "log_index": [0] * len(priced_hashes),
        },
    )


def _run_main(monkeypatch, builder, tmp_path, *, extra_args=()):
    report = StringIO()
    monkeypatch.setattr(builder, "console", builder.Console(file=report, width=140))
    monkeypatch.setattr(builder, "fetch_markets", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        sys,
        "argv",
        ["build_usd_funding_flows.py", "--output-dir", str(tmp_path), *extra_args],
    )
    return report


def test_missing_price_and_receiving_event_fail_without_replacing_output(tmp_path, monkeypatch):
    from scripts import build_usd_funding_flows as builder

    network_dir = tmp_path / "arbitrum"
    _inputs(network_dir, missing_price=True, missing_receiving=True)
    output = network_dir / "usd_flows/ETH/events.parquet"
    output.parent.mkdir(parents=True)
    prior_bytes = b"previous output must remain untouched"
    output.write_bytes(prior_bytes)

    report = _run_main(
        monkeypatch,
        builder,
        tmp_path,
        extra_args=("--min-completeness", "0.9"),
    )
    with pytest.raises(SystemExit) as exc:
        builder.main()

    assert exc.value.code == 1
    report_text = report.getvalue()
    assert "Rejected for missing oracle price among eligible events: 2" in report_text
    assert "1 paying events, 0 receiving events" in report_text
    assert "Completeness: 40.00% (minimum 90.00%)" in report_text
    assert output.read_bytes() == prior_bytes


def test_load_and_dedup_accepts_mixed_legacy_raw_schemas(tmp_path):
    from scripts import build_usd_funding_flows as builder

    raw_dir = tmp_path / "raw" / "fee_per_size"
    legacy = {
        "symbol": ["ETH"],
        "market": ["0xmarket"],
        "collateral_token": ["0xtoken"],
        "is_long": [True],
        "delta": ["100"],
        "value": ["100"],
        "block_number": [1],
        "block_timestamp": [1_760_000_000],
        "block_datetime": ["2025-10-09T08:53:20+00:00"],
        "transaction_hash": ["0x01"],
        "log_index": [0],
        "longs_pay_shorts": [True],
    }
    clean = {key: values for key, values in legacy.items() if key != "longs_pay_shorts"}
    (raw_dir / "ETH").mkdir(parents=True)
    (raw_dir / "BTC").mkdir(parents=True)
    pl.DataFrame(legacy).write_parquet(raw_dir / "ETH" / "data.parquet")
    pl.DataFrame(clean).with_columns(
        pl.lit("BTC").alias("symbol"),
        pl.lit("0xbetc").alias("market"),
        pl.lit("0x02").alias("transaction_hash"),
    ).write_parquet(raw_dir / "BTC" / "data.parquet")

    events = builder.load_and_dedup(raw_dir, "funding_fee")

    assert events.height == 2
    assert "longs_pay_shorts" not in events.columns


def test_complete_data_passes_and_writes_output(tmp_path, monkeypatch):
    from scripts import build_usd_funding_flows as builder

    _inputs(tmp_path / "arbitrum")
    report = _run_main(monkeypatch, builder, tmp_path)
    builder.main()

    output = tmp_path / "arbitrum/usd_flows/ETH/events.parquet"
    assert output.exists()
    assert not pl.read_parquet(output).is_empty()
    assert "Completeness: 100.00% (minimum 95.00%)" in report.getvalue()


def test_duplicate_direction_rows_do_not_push_completeness_over_one():
    from scripts.build_usd_funding_flows import flow_completeness

    paying = pl.DataFrame({"transaction_hash": ["a", "a"], "is_long": [True, False]})
    receiving = pl.DataFrame({"transaction_hash": ["a"], "is_long": [False]})

    assert flow_completeness(paying, receiving, 0, 0, 0) == 1.0


def test_empty_oracle_file_fails_clearly(tmp_path, monkeypatch):
    from scripts import build_usd_funding_flows as builder

    network_dir = tmp_path / "arbitrum"
    _inputs(network_dir)
    _write_input(
        network_dir / "raw/oracle_prices/data.parquet",
        {"transaction_hash": [], "token": [], "max_price": [], "log_index": []},
    )
    report = _run_main(monkeypatch, builder, tmp_path)

    with pytest.raises(SystemExit) as exc:
        builder.main()

    assert exc.value.code == 1
    assert "Oracle price data is empty; no flows can be priced." in report.getvalue()


def test_market_with_no_flows_keeps_success_exit(tmp_path, monkeypatch):
    from scripts import build_usd_funding_flows as builder

    _inputs(tmp_path / "arbitrum")
    report = _run_main(monkeypatch, builder, tmp_path, extra_args=("--market", "BTC/USD"))

    with pytest.raises(SystemExit) as exc:
        builder.main()

    assert exc.value.code == 0
    assert "No events for market BTC/USD" in report.getvalue()


def test_present_market_with_no_surviving_flows_fails(tmp_path, monkeypatch):
    from scripts import build_usd_funding_flows as builder

    network_dir = tmp_path / "arbitrum"
    _inputs(network_dir)
    receiving_path = network_dir / "raw/claimable_fee_per_size/ETH/data.parquet"
    receiving = pl.read_parquet(receiving_path).with_columns(
        pl.Series("transaction_hash", ["x", "y", "z", "w"])
    )
    receiving.write_parquet(receiving_path)
    report = _run_main(monkeypatch, builder, tmp_path, extra_args=("--market", "ETH/USD"))

    with pytest.raises(SystemExit) as exc:
        builder.main()

    assert exc.value.code == 1
    assert "No flows survived for market ETH/USD" in report.getvalue()
