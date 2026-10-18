"""CLI regression tests for explicit USD flow completeness accounting."""

import sys
from io import StringIO

import polars as pl


def _write_input(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)


def test_missing_price_and_receiving_event_fail_without_replacing_output(
    tmp_path, monkeypatch, capsys
):
    from scripts import build_usd_funding_flows as builder

    network_dir = tmp_path / "arbitrum"
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
        "delta": ["100", "100", "100", "100"],
        "symbol": ["ETH"] * 4,
    }
    _write_input(
        network_dir / "raw/fee_per_size/ETH/data.parquet",
        common,
    )
    # The receiving event for d is deliberately absent.
    _write_input(
        network_dir / "raw/claimable_fee_per_size/ETH/data.parquet",
        {key: values[:3] for key, values in common.items()},
    )
    # b has no price, while a, c and d are priced.
    _write_input(
        network_dir / "raw/oracle_prices/data.parquet",
        {
            "transaction_hash": ["a", "c", "d"],
            "token": ["0xtoken"] * 3,
            "max_price": ["1000000000000000000000000000000"] * 3,
            "log_index": [0] * 3,
        },
    )

    output = network_dir / "usd_flows/ETH/events.parquet"
    output.parent.mkdir(parents=True)
    prior_bytes = b"previous output must remain untouched"
    output.write_bytes(prior_bytes)

    monkeypatch.setattr(builder, "fetch_markets", lambda *args, **kwargs: {})
    report = StringIO()
    monkeypatch.setattr(builder, "console", builder.Console(file=report, width=140))
    monkeypatch.setattr(
        sys,
        "argv",
        ["build_usd_funding_flows.py", "--output-dir", str(tmp_path), "--min-completeness", "0.9"],
    )
    try:
        builder.main()
    except SystemExit as exc:
        assert exc.code == 1
    else:
        raise AssertionError("incomplete flows should exit non-zero")

    report_text = report.getvalue()
    assert "Rejected for missing oracle price: 2 events" in report_text
    assert "1 paying events, 0 receiving events" in report_text
    assert "Completeness: 40.00% (minimum 90.00%)" in report_text
    assert output.read_bytes() == prior_bytes
