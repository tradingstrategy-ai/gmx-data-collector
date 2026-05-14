"""End-to-end test: gate prevents API calls when daily files exist."""

from unittest.mock import patch

import pandas as pd
import polars as pl
import pyarrow.feather as feather
import pytest


@pytest.fixture
def seeded_dir(tmp_path):
    """Seed all daily-stamped files for 2026-05-14 plus all 6 timeframes of
    OHLCV with future dates so every gate fires.

    Returns the directory we will pass to ``--output-dir`` (a ``user_data``
    sub-dir, mirroring the production layout where the report lands at
    ``output_dir.parent / data_report.txt``).
    """
    output_dir = tmp_path / "user_data"
    gmx = output_dir / "data" / "gmx"

    snapshot_cols_template = {
        "name": "BTC/USD",
        "is_swap_only": False,
        "is_listed": True,
        "market_token": "0x0",
        "open_interest_long": "0",
        "open_interest_short": "0",
        "index_token": "0x0",
        "long_token": "0x0",
        "short_token": "0x0",
        "listing_date": "",
        "pool_amount_long": "0",
        "pool_amount_short": "0",
        "available_liquidity_long": "0",
        "available_liquidity_short": "0",
        "funding_rate_long": "0",
        "funding_rate_short": "0",
        "borrowing_rate_long": "0",
        "borrowing_rate_short": "0",
        "net_rate_long": "0",
        "net_rate_short": "0",
        "symbol": "BTC",
        "date": "2026-05-14",
    }
    for sub, rows in (("snapshots", 135), ("tickers", 126), ("apy", 945)):
        d = gmx / sub
        d.mkdir(parents=True, exist_ok=True)
        if sub == "snapshots":
            cols = {k: [v] * rows for k, v in snapshot_cols_template.items()}
        else:
            cols = {"col": list(range(rows))}
        pl.DataFrame(cols).write_parquet(str(d / "2026-05-14.parquet"))

    # Seed an OHLCV feather for every timeframe so every (BTC, tf) hits the gate.
    fut = gmx / "futures"
    fut.mkdir(parents=True, exist_ok=True)
    future_df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2099-12-31"], utc=True).as_unit("ns"),
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [0.0],
        }
    )
    for tf in ("1m", "5m", "15m", "1h", "4h", "1d"):
        feather.write_feather(future_df, fut / f"BTC_USDC_USDC-{tf}-futures.feather")

    return output_dir


def test_gate_skips_tickers_and_apy_api(seeded_dir):
    """Verify get_tickers() / get_apy() / get_candlesticks_dataframe() are
    never called when on-disk data is current."""
    import sys

    from scripts import collect_daily_snapshot as cds

    def raise_called(*args, **kwargs):
        raise AssertionError("API must not be called when gate fires")

    class StubAPI:
        def __init__(self, chain):
            pass

        def get_markets_info(self):
            # Markets API is NOT gated by design (see spec); return one
            # listed market so the OHLCV loop has a symbol to iterate.
            return {"markets": [{"name": "BTC/USD", "isListed": True}]}

        def get_tickers(self, use_cache=False):
            raise_called()

        def get_apy(self, period, use_cache=False):
            raise_called()

        def get_candlesticks_dataframe(self, *args, **kwargs):
            raise_called()

    argv = [
        "collect_daily_snapshot.py",
        "--output-dir",
        str(seeded_dir),
        "--date",
        "2026-05-14",
    ]
    with patch("scripts.collect_daily_snapshot.GMXAPI", StubAPI), patch.object(sys, "argv", argv):
        cds.main()

    # Report lands at output_dir.parent / data_report.txt
    report_path = seeded_dir.parent / "data_report.txt"
    report = report_path.read_text()
    assert "## Skipped (already current)" in report
    assert "Markets snapshots:" in report
    assert "Tickers:" in report
    assert "APY:" in report
    # All 6 timeframes for BTC were seeded with future dates → all SKIPPED
    for tf in ("1m", "5m", "15m", "1h", "4h", "1d"):
        assert f"OHLCV {tf}:" in report
