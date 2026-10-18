"""Tests for live GMX V2 funding rate fetcher."""

import importlib.util
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import polars as pl
import pyarrow.feather as feather

from gmx_historical_data.freqtrade_exporter import _drop_rejected_funding_hours
from gmx_historical_data.live_funding import fetch_live_funding_rates, upsert_live_rates_to_feather

_HOURS_PER_YEAR = 365.25 * 24


def _load_export_feather():
    path = Path(__file__).resolve().parents[1] / "scripts" / "extract_unified_funding.py"
    spec = importlib.util.spec_from_file_location("live_funding_export_script", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.export_feather


def _make_api_response(markets):
    return {"markets": markets}


def test_fetch_converts_annualized_to_hourly():
    """API value / 1e30 / 8766 = per-hour rate."""
    # ETH verified value from real API call on 2026-02-19
    eth_long_raw = 92443962298858360338273696000
    expected_hourly = -eth_long_raw / 1e30 / _HOURS_PER_YEAR  # API positive means longs receive

    markets = [
        {
            "name": "ETH/USD [ETH-USDC]",
            "fundingRateLong": str(eth_long_raw),
            "fundingRateShort": "-135433929302153055533393494714",
            "openInterestLong": "1000000",
            "openInterestShort": "800000",
            "isListed": True,
        }
    ]
    with patch("gmx_historical_data.live_funding.GMXAPI") as mock_cls:
        mock_api = MagicMock()
        mock_api.get_markets_info.return_value = _make_api_response(markets)
        mock_cls.return_value = mock_api

        rates = fetch_live_funding_rates()

    assert "ETH" in rates
    assert abs(rates["ETH"] - expected_hourly) < 1e-12


def test_fetch_deduplicates_by_open_interest():
    """When two markets share a symbol, picks the one with higher OI."""
    markets = [
        {
            "name": "ETH/USD [ETH-USDC]",
            "fundingRateLong": "1000000000000000000000000000000",  # 1e30 -> 1/8766/h
            "fundingRateShort": "0",
            "openInterestLong": "5000000",
            "openInterestShort": "4000000",  # total OI = 9M
            "isListed": True,
        },
        {
            "name": "ETH/USD [wstETH-USDe]",
            "fundingRateLong": "2000000000000000000000000000000",  # 2e30 -> 2/8766/h
            "fundingRateShort": "0",
            "openInterestLong": "500000",
            "openInterestShort": "300000",  # total OI = 800k (lower)
            "isListed": True,
        },
    ]
    with patch("gmx_historical_data.live_funding.GMXAPI") as mock_cls:
        mock_api = MagicMock()
        mock_api.get_markets_info.return_value = _make_api_response(markets)
        mock_cls.return_value = mock_api

        rates = fetch_live_funding_rates()

    # Should pick ETH-USDC market (higher OI); API positive means longs receive.
    assert abs(rates["ETH"] - (-1.0 / _HOURS_PER_YEAR)) < 1e-12


def test_fetch_skips_unlisted_markets():
    """Markets with isListed=False are excluded."""
    markets = [
        {
            "name": "APE/USD [APE-USDC]",
            "fundingRateLong": "1000000000000000000000000000000",
            "fundingRateShort": "0",
            "openInterestLong": "0",
            "openInterestShort": "0",
            "isListed": False,
        }
    ]
    with patch("gmx_historical_data.live_funding.GMXAPI") as mock_cls:
        mock_api = MagicMock()
        mock_api.get_markets_info.return_value = _make_api_response(markets)
        mock_cls.return_value = mock_api

        rates = fetch_live_funding_rates()

    assert "APE" not in rates


def test_fetch_inverts_negative_funding_rate_long_pays():
    """GMX negative fundingRateLong means longs pay, positive in our export."""
    raw = -92443962298858360338273696000
    markets = [
        {
            "name": "ETH/USD [ETH-USDC]",
            "fundingRateLong": str(raw),
            "fundingRateShort": "0",
            "openInterestLong": "1000000",
            "openInterestShort": "800000",
            "isListed": True,
        }
    ]
    with patch("gmx_historical_data.live_funding.GMXAPI") as mock_cls:
        mock_api = MagicMock()
        mock_api.get_markets_info.return_value = _make_api_response(markets)
        mock_cls.return_value = mock_api

        rates = fetch_live_funding_rates()

    assert rates["ETH"] > 0
    assert abs(rates["ETH"] - (-raw / 1e30 / _HOURS_PER_YEAR)) < 1e-12


def test_fetch_skips_swap_only_markets():
    """Markets without '/' in name (swap-only) are excluded."""
    markets = [
        {
            "name": "SWAP-ONLY [USDC-USDT]",
            "fundingRateLong": "0",
            "fundingRateShort": "0",
            "openInterestLong": "0",
            "openInterestShort": "0",
            "isListed": True,
        }
    ]
    with patch("gmx_historical_data.live_funding.GMXAPI") as mock_cls:
        mock_api = MagicMock()
        mock_api.get_markets_info.return_value = _make_api_response(markets)
        mock_cls.return_value = mock_api

        rates = fetch_live_funding_rates()

    assert len(rates) == 0


# =============================================================================
# upsert_live_rates_to_feather tests
# =============================================================================


def _make_feather_file(directory: Path, symbol: str, rows: list[dict]) -> Path:
    """Create a minimal funding rate feather file for testing."""
    gmx_dir = directory / "gmx" / "futures"
    gmx_dir.mkdir(parents=True, exist_ok=True)
    filepath = gmx_dir / f"{symbol}_USDC_USDC-1h-funding_rate.feather"
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    feather.write_feather(df, filepath)
    return filepath


def test_upsert_appends_new_row():
    """New timestamp is appended; file gains one row."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        _make_feather_file(
            tmppath,
            "ETH",
            [
                {
                    "date": "2026-02-19 06:00:00",
                    "open": 1e-5,
                    "high": 0.0,
                    "low": 0.0,
                    "close": 0.0,
                    "volume": 0.0,
                },
                {
                    "date": "2026-02-19 07:00:00",
                    "open": 1e-5,
                    "high": 0.0,
                    "low": 0.0,
                    "close": 0.0,
                    "volume": 0.0,
                },
            ],
        )

        with patch("gmx_historical_data.live_funding._now_utc") as mock_now:
            mock_now.return_value = datetime(2026, 2, 19, 8, 30, 0, tzinfo=UTC)
            count = upsert_live_rates_to_feather(tmppath, {"ETH": 1.1e-5})

        assert count == 1
        df = pd.read_feather(tmppath / "gmx" / "futures" / "ETH_USDC_USDC-1h-funding_rate.feather")
        assert len(df) == 3
        assert df["date"].iloc[-1] == pd.Timestamp("2026-02-19 08:00:00", tz="UTC")
        assert abs(df["open"].iloc[-1] - 1.1e-5) < 1e-12


def test_upsert_overwrites_same_hour():
    """Row at same hour boundary is replaced, not duplicated."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        _make_feather_file(
            tmppath,
            "ETH",
            [
                {
                    "date": "2026-02-19 08:00:00",
                    "open": 9e-6,
                    "high": 0.0,
                    "low": 0.0,
                    "close": 0.0,
                    "volume": 0.0,
                }
            ],
        )

        with patch("gmx_historical_data.live_funding._now_utc") as mock_now:
            mock_now.return_value = datetime(2026, 2, 19, 8, 45, 0, tzinfo=UTC)
            upsert_live_rates_to_feather(tmppath, {"ETH": 1.2e-5})

        df = pd.read_feather(tmppath / "gmx" / "futures" / "ETH_USDC_USDC-1h-funding_rate.feather")
        assert len(df) == 1
        assert abs(df["open"].iloc[0] - 1.2e-5) < 1e-12


def test_upsert_skips_missing_file():
    """Symbol with no feather file is silently skipped; count reflects actual updates."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        with patch("gmx_historical_data.live_funding._now_utc") as mock_now:
            mock_now.return_value = datetime(2026, 2, 19, 8, 0, 0, tzinfo=UTC)
            count = upsert_live_rates_to_feather(tmppath, {"BTC": 5e-6})

        assert count == 0


def test_upsert_respects_market_filter():
    """market_filter='ETH/USD' only updates ETH, skips BTC."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        _make_feather_file(
            tmppath,
            "ETH",
            [
                {
                    "date": "2026-02-19 07:00:00",
                    "open": 1e-5,
                    "high": 0.0,
                    "low": 0.0,
                    "close": 0.0,
                    "volume": 0.0,
                }
            ],
        )
        _make_feather_file(
            tmppath,
            "BTC",
            [
                {
                    "date": "2026-02-19 07:00:00",
                    "open": 2e-6,
                    "high": 0.0,
                    "low": 0.0,
                    "close": 0.0,
                    "volume": 0.0,
                }
            ],
        )

        with patch("gmx_historical_data.live_funding._now_utc") as mock_now:
            mock_now.return_value = datetime(2026, 2, 19, 8, 0, 0, tzinfo=UTC)
            count = upsert_live_rates_to_feather(
                tmppath, {"ETH": 1.1e-5, "BTC": 2.1e-6}, market_filter="ETH/USD"
            )

        assert count == 1
        eth_df = pd.read_feather(
            tmppath / "gmx" / "futures" / "ETH_USDC_USDC-1h-funding_rate.feather"
        )
        btc_df = pd.read_feather(
            tmppath / "gmx" / "futures" / "BTC_USDC_USDC-1h-funding_rate.feather"
        )
        assert len(eth_df) == 2
        assert len(btc_df) == 1


def test_export_feather_output_is_updated_by_live_upsert(tmp_path):
    """The unified export and live upsert share the same feather directory."""
    network_dir = tmp_path / "source" / "funding" / "arbitrum"
    rates_dir = network_dir / "rates" / "ETH"
    rates_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "timestamp": pl.Series([datetime(2026, 2, 19, 7, tzinfo=UTC)]),
            "funding_rate_hourly": [1e-5],
            "longs_pay_shorts": [True],
        }
    ).write_parquet(rates_dir / "1h.parquet")

    export_feather = _load_export_feather()
    export_feather(network_dir, tmp_path)
    destination = tmp_path / "gmx" / "futures" / "ETH_USDC_USDC-1h-funding_rate.feather"
    assert destination.is_file()

    with patch("gmx_historical_data.live_funding._now_utc") as mock_now:
        mock_now.return_value = datetime(2026, 2, 19, 8, 30, tzinfo=UTC)
        count = upsert_live_rates_to_feather(tmp_path, {"ETH": 1.1e-5})

    assert count == 1
    rows = pd.read_feather(destination)
    assert len(rows) == 2
    assert rows["date"].iloc[-1] == pd.Timestamp("2026-02-19 08:00:00", tz="UTC")
    assert rows["open"].iloc[-1] == 1.1e-5


def test_rejected_hour_anti_join_handles_tz_naive_destination():
    """Legacy naive dates are aligned to the source's UTC date dtype before joining."""
    existing = pl.DataFrame(
        {
            "date": pl.Series(
                [datetime(2026, 2, 19, hour) for hour in (7, 8)],
                dtype=pl.Datetime("ns"),
            ),
            "open": [1e-5, 2e-5],
        }
    )

    kept, removed = _drop_rejected_funding_hours(existing, [datetime(2026, 2, 19, 8, tzinfo=UTC)])

    assert removed == 1
    assert kept.schema["date"] == pl.Datetime("ns", "UTC")
    assert kept["open"].to_list() == [1e-5]
