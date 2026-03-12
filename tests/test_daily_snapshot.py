"""Tests for daily snapshot collection helpers."""

import pandas as pd
import pyarrow.feather as feather


def _make_ohlcv(dates, close_values):
    """Build a minimal OHLCV DataFrame for testing."""
    return pd.DataFrame(
        {
            "date": pd.to_datetime(dates, utc=True).as_unit("ns"),
            "open": close_values,
            "high": close_values,
            "low": close_values,
            "close": close_values,
            "volume": 0.0,
        }
    )


class TestMergeFeather:
    """Tests for the _merge_feather helper."""

    def test_new_file_created(self, tmp_path):
        """First run creates the file from scratch."""
        from scripts.collect_daily_snapshot import _merge_feather

        filepath = tmp_path / "TEST_USDC_USDC-1d-futures.feather"
        new_df = _make_ohlcv(["2026-03-10", "2026-03-11"], [100.0, 110.0])

        _merge_feather(new_df, filepath)

        result = pd.read_feather(filepath)
        assert len(result) == 2
        assert list(result["close"]) == [100.0, 110.0]

    def test_append_no_overlap(self, tmp_path):
        """New rows are appended when no overlap exists."""
        from scripts.collect_daily_snapshot import _merge_feather

        filepath = tmp_path / "TEST_USDC_USDC-1d-futures.feather"
        existing = _make_ohlcv(["2026-03-10"], [100.0])
        feather.write_feather(existing, filepath)

        new_df = _make_ohlcv(["2026-03-11", "2026-03-12"], [110.0, 120.0])
        _merge_feather(new_df, filepath)

        result = pd.read_feather(filepath)
        assert len(result) == 3
        assert list(result["close"]) == [100.0, 110.0, 120.0]

    def test_overlap_keeps_new(self, tmp_path):
        """Overlapping timestamps use new data, existing data preserved."""
        from scripts.collect_daily_snapshot import _merge_feather

        filepath = tmp_path / "TEST_USDC_USDC-1d-futures.feather"
        existing = _make_ohlcv(["2026-03-10", "2026-03-11"], [100.0, 110.0])
        feather.write_feather(existing, filepath)

        new_df = _make_ohlcv(["2026-03-11", "2026-03-12"], [115.0, 120.0])
        _merge_feather(new_df, filepath)

        result = pd.read_feather(filepath)
        assert len(result) == 3
        assert list(result["close"]) == [100.0, 115.0, 120.0]

    def test_existing_data_never_lost(self, tmp_path):
        """Historical data not in new fetch is always preserved."""
        from scripts.collect_daily_snapshot import _merge_feather

        filepath = tmp_path / "TEST_USDC_USDC-1d-futures.feather"
        existing = _make_ohlcv(["2026-03-08", "2026-03-09", "2026-03-10"], [80.0, 90.0, 100.0])
        feather.write_feather(existing, filepath)

        new_df = _make_ohlcv(["2026-03-11"], [110.0])
        _merge_feather(new_df, filepath)

        result = pd.read_feather(filepath)
        assert len(result) == 4
        assert list(result["close"]) == [80.0, 90.0, 100.0, 110.0]

    def test_sorted_output(self, tmp_path):
        """Output is always sorted by date regardless of input order."""
        from scripts.collect_daily_snapshot import _merge_feather

        filepath = tmp_path / "TEST_USDC_USDC-1d-futures.feather"
        new_df = _make_ohlcv(["2026-03-12", "2026-03-10", "2026-03-11"], [120.0, 100.0, 110.0])

        _merge_feather(new_df, filepath)

        result = pd.read_feather(filepath)
        assert list(result["close"]) == [100.0, 110.0, 120.0]


class TestExtractSymbols:
    """Tests for the _extract_symbols helper."""

    def test_extracts_unique_listed_perp_symbols(self):
        from scripts.collect_daily_snapshot import _extract_symbols

        markets = [
            {"name": "ETH/USD [ETH-USDC]", "isListed": True},
            {"name": "ETH/USD [ETH-ETH]", "isListed": True},
            {"name": "BTC/USD [WBTC.b-USDC]", "isListed": True},
            {"name": "USDC-USDT", "isListed": True},  # swap-only, no "/"
            {"name": "DOGE/USD [ETH-USDC]", "isListed": False},  # unlisted
        ]
        symbols = _extract_symbols(markets)
        assert symbols == ["BTC", "ETH"]

    def test_empty_markets(self):
        from scripts.collect_daily_snapshot import _extract_symbols

        assert _extract_symbols([]) == []


class TestCollectApy:
    """Tests for the APY collection function."""

    def test_flatten_apy_data(self):
        """APY response is flattened with market_token and period columns."""
        from scripts.collect_daily_snapshot import _flatten_apy

        raw_apy = {
            "markets": {
                "0xabc": {"apy": 0.05, "baseApy": 0.04, "bonusApr": 0.01},
                "0xdef": {"apy": 0.10, "baseApy": 0.10, "bonusApr": 0.0},
            },
            "glvs": {
                "0x111": {"apy": 0.03, "baseApy": 0.03, "bonusApr": 0.0},
            },
        }
        df = _flatten_apy(raw_apy, "30d", "2026-03-12")
        assert len(df) == 3  # 2 markets + 1 glv
        assert "market_token" in df.columns
        assert "period" in df.columns
        assert "apy" in df.columns
        assert "type" in df.columns
        assert set(df["type"]) == {"market", "glv"}
        assert all(df["period"] == "30d")

    def test_flatten_apy_empty(self):
        """Empty APY response returns empty DataFrame."""
        from scripts.collect_daily_snapshot import _flatten_apy

        df = _flatten_apy({}, "30d", "2026-03-12")
        assert len(df) == 0


class TestCollectTickers:
    """Tests for the ticker collection function."""

    def test_flatten_ticker_data(self):
        """Ticker list is flattened to a DataFrame with correct columns."""
        from scripts.collect_daily_snapshot import _flatten_tickers

        raw_tickers = [
            {
                "tokenAddress": "0xabc",
                "tokenSymbol": "ETH",
                "minPrice": "330000000000",
                "maxPrice": "331000000000",
                "updatedAt": 1773308683207,
                "timestamp": 1773308682,
            },
            {
                "tokenAddress": "0xdef",
                "tokenSymbol": "BTC",
                "minPrice": "8300000000000",
                "maxPrice": "8310000000000",
                "updatedAt": 1773308683207,
                "timestamp": 1773308682,
            },
        ]
        df = _flatten_tickers(raw_tickers, "2026-03-12")
        assert len(df) == 2
        assert "token_symbol" in df.columns
        assert "min_price" in df.columns
        assert "max_price" in df.columns
        assert "date" in df.columns
        assert list(df["token_symbol"]) == ["ETH", "BTC"]
