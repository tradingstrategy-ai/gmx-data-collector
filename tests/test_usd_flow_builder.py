"""Tests for USD funding flow builder functions."""

import polars as pl
import pytest


class TestBuildSingleTokenSet:
    """Tests for single-token market detection."""

    def test_detects_single_token(self):
        """Single-token market (BTC_WBTC.b-WBTC.b) is detected."""
        from scripts.build_usd_funding_flows import build_single_token_set

        markets = {
            "0xaaa": {
                "symbol": "BTC/USD [WBTC.b-WBTC.b]",
                "longTokenSymbol": "WBTC.b",
                "shortTokenSymbol": "WBTC.b",
                "indexToken": "BTC",
            },
            "0xbbb": {
                "symbol": "ETH/USD",
                "longTokenSymbol": "WETH",
                "shortTokenSymbol": "USDC",
                "indexToken": "ETH",
            },
        }
        result = build_single_token_set(markets)
        assert "0xaaa" in result
        assert "0xbbb" not in result

    def test_empty_markets(self):
        """Empty registry returns empty set."""
        from scripts.build_usd_funding_flows import build_single_token_set

        assert build_single_token_set({}) == set()

    def test_missing_token_symbols(self):
        """Markets missing token symbol fields are not flagged as single-token."""
        from scripts.build_usd_funding_flows import build_single_token_set

        markets = {
            "0xaaa": {"symbol": "BTC/USD"},
        }
        result = build_single_token_set(markets)
        assert len(result) == 0


class TestComputeAnnualRates:
    """Tests for annualisation with single-token multipliers."""

    def test_annual_rate_basic(self):
        """Basic annualisation: (delta_usd / seconds) * 86400 * 365."""
        from scripts.build_usd_funding_flows import compute_annual_rates

        df = pl.DataFrame(
            {
                "market": ["0xaaa", "0xaaa"],
                "collateral_token": ["0xtoken", "0xtoken"],
                "is_long": [True, True],
                "delta_usd": [0.0, 100.0],
                "block_timestamp": [1691000000, 1691001000],
                "block_number": [1, 2],
            }
        )

        result = compute_annual_rates(df, set(), multiplier=2)
        # 100 / 1000 seconds * 86400 * 365 = 3_153_600
        assert len(result) == 1  # first row filtered (no prev)
        assert abs(result["annual_rate_usd"][0] - 3_153_600.0) < 1.0

    def test_single_token_multiplier(self):
        """Single-token market gets 2x multiplier applied."""
        from scripts.build_usd_funding_flows import compute_annual_rates

        df = pl.DataFrame(
            {
                "market": ["0xaaa", "0xaaa"],
                "collateral_token": ["0xtoken", "0xtoken"],
                "is_long": [True, True],
                "delta_usd": [0.0, 100.0],
                "block_timestamp": [1691000000, 1691001000],
                "block_number": [1, 2],
            }
        )

        result = compute_annual_rates(df, {"0xaaa"}, multiplier=2)
        # 3_153_600 * 2 = 6_307_200
        assert abs(result["annual_rate_usd"][0] - 6_307_200.0) < 1.0

    def test_zero_seconds_diff_filtered(self):
        """Events at the same timestamp are filtered out (zero seconds_diff)."""
        from scripts.build_usd_funding_flows import compute_annual_rates

        df = pl.DataFrame(
            {
                "market": ["0xaaa", "0xaaa", "0xaaa"],
                "collateral_token": ["0xtoken", "0xtoken", "0xtoken"],
                "is_long": [True, True, True],
                "delta_usd": [0.0, 50.0, 100.0],
                "block_timestamp": [1691000000, 1691000000, 1691001000],
                "block_number": [1, 2, 3],
            }
        )

        result = compute_annual_rates(df, set(), multiplier=2)
        # Row 1: no prev → filtered
        # Row 2: seconds_diff = 0 → filtered
        # Row 3: seconds_diff = 1000 → kept
        assert len(result) == 1

    def test_empty_dataframe(self):
        """Empty input returns empty output."""
        from scripts.build_usd_funding_flows import compute_annual_rates

        df = pl.DataFrame(
            {
                "market": pl.Series([], dtype=pl.Utf8),
                "collateral_token": pl.Series([], dtype=pl.Utf8),
                "is_long": pl.Series([], dtype=pl.Boolean),
                "delta_usd": pl.Series([], dtype=pl.Float64),
                "block_timestamp": pl.Series([], dtype=pl.Int64),
                "block_number": pl.Series([], dtype=pl.Int64),
            }
        )

        result = compute_annual_rates(df, set(), multiplier=2)
        assert result.is_empty()


class TestBuildUsdFlows:
    """Tests for the join and aggregation logic."""

    def test_direction_from_join(self):
        """Paying side is_long=True + receiving is_long_cf=False = longs pay."""
        from scripts.build_usd_funding_flows import build_usd_flows

        ff = pl.DataFrame(
            {
                "transaction_hash": ["0xtx1"],
                "market": ["0xaaa"],
                "collateral_token": ["0xtoken"],
                "symbol": ["ETH"],
                "is_long": [True],
                "annual_rate_usd": [1000.0],
                "block_timestamp": [1000],
                "block_number": [1],
            }
        )

        cf = pl.DataFrame(
            {
                "transaction_hash": ["0xtx1"],
                "market": ["0xaaa"],
                "collateral_token": ["0xtoken"],
                "is_long": [False],
                "annual_rate_usd": [900.0],
            }
        )

        result = build_usd_flows(ff, cf)
        assert len(result) == 1
        assert result["is_long"][0] == True
        assert result["is_long_cf"][0] == False
        # longs pay: long_funding_rate = funding_rate_ff
        assert result["long_funding_rate"][0] == result["funding_rate_ff"][0]

    def test_empty_join(self):
        """No matching transactions produces empty result."""
        from scripts.build_usd_funding_flows import build_usd_flows

        ff = pl.DataFrame(
            {
                "transaction_hash": ["0xtx1"],
                "market": ["0xaaa"],
                "collateral_token": ["0xtoken"],
                "symbol": ["ETH"],
                "is_long": [True],
                "annual_rate_usd": [1000.0],
                "block_timestamp": [1000],
                "block_number": [1],
            }
        )

        cf = pl.DataFrame(
            {
                "transaction_hash": ["0xtx_different"],
                "market": ["0xaaa"],
                "collateral_token": ["0xtoken"],
                "is_long": [False],
                "annual_rate_usd": [900.0],
            }
        )

        result = build_usd_flows(ff, cf)
        assert result.is_empty()


class TestJoinOraclePrices:
    """Tests for oracle price join logic."""

    def test_basic_join(self):
        """Joining events with oracle prices produces delta_usd."""
        from scripts.build_usd_funding_flows import join_oracle_prices

        events = pl.DataFrame(
            {
                "transaction_hash": ["0xtx1"],
                "collateral_token": ["0xtoken"],
                "delta": ["1000000"],
            }
        )

        oracle = pl.DataFrame(
            {
                "transaction_hash": ["0xtx1"],
                "token": ["0xtoken"],
                "max_price": ["2000000"],
                "log_index": [1],
            }
        )

        result = join_oracle_prices(events, oracle)
        assert "delta_usd" in result.columns
        # 1000000 * 2000000 = 2e12
        assert result["delta_usd"][0] == 1000000.0 * 2000000.0

    def test_empty_events(self):
        """Empty events returns empty with delta_usd column."""
        from scripts.build_usd_funding_flows import join_oracle_prices

        events = pl.DataFrame(
            {
                "transaction_hash": pl.Series([], dtype=pl.Utf8),
                "collateral_token": pl.Series([], dtype=pl.Utf8),
                "delta": pl.Series([], dtype=pl.Utf8),
            }
        )

        oracle = pl.DataFrame(
            {
                "transaction_hash": ["0xtx1"],
                "token": ["0xtoken"],
                "max_price": ["2000000"],
                "log_index": [1],
            }
        )

        result = join_oracle_prices(events, oracle)
        assert "delta_usd" in result.columns
        assert result.is_empty()
