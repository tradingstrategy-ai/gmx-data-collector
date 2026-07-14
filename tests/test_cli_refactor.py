"""Smoke tests for CLI refactor helpers."""

from gmx_historical_data.cli import _filter_and_categorize_symbols


class TestFilterAndCategorizeSymbols:
    """Tests for the symbol filtering/categorization helper."""

    def test_excludes_deprecated_symbols(self):
        """Excluded symbols are filtered out."""
        chainlink, non_chainlink, excluded = _filter_and_categorize_symbols(
            ["ETH", "APE_DEPRECATED", "BTC"]
        )
        assert excluded == 1
        assert "APE_DEPRECATED" not in chainlink
        assert "APE_DEPRECATED" not in non_chainlink

    def test_separates_chainlink_and_non_chainlink(self):
        """Symbols are categorized correctly."""
        chainlink, non_chainlink, _ = _filter_and_categorize_symbols(
            ["ETH", "BTC", "SUI"]  # ETH, BTC have Chainlink; SUI doesn't
        )
        assert "ETH" in chainlink
        assert "BTC" in chainlink
        assert "SUI" in non_chainlink

    def test_chainlink_only_skips_non_chainlink(self):
        """With chainlink_only=True, non-Chainlink symbols are dropped."""
        chainlink, non_chainlink, _ = _filter_and_categorize_symbols(
            ["ETH", "SUI"], chainlink_only=True
        )
        assert "ETH" in chainlink
        assert non_chainlink == []

    def test_empty_input(self):
        """Empty input returns empty results."""
        chainlink, non_chainlink, excluded = _filter_and_categorize_symbols([])
        assert chainlink == []
        assert non_chainlink == []
        assert excluded == 0

    def test_skips_disabled_symbols(self):
        """Disabled markets are excluded from both buckets."""
        chainlink, non_chainlink, excluded = _filter_and_categorize_symbols(
            ["OM", "BONK"], disabled_symbols={"OM"}
        )
        assert chainlink == []
        assert non_chainlink == ["BONK"]
        assert excluded == 1
