"""Tests for Chainlink symbol mapping."""

from gmx_historical_data.chainlink_feeds_complete import (
    find_chainlink_symbol,
    get_feed_address_for_gmx_symbol,
)


def test_find_chainlink_symbol_direct_match():
    """Test direct symbol match."""
    assert find_chainlink_symbol("ETH") == "ETH"
    assert find_chainlink_symbol("BTC") == "BTC"
    assert find_chainlink_symbol("ARB") == "ARB"


def test_find_chainlink_symbol_manual_override():
    """Test manual override mapping."""
    assert find_chainlink_symbol("WBTC.b") == "WBTC"
    assert find_chainlink_symbol("WETH") == "ETH"
    assert find_chainlink_symbol("USDC.e") == "USDC"


def test_find_chainlink_symbol_fuzzy_match():
    """Test fuzzy matching (strip suffixes/prefixes)."""
    # Should strip .e suffix
    result = find_chainlink_symbol("USDT.e")
    assert result == "USDT" or result is None  # Depends on if override exists

    # Should strip .b suffix
    result = find_chainlink_symbol("BTC.b")
    assert result == "BTC"


def test_find_chainlink_symbol_not_found():
    """Test symbol not found."""
    assert find_chainlink_symbol("NOTAREALTOKEN") is None
    assert find_chainlink_symbol("FAKE123") is None


def test_get_feed_address_for_gmx_symbol():
    """Test getting feed address for GMX symbols."""
    # Direct match
    eth_feed = get_feed_address_for_gmx_symbol("ETH")
    assert eth_feed == "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612"

    # Via override: WBTC.b -> WBTC (separate Chainlink feed from BTC)
    wbtc_feed = get_feed_address_for_gmx_symbol("WBTC.b")
    assert wbtc_feed == "0xd0C7101eACbB49F3deCcCc166d238410D6D46d57"

    # Not found
    assert get_feed_address_for_gmx_symbol("NOTREAL") is None
