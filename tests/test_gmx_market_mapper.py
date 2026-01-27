"""Tests for GMX market address to symbol mapping."""

import pytest
from web3 import Web3
from gmx_historical_data.gmx_market_mapper import GMXMarketMapper


@pytest.fixture
def web3():
    """Web3 instance for Arbitrum."""
    import os

    rpc_url = os.environ.get("JSON_RPC_ARBITRUM")
    if not rpc_url:
        pytest.skip("JSON_RPC_ARBITRUM not set")
    return Web3(Web3.HTTPProvider(rpc_url))


def test_get_market_symbol_mapping(web3):
    """Test fetching market address to symbol mapping."""
    mapper = GMXMarketMapper(web3)

    mapping = mapper.get_market_symbol_mapping()

    # Should have mappings for all GMX markets
    # GMX V2 typically has 100+ markets on Arbitrum as of Jan 2025
    assert len(mapping) > 50  # At least 50 markets

    # All keys should be lowercase addresses
    for market_addr, symbol in mapping.items():
        assert market_addr.startswith("0x")
        assert market_addr == market_addr.lower()
        assert isinstance(symbol, str)
        assert len(symbol) > 0


def test_get_symbol_for_market(web3):
    """Test getting symbol for a specific market."""
    mapper = GMXMarketMapper(web3)

    # Use a known market (will fail until we have real data)
    # This is a placeholder - will update with real address
    market_addr = "0x70d95587d40a2caf56bd97485ab3eec10bee6336"  # ETH market

    symbol = mapper.get_symbol_for_market(market_addr)

    assert symbol == "ETH"


def test_get_symbol_for_unknown_market(web3):
    """Test getting symbol for unknown market returns None."""
    mapper = GMXMarketMapper(web3)

    fake_addr = "0x0000000000000000000000000000000000000000"
    symbol = mapper.get_symbol_for_market(fake_addr)

    assert symbol is None
