"""Tests for GMX token discovery."""

from gmx_historical_data.gmx_token_discovery import GMXTokenDiscovery


def test_fetch_all_tokens():
    """Test fetching all GMX tokens."""
    discovery = GMXTokenDiscovery(chain="arbitrum")
    tokens = discovery.fetch_all_tokens()

    # Should get ~97 tokens
    assert len(tokens) > 90
    assert len(tokens) < 200

    # Check token structure
    assert all(isinstance(t, dict) for t in tokens)
    assert all("symbol" in t for t in tokens)
    assert all("address" in t for t in tokens)

    # ETH should be in the list
    symbols = [t["symbol"] for t in tokens]
    assert "ETH" in symbols


def test_get_supported_symbols():
    """Test getting list of symbol strings."""
    discovery = GMXTokenDiscovery(chain="arbitrum")
    symbols = discovery.get_supported_symbols()

    assert len(symbols) > 90
    assert "ETH" in symbols
    assert "BTC" in symbols or "WBTC" in symbols or "BTC.b" in symbols
    assert all(isinstance(s, str) for s in symbols)
