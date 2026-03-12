"""Tests for RPC provider with fallback support."""

import pytest
from unittest.mock import Mock, patch
from web3 import Web3
from gmx_historical_data.rpc_provider import MultiRPCProvider, RPCProviderError


def test_create_multi_provider_single_url():
    """Test creating provider with single RPC URL."""
    rpc_urls = ["https://arb1.arbitrum.io/rpc"]
    provider = MultiRPCProvider(rpc_urls)

    assert provider.web3 is not None
    assert provider.current_provider_index == 0
    assert len(provider.rpc_urls) == 1


def test_create_multi_provider_multiple_urls():
    """Test creating provider with multiple RPC URLs."""
    rpc_urls = [
        "https://arb1.arbitrum.io/rpc",
        "https://arbitrum.llamarpc.com",
        "https://rpc.ankr.com/arbitrum",
    ]
    provider = MultiRPCProvider(rpc_urls)

    assert provider.web3 is not None
    assert len(provider.rpc_urls) == 3


def test_fallback_on_provider_failure():
    """Test automatic fallback when primary provider fails."""
    rpc_urls = [
        "https://failing-rpc.example.com",
        "https://working-rpc.example.com",
    ]

    with patch("gmx_historical_data.rpc_provider.HTTPProvider") as mock_provider_class:
        with patch("gmx_historical_data.rpc_provider.Web3") as mock_web3_class:
            # Create mock instances
            failing_provider = Mock()
            working_provider = Mock()

            # Mock Web3 instances
            failing_web3 = Mock()
            failing_eth = Mock()
            # Make block_number raise when accessed
            type(failing_eth).block_number = property(
                lambda self: (_ for _ in ()).throw(Exception("Connection failed"))
            )
            failing_web3.eth = failing_eth

            working_web3 = Mock()
            working_eth = Mock()
            working_eth.block_number = 12345
            working_web3.eth = working_eth

            # Setup provider class to return our mock providers
            mock_provider_class.side_effect = [failing_provider, working_provider]

            # Setup Web3 class to return our mock web3 instances
            mock_web3_class.side_effect = [failing_web3, working_web3]

            provider = MultiRPCProvider(rpc_urls, auto_fallback=True)

            # Should have fallen back to second provider
            assert provider.current_provider_index == 1


def test_retry_with_backoff():
    """Test retry logic with exponential backoff."""
    rpc_urls = ["https://arb1.arbitrum.io/rpc"]
    provider = MultiRPCProvider(rpc_urls)

    call_count = 0

    def failing_function():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise Exception("Temporary failure")
        return "success"

    result = provider.call_with_retry(failing_function, max_retries=3)

    assert result == "success"
    assert call_count == 3


def test_no_valid_providers_raises_error():
    """Test that error is raised when no providers work."""
    with pytest.raises(RPCProviderError):
        MultiRPCProvider([])
