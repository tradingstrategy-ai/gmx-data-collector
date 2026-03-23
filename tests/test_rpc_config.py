"""Tests for RPC configuration parsing and fallback URL handling."""

import pytest

from gmx_historical_data.config import CollectionConfig
from gmx_historical_data.daemon.config import DaemonConfig


class TestCollectionConfigRPCUrls:
    """Test CollectionConfig RPC URL parsing and handling."""

    def test_single_rpc_url(self):
        """Test configuration with single RPC URL."""
        config = CollectionConfig(rpc_url="https://arb1.arbitrum.io/rpc")
        urls = config.get_all_rpc_urls()

        assert len(urls) == 1
        assert urls[0] == "https://arb1.arbitrum.io/rpc"

    def test_comma_separated_rpc_urls(self):
        """Test parsing comma-separated RPC URLs."""
        config = CollectionConfig(
            rpc_url="https://arb1.arbitrum.io/rpc,https://arbitrum.llamarpc.com"
        )
        urls = config.get_all_rpc_urls()

        assert len(urls) == 2
        assert urls[0] == "https://arb1.arbitrum.io/rpc"
        assert urls[1] == "https://arbitrum.llamarpc.com"

    def test_comma_separated_with_spaces(self):
        """Test parsing comma-separated URLs with whitespace."""
        config = CollectionConfig(
            rpc_url="https://arb1.arbitrum.io/rpc , https://arbitrum.llamarpc.com , https://rpc.ankr.com/arbitrum"
        )
        urls = config.get_all_rpc_urls()

        assert len(urls) == 3
        assert urls[0] == "https://arb1.arbitrum.io/rpc"
        assert urls[1] == "https://arbitrum.llamarpc.com"
        assert urls[2] == "https://rpc.ankr.com/arbitrum"

    def test_explicit_fallback_urls(self):
        """Test configuration with explicit fallback URLs."""
        config = CollectionConfig(
            rpc_url="https://arb1.arbitrum.io/rpc",
            fallback_rpc_urls=["https://arbitrum.llamarpc.com", "https://rpc.ankr.com/arbitrum"],
        )
        urls = config.get_all_rpc_urls()

        assert len(urls) == 3
        assert urls[0] == "https://arb1.arbitrum.io/rpc"
        assert urls[1] == "https://arbitrum.llamarpc.com"
        assert urls[2] == "https://rpc.ankr.com/arbitrum"

    def test_combined_comma_separated_and_fallbacks(self):
        """Test combining comma-separated primary with explicit fallbacks."""
        config = CollectionConfig(
            rpc_url="https://arb1.arbitrum.io/rpc,https://arbitrum.llamarpc.com",
            fallback_rpc_urls=[
                "https://rpc.ankr.com/arbitrum",
                "https://arbitrum.blockpi.network/v1/rpc/public",
            ],
        )
        urls = config.get_all_rpc_urls()

        assert len(urls) == 4
        assert urls[0] == "https://arb1.arbitrum.io/rpc"
        assert urls[1] == "https://arbitrum.llamarpc.com"
        assert urls[2] == "https://rpc.ankr.com/arbitrum"
        assert urls[3] == "https://arbitrum.blockpi.network/v1/rpc/public"

    def test_deduplication(self):
        """Test that duplicate URLs are removed while preserving order."""
        config = CollectionConfig(
            rpc_url="https://arb1.arbitrum.io/rpc,https://arbitrum.llamarpc.com,https://arb1.arbitrum.io/rpc",
            fallback_rpc_urls=["https://arbitrum.llamarpc.com", "https://rpc.ankr.com/arbitrum"],
        )
        urls = config.get_all_rpc_urls()

        assert len(urls) == 3
        assert urls[0] == "https://arb1.arbitrum.io/rpc"
        assert urls[1] == "https://arbitrum.llamarpc.com"
        assert urls[2] == "https://rpc.ankr.com/arbitrum"

    def test_empty_rpc_url(self):
        """Test handling of empty RPC URL."""
        config = CollectionConfig(rpc_url="")
        urls = config.get_all_rpc_urls()

        assert len(urls) == 0

    def test_empty_with_fallbacks(self):
        """Test handling of empty primary with fallbacks."""
        config = CollectionConfig(
            rpc_url="",
            fallback_rpc_urls=["https://arbitrum.llamarpc.com", "https://rpc.ankr.com/arbitrum"],
        )
        urls = config.get_all_rpc_urls()

        assert len(urls) == 2
        assert urls[0] == "https://arbitrum.llamarpc.com"
        assert urls[1] == "https://rpc.ankr.com/arbitrum"

    def test_empty_comma_separated_ignored(self):
        """Test that empty strings in comma-separated list are ignored."""
        config = CollectionConfig(
            rpc_url="https://arb1.arbitrum.io/rpc,,https://arbitrum.llamarpc.com,,"
        )
        urls = config.get_all_rpc_urls()

        assert len(urls) == 2
        assert urls[0] == "https://arb1.arbitrum.io/rpc"
        assert urls[1] == "https://arbitrum.llamarpc.com"

    def test_fallback_rpc_urls_default_initialization(self):
        """Test that fallback_rpc_urls is initialized to empty list if None."""
        config = CollectionConfig(rpc_url="https://arb1.arbitrum.io/rpc")

        assert config.fallback_rpc_urls is not None
        assert isinstance(config.fallback_rpc_urls, list)
        assert len(config.fallback_rpc_urls) == 0


class TestDaemonConfigRPCUrls:
    """Test DaemonConfig RPC URL parsing and handling."""

    def test_fallback_rpc_urls_default_initialization(self):
        """Test that fallback_rpc_urls is initialized to empty list if None."""
        config = DaemonConfig(rpc_url="https://arb1.arbitrum.io/rpc")

        assert config.fallback_rpc_urls is not None
        assert isinstance(config.fallback_rpc_urls, list)
        assert len(config.fallback_rpc_urls) == 0

    def test_fallback_rpc_urls_explicit(self):
        """Test explicit fallback URLs in DaemonConfig."""
        config = DaemonConfig(
            rpc_url="https://arb1.arbitrum.io/rpc",
            fallback_rpc_urls=["https://arbitrum.llamarpc.com"],
        )

        assert len(config.fallback_rpc_urls) == 1
        assert config.fallback_rpc_urls[0] == "https://arbitrum.llamarpc.com"

    def test_from_env_single_rpc(self, monkeypatch):
        """Test from_env with single RPC URL."""
        monkeypatch.setenv("JSON_RPC_ARBITRUM", "https://arb1.arbitrum.io/rpc")
        config = DaemonConfig.from_env()

        assert config.rpc_url == "https://arb1.arbitrum.io/rpc"
        assert len(config.fallback_rpc_urls) == 0

    def test_from_env_with_fallbacks(self, monkeypatch):
        """Test from_env with fallback RPC URLs."""
        monkeypatch.setenv("JSON_RPC_ARBITRUM", "https://arb1.arbitrum.io/rpc")
        monkeypatch.setenv(
            "FALLBACK_RPC_URLS", "https://arbitrum.llamarpc.com,https://rpc.ankr.com/arbitrum"
        )
        config = DaemonConfig.from_env()

        assert config.rpc_url == "https://arb1.arbitrum.io/rpc"
        assert len(config.fallback_rpc_urls) == 2
        assert config.fallback_rpc_urls[0] == "https://arbitrum.llamarpc.com"
        assert config.fallback_rpc_urls[1] == "https://rpc.ankr.com/arbitrum"

    def test_from_env_comma_separated_primary(self, monkeypatch):
        """Test from_env with comma-separated primary RPC URLs."""
        monkeypatch.setenv(
            "JSON_RPC_ARBITRUM", "https://arb1.arbitrum.io/rpc,https://arbitrum.llamarpc.com"
        )
        config = DaemonConfig.from_env()

        assert config.rpc_url == "https://arb1.arbitrum.io/rpc,https://arbitrum.llamarpc.com"
        assert len(config.fallback_rpc_urls) == 0

    def test_from_env_empty_fallbacks_env_var(self, monkeypatch):
        """Test from_env with empty FALLBACK_RPC_URLS env var."""
        monkeypatch.setenv("JSON_RPC_ARBITRUM", "https://arb1.arbitrum.io/rpc")
        monkeypatch.setenv("FALLBACK_RPC_URLS", "")
        config = DaemonConfig.from_env()

        assert config.rpc_url == "https://arb1.arbitrum.io/rpc"
        assert len(config.fallback_rpc_urls) == 0

    def test_from_env_fallbacks_with_spaces(self, monkeypatch):
        """Test from_env parsing fallbacks with whitespace."""
        monkeypatch.setenv("JSON_RPC_ARBITRUM", "https://arb1.arbitrum.io/rpc")
        monkeypatch.setenv(
            "FALLBACK_RPC_URLS", " https://arbitrum.llamarpc.com , https://rpc.ankr.com/arbitrum "
        )
        config = DaemonConfig.from_env()

        assert len(config.fallback_rpc_urls) == 2
        assert config.fallback_rpc_urls[0] == "https://arbitrum.llamarpc.com"
        assert config.fallback_rpc_urls[1] == "https://rpc.ankr.com/arbitrum"

    def test_from_env_missing_rpc_url_raises(self, monkeypatch):
        """Test that from_env raises ValueError when JSON_RPC_ARBITRUM is missing."""
        monkeypatch.delenv("JSON_RPC_ARBITRUM", raising=False)

        with pytest.raises(ValueError, match="JSON_RPC_ARBITRUM environment variable is required"):
            DaemonConfig.from_env()
