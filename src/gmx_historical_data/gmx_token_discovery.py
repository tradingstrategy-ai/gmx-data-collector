"""Discover all tokens supported by GMX using the official API."""

from dataclasses import dataclass
from typing import Any

import requests


@dataclass
class GMXToken:
    """GMX token metadata.

    :param symbol: Token symbol (e.g., 'ETH', 'BTC')
    :param address: Token contract address
    :param decimals: Token decimals
    :param is_stable: Whether this is a stablecoin
    """

    symbol: str
    address: str
    decimals: int
    is_stable: bool = False


class GMXTokenDiscovery:
    """Discover tokens supported by GMX.

    :param chain: Blockchain network (e.g., 'arbitrum', 'avalanche')
    """

    # GMX API endpoints by chain
    API_ENDPOINTS = {
        "arbitrum": "https://arbitrum-api.gmxinfra2.io",
        "avalanche": "https://avalanche-api.gmxinfra.io",
    }

    def __init__(self, chain: str = "arbitrum") -> None:
        """Initialize GMX token discovery.

        :param chain: Blockchain network
        """
        if chain not in self.API_ENDPOINTS:
            raise ValueError(
                f"Unsupported chain: {chain}. Must be one of {list(self.API_ENDPOINTS.keys())}"
            )

        self.chain = chain
        self.base_url = self.API_ENDPOINTS[chain]

    def fetch_all_tokens(self) -> list[dict[str, Any]]:
        """Fetch all tokens supported by GMX.

        :return: List of token dictionaries with symbol, address, decimals
        """
        url = f"{self.base_url}/tokens"

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

            # API returns {"tokens": [...]}
            if isinstance(data, dict) and "tokens" in data:
                tokens = data["tokens"]
            elif isinstance(data, list):
                tokens = data
            else:
                raise ValueError(f"Expected dict with 'tokens' key or list, got {type(data)}")

            if not isinstance(tokens, list):
                raise ValueError(f"Expected list of tokens, got {type(tokens)}")

            return tokens

        except requests.RequestException as e:
            raise RuntimeError(f"Failed to fetch GMX tokens from {url}: {e}") from e

    def get_supported_symbols(self) -> list[str]:
        """Get list of all GMX-supported token symbols.

        :return: List of token symbols (e.g., ['ETH', 'BTC', ...])
        """
        tokens = self.fetch_all_tokens()
        return [token["symbol"] for token in tokens]
