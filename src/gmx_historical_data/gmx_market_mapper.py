"""Map GMX market addresses to token symbols.

Uses eth_defi.gmx.core.markets to get market information and build
a mapping from market contract addresses to index token symbols.
"""

from web3 import Web3
from web3.exceptions import Web3Exception
from eth_defi.gmx.config import GMXConfig
from eth_defi.gmx.core.markets import Markets


class GMXMarketMapper:
    """Maps GMX market addresses to token symbols.

    :param web3: Web3 instance connected to Arbitrum
    """

    def __init__(self, web3: Web3):
        """Initialize market mapper.

        :param web3: Web3 instance
        """
        self.web3 = web3
        self._mapping_cache: dict[str, str] | None = None

    def get_market_symbol_mapping(self) -> dict[str, str]:
        """Get mapping of market addresses to token symbols.

        :return: Dict mapping market address (lowercase) to symbol
        :raises RuntimeError: If unable to fetch markets from GMX contracts
        """
        if self._mapping_cache is not None:
            return self._mapping_cache

        try:
            # Get GMX config and markets
            config = GMXConfig(self.web3)
            markets = Markets(config)
            available_markets = markets.get_available_markets()

            # Build mapping from market address to symbol
            # available_markets is a dict: {market_address: market_data}
            mapping = {}
            for market_addr, market_data in available_markets.items():
                # Normalize address to lowercase
                addr_lower = market_addr.lower()
                # Extract base symbol from market_metadata (not market_symbol which may have suffix)
                # market_symbol may have suffixes like "ETH2", "ARB2" for different markets
                metadata = market_data.get("market_metadata", {})
                symbol = metadata.get("symbol", "") or market_data.get(
                    "market_symbol", ""
                )
                mapping[addr_lower] = symbol

            self._mapping_cache = mapping
            return mapping
        except (Web3Exception, AssertionError) as e:
            # AssertionError can be raised by GMXConfig for unsupported networks
            raise RuntimeError(
                f"Failed to fetch GMX markets. Check your Web3 connection and network support: {e}"
            ) from e
        except Exception as e:
            raise RuntimeError(f"Unexpected error fetching GMX markets: {e}") from e

    def get_symbol_for_market(self, market_address: str) -> str | None:
        """Get symbol for a specific market address.

        :param market_address: Market contract address
        :return: Token symbol or None if not found
        """
        mapping = self.get_market_symbol_mapping()
        return mapping.get(market_address.lower())
