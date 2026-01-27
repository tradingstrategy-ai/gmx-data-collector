"""Map GMX token addresses to symbols for non-Chainlink markets.

Uses eth_defi.gmx.core.markets to get market information and build
a mapping from index token addresses to symbols, filtered to exclude
markets that have public Chainlink price feeds.
"""

from web3 import Web3
from web3.exceptions import Web3Exception
from eth_defi.gmx.config import GMXConfig
from eth_defi.gmx.core.markets import Markets

from gmx_historical_data.daemon.config import get_gmx_markets_with_chainlink_feeds


class GMXTokenMapper:
    """Map token addresses to symbols for non-Chainlink GMX markets.

    :param web3: Web3 instance connected to Arbitrum
    """

    def __init__(self, web3: Web3):
        """Initialize token mapper.

        :param web3: Web3 instance
        """
        self.web3 = web3
        self._all_tokens_cache: dict[str, str] | None = None
        self._non_chainlink_cache: dict[str, str] | None = None
        self._token_decimals_cache: dict[str, int] | None = None

    def get_all_token_mapping(self) -> dict[str, str]:
        """Get mapping of all token addresses to symbols.

        :return: Dict mapping token address (lowercase) to symbol
        :raises RuntimeError: If unable to fetch markets from GMX contracts
        """
        if self._all_tokens_cache is not None:
            return self._all_tokens_cache

        try:
            # Get GMX config and markets
            config = GMXConfig(self.web3)
            markets = Markets(config)
            available_markets = markets.get_available_markets()

            # Build mapping from index token address to symbol
            mapping = {}
            for market_addr, market_data in available_markets.items():
                # Get index token address (the underlying asset)
                index_token = market_data.get("index_token_address", "")
                if not index_token:
                    continue

                # Normalize address to lowercase
                addr_lower = index_token.lower()

                # Extract base token symbol from market_metadata (not market_symbol)
                # market_symbol may have suffixes like "ETH2", "ARB2" for different markets
                # but market_metadata.symbol has the base symbol that GMX API accepts
                metadata = market_data.get("market_metadata", {})
                symbol = metadata.get("symbol", "") or market_data.get("market_symbol", "")
                if symbol:
                    mapping[addr_lower] = symbol

            self._all_tokens_cache = mapping
            return mapping

        except (Web3Exception, AssertionError) as e:
            raise RuntimeError(
                f"Failed to fetch GMX markets. "
                f"Check your Web3 connection and network support: {e}"
            ) from e
        except Exception as e:
            raise RuntimeError(f"Unexpected error fetching GMX markets: {e}") from e

    def get_non_chainlink_tokens(self) -> dict[str, str]:
        """Get mapping of token addresses to symbols for non-Chainlink markets.

        Filters out the 34 markets that have public Chainlink price feeds,
        returning only the 84 markets that require oracle event collection.

        :return: Dict mapping token address (lowercase) to symbol
        """
        if self._non_chainlink_cache is not None:
            return self._non_chainlink_cache

        # Get Chainlink-supported symbols
        chainlink_symbols = set(get_gmx_markets_with_chainlink_feeds())

        # Get all token mappings
        all_tokens = self.get_all_token_mapping()

        # Filter to non-Chainlink markets
        non_chainlink = {
            addr: symbol
            for addr, symbol in all_tokens.items()
            if symbol not in chainlink_symbols
        }

        self._non_chainlink_cache = non_chainlink
        return non_chainlink

    def get_chainlink_tokens(self) -> dict[str, str]:
        """Get mapping of token addresses to symbols for Chainlink markets.

        Returns only the 34 markets that have public Chainlink price feeds.

        :return: Dict mapping token address (lowercase) to symbol
        """
        # Get Chainlink-supported symbols
        chainlink_symbols = set(get_gmx_markets_with_chainlink_feeds())

        # Get all token mappings
        all_tokens = self.get_all_token_mapping()

        # Filter to Chainlink markets only
        chainlink = {
            addr: symbol
            for addr, symbol in all_tokens.items()
            if symbol in chainlink_symbols
        }

        return chainlink

    def get_symbol_for_token(self, token_address: str) -> str | None:
        """Get symbol for a specific token address.

        :param token_address: Token contract address
        :return: Token symbol or None if not found
        """
        mapping = self.get_all_token_mapping()
        return mapping.get(token_address.lower())

    def get_token_addresses_for_symbols(
        self, symbols: list[str]
    ) -> dict[str, str]:
        """Get token addresses for a list of symbols.

        :param symbols: List of token symbols
        :return: Dict mapping symbol to token address (lowercase)
        """
        all_tokens = self.get_all_token_mapping()

        # Invert the mapping (symbol -> address)
        symbol_to_addr = {symbol: addr for addr, symbol in all_tokens.items()}

        return {
            symbol: symbol_to_addr[symbol]
            for symbol in symbols
            if symbol in symbol_to_addr
        }

    def get_token_decimals(self) -> dict[str, int]:
        """Get mapping of token symbols to decimals.

        GMX uses 30-decimal precision internally. To convert raw prices to
        human-readable USD values, use: price = raw / 10^(30 - token_decimals)

        :return: Dict mapping token symbol to decimals
        """
        if self._token_decimals_cache is not None:
            return self._token_decimals_cache

        try:
            config = GMXConfig(self.web3)
            markets = Markets(config)
            available_markets = markets.get_available_markets()

            decimals_map = {}
            for market_data in available_markets.values():
                # Use base symbol from market_metadata (not market_symbol which may have suffix)
                metadata = market_data.get("market_metadata", {})
                symbol = metadata.get("symbol", "") or market_data.get("market_symbol", "")
                decimals = metadata.get("decimals")

                if symbol and decimals is not None:
                    decimals_map[symbol] = decimals

            self._token_decimals_cache = decimals_map
            return decimals_map

        except (Web3Exception, AssertionError) as e:
            raise RuntimeError(
                f"Failed to fetch GMX market decimals: {e}"
            ) from e

    def get_decimals_for_symbol(self, symbol: str) -> int | None:
        """Get decimals for a specific token symbol.

        :param symbol: Token symbol (e.g., 'ETH', 'SUI')
        :return: Token decimals or None if not found
        """
        decimals_map = self.get_token_decimals()
        return decimals_map.get(symbol)
