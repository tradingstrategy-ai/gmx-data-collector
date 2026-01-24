"""Chainlink price feed addresses for GMX tokens on Arbitrum.

These are PROXY addresses. The actual aggregator addresses that emit events
must be discovered via proxy.aggregator() calls.

Token list updated from GMX API: https://arbitrum-api.gmxinfra2.io/tokens
Only includes tokens with active Chainlink price feeds on Arbitrum.
"""

# GMX Token Symbol -> Chainlink Proxy Address mapping (Arbitrum Mainnet)
# Source: https://docs.chain.link/data-feeds/price-feeds/addresses?network=arbitrum
CHAINLINK_FEEDS_ARBITRUM: dict[str, str] = {
    # Major assets
    "ETH": "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",
    "WBTC.b": "0x6ce185860a4963106506C203335A2910413708e9",  # BTC feed
    "BTC": "0x6ce185860a4963106506C203335A2910413708e9",
    # Stablecoins
    "USDC": "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3",
    "USDC.e": "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3",  # Same as USDC
    "USDT": "0x3f3f5dF88dC9F13eac63DF89EC16ef6e7E25DdE7",
    "DAI": "0xc5C8E77B397E531B8EC06BFb0048328B30E9eCfB",
    # DeFi tokens (GMX-supported)
    "ARB": "0xb2A824043730FE05F3DA2efaFa1CBbe83fa548D6",
    "LINK": "0x86E53CF1B870786351Da77A57575e79CB55812CB",
    "UNI": "0x9C917083fDb403ab5ADbEC26Ee294f6EcAda2720",
    "AAVE": "0xaD1d5344AaDE45F43E596773Bcc4c423EAbdD034",
    "GMX": "0xDB98056FecFff59D032aB628337A4887110df3dB",
    # Layer 1s (GMX-supported)
    "SOL": "0x24ceA4b8ce57cdA5058b924B9B9987992450590c",
    "AVAX": "0x8bf61728eeDCE2F32c456454d87B5d6eD6150208",
    "BNB": "0x6970460aabF80C5BE983C6b74e5D06dEDCA95D4A",
    "OP": "0x205aaD468a11fd5D34fA7211bC6Bad5b3deB9b98",
    # Meme tokens (GMX-supported)
    "PEPE": "0x02DEd5a7EDDA750E3Eb240b54B5cBDb4Eaf4e2f1",
    "WIF": "0x4b71024A3C47661F6f8a93C59d28C60CED5666De",
}


def get_feed_address(symbol: str) -> str:
    """Get Chainlink proxy address for a token symbol.

    :param symbol: Token symbol (e.g., 'ETH', 'BTC')
    :return: Chainlink proxy contract address
    :raises KeyError: If symbol not found in mapping
    """
    symbol = symbol.upper()
    if symbol not in CHAINLINK_FEEDS_ARBITRUM:
        raise KeyError(
            f"No Chainlink feed found for {symbol}. "
            f"Available symbols: {', '.join(sorted(CHAINLINK_FEEDS_ARBITRUM.keys()))}"
        )
    return CHAINLINK_FEEDS_ARBITRUM[symbol]


def get_all_symbols() -> list[str]:
    """Get list of all supported token symbols.

    :return: List of token symbols with Chainlink feeds
    """
    return sorted(CHAINLINK_FEEDS_ARBITRUM.keys())
