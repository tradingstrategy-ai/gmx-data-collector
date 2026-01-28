"""Complete Chainlink price feed addresses for Arbitrum Mainnet.

Source: https://docs.chain.link/data-feeds/price-feeds/addresses?network=arbitrum
Last updated: 2026-01-24

These are PROXY addresses that emit AnswerUpdated events through their aggregator contracts.
Use aggregator_discovery.py to get the actual aggregator addresses for event collection.
"""

# Chainlink Price Feeds on Arbitrum Mainnet (Proxy Addresses)
# Format: "SYMBOL": "proxy_address"
# Source: https://data.chain.link/feeds/arbitrum/mainnet/ (verified 2026-01-28)
CHAINLINK_FEEDS_ARBITRUM: dict[str, str] = {
    # ========================================
    # Major Cryptocurrencies
    # ========================================
    "BTC": "0x6ce185860a4963106506C203335A2910413708e9",  # Bitcoin - Most liquid crypto asset
    "ETH": "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",  # Ethereum - Smart contract platform
    "WBTC": "0xd0C7101eACbB49F3deCcCc166d238410D6D46d57",  # Wrapped Bitcoin - ERC20 BTC (separate feed)
    "WETH": "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",  # Wrapped Ethereum - ERC20 ETH (same feed as ETH)
    # ========================================
    # Stablecoins
    # ========================================
    "USDC": "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3",  # USD Coin - Circle's USD stablecoin
    "USDT": "0x3f3f5dF88dC9F13eac63DF89EC16ef6e7E25DdE7",  # Tether - Largest stablecoin by market cap
    "DAI": "0xc5C8E77B397E531B8EC06BFb0048328B30E9eCfB",  # Dai - MakerDAO's decentralized stablecoin
    "FRAX": "0x0809E3d38d1B4214958faf06D8b1B1a2b73f2ab8",  # Frax - Fractional-algorithmic stablecoin
    # ========================================
    # Layer 1 Blockchain Tokens
    # ========================================
    "ARB": "0xb2A824043730FE05F3DA2efaFa1CBbe83fa548D6",  # Arbitrum - Layer 2 scaling solution for Ethereum
    "SOL": "0x24ceA4b8ce57cdA5058b924B9B9987992450590c",  # Solana - High-performance blockchain
    "AVAX": "0x8bf61728eeDCE2F32c456454d87B5d6eD6150208",  # Avalanche - Smart contracts platform
    "BNB": "0x6970460aabF80C5BE983C6b74e5D06dEDCA95D4A",  # BNB - Binance ecosystem token
    "POL": "0x82BA56a2fADF9C14f17D08bc51bDA0bDB83A8934",  # Polygon (formerly MATIC) - Ethereum scaling
    "OP": "0x205aaD468a11fd5D34fA7211bC6Bad5b3deB9b98",  # Optimism - Ethereum Layer 2 with optimistic rollups
    "ATOM": "0xCDA67618e51762235eacA373894F0C79256768fa",  # Cosmos - Inter-blockchain communication protocol
    "NEAR": "0xBF5C3fB2633e924598A46B9D07a174a9DBcF57C0",  # Near Protocol - Sharded proof-of-stake blockchain
    "SEI": "0xCc9742d77622eE9abBF1Df03530594f9097bDcB3",  # Sei - High-performance Layer 1 blockchain
    "TAO": "0x6aCcBB82aF71B8a576B4C05D4aF92A83A035B991",  # Bittensor - Decentralized machine learning network
    # ========================================
    # DeFi Protocol Tokens
    # ========================================
    "AAVE": "0xaD1d5344AaDE45F43E596773Bcc4c423EAbdD034",  # Aave - Decentralized lending protocol
    "CRV": "0xaebDA2c976cfd1eE1977Eac079B4382acb849325",  # Curve - DEX optimized for stablecoins
    "UNI": "0x9C917083fDb403ab5ADbEC26Ee294f6EcAda2720",  # Uniswap - Leading decentralized exchange
    "LINK": "0x86E53CF1B870786351Da77A57575e79CB55812CB",  # Chainlink - Decentralized oracle network
    "GMX": "0xDB98056FecFff59D032aB628337A4887110df3dB",  # GMX - Decentralized perpetual exchange
    "LDO": "0xA43A34030088E6510FecCFb77E88ee5e7ed0fE64",  # Lido DAO - Liquid staking governance token
    "COMP": "0xe7C53FFd03Eb6ceF7d208bC4C13446c76d1E5884",  # Compound - Algorithmic money market protocol
    "MKR": "0xdE9f0894670c4EFcacF370426F10C3AD2Cdf147e",  # Maker - MakerDAO governance token
    "SNX": "0x054296f0D036b95531B4E14aFB578B80CFb41252",  # Synthetix - Synthetic asset issuance protocol
    "SUSHI": "0xb2A8BA74cbca38508BA1632761b56C897060147C",  # SushiSwap - Community-driven DEX
    "YFI": "0x745Ab5b69E01E2BE1104Ca84937Bb71f96f5fB21",  # Yearn Finance - Yield optimization protocol
    "BAL": "0xBE5eA816870D11239c543F84b71439511D70B94f",  # Balancer - Automated portfolio manager and DEX
    "1INCH": "0x4bC735Ef24bf286983024CAd5D03f0738865Aaef",  # 1inch - DEX aggregator
    "PENDLE": "0x66853E19d73c0F9301fe099c324A1E9726953433",  # Pendle - Yield tokenization and trading
    "RDNT": "0x20d0Fcab0ECFD078B036b6CAf1FaC69A6453b352",  # Radiant Capital - Cross-chain lending protocol
    # ========================================
    # Liquid Staking Derivatives (NOTE: Only ETH-denominated feeds available, not USD)
    # ========================================
    "STETH": "0x07C5b924399cc23c24a95c8743DE4006a32b7f2a",  # Staked ETH - Lido liquid staking (stETH/USD)
    # NOTE: wstETH, rETH, cbETH only have ETH-denominated feeds on Arbitrum:
    # - wstETH/ETH: 0xb523AE262D20A936BC152e6023996e46FDC2A95D
    # - rETH/ETH: 0xD6aB2298946840262FcC278fF31516D39fF611eF
    # - cbETH/ETH: 0xa668682974E3f121185a3cD94f00322beC674275
    # To get USD prices, multiply by ETH/USD feed
    # ========================================
    # Meme Tokens
    # ========================================
    "DOGE": "0x9A7FB1b3950837a8D9b40517626E11D4127C098C",  # Dogecoin - Original meme cryptocurrency
    "SHIB": "0x0E278D14B4bf6429dDB0a1B353e2Ae8A4e128C93",  # Shiba Inu - Ethereum-based dog-themed token
    "PEPE": "0x02DEd5a7EDDA750E3Eb240b54437a54d57b74dBE",  # Pepe - Frog-themed meme token
    "WIF": "0xF7Ee427318d2Bd0EEd3c63382D0d52Ad8A68f90D",  # Dogwifhat - Solana meme token
    # NOTE: BONK does not have a Chainlink feed on Arbitrum
    # ========================================
    # Additional Cryptocurrencies
    # ========================================
    "APE": "0x221912ce795669f628c51c69b7d0873eDA9C03bB",  # ApeCoin - Governance token for APE ecosystem
    "LTC": "0x5698690a7B7B84F6aa985ef7690A8A7288FBc9c8",  # Litecoin - Peer-to-peer cryptocurrency
    "XRP": "0xB4AD57B52aB9141de9926a3e0C8dc6264c2ef205",  # Ripple - Payment settlement and remittance network
    # NOTE: FTM, FIL, BCH do not have Chainlink feeds on Arbitrum
}

# Manual symbol overrides for GMX to Chainlink mapping
# Use this when GMX uses different symbols than Chainlink
# NOTE: Keys must be uppercase to match the uppercased input in find_chainlink_symbol()
GMX_TO_CHAINLINK_SYMBOL_OVERRIDES: dict[str, str] = {
    "WBTC.B": "WBTC",
    "WBTC": "WBTC",
    "BTC.B": "BTC",
    "WETH": "ETH",
    "USDC.E": "USDC",
    "USDT.E": "USDT",
    "DAI.E": "DAI",
    "MATIC": "POL",  # Polygon rebranded to POL - use POL feed for MATIC
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
    """Get list of all supported token symbols with Chainlink feeds.

    :return: List of token symbols
    """
    return sorted(CHAINLINK_FEEDS_ARBITRUM.keys())


def find_chainlink_symbol(gmx_symbol: str) -> str | None:
    """Try to find matching Chainlink symbol for a GMX token symbol.

    Uses hybrid approach:
    1. Check manual overrides first
    2. Try direct match
    3. Try fuzzy match (strip .e, .b, W prefix)

    :param gmx_symbol: GMX token symbol
    :return: Chainlink symbol if found, None otherwise
    """
    gmx_symbol = gmx_symbol.upper()

    # 1. Check manual overrides
    if gmx_symbol in GMX_TO_CHAINLINK_SYMBOL_OVERRIDES:
        chainlink_symbol = GMX_TO_CHAINLINK_SYMBOL_OVERRIDES[gmx_symbol]
        if chainlink_symbol in CHAINLINK_FEEDS_ARBITRUM:
            return chainlink_symbol

    # 2. Try direct match
    if gmx_symbol in CHAINLINK_FEEDS_ARBITRUM:
        return gmx_symbol

    # 3. Try fuzzy match - strip common suffixes and prefixes
    normalized = gmx_symbol

    # Remove common suffixes (uppercase because gmx_symbol is already uppercased)
    for suffix in [".E", ".B"]:
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
            if normalized in CHAINLINK_FEEDS_ARBITRUM:
                return normalized

    # Remove W prefix for wrapped tokens
    if normalized.startswith("W") and len(normalized) > 1:
        unwrapped = normalized[1:]
        if unwrapped in CHAINLINK_FEEDS_ARBITRUM:
            return unwrapped

    return None


def get_feed_address_for_gmx_symbol(gmx_symbol: str) -> str | None:
    """Get Chainlink feed address for a GMX token symbol.

    :param gmx_symbol: GMX token symbol
    :return: Chainlink proxy address if found, None otherwise
    """
    chainlink_symbol = find_chainlink_symbol(gmx_symbol)
    if chainlink_symbol:
        return CHAINLINK_FEEDS_ARBITRUM[chainlink_symbol]
    return None
