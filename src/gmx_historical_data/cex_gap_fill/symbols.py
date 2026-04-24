"""Static GMX <-> CEX symbol mappings.

Copied verbatim from ``gmx-strategies/scripts/merge_gmx_binance.py`` and
``gmx-strategies/plugins/pairlist/HistoricalVolumePairList.py``. Keep in sync
manually when those change.
"""

GMX_TO_BINANCE_NAME: dict[str, str] = {
    "BONK": "1000BONK",
    "FLOKI": "1000FLOKI",
    "PEPE": "1000PEPE",
    "SHIB": "1000SHIB",
    "SATS": "1000SATS",
}

PRICE_DIVISORS: dict[str, int] = {
    "BONK": 1000,
    "FLOKI": 1000,
    "PEPE": 1000,
    "SHIB": 1000,
    "SATS": 1000,
}


def normalize_k_prefix(ticker: str) -> str:
    """Normalize Hyperliquid-style k-prefix to uppercase K-prefix.

    :param ticker: Raw ticker, e.g. ``kPEPE``.
    :returns: Normalized ticker, e.g. ``KPEPE``.
    """
    if len(ticker) > 1 and ticker[0] == "k" and ticker[1].isupper():
        return "K" + ticker[1:]
    return ticker


CEX_QUOTE_SETTLE = "USDT"  # Both Binance & Bybit linear perps.


def gmx_symbol_to_cex_base(gmx_symbol: str) -> str:
    """Resolve GMX symbol to CEX base (no quote/settle).

    Pipeline: K-prefix normalization, then 1000x remap.
    If neither applies, the symbol is returned unchanged.

    :param gmx_symbol: e.g. ``BONK``, ``kPEPE``, ``BTC``.
    :returns: e.g. ``1000BONK``, ``1000PEPE``, ``BTC``.
    """
    normalized = normalize_k_prefix(gmx_symbol)
    # Strip leading K if present and look up bare name for 1000x mapping.
    if normalized.startswith("K") and len(normalized) > 1 and normalized[1].isupper():
        bare = normalized[1:]
    else:
        bare = normalized
    if bare in GMX_TO_BINANCE_NAME:
        return GMX_TO_BINANCE_NAME[bare]
    return normalized


def gmx_symbol_to_cex_pair(gmx_symbol: str) -> str:
    """Build CEX linear-perp pair string, e.g. ``BTC/USDT:USDT``."""
    base = gmx_symbol_to_cex_base(gmx_symbol)
    return f"{base}/{CEX_QUOTE_SETTLE}:{CEX_QUOTE_SETTLE}"


def price_scale_for(gmx_symbol: str) -> float:
    """Multiplier to apply to CEX OHLC columns so they line up with GMX prices.

    For 1000x-prefixed tokens: CEX price × (1 / 1000) = GMX price.

    :param gmx_symbol: GMX-side symbol e.g. ``BONK``.
    :returns: Scale factor; 1.0 for standard tokens.
    """
    normalized = normalize_k_prefix(gmx_symbol)
    if normalized.startswith("K") and len(normalized) > 1 and normalized[1].isupper():
        bare = normalized[1:]
    else:
        bare = normalized
    if bare in PRICE_DIVISORS:
        return 1.0 / PRICE_DIVISORS[bare]
    return 1.0
