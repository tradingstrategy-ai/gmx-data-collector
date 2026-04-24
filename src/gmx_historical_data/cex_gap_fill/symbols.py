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
