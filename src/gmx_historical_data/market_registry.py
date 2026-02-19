"""Dynamic GMX market registry using GMXAPI REST API.

Replaces hardcoded MARKETS dicts in extraction scripts with a live fetch
from ``arbitrum-api.gmxinfra.io``. No RPC connection required — the
GMXAPI uses HTTP REST only.

Disk caching keeps latency low on repeated invocations:
- Cache lives at ``~/.cache/gmx_historical_data/markets_{chain}.json``
- Cache is valid for 24 hours
- Falls back to stale cache if the API is unreachable
- ``force_refresh=True`` bypasses the cache

Naming convention (backward-compatible with existing data directories):

For perpetual markets:
  - Group markets by index token address
  - The market with the earliest ``listingDate`` in each group is the **primary**
  - Primary market → ``"BASE/USD"`` (no bracket suffix)
  - Other markets in the group → use the API ``name`` field as-is
    (e.g. ``"BTC/USD [WBTC.b-WBTC.b]"``)

For swap-only markets (indexToken == zero address):
  - Use the API ``name`` field as-is (e.g. ``"SWAP-ONLY [USDC-USDT]"``)
  - ``indexToken`` field in the result is ``None``

The :func:`market_symbol` function converts a full market symbol string
to the filesystem-safe directory name:
  - ``"ETH/USD"`` → ``"ETH"``
  - ``"BTC/USD [WBTC.b-WBTC.b]"`` → ``"BTC_WBTC.b-WBTC.b"``
  - ``"SWAP-ONLY [USDC-USDT]"`` → ``"SWAP-ONLY_USDC-USDT"``
  - Unknown address → ``address[:10]`` (rare fallback)
"""

import json
import logging
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Zero address — marks swap-only markets in the GMX protocol
_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# Default disk-cache location
_DEFAULT_CACHE_DIR = Path.home() / ".cache" / "gmx_historical_data"

# Cache TTL in seconds (24 hours)
_CACHE_MAX_AGE = 86_400


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_base_symbol(api_name: str) -> str:
    """Extract the base token symbol from an API market name.

    :param api_name: Market name from the GMX API (e.g. ``"BTC/USD [WBTC.b-USDC]"``).
    :returns: Base part only (e.g. ``"BTC/USD"``).

    Examples::

        "BTC/USD [WBTC.b-USDC]"   → "BTC/USD"
        "ETH/USD"                  → "ETH/USD"
        "SWAP-ONLY [USDC-USDT]"   → "SWAP-ONLY"
    """
    bracket = api_name.find("[")
    if bracket != -1:
        return api_name[:bracket].rstrip()
    return api_name


def _fetch_from_api(chain: str) -> list[dict]:
    """Fetch the raw markets list from the GMX REST API.

    :param chain: Chain name (``"arbitrum"`` or ``"avalanche"``).
    :returns: List of raw market dicts from the ``/markets`` endpoint.
    :raises RuntimeError: If the API is unreachable and no cache fallback exists.
    """
    from eth_defi.gmx.api import GMXAPI

    api = GMXAPI(chain=chain)
    data = api.get_markets(use_cache=False)
    markets = data.get("markets", [])
    if not isinstance(markets, list):
        raise RuntimeError(
            f"Unexpected /markets response format for chain={chain}: {type(markets)}"
        )
    return markets


def _build_registry(raw_markets: list[dict]) -> dict[str, dict]:
    """Build the market registry dict from raw API market objects.

    Applies primary-vs-alt naming convention so primary perpetual markets
    get short names (``"ETH/USD"``) while alt-collateral variants keep
    the full API name with brackets (``"ETH/USD [ETH-ETH]"``).

    :param raw_markets: List of market dicts from ``/markets`` endpoint.
    :returns: Dict keyed by **lowercase** market token address.

    Each value has::

        {
            "symbol": str,            # e.g. "BTC/USD" or "BTC/USD [WBTC.b-WBTC.b]"
            "indexToken": str | None, # Base token symbol or None for swap-only
            "longTokenSymbol": str | None,
            "shortTokenSymbol": str | None,
            "listingDate": str,       # ISO 8601 date string
        }
    """
    # Group perpetual markets by index token address (lowercase)
    by_index: dict[str, list[dict]] = defaultdict(list)
    for m in raw_markets:
        idx = m.get("indexToken", _ZERO_ADDRESS).lower()
        by_index[idx].append(m)

    registry: dict[str, dict] = {}

    for idx_addr, group in by_index.items():
        is_swap_only = idx_addr == _ZERO_ADDRESS

        # Sort by listing date ascending (earliest first = primary)
        group_sorted = sorted(group, key=lambda m: m.get("listingDate", ""))

        for rank, m in enumerate(group_sorted):
            addr_lower = m["marketToken"].lower()
            api_name: str = m.get("name", "")

            if is_swap_only:
                # Swap-only: use API name as-is, indexToken = None
                symbol = api_name
                index_token_sym = None
            else:
                # Extract long/short token symbols from bracket in API name
                # e.g. "BTC/USD [WBTC.b-USDC]" → long="WBTC.b", short="USDC"
                bracket_start = api_name.find("[")
                long_sym: Optional[str] = None
                short_sym: Optional[str] = None
                if bracket_start != -1:
                    bracket_end = api_name.rfind("]")
                    content = api_name[bracket_start + 1 : bracket_end].strip()
                    parts = content.split("-", 1)
                    if len(parts) == 2:
                        long_sym, short_sym = parts[0].strip(), parts[1].strip()

                # Derive the index token symbol from the market name
                # "BTC/USD [...]" → "BTC"
                base_and_quote = _extract_base_symbol(api_name)  # "BTC/USD"
                parts_slash = base_and_quote.split("/")
                index_token_sym = parts_slash[0].strip() if parts_slash else api_name

                if rank == 0:
                    # Primary market: use short name without brackets
                    symbol = base_and_quote  # e.g. "BTC/USD"
                else:
                    # Alt-collateral market: keep full API name
                    symbol = api_name  # e.g. "BTC/USD [WBTC.b-WBTC.b]"

            registry[addr_lower] = {
                "symbol": symbol,
                "indexToken": index_token_sym,
                "longTokenSymbol": long_sym if not is_swap_only else None,
                "shortTokenSymbol": short_sym if not is_swap_only else None,
                "listingDate": m.get("listingDate", ""),
                "isListed": m.get("isListed", True),
            }

    return registry


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_markets(
    chain: str = "arbitrum",
    cache_dir: Optional[Path] = None,
    force_refresh: bool = False,
) -> dict[str, dict]:
    """Fetch and return the GMX market registry for *chain*.

    On the first call the data is fetched from the GMX REST API and written
    to a local cache file.  Subsequent calls within 24 hours read from cache.
    If the API is unreachable the stale cache is returned as a fallback.

    :param chain: Chain name — ``"arbitrum"`` (default) or ``"avalanche"``.
    :param cache_dir: Directory for the disk cache.  Defaults to
        ``~/.cache/gmx_historical_data/``.
    :param force_refresh: If ``True``, ignore any cached data and re-fetch.
    :returns: Dict keyed by lowercase market token address.  Each value
        contains ``symbol``, ``indexToken``, ``longTokenSymbol``,
        ``shortTokenSymbol``, and ``listingDate``.
    :raises RuntimeError: If the API is unreachable *and* no cache exists.
    """
    cache_path = (cache_dir or _DEFAULT_CACHE_DIR) / f"markets_{chain}.json"

    # Try to load from cache
    if not force_refresh and cache_path.exists():
        try:
            with open(cache_path) as f:
                cached = json.load(f)
            age = time.time() - cached.get("_fetched_at", 0)
            if age < _CACHE_MAX_AGE:
                logger.debug(
                    "Using cached GMX markets for %s (age %.0fs)", chain, age
                )
                return cached["markets"]
            else:
                logger.debug("Cache stale (%.0fs old), refreshing…", age)
        except (json.JSONDecodeError, OSError, KeyError):
            logger.debug("Cache read failed, fetching fresh data…")

    # Fetch from API
    try:
        raw = _fetch_from_api(chain)
        registry = _build_registry(raw)
        # Persist to cache
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_path, "w") as f:
            json.dump({"_fetched_at": time.time(), "markets": registry}, f)
        logger.info("Fetched %d GMX markets for %s", len(registry), chain)
        return registry

    except Exception as exc:
        logger.warning("GMX API fetch failed: %s", exc)
        # Fall back to stale cache if available
        if cache_path.exists():
            try:
                with open(cache_path) as f:
                    cached = json.load(f)
                logger.warning(
                    "Using stale cache (%.0f hours old) as fallback.",
                    (time.time() - cached.get("_fetched_at", 0)) / 3600,
                )
                return cached["markets"]
            except (json.JSONDecodeError, OSError, KeyError):
                pass
        raise RuntimeError(
            f"Failed to fetch GMX markets for {chain} and no cache available: {exc}"
        ) from exc


def market_symbol(address: str, markets: dict[str, dict]) -> str:
    """Return the filesystem-safe directory name for a market address.

    Converts a full market symbol to a short name suitable for use as a
    directory or file-name component:

    - ``"ETH/USD"`` → ``"ETH"``
    - ``"BTC/USD [WBTC.b-WBTC.b]"`` → ``"BTC_WBTC.b-WBTC.b"``
    - ``"SWAP-ONLY [USDC-USDT]"`` → ``"SWAP-ONLY_USDC-USDT"``
    - Unknown address → ``address[:10]`` (truncated fallback)

    :param address: Market contract address (any case).
    :param markets: Registry dict from :func:`fetch_markets`.
    :returns: Short symbol string safe for filesystem use.
    """
    info = markets.get(address.lower())
    if not info:
        return address[:10]

    sym: str = info["symbol"]
    # Split off "/USD" or "/USD [...]"
    base = sym.split("/")[0]
    bracket = sym.find("[")
    if bracket != -1:
        suffix = sym[bracket + 1 : sym.rfind("]")].strip()
        return f"{base}_{suffix}"
    # Sanitise: remove chars that break filesystems / polars glob
    return base.replace("[", "").replace("]", "").replace(" ", "_")


def get_index_token(address: str, markets: dict[str, dict]) -> Optional[str]:
    """Return the index token symbol for a market address, or ``None``.

    Returns ``None`` for swap-only markets (where ``indexToken`` is the zero
    address) and for unknown addresses.

    :param address: Market contract address (any case).
    :param markets: Registry dict from :func:`fetch_markets`.
    :returns: Index token symbol (e.g. ``"ETH"``), or ``None``.
    """
    info = markets.get(address.lower())
    if not info:
        return None
    return info.get("indexToken")
