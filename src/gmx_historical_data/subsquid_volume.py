"""Fetch 24h trading volume per GMX market from Subsquid GraphQL.

This is a standalone lightweight client — no dependency on web3-ethereum-defi.
The GMX REST API (gmxinfra.io) does not expose volume data; volume is only
available through the Subsquid indexer that processes on-chain trade events.

Volume values use GMX's 30-decimal fixed-point encoding and are converted
to :class:`~decimal.Decimal` USD values before returning.

Usage::

    from gmx_historical_data.subsquid_volume import fetch_daily_volumes

    volumes = fetch_daily_volumes()
    for market_addr, vol_usd in volumes.items():
        print(f"{market_addr}: ${vol_usd:,.0f}")
"""

import logging
import time
from decimal import Decimal

import requests
from eth_utils import to_checksum_address

logger = logging.getLogger(__name__)

# Subsquid GraphQL endpoints per chain.
SUBSQUID_URLS: dict[str, str] = {
    "arbitrum": "https://gmx.squids.live/gmx-synthetics-arbitrum:prod/api/graphql",
    "avalanche": "https://gmx.squids.live/gmx-synthetics-avalanche:prod/api/graphql",
}

# GMX uses 30-decimal fixed-point for USD values.
_GMX_USD_PRECISION = Decimal(10**30)

# Retry configuration for transient HTTP errors.
_MAX_RETRIES = 3
_RETRY_BACKOFF = 2  # seconds, doubled each retry


def _query_subsquid(query: str, chain: str = "arbitrum", timeout: int = 30) -> dict:
    """Execute a GraphQL query against the Subsquid endpoint.

    :param query: GraphQL query string.
    :param chain: Chain name (``"arbitrum"`` or ``"avalanche"``).
    :param timeout: HTTP request timeout in seconds.
    :returns: The ``"data"`` portion of the GraphQL response.
    :raises KeyError: If the chain is not supported.
    :raises requests.HTTPError: If the endpoint is unreachable after retries.
    """
    url = SUBSQUID_URLS[chain]
    last_err = None
    for attempt in range(_MAX_RETRIES):
        try:
            resp = requests.post(
                url,
                json={"query": query},
                timeout=timeout,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            result = resp.json()
            if "errors" in result:
                raise RuntimeError(f"Subsquid GraphQL errors: {result['errors']}")
            return result.get("data", {})
        except (requests.RequestException, RuntimeError) as e:
            last_err = e
            if attempt < _MAX_RETRIES - 1:
                wait = _RETRY_BACKOFF * (2**attempt)
                logger.warning(
                    "Subsquid query failed (attempt %d/%d): %s — retrying in %ds",
                    attempt + 1,
                    _MAX_RETRIES,
                    e,
                    wait,
                )
                time.sleep(wait)
    raise last_err  # type: ignore[misc]


def fetch_daily_volumes(
    chain: str = "arbitrum",
    timeout: int = 30,
) -> dict[str, Decimal]:
    """Fetch 24h trading volume per market from Subsquid.

    Mirrors the GMX TypeScript SDK's ``getDailyVolumes()`` method.
    Queries the ``positionsVolume`` entity with a 1-day period filter.

    :param chain: Chain name (``"arbitrum"`` or ``"avalanche"``).
    :param timeout: HTTP request timeout in seconds.
    :returns: Dict mapping checksummed market addresses to daily volume in USD.
    :raises requests.HTTPError: If the Subsquid endpoint is unreachable.
    :raises KeyError: If the chain is not supported.
    """
    query = '{ positionsVolume(where: {period: "1d"}) { market volume } }'
    data = _query_subsquid(query, chain=chain, timeout=timeout)

    volumes: dict[str, Decimal] = {}
    for entry in data.get("positionsVolume", []):
        market = to_checksum_address(entry["market"])
        raw = Decimal(entry["volume"])
        volumes[market] = raw / _GMX_USD_PRECISION

    logger.info("Fetched 24h volume for %d markets (chain=%s)", len(volumes), chain)
    return volumes


def fetch_volume_history(
    days: int = 30,
    chain: str = "arbitrum",
    timeout: int = 30,
) -> list[dict]:
    """Fetch historical daily aggregate volume from Subsquid.

    Queries ``volumeInfos`` with ``period: "1d"`` for protocol-wide daily
    totals, providing historical volume context beyond the current 24h window.

    :param days: Number of past daily snapshots to fetch.
    :param chain: Chain name.
    :param timeout: HTTP request timeout in seconds.
    :returns:
        List of dicts sorted by timestamp descending, each with keys:
        ``timestamp`` (int), ``volume_usd`` (Decimal), ``margin_volume_usd``
        (Decimal), ``swap_volume_usd`` (Decimal).
    """
    query = f"""{{
      volumeInfos(
        where: {{period_eq: "1d"}}
        orderBy: timestamp_DESC
        limit: {days}
      ) {{
        volumeUsd
        marginVolumeUsd
        swapVolumeUsd
        timestamp
      }}
    }}"""
    data = _query_subsquid(query, chain=chain, timeout=timeout)

    history = []
    for entry in data.get("volumeInfos", []):
        history.append(
            {
                "timestamp": int(entry["timestamp"]),
                "volume_usd": Decimal(entry["volumeUsd"]) / _GMX_USD_PRECISION,
                "margin_volume_usd": Decimal(entry["marginVolumeUsd"]) / _GMX_USD_PRECISION,
                "swap_volume_usd": Decimal(entry["swapVolumeUsd"]) / _GMX_USD_PRECISION,
            }
        )

    logger.info("Fetched %d daily volume snapshots (chain=%s)", len(history), chain)
    return history
