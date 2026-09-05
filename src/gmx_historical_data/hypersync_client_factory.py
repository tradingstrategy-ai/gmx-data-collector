"""Shared HyperSync client construction with key-pool rotation.

The standalone ``scripts/extract_*.py`` helpers each used to duplicate::

    api_token = raw_token.replace(",", " ").split()[0] if raw_token else None

which silently discarded every key but the first from a configured
``HYPERSYNC_API_TOKEN`` pool, so those scripts got none of the ``429``
mitigation the pool was configured for -- unlike ``collect-update``
(``cli.py``), which already builds a
:class:`~gmx_historical_data.hypersync_key_rotator.HyperSyncKeyRotator` when
the token contains a comma or space. This gap was folded into the
2026-09-05 GMX/OP truncated-parquet incident writeup as its own finding: the
extract scripts back ``extract-all`` / ``oi`` / ``pool-liquidity``, which the
daily ``refresh-data`` chain runs, so they carry real ``429`` exposure while
getting none of the mitigation the pool was configured for.

This module gives every extract script the same rotation behaviour from one
shared call site instead of seven duplicated (and silently lossy) one-liners.
"""

import logging
from typing import Any

from hypersync import ClientConfig, HypersyncClient, Query, StreamConfig

from gmx_historical_data.hypersync_key_rotator import HyperSyncKeyRotator

logger = logging.getLogger(__name__)

# Substrings that identify a HyperSync error as rate-limit-shaped, mirroring
# the heuristic oracle_price_collector.retry_with_backoff() already uses for
# the collect-update path.
_RATE_LIMIT_MARKERS = ("rate limit", "too many requests", "429", "quota")


def build_hypersync_client_or_rotator(
    raw_token: str | None,
    endpoint: str,
) -> HyperSyncKeyRotator | HypersyncClient | None:
    """Build a HyperSync client or key-rotating pool from a token value.

    :param raw_token: Raw ``HYPERSYNC_API_TOKEN`` environment value --
        comma- or space-separated for a key pool, a single key, or
        ``None``/empty when unset.
    :param endpoint: HyperSync endpoint URL.
    :returns: A :class:`HyperSyncKeyRotator` when ``raw_token`` contains more
        than one key, a plain single-key :class:`HypersyncClient` when it
        contains exactly one, or ``None`` when unset/empty.
    """
    if not raw_token:
        return None

    keys = [k for k in raw_token.replace(",", " ").split() if k]
    if not keys:
        return None

    if len(keys) > 1:
        return HyperSyncKeyRotator(raw_token)

    return HypersyncClient(ClientConfig(url=endpoint, bearer_token=keys[0]))


class RotatingHypersyncClient:
    """Uniform client-pool wrapper with ``429`` rotation for extract scripts.

    Wraps :func:`build_hypersync_client_or_rotator` so a script only needs
    :attr:`client` for the currently active :class:`HypersyncClient` and
    :meth:`rotate_on_error` to rotate after a rate-limit response --
    mirroring the ``clients`` / ``client_index`` pairing
    :class:`~gmx_historical_data.oracle_price_collector.OraclePriceCollector`
    already uses for ``collect-update``, so the extract scripts rotate on
    ``429`` exactly as the collect path does.

    Proxies :meth:`get_height` and :meth:`stream` to the active client, so
    it is a drop-in replacement anywhere a bare :class:`HypersyncClient` was
    previously passed around.

    :param raw_token: Raw ``HYPERSYNC_API_TOKEN`` value, or ``None``.
    :param endpoint: HyperSync endpoint URL.
    """

    def __init__(self, raw_token: str | None, endpoint: str) -> None:
        """Initialize the client pool.

        :param raw_token: Raw ``HYPERSYNC_API_TOKEN`` value, or ``None``.
        :param endpoint: HyperSync endpoint URL.
        """
        pool = build_hypersync_client_or_rotator(raw_token, endpoint)

        self.key_rotator: HyperSyncKeyRotator | None = None
        if isinstance(pool, HyperSyncKeyRotator):
            self.key_rotator = pool
            self._clients = pool.get_clients(endpoint)
            logger.info(
                "HyperSync key rotation enabled with %d API key(s) for %s",
                pool.total_keys,
                endpoint,
            )
        elif isinstance(pool, HypersyncClient):
            self._clients = [pool]
        else:
            self._clients = [HypersyncClient(ClientConfig(url=endpoint, bearer_token=None))]
            logger.warning(
                "No HYPERSYNC_API_TOKEN set for %s -- may hit 403/rate-limit errors", endpoint
            )

        self._client_index = 0

    @property
    def client(self) -> HypersyncClient:
        """Return the currently active HyperSync client.

        :returns: Active :class:`HypersyncClient` for the current key index.
        """
        return self._clients[self._client_index]

    @property
    def total_keys(self) -> int:
        """Number of keys in the pool.

        :returns: Number of pooled keys (1 for a single-key or unauthenticated client).
        """
        return len(self._clients)

    async def get_height(self) -> int:
        """Proxy to the active client's ``get_height()``.

        :returns: Latest chain height per the active client.
        """
        return await self.client.get_height()

    async def stream(self, query: Query, config: StreamConfig) -> Any:
        """Proxy to the active client's ``stream()``.

        :param query: HyperSync ``Query``.
        :param config: HyperSync ``StreamConfig``.
        :returns: The stream object returned by the active client.
        """
        return await self.client.stream(query, config)

    def rotate_on_error(self, exc: BaseException) -> bool:
        """Rotate to the next key if ``exc`` looks like a rate-limit response.

        :param exc: Exception raised by the failed HyperSync call.
        :returns: ``True`` if rotation occurred and the caller should retry
            immediately with the new :attr:`client`, without counting it
            against a normal retry budget; ``False`` if there is no pool to
            rotate, the error isn't rate-limit-shaped, or every key has
            already failed.
        """
        if self.key_rotator is None:
            return False

        message = str(exc).lower()
        if not any(marker in message for marker in _RATE_LIMIT_MARKERS):
            return False

        try:
            self.key_rotator.rotate()
        except RuntimeError:
            logger.error("All HyperSync API keys exhausted; cannot rotate further")
            return False

        self._client_index = self.key_rotator.current_index
        logger.warning(
            "Rotated HyperSync API key after rate-limit response (now key %d/%d)",
            self._client_index + 1,
            self.total_keys,
        )
        return True
