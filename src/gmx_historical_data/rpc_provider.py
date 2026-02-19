"""Multi-provider RPC wrapper with automatic failover and retry logic."""

import logging
import time
from collections.abc import Callable
from typing import Any

from web3 import Web3
from web3.exceptions import Web3Exception
from web3.providers import HTTPProvider

logger = logging.getLogger(__name__)


class RPCProviderError(Exception):
    """Raised when all RPC providers fail."""

    pass


class MultiRPCProvider:
    """Web3 provider with automatic failover to backup RPC endpoints.

    Features:
    - Automatic failover when primary RPC fails
    - Exponential backoff retry logic
    - Provider health tracking
    - Configurable retry parameters

    :param rpc_urls: List of RPC endpoint URLs (primary first, then fallbacks)
    :param max_retries: Maximum retry attempts per provider
    :param initial_backoff: Initial backoff time in seconds
    :param max_backoff: Maximum backoff time in seconds
    :param auto_fallback: If True, automatically try next provider on failure
    """

    def __init__(
        self,
        rpc_urls: list[str],
        max_retries: int = 3,
        initial_backoff: float = 2.0,
        max_backoff: float = 60.0,
        auto_fallback: bool = True,
    ):
        """Initialize multi-provider RPC wrapper.

        :param rpc_urls: List of RPC URLs (at least 1 required)
        :param max_retries: Max retries per operation
        :param initial_backoff: Initial retry backoff (seconds)
        :param max_backoff: Maximum retry backoff (seconds)
        :param auto_fallback: Enable automatic provider failover
        :raises RPCProviderError: If no valid RPC URLs provided
        """
        if not rpc_urls:
            raise RPCProviderError("At least one RPC URL is required")

        self.rpc_urls = rpc_urls
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.auto_fallback = auto_fallback

        self.current_provider_index = 0
        self.provider_failure_counts: dict[int, int] = {}

        # Initialize Web3 with first working provider
        self.web3 = self._create_web3_instance()

        if self.web3 is None:
            raise RPCProviderError(f"Failed to connect to any RPC provider: {rpc_urls}")

        logger.info(
            f"Initialized MultiRPCProvider with {len(rpc_urls)} provider(s). "
            f"Active: {self.get_current_rpc_url()}"
        )

    def _create_web3_instance(self) -> Web3 | None:
        """Create Web3 instance with first working provider.

        :return: Web3 instance or None if all providers fail
        """
        for idx, url in enumerate(self.rpc_urls):
            try:
                provider = HTTPProvider(url, request_kwargs={"timeout": 30})
                web3 = Web3(provider)

                # Test connection
                web3.eth.block_number

                self.current_provider_index = idx
                logger.info(f"Connected to RPC provider {idx + 1}/{len(self.rpc_urls)}: {url}")
                return web3

            except Exception as e:
                logger.warning(
                    f"Failed to connect to RPC {idx + 1}/{len(self.rpc_urls)} ({url}): {e}"
                )
                self.provider_failure_counts[idx] = self.provider_failure_counts.get(idx, 0) + 1
                continue

        return None

    def get_current_rpc_url(self) -> str:
        """Get currently active RPC URL.

        :return: Current RPC endpoint URL
        """
        return self.rpc_urls[self.current_provider_index]

    def switch_to_next_provider(self) -> bool:
        """Switch to next available RPC provider.

        :return: True if switched successfully, False if no more providers
        """
        if not self.auto_fallback:
            return False

        start_idx = self.current_provider_index

        # Try each provider once
        for offset in range(1, len(self.rpc_urls)):
            next_idx = (start_idx + offset) % len(self.rpc_urls)

            try:
                url = self.rpc_urls[next_idx]
                provider = HTTPProvider(url, request_kwargs={"timeout": 30})
                web3 = Web3(provider)

                # Test connection
                web3.eth.block_number

                self.web3 = web3
                self.current_provider_index = next_idx

                logger.warning(
                    f"Switched to fallback RPC provider {next_idx + 1}/{len(self.rpc_urls)}: {url}"
                )
                return True

            except Exception as e:
                logger.warning(f"Fallback provider {next_idx + 1} also failed: {e}")
                self.provider_failure_counts[next_idx] = (
                    self.provider_failure_counts.get(next_idx, 0) + 1
                )
                continue

        logger.error("All RPC providers exhausted")
        return False

    def call_with_retry(
        self,
        func: Callable[[], Any],
        max_retries: int | None = None,
        error_msg: str = "RPC call failed",
    ) -> Any:
        """Execute function with retry logic and automatic provider failover.

        :param func: Function to execute (should be idempotent)
        :param max_retries: Override default max_retries
        :param error_msg: Custom error message for logging
        :return: Function result
        :raises RPCProviderError: If all retries and providers fail
        """
        max_retries = max_retries or self.max_retries
        last_error = None

        for attempt in range(max_retries):
            try:
                return func()

            except (Web3Exception, Exception) as e:
                last_error = e

                # Log the failure
                logger.warning(
                    f"{error_msg} (attempt {attempt + 1}/{max_retries}, "
                    f"provider: {self.get_current_rpc_url()}): {e}"
                )

                # Try switching provider before retrying
                if self.auto_fallback and attempt < max_retries - 1:
                    if self.switch_to_next_provider():
                        # Successfully switched, retry immediately
                        continue

                # Calculate backoff with exponential increase
                if attempt < max_retries - 1:
                    backoff = min(self.initial_backoff * (2**attempt), self.max_backoff)
                    logger.info(f"Retrying in {backoff:.1f}s...")
                    time.sleep(backoff)

        # All retries failed
        raise RPCProviderError(
            f"{error_msg} after {max_retries} attempts across all providers. "
            f"Last error: {last_error}"
        )

    def get_provider_stats(self) -> dict[str, Any]:
        """Get statistics about provider usage and failures.

        :return: Dictionary with provider statistics
        """
        return {
            "total_providers": len(self.rpc_urls),
            "current_provider": self.get_current_rpc_url(),
            "current_index": self.current_provider_index,
            "failure_counts": self.provider_failure_counts,
        }
