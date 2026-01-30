"""HyperSync API key rotation for rate limit handling.

This module provides functionality to rotate between multiple HyperSync API keys
to handle rate limiting when collecting oracle data.
"""

import logging

logger = logging.getLogger(__name__)


class HyperSyncKeyRotator:
    """Rotates between multiple HyperSync API keys to handle rate limits.

    :param api_keys: Space-separated string of API keys
    :raises ValueError: If no API keys are provided
    """

    def __init__(self, api_keys: str):
        """Initialize the key rotator.

        :param api_keys: Space-separated string of API keys
        :raises ValueError: If no API keys are provided
        """
        # Parse space-separated keys
        self.keys = [k.strip() for k in api_keys.split() if k.strip()]

        if not self.keys:
            raise ValueError("At least one API key must be provided")

        self.current_index = 0
        self.failed_keys = set()

        logger.info(f"Initialized HyperSyncKeyRotator with {len(self.keys)} API key(s)")

    @property
    def current_key(self) -> str:
        """Get the current API key.

        :return: The current API key
        """
        return self.keys[self.current_index]

    @property
    def total_keys(self) -> int:
        """Get the total number of API keys.

        :return: The total number of API keys
        """
        return len(self.keys)

    def rotate(self) -> str:
        """Rotate to the next non-failed key.

        :return: The next available API key
        :raises RuntimeError: If all keys have failed
        """
        # Check if all keys have failed
        if len(self.failed_keys) >= len(self.keys):
            logger.error("All HyperSync API keys have failed")
            raise RuntimeError("All HyperSync API keys have failed")

        # Find next non-failed key
        attempts = 0
        max_attempts = len(self.keys)

        while attempts < max_attempts:
            # Move to next index
            self.current_index = (self.current_index + 1) % len(self.keys)

            # Check if this key has failed
            if self.keys[self.current_index] not in self.failed_keys:
                logger.debug(f"Rotated to API key at index {self.current_index}")
                return self.keys[self.current_index]

            attempts += 1

        # This should not be reached if the check at the beginning works correctly
        logger.error("All HyperSync API keys have failed")
        raise RuntimeError("All HyperSync API keys have failed")

    def mark_failed(self, api_key: str) -> None:
        """Mark an API key as failed.

        :param api_key: The API key to mark as failed
        """
        if api_key in self.keys:
            self.failed_keys.add(api_key)
            logger.warning(f"Marked API key as failed: {api_key[:8]}...")
        else:
            logger.debug(f"Attempted to mark non-existent key as failed: {api_key[:8]}...")

    def reset_failures(self) -> None:
        """Reset all failed key tracking.

        This allows all keys to be used again.
        """
        self.failed_keys.clear()
        logger.info("Reset all failed API keys")
