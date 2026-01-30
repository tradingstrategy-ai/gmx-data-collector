"""Tests for enhanced error handling in oracle price collector.

Tests progressive rate limiting and HyperSync API key rotation.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from gmx_historical_data.oracle_price_collector import retry_with_backoff
from gmx_historical_data.hypersync_key_rotator import HyperSyncKeyRotator


@pytest.mark.asyncio
async def test_retry_with_progressive_delays():
    """Test retry_with_backoff uses progressive delays: 2s, 5s, 10s, 30s, 60s."""
    call_count = 0
    sleep_durations = []

    async def failing_operation():
        nonlocal call_count
        call_count += 1
        if call_count <= 5:
            raise RuntimeError(f"Attempt {call_count} failed")
        return "success"

    # Mock asyncio.sleep to capture delay durations
    original_sleep = asyncio.sleep

    async def mock_sleep(duration):
        sleep_durations.append(duration)
        await original_sleep(0)  # Don't actually sleep in tests

    with patch("asyncio.sleep", side_effect=mock_sleep):
        result = await retry_with_backoff(
            failing_operation,
            max_retries=5,
            operation_name="test_operation",
        )

    assert result == "success"
    assert call_count == 6  # Initial attempt + 5 retries

    # Check progressive delays (allowing for jitter)
    # Expected delays: 2.0, 5.0, 10.0, 30.0, 60.0
    expected_delays = [2.0, 5.0, 10.0, 30.0, 60.0]
    assert len(sleep_durations) == 5

    for i, (actual, expected) in enumerate(zip(sleep_durations, expected_delays)):
        # Allow 10% jitter tolerance
        assert (
            expected <= actual <= expected * 1.1
        ), f"Delay {i+1}: expected ~{expected}s, got {actual}s"


@pytest.mark.asyncio
async def test_key_rotation_on_rate_limit():
    """Test that rate limit errors trigger key rotation without counting as retry."""
    call_count = 0
    rotated_keys = []

    # Create a mock key rotator
    mock_rotator = MagicMock(spec=HyperSyncKeyRotator)
    mock_rotator.current_key = "key1"

    def mock_rotate():
        if len(rotated_keys) == 0:
            mock_rotator.current_key = "key2"
            rotated_keys.append("key2")
        elif len(rotated_keys) == 1:
            mock_rotator.current_key = "key3"
            rotated_keys.append("key3")
        return mock_rotator.current_key

    mock_rotator.rotate = mock_rotate
    mock_rotator.mark_failed = MagicMock()

    async def rate_limited_operation():
        nonlocal call_count
        call_count += 1

        # First two attempts: rate limit errors
        if call_count == 1:
            raise RuntimeError("Error: rate limit exceeded")
        elif call_count == 2:
            raise RuntimeError("Error: too many requests")
        # Third attempt: success
        return "success"

    with patch("asyncio.sleep", new_callable=AsyncMock):
        result = await retry_with_backoff(
            rate_limited_operation,
            max_retries=5,
            operation_name="test_rate_limit",
            key_rotator=mock_rotator,
        )

    assert result == "success"
    assert call_count == 3  # Initial + 2 rate limit retries (not counted as retries)
    assert len(rotated_keys) == 2  # Should have rotated twice
    assert mock_rotator.current_key == "key3"


@pytest.mark.asyncio
async def test_all_keys_exhausted_raises_error():
    """Test that when all keys fail, RuntimeError is raised."""
    call_count = 0

    # Create a mock key rotator that fails after 3 rotations
    mock_rotator = MagicMock(spec=HyperSyncKeyRotator)
    mock_rotator.current_key = "key1"

    def mock_rotate():
        nonlocal call_count
        if call_count >= 4:  # After 4 attempts (1 initial + 3 rotations)
            raise RuntimeError("All HyperSync API keys have failed")
        mock_rotator.current_key = f"key{call_count + 1}"
        return mock_rotator.current_key

    mock_rotator.rotate = mock_rotate
    mock_rotator.mark_failed = MagicMock()

    async def always_rate_limited():
        nonlocal call_count
        call_count += 1
        raise RuntimeError("Error: rate limit exceeded")

    with patch("asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(RuntimeError, match="All HyperSync API keys have failed"):
            await retry_with_backoff(
                always_rate_limited,
                max_retries=5,
                operation_name="test_all_keys_failed",
                key_rotator=mock_rotator,
            )


@pytest.mark.asyncio
async def test_retry_without_key_rotator_backward_compatible():
    """Test that retry works without key_rotator (backward compatibility)."""
    call_count = 0

    async def failing_operation():
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            raise RuntimeError(f"Attempt {call_count} failed")
        return "success"

    with patch("asyncio.sleep", new_callable=AsyncMock):
        result = await retry_with_backoff(
            failing_operation,
            max_retries=5,
            operation_name="test_backward_compat",
            key_rotator=None,  # No key rotator
        )

    assert result == "success"
    assert call_count == 3


@pytest.mark.asyncio
async def test_rate_limit_detection_variations():
    """Test that various rate limit error messages are detected correctly."""
    rate_limit_messages = [
        "rate limit exceeded",
        "too many requests",
        "HTTP 429 error",
        "quota exceeded",
        "Rate Limit",  # Case insensitive
        "TOO MANY REQUESTS",  # Case insensitive
    ]

    for i, msg in enumerate(rate_limit_messages):
        call_count = 0
        mock_rotator = MagicMock(spec=HyperSyncKeyRotator)
        mock_rotator.current_key = "key1"
        mock_rotator.rotate = MagicMock(return_value="key2")
        mock_rotator.mark_failed = MagicMock()

        async def rate_limited_with_message():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError(f"Error: {msg}")
            return "success"

        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await retry_with_backoff(
                rate_limited_with_message,
                max_retries=5,
                operation_name=f"test_rate_limit_{i}",
                key_rotator=mock_rotator,
            )

        assert result == "success", f"Failed for message: {msg}"
        assert (
            mock_rotator.rotate.called
        ), f"Key rotation not triggered for message: {msg}"


@pytest.mark.asyncio
async def test_non_rate_limit_error_uses_normal_retry():
    """Test that non-rate-limit errors use normal retry logic with delays."""
    call_count = 0
    sleep_called = []

    async def failing_with_generic_error():
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            raise RuntimeError("Generic error")
        return "success"

    async def mock_sleep(duration):
        sleep_called.append(duration)

    mock_rotator = MagicMock(spec=HyperSyncKeyRotator)
    mock_rotator.rotate = MagicMock()

    with patch("asyncio.sleep", side_effect=mock_sleep):
        result = await retry_with_backoff(
            failing_with_generic_error,
            max_retries=5,
            operation_name="test_normal_retry",
            key_rotator=mock_rotator,
        )

    assert result == "success"
    assert call_count == 3
    assert len(sleep_called) == 2  # Should have slept for retries
    assert not mock_rotator.rotate.called  # Should NOT rotate for non-rate-limit errors


@pytest.mark.asyncio
async def test_traceback_logging_on_error():
    """Test that full tracebacks are logged for errors."""
    call_count = 0

    async def failing_operation():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ValueError("Detailed error with traceback")
        return "success"

    # Capture log output
    with patch("asyncio.sleep", new_callable=AsyncMock):
        with patch("gmx_historical_data.oracle_price_collector.console") as mock_console:
            with patch(
                "gmx_historical_data.oracle_price_collector.logger"
            ) as mock_logger:
                result = await retry_with_backoff(
                    failing_operation,
                    max_retries=5,
                    operation_name="test_traceback",
                )

    assert result == "success"
    # Verify that console.print was called for traceback
    assert mock_console.print.called, "Console.print should be called for traceback"
    # Verify logger.warning was called
    assert mock_logger.warning.called, "Logger.warning should be called for error"
