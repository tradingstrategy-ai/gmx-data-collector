"""Tests for HyperSync API key rotation."""

import pytest

from gmx_historical_data.hypersync_key_rotator import HyperSyncKeyRotator


def test_parse_single_key():
    """Test parsing a single API key."""
    rotator = HyperSyncKeyRotator("single_key")
    assert rotator.total_keys == 1
    assert rotator.current_key == "single_key"


def test_parse_multiple_keys():
    """Test parsing multiple space-separated API keys."""
    rotator = HyperSyncKeyRotator("key1 key2 key3")
    assert rotator.total_keys == 3
    assert rotator.current_key == "key1"


def test_parse_keys_with_extra_whitespace():
    """Test parsing keys with extra whitespace."""
    rotator = HyperSyncKeyRotator("  key1   key2  key3  ")
    assert rotator.total_keys == 3
    assert rotator.current_key == "key1"


def test_rotate_to_next_key():
    """Test rotating to the next key."""
    rotator = HyperSyncKeyRotator("key1 key2 key3")
    assert rotator.current_key == "key1"

    next_key = rotator.rotate()
    assert next_key == "key2"
    assert rotator.current_key == "key2"

    next_key = rotator.rotate()
    assert next_key == "key3"
    assert rotator.current_key == "key3"


def test_rotate_wraps_around():
    """Test that rotation wraps around to the first key."""
    rotator = HyperSyncKeyRotator("key1 key2")
    rotator.rotate()  # Move to key2
    next_key = rotator.rotate()  # Should wrap to key1
    assert next_key == "key1"
    assert rotator.current_key == "key1"


def test_mark_key_as_failed():
    """Test marking a key as failed and skipping it during rotation."""
    rotator = HyperSyncKeyRotator("key1 key2 key3")
    assert rotator.current_key == "key1"

    # Mark key2 as failed
    rotator.mark_failed("key2")

    # Rotate should skip key2 and go to key3
    next_key = rotator.rotate()
    assert next_key == "key3"
    assert rotator.current_key == "key3"

    # Rotate should wrap to key1 (skipping key2)
    next_key = rotator.rotate()
    assert next_key == "key1"


def test_mark_current_key_as_failed():
    """Test marking the current key as failed moves to next available key."""
    rotator = HyperSyncKeyRotator("key1 key2 key3")
    assert rotator.current_key == "key1"

    # Mark current key as failed
    rotator.mark_failed("key1")

    # Rotate should skip key1 and go to key2
    next_key = rotator.rotate()
    assert next_key == "key2"


def test_all_keys_failed_raises_error():
    """Test that RuntimeError is raised when all keys are marked as failed."""
    rotator = HyperSyncKeyRotator("key1 key2 key3")

    # Mark all keys as failed
    rotator.mark_failed("key1")
    rotator.mark_failed("key2")
    rotator.mark_failed("key3")

    # Attempting to rotate should raise RuntimeError
    with pytest.raises(RuntimeError, match="All HyperSync API keys have failed"):
        rotator.rotate()


def test_reset_failures():
    """Test resetting failed keys."""
    rotator = HyperSyncKeyRotator("key1 key2 key3")

    # Mark some keys as failed
    rotator.mark_failed("key1")
    rotator.mark_failed("key2")

    # Reset failures
    rotator.reset_failures()

    # Should be able to use all keys again
    assert rotator.current_key == "key1"
    next_key = rotator.rotate()
    assert next_key == "key2"


def test_empty_keys_string():
    """Test that empty or whitespace-only string raises ValueError."""
    with pytest.raises(ValueError, match="At least one API key must be provided"):
        HyperSyncKeyRotator("")

    with pytest.raises(ValueError, match="At least one API key must be provided"):
        HyperSyncKeyRotator("   ")


def test_mark_invalid_key_as_failed():
    """Test marking a non-existent key as failed (should be ignored)."""
    rotator = HyperSyncKeyRotator("key1 key2 key3")

    # Mark a key that doesn't exist - should not cause issues
    rotator.mark_failed("nonexistent_key")

    # Normal rotation should still work
    next_key = rotator.rotate()
    assert next_key == "key2"
