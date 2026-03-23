"""Tests for GMX event parsing."""

import pytest
from web3 import Web3

from gmx_historical_data.gmx_event_parser import (
    get_event_name_hash,
    parse_position_event,
)


def test_get_event_name_hash():
    """Test computing keccak256 hash of event names."""
    # Known hash for PositionIncrease
    position_increase_hash = get_event_name_hash("PositionIncrease")

    assert isinstance(position_increase_hash, str)
    assert len(position_increase_hash) == 64  # 32 bytes = 64 hex chars
    assert position_increase_hash == position_increase_hash.lower()


def test_parse_position_event_validation():
    """Test validation in parse_position_event."""
    # Using None for web3 since validation should happen before web3 is used
    web3 = None
    block_timestamps = {12345678: 1704067200}

    # Test 1: Missing required keys in log_dict
    incomplete_log = {
        "block_number": 12345678,
        # Missing transaction_hash and log_index
    }
    with pytest.raises(ValueError, match="missing required keys"):
        parse_position_event(web3, incomplete_log, block_timestamps)

    # Test 2: Missing block timestamp (validates before decode_gmx_event call)
    # Note: This test validates our early validation catches missing timestamps
    # before attempting to decode the event
    log_dict_no_timestamp = {
        "block_number": 99999999,  # Not in block_timestamps
        "transaction_hash": "0xabcd1234" + "0" * 56,
        "log_index": 5,
        "address": "0xC8ee91A54287DB53897056e12D9819156D3822Fb",
        "topics": ["0x" + "0" * 64],
        "data": "0x" + "0" * 128,
    }
    with pytest.raises(ValueError, match="Block timestamp missing"):
        parse_position_event(web3, log_dict_no_timestamp, block_timestamps)


@pytest.mark.skip(reason="Requires realistic event data with proper ABI encoding")
def test_parse_position_event_integration():
    """Test parsing a real position event (requires realistic test data).

    This test is skipped because it needs properly ABI-encoded event data
    that matches the GMX EventEmitter contract. To enable this test, provide
    a real log from GMX contract with proper topics and data encoding.
    """
    # Example of what realistic test data would look like:
    log_dict = {
        "block_number": 12345678,
        "transaction_hash": "0xabcd1234" + "0" * 56,
        "log_index": 5,
        "address": "0xC8ee91A54287DB53897056e12D9819156D3822Fb",
        "topics": [
            "0x" + "0" * 64,  # topic0: EventLog signature
            get_event_name_hash("PositionIncrease"),  # topic1: event name hash
        ],
        "data": "0x" + "0" * 128,  # Would need proper ABI-encoded data
    }

    block_timestamps = {12345678: 1704067200}
    web3 = Web3()

    event = parse_position_event(web3, log_dict, block_timestamps)

    assert event.block_number == 12345678
    assert event.event_name == "PositionIncrease"
    assert event.execution_price > 0
