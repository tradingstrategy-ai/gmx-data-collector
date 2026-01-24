"""Decode Chainlink AnswerUpdated events from HyperSync data.

Event signature:
    event AnswerUpdated(int256 indexed current, uint256 indexed roundId, uint256 timestamp)

Topic layout:
    topic0: event signature hash (0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f)
    topic1: current (price) - indexed int256
    topic2: roundId - indexed uint256
    data: timestamp - non-indexed uint256
"""

from dataclasses import dataclass
from typing import Any
from eth_abi import decode
from web3 import Web3


@dataclass
class AnswerUpdatedEvent:
    """Decoded AnswerUpdated event data.

    :param block_number: Block number where event was emitted
    :param block_timestamp: Block timestamp (seconds since epoch)
    :param transaction_hash: Transaction hash (hex string)
    :param log_index: Log index within the transaction
    :param aggregator_address: Address of aggregator contract that emitted event
    :param price: Price value (raw int256, needs scaling by decimals)
    :param round_id: Chainlink round ID
    :param timestamp: Event timestamp (seconds since epoch)
    """

    block_number: int
    block_timestamp: int
    transaction_hash: str
    log_index: int
    aggregator_address: str
    price: int
    round_id: int
    timestamp: int

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage.

        :return: Dictionary representation
        """
        return {
            "block_number": self.block_number,
            "block_timestamp": self.block_timestamp,
            "transaction_hash": self.transaction_hash,
            "log_index": self.log_index,
            "aggregator_address": self.aggregator_address,
            "price": self.price,
            "round_id": self.round_id,
            "timestamp": self.timestamp,
        }


class EventDecoder:
    """Decode AnswerUpdated events from HyperSync log data."""

    ANSWER_UPDATED_TOPIC = (
        "0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f"
    )

    @staticmethod
    def decode_event(log_data: dict[str, Any]) -> AnswerUpdatedEvent:
        """Decode a single AnswerUpdated event from HyperSync log data.

        :param log_data: Log data from HyperSync response
        :return: Decoded event data
        :raises ValueError: If event signature doesn't match AnswerUpdated
        """
        # Validate event signature
        topics = log_data.get("topics", [])
        if not topics or topics[0] != EventDecoder.ANSWER_UPDATED_TOPIC:
            raise ValueError(
                f"Invalid event signature. Expected {EventDecoder.ANSWER_UPDATED_TOPIC}, "
                f"got {topics[0] if topics else 'no topics'}"
            )

        # Decode indexed parameters from topics
        # topic1: price (int256)
        price = decode(["int256"], bytes.fromhex(topics[1][2:]))[0]

        # topic2: roundId (uint256)
        round_id = decode(["uint256"], bytes.fromhex(topics[2][2:]))[0]

        # Decode non-indexed parameters from data
        # data: timestamp (uint256)
        data_bytes = (
            bytes.fromhex(log_data["data"][2:]) if log_data.get("data") else b""
        )
        timestamp = decode(["uint256"], data_bytes)[0] if data_bytes else 0

        return AnswerUpdatedEvent(
            block_number=log_data.get("block_number", 0),
            block_timestamp=log_data.get("block_timestamp", 0),
            transaction_hash=log_data.get("transaction_hash", ""),
            log_index=log_data.get("log_index", 0),
            aggregator_address=Web3.to_checksum_address(log_data.get("address", "")),
            price=price,
            round_id=round_id,
            timestamp=timestamp,
        )

    @staticmethod
    def decode_events(logs: list[dict[str, Any]]) -> list[AnswerUpdatedEvent]:
        """Decode multiple AnswerUpdated events.

        :param logs: List of log data from HyperSync response
        :return: List of decoded events
        """
        events = []
        for log in logs:
            try:
                event = EventDecoder.decode_event(log)
                events.append(event)
            except (ValueError, KeyError, IndexError) as e:
                # Log error but continue processing other events
                print(f"Warning: Failed to decode event: {e}")
                continue
        return events


def scale_price(raw_price: int, decimals: int = 8) -> float:
    """Scale raw Chainlink price to human-readable value.

    Chainlink prices typically use 8 decimals for USD pairs.

    :param raw_price: Raw price from event (int256)
    :param decimals: Number of decimals (default: 8)
    :return: Scaled price as float
    """
    return raw_price / (10**decimals)
