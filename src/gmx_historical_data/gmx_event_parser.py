"""Parse GMX position events using eth_defi.

Integrates with eth_defi.gmx.events to decode PositionIncrease and
PositionDecrease events from GMX EventEmitter contract.
"""

from dataclasses import dataclass
from eth_utils import keccak
from web3 import Web3
from eth_defi.gmx.events import decode_gmx_event, GMXEventData


@dataclass
class GMXPositionEvent:
    """Parsed GMX position event for price reconstruction.

    :param block_number: Block number
    :param block_timestamp: Block timestamp (Unix seconds)
    :param transaction_hash: Transaction hash
    :param log_index: Log index within transaction
    :param event_name: Event name ("PositionIncrease" or "PositionDecrease")
    :param market: Market contract address
    :param account: Trader address
    :param is_long: True for long position, False for short
    :param index_token_price_min: Oracle minimum price (30 decimals, from Chainlink)
    :param index_token_price_max: Oracle maximum price (30 decimals, from Chainlink)
    :param execution_price: Execution price (30 decimals, includes price impact)
    :param size_delta_usd: Position size change in USD (30 decimals)
    :param size_delta_in_tokens: Position size change in tokens
    :param price_impact_usd: Price impact in USD (30 decimals, can be negative)
    :param position_key: Unique position identifier
    :param collateral_token: Collateral token address
    """

    block_number: int
    block_timestamp: int
    transaction_hash: str
    log_index: int
    event_name: str
    market: str
    account: str
    is_long: bool
    index_token_price_min: int
    index_token_price_max: int
    execution_price: int
    size_delta_usd: int
    size_delta_in_tokens: int
    price_impact_usd: int
    position_key: str
    collateral_token: str


def get_event_name_hash(event_name: str) -> str:
    """Compute keccak256 hash of event name.

    GMX uses the hash of event name strings as topic[1] in EventLog events.

    :param event_name: Event name (e.g., "PositionIncrease")
    :return: Keccak256 hash as hex string without 0x prefix
    """
    hash_bytes = keccak(text=event_name)
    return hash_bytes.hex()


def parse_position_event(
    web3: Web3,
    log_dict: dict,
    block_timestamps: dict[int, int],
) -> GMXPositionEvent:
    """Parse HyperSync log dict into GMX position event.

    :param web3: Web3 instance
    :param log_dict: Log dictionary from HyperSync
    :param block_timestamps: Mapping of block number to timestamp
    :return: Parsed position event
    :raises ValueError: If log_dict is missing required fields, event decode fails,
                       block timestamp is missing, or event is not a position event
    """
    # Validate log_dict structure (before calling decode_gmx_event)
    required_keys = ["block_number", "transaction_hash", "log_index"]
    missing_keys = [key for key in required_keys if key not in log_dict]
    if missing_keys:
        raise ValueError(f"log_dict missing required keys: {missing_keys}")

    # Validate block timestamp exists (before calling decode_gmx_event)
    # This prevents data corruption from using Unix epoch 0 as default
    block_number = log_dict["block_number"]
    if block_number not in block_timestamps:
        raise ValueError(
            f"Block timestamp missing for block {block_number}. "
            f"Cannot use Unix epoch 0 as it leads to data corruption."
        )

    # Convert snake_case field names to camelCase for eth_defi compatibility
    # eth_defi expects full Ethereum log format with camelCase fields
    # HyperSync provides: block_number, block_hash, transaction_hash, transaction_index, log_index, etc.
    eth_defi_log_dict = {
        "blockNumber": log_dict["block_number"],
        "blockHash": log_dict.get("block_hash", ""),
        "transactionHash": log_dict["transaction_hash"],
        "transactionIndex": log_dict.get("transaction_index", 0),
        "logIndex": log_dict["log_index"],
        "address": log_dict.get("address", ""),
        "topics": log_dict.get("topics", []),
        "data": log_dict.get("data", "0x"),
    }

    # Use eth_defi to decode event
    event_data: GMXEventData = decode_gmx_event(web3, eth_defi_log_dict)

    if event_data is None:
        raise ValueError(f"Failed to decode event from log: {log_dict}")

    # Validate event is a position event
    if event_data.event_name not in ["PositionIncrease", "PositionDecrease"]:
        raise ValueError(
            f"Expected PositionIncrease or PositionDecrease, "
            f"got {event_data.event_name}"
        )

    # Extract position-specific fields - trust eth_defi to return correct types
    # Convert bytes32 position_key to hex string for storage
    position_key_bytes = event_data.get_bytes32("positionKey")
    position_key_hex = (
        position_key_bytes.hex()
        if isinstance(position_key_bytes, bytes)
        else str(position_key_bytes)
    )

    return GMXPositionEvent(
        block_number=block_number,
        block_timestamp=block_timestamps[block_number],
        transaction_hash=log_dict["transaction_hash"],
        log_index=log_dict["log_index"],
        event_name=event_data.event_name,
        market=event_data.get_address("market"),
        account=event_data.get_address("account"),
        is_long=event_data.get_bool("isLong"),
        index_token_price_min=event_data.get_uint("indexTokenPrice.min"),
        index_token_price_max=event_data.get_uint("indexTokenPrice.max"),
        execution_price=event_data.get_uint("executionPrice"),
        size_delta_usd=event_data.get_uint("sizeDeltaUsd"),
        size_delta_in_tokens=event_data.get_uint("sizeDeltaInTokens"),
        price_impact_usd=event_data.get_int("priceImpactUsd"),
        position_key=position_key_hex,
        collateral_token=event_data.get_address("collateralToken"),
    )
