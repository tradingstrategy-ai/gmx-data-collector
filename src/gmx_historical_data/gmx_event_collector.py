"""Collect GMX position events using HyperSync.

Queries GMX EventEmitter contract for PositionIncrease and PositionDecrease
events, which contain execution prices from real trades.
"""

import logging
from web3 import Web3
from hypersync import HypersyncClient, ClientConfig, Query, LogSelection, FieldSelection, LogField, BlockField
from eth_utils import keccak

from gmx_historical_data.config import EVENT_EMITTER_ADDRESS
from gmx_historical_data.gmx_event_parser import GMXPositionEvent, parse_position_event


# EventLog1 signature hash from GMX EventEmitter contract
# EventLog1(address,string,string,bytes32,EventData)
# This is topic0 for most GMX events including PositionIncrease/PositionDecrease
EVENTLOG1_SIGNATURE = "0x137a44067c8961cd7e1d876f4754a5a3a75989b4552f1843fc69c3b372def160"


def get_position_event_hashes() -> list[str]:
    """Get event name hashes for PositionIncrease and PositionDecrease.

    :return: List of keccak256 hashes (hex strings with 0x prefix for HyperSync)
    """
    position_increase = "0x" + keccak(text="PositionIncrease").hex()
    position_decrease = "0x" + keccak(text="PositionDecrease").hex()
    return [position_increase, position_decrease]


class GMXEventCollector:
    """Collect GMX position events via HyperSync.

    :param hypersync_endpoint: HyperSync API endpoint
    :param rpc_url: Arbitrum RPC URL for Web3 operations
    :param api_token: Optional HyperSync API token for authentication
    """

    def __init__(
        self,
        hypersync_endpoint: str,
        rpc_url: str,
        api_token: str | None = None,
    ):
        self.hypersync_endpoint = hypersync_endpoint
        self.web3 = Web3(Web3.HTTPProvider(rpc_url))

        # Initialize HyperSync client
        config = ClientConfig(url=hypersync_endpoint, bearer_token=api_token)
        self.client = HypersyncClient(config)

    def build_query(
        self,
        start_block: int = 0,
        end_block: int | None = None,
    ) -> Query:
        """Build HyperSync query for position events.

        :param start_block: Starting block number
        :param end_block: Ending block number (None = latest)
        :return: HyperSync query
        """
        event_hashes = get_position_event_hashes()

        # Filter by EventLog1 signature (topic0) AND PositionIncrease/PositionDecrease (topic1)
        # This enables server-side filtering at HyperSync for much faster queries
        log_selection = LogSelection(
            address=[EVENT_EMITTER_ADDRESS.lower()],
            topics=[
                [EVENTLOG1_SIGNATURE],  # topic0: EventLog1 only (server-side filter)
                event_hashes,  # topic1: PositionIncrease or PositionDecrease
            ],
        )

        field_selection = FieldSelection(
            log=[
                LogField.BLOCK_NUMBER,
                LogField.BLOCK_HASH,
                LogField.TRANSACTION_HASH,
                LogField.TRANSACTION_INDEX,
                LogField.LOG_INDEX,
                LogField.ADDRESS,
                LogField.TOPIC0,
                LogField.TOPIC1,
                LogField.TOPIC2,
                LogField.TOPIC3,
                LogField.DATA,
            ],
            block=[
                BlockField.NUMBER,
                BlockField.TIMESTAMP,
            ],
        )

        return Query(
            from_block=start_block,
            to_block=end_block,
            logs=[log_selection],
            field_selection=field_selection,
        )

    async def collect_position_events(
        self,
        start_block: int = 0,
        end_block: int | None = None,
    ) -> list[GMXPositionEvent]:
        """Collect position events from HyperSync.

        :param start_block: Starting block number
        :param end_block: Ending block number (None = latest)
        :return: List of parsed position events
        """
        query = self.build_query(start_block, end_block)

        # Execute query
        response = await self.client.get(query)

        # Build block timestamp mapping
        # HyperSync may return timestamps as hex strings, convert to int
        block_timestamps = {}
        if response.data.blocks:
            for block in response.data.blocks:
                timestamp = block.timestamp
                # Convert hex string to int if needed
                if isinstance(timestamp, str) and timestamp.startswith("0x"):
                    timestamp = int(timestamp, 16)
                elif isinstance(timestamp, str):
                    timestamp = int(timestamp)
                block_timestamps[block.number] = timestamp

        # Parse events
        events = []
        if response.data.logs:
            for log in response.data.logs:
                try:
                    # Convert HyperSync log to dict format
                    log_dict = {
                        "block_number": log.block_number,
                        "block_hash": log.block_hash or "",
                        "transaction_hash": log.transaction_hash or "",
                        "transaction_index": log.transaction_index if log.transaction_index is not None else 0,
                        "log_index": log.log_index if log.log_index is not None else 0,
                        "address": log.address or "",
                        "topics": [t for t in (log.topics or []) if t is not None],
                        "data": log.data or "0x",
                    }

                    event = parse_position_event(self.web3, log_dict, block_timestamps)
                    events.append(event)

                except Exception as e:
                    # Skip events that fail to parse
                    logging.warning("Failed to parse event: %s", e)
                    continue

        return events
