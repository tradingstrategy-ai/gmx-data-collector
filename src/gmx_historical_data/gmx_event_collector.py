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
    """

    def __init__(
        self,
        hypersync_endpoint: str,
        rpc_url: str,
    ):
        self.hypersync_endpoint = hypersync_endpoint
        self.web3 = Web3(Web3.HTTPProvider(rpc_url))

        # Initialize HyperSync client
        config = ClientConfig(url=hypersync_endpoint)
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

        # GMX uses EventLog/EventLog1/EventLog2 with event name hash in topic[1]
        log_selection = LogSelection(
            address=[EVENT_EMITTER_ADDRESS.lower()],
            topics=[
                [],  # topic0: EventLog signature (any variant)
                event_hashes,  # topic1: PositionIncrease or PositionDecrease
            ],
        )

        field_selection = FieldSelection(
            log=[
                LogField.BLOCK_NUMBER,
                LogField.TRANSACTION_HASH,
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
        block_timestamps = {}
        if response.data.blocks:
            for block in response.data.blocks:
                block_timestamps[block.number] = block.timestamp

        # Parse events
        events = []
        if response.data.logs:
            for log in response.data.logs:
                try:
                    # Convert HyperSync log to dict format
                    log_dict = {
                        "block_number": log.block_number,
                        "transaction_hash": log.transaction_hash or "",
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
