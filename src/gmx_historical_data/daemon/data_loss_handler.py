"""Data loss handling and tracking for the daemon.

Tracks permanent data loss events when collection gaps exceed
the GMX API's sliding window retention period.
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone


logger = logging.getLogger(__name__)


@dataclass
class DataLossEvent:
    """Record of a permanent data loss event.

    :param symbol: Token symbol (e.g., 'ETH').
    :param timeframe: Timeframe string (e.g., '1h').
    :param detected_at: When the data loss was detected.
    :param our_latest: Our last stored timestamp before the gap.
    :param api_earliest: Earliest timestamp available from API.
    :param lost_candles: Estimated number of candles permanently lost.
    :param lost_timespan: Human-readable description of lost time period.
    """

    symbol: str
    timeframe: str
    detected_at: datetime
    our_latest: datetime | None
    api_earliest: datetime | None
    lost_candles: int
    lost_timespan: str | None

    def to_dict(self) -> dict:
        """Convert event to dictionary for logging/serialization.

        :return: Dictionary representation with ISO formatted datetimes.
        """
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "detected_at": self.detected_at.isoformat(),
            "our_latest": self.our_latest.isoformat() if self.our_latest else None,
            "api_earliest": self.api_earliest.isoformat()
            if self.api_earliest
            else None,
            "lost_candles": self.lost_candles,
            "lost_timespan": self.lost_timespan,
        }


class DataLossHandler:
    """Handle and track data loss events.

    Logs CRITICAL errors for data loss and maintains history
    for monitoring and alerting purposes.
    """

    def __init__(self):
        """Initialize data loss handler."""
        self.loss_history: list[DataLossEvent] = []

    def handle_gap_result(
        self,
        symbol: str,
        timeframe: str,
        gap_result,
    ) -> DataLossEvent | None:
        """Process a gap detection result and handle any data loss.

        :param symbol: Token symbol.
        :param timeframe: Timeframe string.
        :param gap_result: GapDetectionResult from AdaptiveGapDetector.
        :return: DataLossEvent if data loss detected, None otherwise.
        """
        if not gap_result.has_data_loss:
            return None

        event = DataLossEvent(
            symbol=symbol,
            timeframe=timeframe,
            detected_at=datetime.now(timezone.utc),
            our_latest=gap_result.our_latest,
            api_earliest=gap_result.api_earliest,
            lost_candles=gap_result.lost_candles_estimate,
            lost_timespan=gap_result.lost_timespan,
        )

        # Record in history
        self.loss_history.append(event)

        # Log as CRITICAL with structured JSON
        logger.critical(
            json.dumps(
                {
                    "event": "data_loss_detected",
                    **event.to_dict(),
                }
            )
        )

        return event

    def get_loss_summary(self) -> dict:
        """Get summary of all data loss events.

        :return: Dictionary with loss statistics.
        """
        if not self.loss_history:
            return {
                "total_events": 0,
                "total_candles_lost": 0,
                "affected_symbols": [],
                "affected_timeframes": [],
            }

        affected_symbols = list(set(e.symbol for e in self.loss_history))
        affected_timeframes = list(set(e.timeframe for e in self.loss_history))
        total_candles = sum(e.lost_candles for e in self.loss_history)

        return {
            "total_events": len(self.loss_history),
            "total_candles_lost": total_candles,
            "affected_symbols": affected_symbols,
            "affected_timeframes": affected_timeframes,
            "recent_events": [e.to_dict() for e in self.loss_history[-10:]],
        }

    def clear_history(self) -> None:
        """Clear the loss history."""
        self.loss_history.clear()
