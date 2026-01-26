"""Health monitoring and metrics tracking for the daemon."""

import json
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict
from typing import Dict


logger = logging.getLogger(__name__)


@dataclass
class CycleMetrics:
    """Metrics for a single collection cycle.

    :param cycle_number: Cycle sequence number
    :param start_time: When cycle started
    :param end_time: When cycle ended
    :param duration_seconds: Cycle duration
    :param total_symbols_attempted: Number of symbols attempted
    :param symbols_succeeded: Number of symbols successfully collected
    :param symbols_failed: Number of symbols that failed
    :param candles_added: Candles added per symbol/timeframe
    :param errors: List of error details
    :param data_loss_events: List of data loss events in this cycle
    """

    cycle_number: int
    start_time: datetime
    end_time: datetime | None = None
    duration_seconds: float | None = None
    total_symbols_attempted: int = 0
    symbols_succeeded: int = 0
    symbols_failed: int = 0
    candles_added: Dict[str, Dict[str, int]] = field(default_factory=dict)
    errors: list[dict] = field(default_factory=list)
    data_loss_events: list[dict] = field(default_factory=list)


class HealthMonitor:
    """Monitor daemon health and track collection metrics.

    :param log_metrics: Whether to log metrics as structured JSON
    """

    def __init__(self, log_metrics: bool = True):
        """Initialize health monitor.

        :param log_metrics: Whether to log metrics
        """
        self.log_metrics = log_metrics
        self.daemon_start_time = datetime.now(timezone.utc)
        self.current_cycle: CycleMetrics | None = None
        self.cycle_count = 0
        self.total_cycles_completed = 0
        self.total_cycles_failed = 0
        self.last_successful_cycle: CycleMetrics | None = None
        self.last_cycle_end_time: datetime | None = None

    def start_cycle(self) -> None:
        """Start tracking a new collection cycle."""
        self.cycle_count += 1
        self.current_cycle = CycleMetrics(
            cycle_number=self.cycle_count,
            start_time=datetime.now(timezone.utc),
        )

        if self.log_metrics:
            logger.info(
                json.dumps(
                    {
                        "event": "cycle_start",
                        "cycle_number": self.cycle_count,
                        "timestamp": self.current_cycle.start_time.isoformat(),
                    }
                )
            )

    def record_symbol_success(
        self, symbol: str, candles_by_timeframe: Dict[str, int]
    ) -> None:
        """Record successful collection for a symbol.

        :param symbol: Token symbol
        :param candles_by_timeframe: Candles added per timeframe
        """
        if not self.current_cycle:
            logger.warning("No active cycle to record success")
            return

        self.current_cycle.symbols_succeeded += 1
        self.current_cycle.candles_added[symbol] = candles_by_timeframe

    def record_symbol_failure(self, symbol: str, error: str, timeframe: str = None) -> None:
        """Record failed collection for a symbol.

        :param symbol: Token symbol
        :param error: Error message
        :param timeframe: Optional timeframe that failed
        """
        if not self.current_cycle:
            logger.warning("No active cycle to record failure")
            return

        self.current_cycle.symbols_failed += 1
        self.current_cycle.errors.append(
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "error": error,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )

        if self.log_metrics:
            logger.error(
                json.dumps(
                    {
                        "event": "symbol_failure",
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "error": error,
                        "cycle_number": self.current_cycle.cycle_number,
                    }
                )
            )

    def record_data_loss(self, event) -> None:
        """Record a data loss event for tracking.

        :param event: DataLossEvent from DataLossHandler
        """
        if not self.current_cycle:
            logger.warning("No active cycle to record data loss")
            return

        self.current_cycle.data_loss_events.append(event.to_dict())

        if self.log_metrics:
            logger.critical(
                json.dumps(
                    {
                        "event": "data_loss_recorded",
                        "symbol": event.symbol,
                        "timeframe": event.timeframe,
                        "lost_candles": event.lost_candles,
                        "lost_timespan": event.lost_timespan,
                        "cycle_number": self.current_cycle.cycle_number,
                    }
                )
            )

    def end_cycle(self, total_symbols: int) -> None:
        """End the current collection cycle.

        :param total_symbols: Total number of symbols attempted
        """
        if not self.current_cycle:
            logger.warning("No active cycle to end")
            return

        self.current_cycle.end_time = datetime.now(timezone.utc)
        self.current_cycle.total_symbols_attempted = total_symbols
        self.current_cycle.duration_seconds = (
            self.current_cycle.end_time - self.current_cycle.start_time
        ).total_seconds()

        # Update cumulative stats
        if self.current_cycle.symbols_failed == 0:
            self.total_cycles_completed += 1
            self.last_successful_cycle = self.current_cycle
        else:
            self.total_cycles_failed += 1

        self.last_cycle_end_time = self.current_cycle.end_time

        # Log cycle summary
        if self.log_metrics:
            total_candles = sum(
                sum(tf_candles.values())
                for tf_candles in self.current_cycle.candles_added.values()
            )

            logger.info(
                json.dumps(
                    {
                        "event": "cycle_complete",
                        "cycle_number": self.current_cycle.cycle_number,
                        "duration_seconds": self.current_cycle.duration_seconds,
                        "symbols_attempted": total_symbols,
                        "symbols_succeeded": self.current_cycle.symbols_succeeded,
                        "symbols_failed": self.current_cycle.symbols_failed,
                        "total_candles_added": total_candles,
                        "errors_count": len(self.current_cycle.errors),
                    }
                )
            )

    def get_health_status(self) -> dict:
        """Get current health status for health check endpoint.

        :return: Health status dictionary
        """
        uptime_seconds = (
            datetime.now(timezone.utc) - self.daemon_start_time
        ).total_seconds()

        # Determine status
        if not self.current_cycle:
            status = "healthy"  # Just started
        elif self.current_cycle.symbols_failed > 0:
            # Degraded if some symbols failed
            failure_rate = (
                self.current_cycle.symbols_failed
                / self.current_cycle.total_symbols_attempted
            )
            status = "degraded" if failure_rate < 0.5 else "unhealthy"
        else:
            status = "healthy"

        return {
            "status": status,
            "uptime_seconds": uptime_seconds,
            "daemon_start_time": self.daemon_start_time.isoformat(),
            "total_cycles_completed": self.total_cycles_completed,
            "total_cycles_failed": self.total_cycles_failed,
            "last_cycle_end_time": (
                self.last_cycle_end_time.isoformat()
                if self.last_cycle_end_time
                else None
            ),
            "current_cycle": (
                {
                    "cycle_number": self.current_cycle.cycle_number,
                    "symbols_attempted": self.current_cycle.total_symbols_attempted,
                    "symbols_succeeded": self.current_cycle.symbols_succeeded,
                    "symbols_failed": self.current_cycle.symbols_failed,
                    "errors": self.current_cycle.errors[-5:],  # Last 5 errors
                }
                if self.current_cycle
                else None
            ),
        }

    def get_metrics_summary(self) -> dict:
        """Get summary of all metrics for logging.

        :return: Metrics summary dictionary
        """
        if not self.current_cycle:
            return {
                "total_cycles": self.cycle_count,
                "completed": self.total_cycles_completed,
                "failed": self.total_cycles_failed,
            }

        return {
            "total_cycles": self.cycle_count,
            "completed": self.total_cycles_completed,
            "failed": self.total_cycles_failed,
            "current_cycle": {
                "number": self.current_cycle.cycle_number,
                "succeeded": self.current_cycle.symbols_succeeded,
                "failed": self.current_cycle.symbols_failed,
                "total_attempted": self.current_cycle.total_symbols_attempted,
            },
        }
