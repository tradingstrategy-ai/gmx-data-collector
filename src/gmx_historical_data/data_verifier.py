"""Verify data collection completeness and quality."""

import pandas as pd
from dataclasses import dataclass
from gmx_historical_data.storage import ParquetStorage


@dataclass
class VerificationReport:
    """Data verification results.

    :param symbol: Token symbol
    :param coverage_start: Earliest timestamp in data
    :param coverage_end: Latest timestamp in data
    :param total_candles: Number of candles
    :param gaps_detected: List of time gaps (start, end) tuples
    :param quality_score: Quality score 0-100
    """

    symbol: str
    coverage_start: pd.Timestamp | None
    coverage_end: pd.Timestamp | None
    total_candles: int
    gaps_detected: list[tuple[pd.Timestamp, pd.Timestamp]]
    quality_score: float


class DataVerifier:
    """Verify collected data quality.

    :param storage: ParquetStorage instance
    """

    def __init__(self, storage: ParquetStorage):
        """Initialize data verifier.

        :param storage: ParquetStorage instance
        """
        self.storage = storage

    def verify_symbol(self, symbol: str, timeframe: str = "1h") -> VerificationReport:
        """Verify data for a symbol.

        :param symbol: Token symbol to verify
        :param timeframe: Timeframe to verify (default: 1h)
        :return: VerificationReport with coverage and quality metrics
        """
        df = self.storage.read_candles(timeframe, symbol)

        if df.empty:
            return VerificationReport(
                symbol=symbol,
                coverage_start=None,
                coverage_end=None,
                total_candles=0,
                gaps_detected=[],
                quality_score=0.0,
            )

        gaps = self._detect_gaps(df, timeframe)
        quality = self._calculate_quality_score(df, gaps, timeframe)

        return VerificationReport(
            symbol=symbol,
            coverage_start=df["timestamp"].min(),
            coverage_end=df["timestamp"].max(),
            total_candles=len(df),
            gaps_detected=gaps,
            quality_score=quality,
        )

    def _detect_gaps(
        self,
        df: pd.DataFrame,
        timeframe: str,
        max_gap_multiplier: float = 3.0,
    ) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
        """Detect time gaps in OHLCV data.

        :param df: DataFrame with timestamp column
        :param timeframe: Timeframe (e.g., "1h")
        :param max_gap_multiplier: Multiplier for expected interval to detect gaps
        :return: List of (gap_start, gap_end) tuples
        """
        gaps = []

        # Expected interval per timeframe
        intervals = {
            "1min": pd.Timedelta(minutes=1),
            "5min": pd.Timedelta(minutes=5),
            "15min": pd.Timedelta(minutes=15),
            "1h": pd.Timedelta(hours=1),
            "4h": pd.Timedelta(hours=4),
            "1d": pd.Timedelta(days=1),
        }
        expected_interval = intervals.get(timeframe)
        if not expected_interval:
            return gaps

        # Sort by timestamp
        df = df.sort_values("timestamp")

        # Find gaps larger than expected
        for i in range(len(df) - 1):
            current = df.iloc[i]["timestamp"]
            next_ts = df.iloc[i + 1]["timestamp"]
            actual_gap = next_ts - current

            if actual_gap > expected_interval * max_gap_multiplier:
                gaps.append((current, next_ts))

        return gaps

    def _calculate_quality_score(
        self,
        df: pd.DataFrame,
        gaps: list,
        timeframe: str,
    ) -> float:
        """Calculate data quality score (0-100).

        :param df: DataFrame with timestamp column
        :param gaps: List of detected gaps
        :param timeframe: Timeframe string
        :return: Quality score 0-100
        """
        if df.empty:
            return 0.0

        score = 100.0

        # Penalty for gaps
        gap_penalty = min(len(gaps) * 5, 50)
        score -= gap_penalty

        # Bonus for long history
        time_range = (df["timestamp"].max() - df["timestamp"].min()).days
        if time_range > 365:
            score = min(score + 10, 100)

        # Penalty for low coverage
        intervals_per_day = {
            "1min": 1440,
            "5min": 288,
            "15min": 96,
            "1h": 24,
            "4h": 6,
            "1d": 1,
        }
        expected_per_day = intervals_per_day.get(timeframe, 24)
        expected_total = time_range * expected_per_day
        actual_total = len(df)

        if expected_total > 0:
            coverage_ratio = actual_total / expected_total
            if coverage_ratio < 0.5:
                score -= 20

        return max(0.0, min(100.0, score))
