"""Per-symbol candle depth checks -- catching a truncated coarser fetch.

The daily report has always shown per-symbol OHLCV ranges, but nothing ever
flagged the case where a *coarser* timeframe's historical window is
inexplicably shorter than its own finer sibling for the same pair -- exactly
what happened when 99 pairs' ``1d`` candles started 2026-03-08 while their
``4h`` candles reached back to 2023/2024.

The tricky part is telling that apart from a legitimate shared start date:
many GMX V2 markets launched on the same day, so a large cohort sharing one
earliest date is normal by itself. The signature that actually indicates
truncation is a *lag* between a pair's own finer and coarser timeframes.
"""

from datetime import date

from gmx_historical_data.depth_inversion import (
    DEPTH_INVERSION_MIN_COHORT,
    DEPTH_INVERSION_MIN_LAG_DAYS,
    detect_depth_inversions,
    largest_shared_earliest_cohort,
)


class TestLargestSharedEarliestCohort:
    def test_empty_input(self):
        cohort = largest_shared_earliest_cohort({})

        assert cohort.date is None
        assert cohort.cohort_size == 0
        assert cohort.total_pairs == 0

    def test_all_distinct_dates_yields_cohort_of_one(self):
        earliest = {
            "AAA_USDC_USDC": date(2023, 1, 1),
            "BBB_USDC_USDC": date(2023, 6, 1),
            "CCC_USDC_USDC": date(2024, 1, 1),
        }

        cohort = largest_shared_earliest_cohort(earliest)

        assert cohort.cohort_size == 1
        assert cohort.total_pairs == 3

    def test_majority_sharing_one_date_wins(self):
        launch = date(2023, 7, 20)
        earliest = {
            "AAA_USDC_USDC": launch,
            "BBB_USDC_USDC": launch,
            "CCC_USDC_USDC": launch,
            "DDD_USDC_USDC": date(2024, 3, 1),
        }

        cohort = largest_shared_earliest_cohort(earliest)

        assert cohort.date == launch
        assert cohort.cohort_size == 3
        assert cohort.total_pairs == 4


class TestDetectDepthInversions:
    def _pairs(self, n: int, prefix: str = "SYM") -> list[str]:
        return [f"{prefix}{i}_USDC_USDC" for i in range(n)]

    def test_no_data_yields_no_inversions(self):
        assert detect_depth_inversions({}) == []

    def test_no_lag_between_adjacent_timeframes_is_not_flagged(self):
        launch = date(2023, 7, 20)
        pairs = self._pairs(5)
        earliest_by_tf = {
            "4h": {p: launch for p in pairs},
            "1d": {p: launch for p in pairs},
        }

        assert detect_depth_inversions(earliest_by_tf) == []

    def test_small_lag_under_threshold_is_not_flagged(self):
        pairs = self._pairs(30)
        earliest_by_tf = {
            "4h": {p: date(2023, 7, 20) for p in pairs},
            "1d": {p: date(2023, 7, 22) for p in pairs},  # 2 days, under the floor
        }

        assert detect_depth_inversions(earliest_by_tf) == []

    def test_large_shared_lag_is_flagged_as_truncation_signature(self):
        """This is the real-world shape: 99 pairs' 1d starts materially
        later than their own 4h history."""
        pairs = self._pairs(99)
        earliest_by_tf = {
            "4h": {p: date(2023, 7, 20) for p in pairs},
            "1d": {p: date(2026, 3, 8) for p in pairs},
        }

        inversions = detect_depth_inversions(earliest_by_tf)

        assert len(inversions) == 1
        inv = inversions[0]
        assert inv.finer_tf == "4h"
        assert inv.coarser_tf == "1d"
        assert inv.inverted_pairs == 99
        assert inv.compared_pairs == 99
        assert inv.cohort.date == date(2026, 3, 8)
        assert inv.cohort.cohort_size == 99
        assert inv.is_truncation_signature is True

    def test_small_cohort_of_inverted_pairs_is_not_a_truncation_signature(self):
        """A handful of pairs individually lagging is not the systemic bug
        this check exists to catch -- it needs a shared cohort."""
        pairs = self._pairs(DEPTH_INVERSION_MIN_COHORT - 1)
        earliest_by_tf = {
            "4h": {p: date(2023, 7, 20) for p in pairs},
            "1d": {p: date(2026, 3, 8) for p in pairs},
        }

        inversions = detect_depth_inversions(earliest_by_tf)

        assert len(inversions) == 1
        assert inversions[0].is_truncation_signature is False

    def test_v2_launch_cohort_alone_does_not_trigger(self):
        """Many Chainlink-oracle V2 markets legitimately launched together
        on 2023-07-20 at every timeframe -- a shared earliest date with no
        lag between finer and coarser siblings must never be flagged."""
        pairs = self._pairs(50, prefix="CL")
        launch = date(2023, 7, 20)
        earliest_by_tf = {
            "1h": {p: launch for p in pairs},
            "4h": {p: launch for p in pairs},
        }

        assert detect_depth_inversions(earliest_by_tf) == []

    def test_only_pairs_present_at_both_timeframes_are_compared(self):
        earliest_by_tf = {
            "4h": {"AAA_USDC_USDC": date(2023, 1, 1)},
            "1d": {"BBB_USDC_USDC": date(2026, 1, 1)},
        }

        assert detect_depth_inversions(earliest_by_tf) == []

    def test_non_adjacent_timeframes_are_never_compared(self):
        """1m only ever covers a short rolling window, so comparing it
        against 1d would flag every symbol trivially. Only adjacent
        timeframes in the finer-to-coarser order are compared."""
        pairs = self._pairs(30)
        earliest_by_tf = {
            "1m": {p: date(2026, 9, 1) for p in pairs},
            "1d": {p: date(2020, 1, 1) for p in pairs},
        }

        assert detect_depth_inversions(earliest_by_tf) == []

    def test_multiple_transitions_can_each_be_flagged(self):
        # 15m -> 1h is inverted (1h starts later than its finer sibling);
        # 1h -> 4h is normal (4h reaches back further, as coarser data
        # usually does); 4h -> 1d is inverted again, independently.
        pairs = self._pairs(25)
        earliest_by_tf = {
            "15m": {p: date(2020, 1, 1) for p in pairs},
            "1h": {p: date(2026, 8, 1) for p in pairs},
            "4h": {p: date(2020, 1, 1) for p in pairs},
            "1d": {p: date(2026, 9, 1) for p in pairs},
        }

        inversions = detect_depth_inversions(earliest_by_tf)

        transitions = {(inv.finer_tf, inv.coarser_tf) for inv in inversions}
        assert transitions == {("15m", "1h"), ("4h", "1d")}


def test_default_thresholds_are_documented_constants():
    """Regression guard: these are cited by name in the report, so an
    accidental edit must be visible in a diff of the constant, not buried in
    a magic number somewhere in the detection logic."""
    assert DEPTH_INVERSION_MIN_LAG_DAYS == 7
    assert DEPTH_INVERSION_MIN_COHORT == 20
