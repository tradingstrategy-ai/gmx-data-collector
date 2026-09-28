"""Per-symbol candle depth checks -- did a coarser timeframe's fetch get cut short?

The daily report has always shown per-symbol OHLCV date ranges, but nothing
compared timeframes against *each other* for the same pair. That gap let a
regression like this one ship silently: 99 pairs' ``1d`` candles started
2026-03-08 while their ``4h`` candles reached back to 2023/2024 -- the ``1d``
fetch was truncated, but each pair's own report line looked fine in
isolation.

The obvious signal -- "N pairs share the same earliest date" -- is not
enough on its own: many GMX V2 markets legitimately launched on the same
day, at every timeframe, so a large shared-date cohort is normal by itself
(e.g. the Chainlink-oracle cohort that launched together on 2023-07-20).
What actually distinguishes truncation from a shared launch date is a *lag*
between a pair's own finer and coarser timeframes -- a coarser series should
never start later than its own finer sibling by more than a trading gap, so
a large cohort that also lags its finer neighbour is the real signature.

This module is pure: it takes already-read earliest-candle dates and never
touches the filesystem, so the detection logic can be tested without
fixture feathers. The I/O -- reading ``futures/{PAIR}-{tf}-futures.feather``
-- lives in ``scripts/collect_daily_snapshot.py``, alongside the other
report-data loaders.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date

#: Timeframes checked, finer to coarser. Only *adjacent* pairs in this order
#: are compared (see :func:`detect_depth_inversions`) -- comparing 1m
#: against 1d would flag every symbol trivially, since 1m only ever covers a
#: short rolling window by design.
TIMEFRAME_ORDER: tuple[str, ...] = ("1m", "5m", "15m", "1h", "4h", "1d")

#: A coarser timeframe starting more than this many days after its own
#: finer sibling (for the same pair) is read as a truncated fetch, not
#: normal history. GMX V2 markets can legitimately launch every timeframe on
#: the same day, and coarser timeframes are otherwise expected to reach back
#: *at least* as far as their finer siblings -- a multi-day gap only shows
#: up when one of the two fetches was cut short.
DEPTH_INVERSION_MIN_LAG_DAYS = 7

#: How many pairs must share the exact same lagging (coarser) earliest date
#: before it is reported as a systemic truncation rather than a handful of
#: unrelated per-symbol issues (e.g. one market genuinely relisted late).
DEPTH_INVERSION_MIN_COHORT = 20


@dataclass(frozen=True, slots=True)
class EarliestDateCohort:
    """The largest group of pairs sharing one earliest candle date.

    :param date: The shared earliest date, or ``None`` when there is no data
        at all.
    :param cohort_size: How many pairs share :attr:`date`.
    :param total_pairs: How many pairs had any earliest date to consider.
    """

    date: date | None
    cohort_size: int
    total_pairs: int


def largest_shared_earliest_cohort(earliest_by_pair: dict[str, date]) -> EarliestDateCohort:
    """Find the largest cohort of pairs sharing one earliest candle date.

    Ties are broken by picking the earlier date, so the result is
    deterministic regardless of dict ordering.

    :param earliest_by_pair: Pair -> its earliest candle date, for one
        timeframe.
    :returns: The winning cohort. Empty input yields an all-zero cohort.
    """
    if not earliest_by_pair:
        return EarliestDateCohort(None, 0, 0)
    counts = Counter(earliest_by_pair.values())
    winning_date, size = max(counts.items(), key=lambda kv: (kv[1], -kv[0].toordinal()))
    return EarliestDateCohort(winning_date, size, len(earliest_by_pair))


@dataclass(frozen=True, slots=True)
class DepthInversion:
    """One adjacent timeframe transition where the coarser series lags.

    :param finer_tf: The finer timeframe compared against, e.g. ``'4h'``.
    :param coarser_tf: The timeframe found to start too late, e.g. ``'1d'``.
    :param cohort: The shared-earliest-date cohort among the inverted pairs
        at ``coarser_tf``.
    :param inverted_pairs: How many pairs (of those present at both
        timeframes) lag by more than :data:`DEPTH_INVERSION_MIN_LAG_DAYS`.
    :param compared_pairs: How many pairs had data at both timeframes.
    """

    finer_tf: str
    coarser_tf: str
    cohort: EarliestDateCohort
    inverted_pairs: int
    compared_pairs: int

    @property
    def is_truncation_signature(self) -> bool:
        """Whether this looks like a systemic truncation, not a one-off.

        :returns: ``True`` when the inverted-pairs cohort meets
            :data:`DEPTH_INVERSION_MIN_COHORT`.
        """
        return self.cohort.cohort_size >= DEPTH_INVERSION_MIN_COHORT


def detect_depth_inversions(
    earliest_by_tf: dict[str, dict[str, date]],
    timeframe_order: tuple[str, ...] = TIMEFRAME_ORDER,
    min_lag_days: int = DEPTH_INVERSION_MIN_LAG_DAYS,
) -> list[DepthInversion]:
    """Compare each adjacent timeframe pair for a lagging coarser fetch.

    Only pairs present at *both* timeframes of a transition are compared --
    a pair missing one timeframe entirely is a coverage gap, not a depth
    inversion, and is reported elsewhere.

    :param earliest_by_tf: ``{timeframe: {pair: earliest_date}}``. A
        timeframe with no entries is simply skipped.
    :param timeframe_order: Finer-to-coarser walk order; only adjacent
        entries are compared against each other.
    :param min_lag_days: Days the coarser start must trail the finer one by,
        for one pair, to count as inverted.
    :returns: One :class:`DepthInversion` per adjacent transition that has
        at least one inverted pair, in ``timeframe_order``.
    """
    results: list[DepthInversion] = []
    for finer_tf, coarser_tf in zip(timeframe_order, timeframe_order[1:], strict=False):
        finer = earliest_by_tf.get(finer_tf, {})
        coarser = earliest_by_tf.get(coarser_tf, {})
        common_pairs = set(finer) & set(coarser)
        if not common_pairs:
            continue

        inverted_coarser_dates = {
            pair: coarser[pair]
            for pair in common_pairs
            if (coarser[pair] - finer[pair]).days > min_lag_days
        }
        if not inverted_coarser_dates:
            continue

        cohort = largest_shared_earliest_cohort(inverted_coarser_dates)
        results.append(
            DepthInversion(
                finer_tf=finer_tf,
                coarser_tf=coarser_tf,
                cohort=cohort,
                inverted_pairs=len(inverted_coarser_dates),
                compared_pairs=len(common_pairs),
            )
        )
    return results
