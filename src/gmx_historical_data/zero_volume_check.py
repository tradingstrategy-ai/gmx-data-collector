"""Per-symbol zero-volume check -- collector bug or a genuinely quiet market?

Some GMX markets are listed but barely traded (BOME, BRETT, MEME, MEW, SATS
at the time this was written), so "this pair's recent candles are almost all
zero volume" is not, by itself, evidence that anything is broken. What tells
a dormant market apart from a collector defect is open interest: a pair with
negligible OI *and* zero volume is quiet because nobody is trading it, while
a pair carrying real OI but reporting zero volume is suspicious -- positions
are open, so the underlying market is active even though the volume series
says otherwise.

Pure module: it takes already-computed zero-volume shares and open-interest
totals and never touches the filesystem or a DataFrame, so the classification
rule is testable without fixture candles. The I/O -- reading recent 1h
candles and today's snapshot -- lives in ``scripts/collect_daily_snapshot.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

#: How many days of 1h candles the caller is expected to have looked back
#: over when building ``zero_share_by_pair``. Long enough that one bad hour
#: of missing fills doesn't read as "always zero"; short enough that a
#: market's ancient dormant period doesn't drag a since-revived pair in.
#: Documented here so the report can cite it even though the window itself
#: is applied by the caller, not this module.
ZERO_VOLUME_LOOKBACK_DAYS = 30

#: A pair whose share of zero-volume candles over the lookback window is at
#: or above this is flagged for review. Not 100%: an actively traded market
#: can still miss a fill in an isolated candle, so requiring total silence
#: would undercount real collector bugs.
ZERO_VOLUME_SHARE_THRESHOLD = 0.95

#: Open interest at or below this (in USD) counts as negligible -- the pair
#: is genuinely dormant, not broken. Chosen far below the OI any actively
#: traded GMX market carries, and comfortably above the near-zero OI a
#: listed-but-abandoned market (e.g. BOME/BRETT/MEME/MEW/SATS) actually has.
NEGLIGIBLE_OI_USD = 1_000.0


class ZeroVolumeVerdict(Enum):
    """What a pair's zero-volume streak means, once OI is checked.

    :cvar EXPECTED: Negligible OI alongside the zero volume -- nobody is
        trading this market; nothing to report.
    :cvar SUSPICIOUS: Non-trivial OI despite zero reported volume --
        positions are open, so this looks like a collector defect.
    :cvar UNKNOWN: No OI data for this pair (missing snapshot or OI column).
        Reported as such rather than guessed at.
    """

    EXPECTED = "expected (no activity)"
    SUSPICIOUS = "suspicious (has open interest)"
    UNKNOWN = "unknown (no OI data)"


@dataclass(frozen=True, slots=True)
class ZeroVolumeFinding:
    """One live pair whose recent candles are dominated by zero volume.

    :param pair: Pair stem, e.g. ``BTC_USDC_USDC``.
    :param zero_share: Fraction of the lookback window's candles with
        ``volume == 0``.
    :param candles: How many candles were in the lookback window.
    :param open_interest_usd: Today's OI for this pair, or ``None`` when no
        OI data was available for it.
    :param verdict: What the OI says about whether this is expected.
    """

    pair: str
    zero_share: float
    candles: int
    open_interest_usd: float | None
    verdict: ZeroVolumeVerdict


def classify_zero_volume(
    zero_share_by_pair: dict[str, tuple[float, int]],
    open_interest_by_pair: dict[str, float],
    live_pairs: set[str] | None = None,
    share_threshold: float = ZERO_VOLUME_SHARE_THRESHOLD,
    negligible_oi_usd: float = NEGLIGIBLE_OI_USD,
) -> list[ZeroVolumeFinding]:
    """Classify pairs whose recent candle volume is almost entirely zero.

    :param zero_share_by_pair: Pair -> ``(zero_share, candle_count)`` over
        the lookback window.
    :param open_interest_by_pair: Pair -> today's OI in USD. A pair absent
        from this dict is graded :attr:`ZeroVolumeVerdict.UNKNOWN` rather
        than assumed either way.
    :param live_pairs: Pairs to consider -- callers exclude delisted markets
        before calling this, since a delisted market's frozen history is
        expected to stop trading entirely. ``None`` considers every pair
        present in ``zero_share_by_pair``.
    :param share_threshold: Minimum zero-candle share to flag a pair.
    :param negligible_oi_usd: OI at or below this reads as "no activity".
    :returns: Findings for every pair at or above ``share_threshold``,
        sorted by descending zero share.
    """
    findings: list[ZeroVolumeFinding] = []
    for pair, (share, count) in zero_share_by_pair.items():
        if live_pairs is not None and pair not in live_pairs:
            continue
        if share < share_threshold:
            continue

        oi = open_interest_by_pair.get(pair)
        if oi is None:
            verdict = ZeroVolumeVerdict.UNKNOWN
        elif oi <= negligible_oi_usd:
            verdict = ZeroVolumeVerdict.EXPECTED
        else:
            verdict = ZeroVolumeVerdict.SUSPICIOUS

        findings.append(ZeroVolumeFinding(pair, share, count, oi, verdict))

    findings.sort(key=lambda f: f.zero_share, reverse=True)
    return findings


def suspicious_findings(findings: list[ZeroVolumeFinding]) -> list[ZeroVolumeFinding]:
    """Return the findings whose OI says the silence is not expected.

    :param findings: Output of :func:`classify_zero_volume`.
    :returns: Findings graded :attr:`ZeroVolumeVerdict.SUSPICIOUS`.
    """
    return [f for f in findings if f.verdict is ZeroVolumeVerdict.SUSPICIOUS]
