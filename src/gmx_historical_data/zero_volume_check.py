"""Per-symbol zero-volume check comparing candle and tick-volume sources.

Open interest can remain open for weeks without fresh fills, so it is context
rather than evidence of a collection gap. A suspicious result requires tick
volume in the same window where candles report almost none.

Pure module: the caller supplies precomputed zero-volume shares, open interest,
and tick-volume sums. Filesystem I/O lives in ``scripts/collect_daily_snapshot.py``.
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


class ZeroVolumeVerdict(Enum):
    """What a pair's low candle volume means after comparing tick volume.

    :cvar EXPECTED: No tick-volume evidence contradicts the quiet candles.
    :cvar SUSPICIOUS: Tick volume exists despite effectively-zero candle volume.
    :cvar UNKNOWN: Tick-volume sidecar is unavailable or unreadable.
    """

    EXPECTED = "expected (thin market)"
    SUSPICIOUS = "suspicious (source mismatch)"
    UNKNOWN = "unknown (tick sidecar unavailable)"


@dataclass(frozen=True, slots=True)
class ZeroVolumeFinding:
    """One live pair whose recent candles are dominated by zero volume.

    :param pair: Pair stem, e.g. ``BTC_USDC_USDC``.
    :param zero_share: Fraction of the lookback window's candles with
        ``volume == 0``.
    :param candles: How many candles were in the lookback window.
    :param open_interest_usd: Today's OI for this pair, or ``None`` when no
        OI data was available for it.
    :param verdict: Whether tick volume disagrees with candle volume.
    """

    pair: str
    zero_share: float
    candles: int
    open_interest_usd: float | None
    verdict: ZeroVolumeVerdict


def classify_zero_volume(
    zero_share_by_pair: dict[str, tuple[float, int, float]],
    open_interest_by_pair: dict[str, float],
    live_pairs: set[str] | None = None,
    tick_volume_by_pair: dict[str, float] | None = None,
    share_threshold: float = ZERO_VOLUME_SHARE_THRESHOLD,
) -> list[ZeroVolumeFinding]:
    """Classify pairs whose recent candle volume is almost entirely zero.

    :param zero_share_by_pair: Pair -> ``(zero_share, candle_count, candle_volume_sum)``.
    :param open_interest_by_pair: Pair -> today's OI in USD, shown as context.
    :param tick_volume_by_pair: Pair -> summed perp tick volume over the same
        window. ``None`` means the sidecar is unavailable or unreadable.
    :param live_pairs: Pairs to consider -- callers exclude delisted markets
        before calling this, since a delisted market's frozen history is
        expected to stop trading entirely. ``None`` considers every pair
        present in ``zero_share_by_pair``.
    :param share_threshold: Minimum zero-candle share to flag a pair.
    :returns: Findings for every pair at or above ``share_threshold``,
        sorted by descending zero share.
    """
    findings: list[ZeroVolumeFinding] = []
    for pair, (share, count, candle_volume) in zero_share_by_pair.items():
        if live_pairs is not None and pair not in live_pairs:
            continue
        if share < share_threshold:
            continue

        oi = open_interest_by_pair.get(pair)
        tick_volume = None if tick_volume_by_pair is None else tick_volume_by_pair.get(pair, 0.0)
        if tick_volume is None:
            verdict = ZeroVolumeVerdict.UNKNOWN
        elif candle_volume <= 1e-12 and tick_volume > 0:
            verdict = ZeroVolumeVerdict.SUSPICIOUS
        else:
            verdict = ZeroVolumeVerdict.EXPECTED

        findings.append(ZeroVolumeFinding(pair, share, count, oi, verdict))

    findings.sort(key=lambda f: f.zero_share, reverse=True)
    return findings


def suspicious_findings(findings: list[ZeroVolumeFinding]) -> list[ZeroVolumeFinding]:
    """Return findings where tick volume disagrees with candle volume.

    :param findings: Output of :func:`classify_zero_volume`.
    :returns: Findings graded :attr:`ZeroVolumeVerdict.SUSPICIOUS`.
    """
    return [f for f in findings if f.verdict is ZeroVolumeVerdict.SUSPICIOUS]
