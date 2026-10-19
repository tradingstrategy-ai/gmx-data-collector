"""Per-pair coverage of the hourly funding and mark series the release ships.

:mod:`gmx_historical_data.data_coverage` grades *daily-stamped* types (one
``{date}.parquet`` per run). The funding and mark companions are series files
under ``futures/``, so the question is different: for each pair, does the series
cover what it should -- from its listing, through to the run's cutoff, without
holes -- and does it do so per market rather than for the newest market only?
(#56; the per-market tail reporting of #64.)

Verdict rules (``B`` = blocks the companions, ``T`` = classified and reported):

========  ==============================================================
``B0``    funding state is not ``ready`` (never seeded / restore failed)
``B1``    active pair has no funding file, or its export failed
``B2``    funding starts more than the grace after ``max(listing, epoch)``
``B3``    hole, null/unknown-direction or unreconciled hour in the recent window
``B4``    pipeline did not reach head: extraction failed, a checkpoint is
          missing/behind, or the newest funding bar of the set is behind
``B5``    mark missing or not row-for-row equal to the futures candles
``B6``    non-finite value in the funding series
``B7``    stale tail -- only with ``SeriesThresholds.stale_tail_blocks``
``B8``    the ``1h_datastore`` variant (#47) failed to export, is unreadable, holds a
          non-finite value, or has a rejected/unreconciled hour in the recent window
``T``     tail age per market: ``CURRENT`` / ``LAGGING`` / ``STALE``
========  ==============================================================

A quiet market's tail is *reported*, never forward-filled, and by default does
not block: that is what keeps a stale quiet market visible without letting it
withhold every liquid market's funding (and vice versa, since the tail is
graded per pair, never as one aggregate).

The tail age is taken from the newest funding bar whatever its source: the
daily DataStore tail-fill (#64) extends a quiet market's series with chain-state
reads, and such a tail is as ``CURRENT`` as an event-derived one. Only a market
the tail-fill could not reach (a non-archive RPC, a delisted market) stays
``LAGGING`` / ``STALE``.

The ``1h_datastore`` variant is a sparse series by construction (the pre-V2.2
backfill plus the hours the tail-fill read), so it is graded for integrity (B8)
and not for holes or tail age; the canonical ``1h`` file is the complete series.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

import polars as pl

from gmx_historical_data.funding_state import FACTOR_CHECKPOINT, FEE_CHECKPOINT

#: First hour of ``Funding``-event history (GMX V2.2). Earlier funding needs the
#: archive-RPC DataStore reads (#47) and is reported as ``pre_epoch``, not blocked.
FUNDING_EPOCH = datetime(2025, 8, 19, 12, tzinfo=UTC)

_FUTURES_1H = re.compile(r"^(?P<symbol>.+)_USDC_USDC-1h-futures\.feather$")
_HOUR = timedelta(hours=1)

#: Timeframe token of the DataStore-only funding variant (``...-1h_datastore-funding_rate``).
DATASTORE_TIMEFRAME = "1h_datastore"


class TailStatus(Enum):
    """How far a pair's newest funding bar lags the run's cutoff."""

    CURRENT = "CURRENT"
    LAGGING = "LAGGING"
    STALE = "STALE"


@dataclass(frozen=True, slots=True)
class SeriesThresholds:
    """Every tunable of the gate, in one place.

    :param current_hours: Tail age up to this is ``CURRENT``.
    :param stale_hours: Tail age above this is ``STALE`` (between is ``LAGGING``).
    :param active_candle_hours: A pair is *active* when its newest 1h candle is
        within this many hours of the run.
    :param listing_grace_days: How late after listing/epoch the first funding
        bar may be; also how young a listing may be before missing funding blocks.
    :param recent_window_days: Holes inside this trailing window block; older
        holes are reported (a later run cannot repair them).
    :param head_lag_hours: How far behind the run the extraction checkpoints and
        the newest funding bar may be.
    :param stale_tail_blocks: Make ``STALE`` tails block (``B7``). Off by default: a
        market the DataStore tail-fill cannot reach (non-archive RPC, delisted) stays
        stale and is reported, not blocking.
    """

    current_hours: float = 6.0
    stale_hours: float = 48.0
    active_candle_hours: float = 48.0
    listing_grace_days: float = 7.0
    recent_window_days: float = 30.0
    head_lag_hours: float = 6.0
    stale_tail_blocks: bool = False


@dataclass(frozen=True, slots=True)
class PairCoverage:
    """Grade of one pair. See the module docstring for the codes."""

    symbol: str
    delisted: bool = False
    active: bool = False
    pending: bool = False
    pre_epoch: bool = False
    funding_rows: int = 0
    first_bar: datetime | None = None
    last_bar: datetime | None = None
    tail: TailStatus | None = None
    tail_age_hours: float | None = None
    holes_recent: int = 0
    holes_older: int = 0
    rejected_older: int = 0
    newly_stale: bool = False
    mark_rows: int = 0
    futures_rows: int = 0
    datastore_rows: int = 0
    datastore_last_bar: datetime | None = None
    source_hours: Mapping[str, int] | None = None
    blockers: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SeriesCoverageReport:
    """Result of :func:`assess_series_coverage`."""

    generated_at: datetime
    cutoff: datetime
    state: str
    extract_outcome: str
    checkpoints: Mapping[str, Any]
    set_blockers: tuple[str, ...] = ()
    pairs: tuple[PairCoverage, ...] = field(default_factory=tuple)

    @property
    def passed(self) -> bool:
        """``True`` when nothing, at set or pair level, blocks the companions."""
        return not self.set_blockers and not any(p.blockers for p in self.pairs)

    @property
    def reasons(self) -> list[str]:
        """Every blocker, set-level first, then ``SYMBOL: code: message``."""
        out = list(self.set_blockers)
        for pair in self.pairs:
            out.extend(f"{pair.symbol}: {b}" for b in pair.blockers)
        return out


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _floor_hour(value: datetime) -> datetime:
    return value.replace(minute=0, second=0, microsecond=0)


def _read_series(path: Path, columns: list[str]) -> pl.DataFrame | None:
    """Read a feather; ``None`` when absent, unreadable or empty."""
    if not path.is_file():
        return None
    try:
        frame = pl.read_ipc(path, columns=columns, memory_map=False)
    except Exception:  # noqa: BLE001 - a corrupt companion is "not usable", not a crash
        return None
    if frame.is_empty():
        return None
    if frame["date"].dtype.time_zone is None:  # type: ignore[union-attr]
        frame = frame.with_columns(pl.col("date").dt.replace_time_zone("UTC"))
    return frame.sort("date")


def _bar(frame: pl.DataFrame, idx: int) -> datetime:
    return _utc(frame["date"][idx])


def _missing_hours(frame: pl.DataFrame) -> list[datetime]:
    """Hours absent between the first and last bar of an hourly series."""
    hours = frame.select(pl.col("date").dt.truncate("1h").unique().sort())["date"].to_list()
    present = {h for h in hours}
    first, last = hours[0], hours[-1]
    count = int((last - first) / _HOUR) + 1
    return [first + i * _HOUR for i in range(count) if first + i * _HOUR not in present]


def _grade_datastore_variant(
    companion_dir: Path,
    symbol: str,
    failures: Mapping[str, str],
    rec: Mapping[str, Any],
    recent_from: datetime,
) -> tuple[int, datetime | None, list[str]]:
    """Grade the staged ``1h_datastore`` funding file of one pair (``B8``).

    No source and no file is not a defect (the series is optional until a
    backfill has run); a file that was meant to be there and is broken is.

    :returns: ``(rows, newest bar, blockers)``.
    """
    blockers: list[str] = []
    if symbol in failures:
        blockers.append(f"B8: datastore variant export failed ({failures[symbol]})")
    path = companion_dir / f"{symbol}_USDC_USDC-{DATASTORE_TIMEFRAME}-funding_rate.feather"
    if not path.is_file():
        return 0, None, blockers
    frame = _read_series(path, ["date", "open"])
    if frame is None:
        blockers.append("B8: datastore variant file unreadable or empty")
        return 0, None, blockers
    if any(v is None or not math.isfinite(v) for v in frame["open"].to_list()):
        blockers.append("B8: non-finite value in the datastore variant")
    rejected = [
        h
        for h in _parse_hours(rec.get("unknown_direction_hours"))
        + _parse_hours(rec.get("null_rate_hours"))
        + _parse_hours(rec.get("missing_hours"))
        if h >= recent_from
    ]
    if rejected:
        blockers.append(
            f"B8: {len(set(rejected))} rejected or unreconciled datastore hour(s) in the recent window"
        )
    return len(frame), _bar(frame, -1), blockers


def _parse_hours(values: list[str] | None) -> list[datetime]:
    return [_utc(datetime.fromisoformat(v)) for v in (values or [])]


def read_checkpoint_head(checkpoint_dir: Path, name: str) -> datetime | None:
    """Return a checkpoint's ``last_timestamp`` as UTC, or ``None`` if unusable."""
    try:
        data = json.loads((checkpoint_dir / name).read_text())
        return datetime.fromtimestamp(int(data["last_timestamp"]), tz=UTC)
    except (OSError, ValueError, KeyError, TypeError):
        return None


def check_checkpoint_heads(
    checkpoint_dir: Path, now: datetime, thresholds: SeriesThresholds
) -> tuple[dict[str, Any], list[str]]:
    """Apply the ``B4`` head check to both extraction checkpoints.

    :param checkpoint_dir: ``funding/arbitrum/checkpoints``.
    :param now: The run time (UTC).
    :param thresholds: Supplies ``head_lag_hours``.
    :returns: ``(heads by key as ISO strings or None, B4 blocker messages)``; no
        blockers means both checkpoints are within ``head_lag_hours`` of ``now``.
    """
    heads: dict[str, Any] = {}
    blockers: list[str] = []
    for key, name in (("funding_factor", FACTOR_CHECKPOINT), ("fee_per_size", FEE_CHECKPOINT)):
        head = read_checkpoint_head(checkpoint_dir, name)
        heads[key] = head.isoformat() if head else None
        if head is None:
            blockers.append(f"B4: {key} checkpoint missing or unreadable")
        elif head < now - timedelta(hours=thresholds.head_lag_hours):
            lag = (now - head).total_seconds() / 3600
            blockers.append(
                f"B4: {key} checkpoint is {lag:.1f}h behind the run "
                f"(limit {thresholds.head_lag_hours:g}h)"
            )
    return heads, blockers


def assess_series_coverage(
    *,
    futures_dir: Path,
    companion_dir: Path,
    checkpoint_dir: Path,
    delisted: set[str],
    now: datetime,
    state: str,
    extract_outcome: str,
    export_report: Mapping[str, Any] | None,
    previous: Mapping[str, Any] | None,
    thresholds: SeriesThresholds = SeriesThresholds(),  # noqa: B008 - frozen value object
) -> SeriesCoverageReport:
    """Grade the staged funding and mark companions, pair by pair.

    :param futures_dir: Published ``futures/`` (candles; the source of truth for
        which pairs exist and for the mark parity check).
    :param companion_dir: Directory holding the staged ``-1h-funding_rate`` and
        ``-1h-mark`` feathers to grade.
    :param checkpoint_dir: ``funding/arbitrum/checkpoints``.
    :param delisted: Upper-case symbols on the delisted roster.
    :param now: Run start (UTC); the cutoff is ``floor(now, 1h) - 1h``.
    :param state: ``ready`` / ``absent`` / ``incomplete`` (see ``FundingState``).
    :param extract_outcome: ``success`` / ``failure`` / ``skipped`` /
        ``cancelled``: how the extraction step ended.
    :param export_report: Staged export's reconciliation and failure record.
    :param previous: The previous release's ``funding_coverage.json``, for the
        went-stale ratchet.
    :param thresholds: See :class:`SeriesThresholds`.
    :returns: A :class:`SeriesCoverageReport`.
    """
    now = _utc(now)
    cutoff = _floor_hour(now) - _HOUR
    recent_from = cutoff - timedelta(days=thresholds.recent_window_days)
    grace = timedelta(days=thresholds.listing_grace_days)
    report = export_report or {}
    failures: dict[str, str] = {
        f["symbol"]: f.get("reason", "failed")
        for f in report.get("failures", [])
        if f.get("timeframe", "1h") == "1h"
    }
    mark_failures: dict[str, str] = {
        f["symbol"]: f.get("reason", "failed")
        for f in report.get("mark_failures", [])
        if f.get("timeframe", "1h") == "1h"
    }
    recs = report.get("reconciliations", {})
    ds_recs = report.get("datastore_reconciliations", {})
    ds_failures: dict[str, str] = {
        f["symbol"]: f.get("reason", "failed")
        for f in report.get("failures", [])
        if f.get("timeframe") == DATASTORE_TIMEFRAME
    }
    source_hours = report.get("source_hours", {})
    prev_pairs = (previous or {}).get("pairs", {})

    set_blockers: list[str] = []
    if state != "ready":
        set_blockers.append(
            f"B0: funding state is {state!r}, not 'ready' (seed or restore missing)"
        )
    if extract_outcome != "success":
        set_blockers.append(f"B4: incremental extraction did not complete ({extract_outcome})")

    checkpoints, head_blockers = check_checkpoint_heads(checkpoint_dir, now, thresholds)
    set_blockers.extend(head_blockers)

    pairs: list[PairCoverage] = []
    newest_active_bar: datetime | None = None

    for path in sorted(futures_dir.glob("*-1h-futures.feather")):
        match = _FUTURES_1H.match(path.name)
        if match is None or path.name.startswith("._"):
            continue
        symbol = match.group("symbol")
        candles = _read_series(path, ["date"])
        if candles is None:
            continue
        first_candle, last_candle = _bar(candles, 0), _bar(candles, -1)
        is_delisted = symbol.upper() in delisted
        active = (not is_delisted) and last_candle >= now - timedelta(
            hours=thresholds.active_candle_hours
        )
        pending = active and first_candle > cutoff - grace
        blockers: list[str] = []
        if symbol in mark_failures:
            blockers.append(f"B5: mark export failed ({mark_failures[symbol]})")

        mark = _read_series(companion_dir / f"{symbol}_USDC_USDC-1h-mark.feather", ["date"])
        if mark is None:
            blockers.append("B5: mark file missing or unreadable")
        elif len(mark) != len(candles) or _bar(mark, -1) != last_candle:
            blockers.append(
                f"B5: mark has {len(mark)} bars ending {_bar(mark, -1):%Y-%m-%d %H:%M}, "
                f"futures has {len(candles)} ending {last_candle:%Y-%m-%d %H:%M}"
            )

        funding = _read_series(
            companion_dir / f"{symbol}_USDC_USDC-1h-funding_rate.feather", ["date", "open"]
        )
        ds_rows, ds_last, ds_blockers = (
            (0, None, [])
            if is_delisted or not active
            else _grade_datastore_variant(
                companion_dir, symbol, ds_failures, ds_recs.get(symbol, {}), recent_from
            )
        )
        blockers.extend(ds_blockers)
        base = dict(
            symbol=symbol,
            delisted=is_delisted,
            active=active,
            pending=pending,
            pre_epoch=first_candle < FUNDING_EPOCH,
            mark_rows=0 if mark is None else len(mark),
            futures_rows=len(candles),
            datastore_rows=ds_rows,
            datastore_last_bar=ds_last,
            source_hours=source_hours.get(symbol),
        )

        if is_delisted or not active:
            # A frozen or dormant market: funding cannot be repaired by a later
            # run, so only the derived mark parity can block.
            extra: dict[str, Any] = {}
            if funding is not None:
                extra = dict(
                    funding_rows=len(funding),
                    first_bar=_bar(funding, 0),
                    last_bar=_bar(funding, -1),
                    holes_older=len(_missing_hours(funding)),
                )
            pairs.append(PairCoverage(blockers=tuple(blockers), **base, **extra))
            continue

        if symbol in failures:
            blockers.append(f"B1: funding export failed ({failures[symbol]})")
        if funding is None:
            if not pending and symbol not in failures:
                blockers.append("B1: no funding file for an active pair")
            pairs.append(PairCoverage(blockers=tuple(blockers), **base))
            continue

        first_bar, last_bar = _bar(funding, 0), _bar(funding, -1)
        newest_active_bar = max(newest_active_bar or last_bar, last_bar)

        if first_bar > max(first_candle, FUNDING_EPOCH) + grace:
            blockers.append(
                f"B2: funding starts {first_bar:%Y-%m-%d %H:%M}, more than "
                f"{thresholds.listing_grace_days:g}d after "
                f"{max(first_candle, FUNDING_EPOCH):%Y-%m-%d %H:%M}"
            )

        holes = _missing_hours(funding)
        holes_recent = sum(1 for h in holes if h >= recent_from)
        if holes_recent:
            blockers.append(
                f"B3: {holes_recent} missing hour(s) in the last "
                f"{thresholds.recent_window_days:g}d (first {min(h for h in holes if h >= recent_from):%Y-%m-%d %H:%M})"
            )
        rec = recs.get(symbol, {})
        unknown = [h for h in _parse_hours(rec.get("unknown_direction_hours")) if h >= recent_from]
        nulls = [h for h in _parse_hours(rec.get("null_rate_hours")) if h >= recent_from]
        explained = set(unknown) | set(nulls)
        unreconciled = [
            h
            for h in _parse_hours(rec.get("missing_hours"))
            if h >= recent_from and h not in explained
        ]
        # Rejected hours older than the window cannot block (nothing can repair
        # them) but are accounted for; staging has already dropped them from the
        # output, so no restored value survives at those hours.
        rejected_older = len(
            {
                h
                for h in _parse_hours(rec.get("unknown_direction_hours"))
                + _parse_hours(rec.get("null_rate_hours"))
                if h < recent_from
            }
        )
        for label, hours in (
            ("unknown-direction", unknown),
            ("null-rate", nulls),
            ("unreconciled", unreconciled),
        ):
            if hours:
                blockers.append(f"B3: {len(hours)} {label} hour(s) in the recent window")

        values = funding["open"].to_list()
        if any(v is None or not math.isfinite(v) for v in values):
            blockers.append("B6: non-finite funding value")

        age = (cutoff - last_bar).total_seconds() / 3600
        tail = (
            TailStatus.CURRENT
            if age <= thresholds.current_hours
            else TailStatus.LAGGING
            if age <= thresholds.stale_hours
            else TailStatus.STALE
        )
        if tail is TailStatus.STALE and thresholds.stale_tail_blocks:
            blockers.append(
                f"B7: stale tail, newest funding bar {last_bar:%Y-%m-%d %H:%M} ({age:.0f}h old)"
            )
        was = (prev_pairs.get(symbol) or {}).get("tail")
        pairs.append(
            PairCoverage(
                funding_rows=len(funding),
                first_bar=first_bar,
                last_bar=last_bar,
                tail=tail,
                tail_age_hours=age,
                holes_recent=holes_recent,
                holes_older=len(holes) - holes_recent,
                rejected_older=rejected_older,
                newly_stale=tail is TailStatus.STALE and was == TailStatus.CURRENT.value,
                blockers=tuple(blockers),
                **base,
            )
        )

    if newest_active_bar is not None and newest_active_bar < cutoff - timedelta(
        hours=thresholds.head_lag_hours
    ):
        set_blockers.append(
            f"B4: newest funding bar of any active pair is {newest_active_bar:%Y-%m-%d %H:%M}, "
            f"behind the cutoff {cutoff:%Y-%m-%d %H:%M}"
        )

    return SeriesCoverageReport(
        generated_at=now,
        cutoff=cutoff,
        state=state,
        extract_outcome=extract_outcome,
        checkpoints=checkpoints,
        set_blockers=tuple(set_blockers),
        pairs=tuple(pairs),
    )
