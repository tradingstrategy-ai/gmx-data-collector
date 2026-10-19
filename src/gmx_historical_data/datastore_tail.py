"""DataStore reads at hourly boundaries: tail-fill, gap correction and backfill (#64, #47).

HyperSync only sees a market when it emits a ``Funding`` event, so a quiet
market's hourly series stops at its last event and its interior silent hours
are carried forward rather than known. The chain does know: the DataStore holds
``savedFundingFactorPerSecond`` at every block. This module is the pure part of
reading it at hourly boundaries -- which hours to read for which market, which
block is "the first at or after" a boundary, and the small status/checkpoint
records the release gate and the catch-up workflow read back. The RPC calls
themselves live in ``scripts/extract_funding_datastore.py``.
"""

from __future__ import annotations

import json
import os
import re
from bisect import bisect_left
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from gmx_historical_data.funding_state import BACKFILL_CHECKPOINT

__all__ = [
    "BACKFILL_CHECKPOINT",
    "RpcDeadlineExceeded",
    "covered_hour_count",
    "merge_ranges",
    "uncovered_hours",
    "TAIL_STATUS_FILE",
    "TAIL_WINDOW_DAYS",
    "BlockResolutionError",
    "HistoricalStateUnavailable",
    "TailWindow",
    "compute_tail_windows",
    "describe_tail_status",
    "is_historical_state_error",
    "read_backfill_checkpoint",
    "read_tail_status",
    "resolve_hour_blocks",
    "tail_cutoff",
    "write_backfill_checkpoint",
    "write_tail_status",
]

HOUR = timedelta(hours=1)
HOUR_SECONDS = 3600

#: How far back the tail and the gap correction reach. Older holes cannot be
#: repaired by a later run and are reported by the gate, not read.
TAIL_WINDOW_DAYS = 30

TAIL_STATUS_FILE = "funding_datastore_tail_status.json"

#: A status older than this was written by an earlier run, not this one.
_STATUS_FRESH = timedelta(hours=36)

CHECKPOINT_VERSION = 2


class HistoricalStateUnavailable(RuntimeError):
    """The RPC node cannot serve state at the requested block (not an archive node)."""


class RpcDeadlineExceeded(RuntimeError):
    """The time budget ran out before (or while) an RPC call could be made.

    Raised instead of starting a call, retry or sleep that would cross the deadline;
    callers stop cleanly with whatever is already persisted.
    """


class BlockResolutionError(RuntimeError):
    """An hour boundary could not be mapped to a block."""


_STATE_UNAVAILABLE = re.compile(
    r"historical state\b.{0,200}?\bnot available|missing trie node|state\b.{0,40}\bpruned",
    re.IGNORECASE | re.DOTALL,
)


def is_historical_state_error(message: str) -> bool:
    """Whether an RPC error says the node has no state at that block."""
    return bool(message) and _STATE_UNAVAILABLE.search(message) is not None


# ---------------------------------------------------------------------------
# Which hours to read
# ---------------------------------------------------------------------------


def _floor_hour(value: datetime) -> datetime:
    return value.replace(minute=0, second=0, microsecond=0)


def tail_cutoff(now: datetime) -> datetime:
    """The newest hour a run may write: ``floor(now) - 1h``, as the gate computes it."""
    now = now.replace(tzinfo=UTC) if now.tzinfo is None else now.astimezone(UTC)
    return _floor_hour(now) - HOUR


@dataclass(frozen=True, slots=True)
class TailWindow:
    """The hours to read from the DataStore for one market.

    :param symbol: Directory symbol (``rates/<symbol>``).
    :param market: Market token address (lowercase).
    :param tail_hours: Hours after the last canonical bar, up to the cutoff.
    :param gap_hours: Carried (``is_gap_filled``) hours inside the trailing window.
    """

    symbol: str
    market: str
    tail_hours: tuple[datetime, ...] = ()
    gap_hours: tuple[datetime, ...] = ()

    @property
    def hours(self) -> tuple[datetime, ...]:
        """Tail and gap hours as one sorted, de-duplicated tuple."""
        return tuple(sorted(set(self.tail_hours) | set(self.gap_hours)))


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def compute_tail_windows(
    frames: Mapping[str, pl.DataFrame],
    *,
    cutoff: datetime,
    delisted: Collection[str],
    already_read: Mapping[str, Collection[datetime]] | None = None,
    window_days: int = TAIL_WINDOW_DAYS,
) -> dict[str, TailWindow]:
    """Work out, per market, which hours need a DataStore read.

    For each symbol: every hour from its last canonical bar + 1h up to
    ``cutoff`` (the tail), plus every ``is_gap_filled`` hour -- both limited to
    the trailing ``window_days``. Symbols on the delisted roster are frozen: a
    delisted market's series ends where it ended. A market whose canonical frame
    carries no address is skipped, never guessed. Hours already read from the
    DataStore (``already_read``) are final and are not read twice.

    :param frames: Canonical ``rates/<SYM>/1h.parquet`` per symbol (``timestamp``,
        ``market`` and optionally ``is_gap_filled``).
    :param cutoff: Newest hour to read (see :func:`tail_cutoff`).
    :param delisted: Upper-case symbols that must not be extended.
    :param already_read: Hours per symbol already present in ``1h_datastore.parquet``.
    :param window_days: Trailing window.
    :returns: Windows by symbol; markets with nothing to read are omitted.
    """
    cutoff = _utc(cutoff)
    window_start = cutoff - timedelta(days=window_days)
    frozen = {s.upper() for s in delisted}
    done = {sym: {_utc(h) for h in hrs} for sym, hrs in (already_read or {}).items()}
    out: dict[str, TailWindow] = {}

    for symbol, frame in frames.items():
        if symbol.upper() in frozen or frame.is_empty() or "market" not in frame.columns:
            continue
        markets = frame["market"].drop_nulls()
        if markets.is_empty():
            continue
        stamps = [_utc(t) for t in frame["timestamp"].to_list()]
        last = max(stamps)

        tail: list[datetime] = []
        hour = max(last + HOUR, window_start)
        while hour <= cutoff:
            tail.append(hour)
            hour += HOUR

        gaps: list[datetime] = []
        if "is_gap_filled" in frame.columns:
            flags = frame["is_gap_filled"].fill_null(False).to_list()
            gaps = sorted(
                s for s, flag in zip(stamps, flags) if flag and window_start <= s <= cutoff
            )

        skip = done.get(symbol, set())
        tail_hours = tuple(h for h in tail if h not in skip)
        gap_hours = tuple(h for h in gaps if h not in skip)
        if not tail_hours and not gap_hours:
            continue
        out[symbol] = TailWindow(
            symbol=symbol,
            market=str(markets[-1]).lower(),
            tail_hours=tail_hours,
            gap_hours=gap_hours,
        )
    return out


# ---------------------------------------------------------------------------
# Hour boundary -> first block at or after it
# ---------------------------------------------------------------------------

#: Used only to bracket a boundary from one side; deliberately above any real
#: Arbitrum block rate so the first probe lands on the far side.
_BRACKET_BLOCKS_PER_SECOND = 8
#: Below this bracket width the search just bisects.
_BISECT_BELOW = 16


def resolve_hour_blocks(
    hours: Sequence[int],
    *,
    latest_block: int,
    fetch_timestamps: Callable[[list[int]], dict[int, int]],
    cache: dict[int, int] | None = None,
    max_rounds: int = 80,
) -> dict[int, int]:
    """Map each hour boundary (epoch seconds) to the first block at or after it.

    Block timestamps are monotone, so the search is a bracket per hour driven by
    every ``(block, timestamp)`` pair seen so far: alternate rounds interpolate
    inside the bracket (fast on a smooth clock) and bisect it (bounded on an
    irregular one). Each round probes all unresolved hours in one batched
    ``fetch_timestamps`` call, and ``cache`` carries what was learnt to the next
    call, so neighbouring chunks resolve in a couple of rounds.

    :param hours: Hour boundaries as epoch seconds.
    :param latest_block: A block at or after every requested hour.
    :param fetch_timestamps: ``blocks -> {block: timestamp}`` (one batched RPC).
    :param cache: Known ``block -> timestamp`` pairs; updated in place.
    :param max_rounds: Safety bound on the number of rounds.
    :returns: ``{hour: block}`` for every requested hour.
    :raises BlockResolutionError: If an hour is after ``latest_block`` or the
        search does not converge.
    """
    cache = {} if cache is None else cache
    if latest_block not in cache:
        cache.update(fetch_timestamps([latest_block]))
    head_ts = cache[latest_block]
    late = [h for h in hours if h > head_ts]
    if late:
        raise BlockResolutionError(
            f"hour {late[0]} is after block {latest_block} (timestamp {head_ts})"
        )

    unresolved = sorted(set(hours))
    resolved: dict[int, int] = {}
    for round_no in range(max_rounds):
        if not unresolved:
            return {h: resolved[h] for h in hours}
        blocks = sorted(cache)
        stamps = [cache[b] for b in blocks]
        probes: set[int] = set()
        still: list[int] = []
        for hour in unresolved:
            i = bisect_left(stamps, hour)  # first known block with ts >= hour
            hi = blocks[i]  # exists: the head is known and >= hour
            lo = blocks[i - 1] if i > 0 else None
            if lo is not None and hi - lo == 1:
                resolved[hour] = hi
                continue
            if hi == 0:
                resolved[hour] = 0
                continue
            if lo is None:
                guess = hi - int((stamps[i] - hour) * _BRACKET_BLOCKS_PER_SECOND) - 1
                guess = min(max(guess, 0), hi - 1)
            else:
                width = hi - lo
                if round_no % 2 or width <= _BISECT_BELOW:
                    guess = (lo + hi) // 2
                else:
                    span = max(stamps[i] - stamps[i - 1], 1)
                    guess = lo + (hour - stamps[i - 1]) * width // span
                guess = min(max(guess, lo + 1), hi - 1)
            probes.add(guess)
            still.append(hour)
        unresolved = still
        if probes:
            cache.update(fetch_timestamps(sorted(probes)))
    if not unresolved:
        return {h: resolved[h] for h in hours}
    raise BlockResolutionError(f"{len(unresolved)} hour(s) did not resolve in {max_rounds} rounds")


# ---------------------------------------------------------------------------
# Status and checkpoint records
# ---------------------------------------------------------------------------


def _write_json_atomic(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    os.replace(tmp, path)


def write_tail_status(
    checkpoint_dir: Path,
    *,
    status: str,
    reason: str | None = None,
    hours_requested: int = 0,
    hours_read: int = 0,
    window_start: datetime | None = None,
    window_end: datetime | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Record how this run's tail-fill ended, for the gate to report.

    :param status: ``ok`` / ``partial`` / ``unavailable`` / ``failed``.
    :param reason: Why it was not ``ok``.
    """
    record = {
        "version": 1,
        "status": status,
        "reason": reason,
        "hours_requested": hours_requested,
        "hours_read": hours_read,
        "window_start": window_start.isoformat() if window_start else None,
        "window_end": window_end.isoformat() if window_end else None,
        "updated_at": _utc(now or datetime.now(UTC)).isoformat(),
    }
    _write_json_atomic(Path(checkpoint_dir) / TAIL_STATUS_FILE, record)
    return record


def read_tail_status(checkpoint_dir: Path) -> dict[str, Any] | None:
    """The last recorded tail-fill status, or ``None`` when missing or unreadable."""
    try:
        data = json.loads((Path(checkpoint_dir) / TAIL_STATUS_FILE).read_text())
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) and "status" in data else None


def describe_tail_status(status: dict[str, Any] | None, *, now: datetime) -> str:
    """One line for ``funding_coverage.json`` and the report: ``unavailable (<reason>)`` etc."""
    if status is None:
        return "not run"
    try:
        written = _utc(datetime.fromisoformat(status["updated_at"]))
    except (KeyError, ValueError, TypeError):
        return "not run"
    kind = status.get("status")
    if _utc(now) - written > _STATUS_FRESH:
        return f"not run (last: {kind} at {written:%Y-%m-%d %H:%M}Z)"
    reason = status.get("reason") or "no reason recorded"
    if kind == "ok":
        return f"ok ({status.get('hours_read', 0)} hour(s) read)"
    if kind == "partial":
        return (
            f"partial ({status.get('hours_read', 0)}/{status.get('hours_requested', 0)} "
            f"hour(s) read: {reason})"
        )
    return f"{kind} ({reason})"


Range = tuple[int, int]


def merge_ranges(ranges: Sequence[Sequence[int]]) -> list[list[int]]:
    """Union of inclusive hour ranges ``[first, last]`` (epoch seconds), sorted.

    Ranges that overlap or touch (next range starts within one hour of the
    previous end) are joined.
    """
    merged: list[list[int]] = []
    for first, last in sorted((int(a), int(b)) for a, b in ranges):
        if merged and first <= merged[-1][1] + HOUR_SECONDS:
            merged[-1][1] = max(merged[-1][1], last)
        else:
            merged.append([first, last])
    return merged


def uncovered_hours(covered: Sequence[Sequence[int]], first: int, last: int) -> list[int]:
    """Hours in ``[first, last]`` (step one hour) that no covered range contains."""
    ranges = merge_ranges(covered)
    out: list[int] = []
    idx = 0
    for hour in range(first, last + 1, HOUR_SECONDS):
        while idx < len(ranges) and ranges[idx][1] < hour:
            idx += 1
        if idx < len(ranges) and ranges[idx][0] <= hour <= ranges[idx][1]:
            continue
        out.append(hour)
    return out


def covered_hour_count(covered: Sequence[Sequence[int]]) -> int:
    """Number of hours in a set of inclusive ranges."""
    return sum((b - a) // HOUR_SECONDS + 1 for a, b in merge_ranges(covered))


def write_backfill_checkpoint(
    checkpoint_dir: Path,
    *,
    first_hour: int,
    last_hour: int,
    last_block: int,
    target_hour: int,
    covered: Sequence[Sequence[int]] | None = None,
) -> dict[str, Any]:
    """Durably record which hours have been persisted.

    Written (atomically) only after the hours it covers are on disk, so a killed
    run loses at most the chunk in flight. ``covered`` is the set of inclusive
    ``[first, last]`` hour ranges persisted so far (default: ``[first_hour,
    last_hour]``); resuming reads only the requested hours *outside* it, so widening a
    bounded backfill to an earlier start reads the new prefix. ``complete`` means
    ``[first_hour, target_hour]`` is fully covered. ``last_block`` /
    ``last_timestamp`` follow the shape of the other extraction checkpoints;
    ``covered_hours`` is the monotone progress measure the catch-up report compares.
    """
    ranges = merge_ranges(covered if covered is not None else [[first_hour, last_hour]])
    record = {
        "version": CHECKPOINT_VERSION,
        "symbol": "funding_datastore_backfill",
        "last_block": last_block,
        "last_timestamp": last_hour,
        "first_hour": first_hour,
        "target_hour": target_hour,
        "covered": ranges,
        "covered_hours": covered_hour_count(ranges),
        "complete": not uncovered_hours(ranges, first_hour, target_hour),
        "updated_at": datetime.now(UTC).isoformat(),
    }
    _write_json_atomic(Path(checkpoint_dir) / BACKFILL_CHECKPOINT, record)
    return record


def read_backfill_checkpoint(checkpoint_dir: Path) -> dict[str, Any] | None:
    """The backfill checkpoint, or ``None`` if missing, unreadable or unversioned."""
    try:
        data = json.loads((Path(checkpoint_dir) / BACKFILL_CHECKPOINT).read_text())
    except (OSError, ValueError):
        return None
    if (
        not isinstance(data, dict)
        or data.get("version") != CHECKPOINT_VERSION
        or not isinstance(data.get("last_timestamp"), int)
    ):
        return None
    if "covered" not in data:  # no ranges recorded: treat as one contiguous run
        data["covered"] = [[data.get("first_hour", data["last_timestamp"]), data["last_timestamp"]]]
    data["covered"] = merge_ranges(data["covered"])
    data["covered_hours"] = covered_hour_count(data["covered"])
    return data
