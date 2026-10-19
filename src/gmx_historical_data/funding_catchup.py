"""Progress report for the funding catch-up workflow.

``funding-catchup.yml`` runs ``extract_unified_funding.py --resume`` far longer than the
daily release may, and may be cut off by its step timeout. Checkpoints persist per
batch, so what it must decide afterwards is (a) did the state on disk move, which
means it is worth uploading, and (b) did the checkpoints reach chain head, judged by
the same ``B4`` check the release gate applies.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from gmx_historical_data.datastore_tail import read_backfill_checkpoint
from gmx_historical_data.funding_state import (
    BACKFILL_CHECKPOINT,
    FACTOR_CHECKPOINT,
    FEE_CHECKPOINT,
    _read_checkpoint,
    funding_network_dir,
)
from gmx_historical_data.series_coverage import SeriesThresholds, check_checkpoint_heads

_KEYS = (("funding_factor", FACTOR_CHECKPOINT), ("fee_per_size", FEE_CHECKPOINT))
#: Checkpoints that exist only when the opt-in ``datastore_backfill`` has run. Their
#: absence is normal; progress in them counts as progress worth uploading.
_OPTIONAL_KEYS = (("datastore_backfill", BACKFILL_CHECKPOINT),)

#: ``{key: {"last_block": int, "last_timestamp": int} | None}``
Positions = dict[str, dict[str, int] | None]


@dataclass(frozen=True, slots=True)
class CatchupReport:
    """Outcome of one catch-up run.

    :param advanced: The state on disk moved forward and is safe to upload.
    :param head_reached: Extraction succeeded and both checkpoints pass ``B4``.
    :param markdown: Step-summary text.
    """

    advanced: bool
    head_reached: bool
    markdown: str


def snapshot(gmx_root: Path) -> Positions:
    """Read both checkpoints' positions; ``None`` for a missing or unreadable one."""
    checkpoints = funding_network_dir(gmx_root) / "checkpoints"
    positions: Positions = {}
    for key, name in (*_KEYS, *_OPTIONAL_KEYS):
        data = _read_checkpoint(checkpoints / name)
        if data is None and (key, name) in _OPTIONAL_KEYS:
            continue
        positions[key] = (
            None
            if data is None
            else {
                "last_block": data["last_block"],
                "last_timestamp": int(data.get("last_timestamp") or 0),
            }
        )
        if data is not None and (key, name) in _OPTIONAL_KEYS:
            # Backfill progress is hours covered, not the block reached: widening the
            # range to an earlier start adds hours without raising the last block.
            checkpoint = read_backfill_checkpoint(checkpoints)
            positions[key]["covered_hours"] = (
                checkpoint["covered_hours"] if checkpoint is not None else 0
            )
    return positions


def _progress(position: dict[str, int]) -> tuple[int, int]:
    return (position.get("covered_hours", 0), position["last_block"])


def has_advanced(before: Positions, after: Positions) -> bool:
    """Whether ``after`` is a strictly newer, still-complete state than ``before``.

    A state with an unreadable checkpoint would restore as incomplete and shadow the
    good one, and a regressed one would throw progress away, so either is refused.
    """
    moved = False
    for key, _ in _KEYS:
        new, old = after.get(key), before.get(key)
        if new is None:
            return False
        if old is None:
            continue
        if new["last_block"] < old["last_block"]:
            return False
        moved = moved or new["last_block"] > old["last_block"]
    for key, _ in _OPTIONAL_KEYS:
        new, old = after.get(key), before.get(key)
        if old is not None and (new is None or _progress(new) < _progress(old)):
            return False  # an optional checkpoint that vanished or regressed is not a newer state
        if new is not None and (old is None or _progress(new) > _progress(old)):
            moved = True
    return moved


def _stamp(position: dict[str, int] | None) -> str:
    if position is None:
        return "missing"
    when = datetime.fromtimestamp(position["last_timestamp"], tz=UTC)
    return f"{when:%Y-%m-%d %H:%M} UTC (block {position['last_block']:,})"


def build_report(
    before: Positions,
    gmx_root: Path,
    *,
    now: datetime,
    extract_outcome: str,
    thresholds: SeriesThresholds | None = None,
) -> CatchupReport:
    """Summarise a catch-up run against the state now on disk.

    :param before: :func:`snapshot` taken before extraction.
    :param gmx_root: The ``user_data/data/gmx`` directory, read for the "after" side.
    :param now: Current time (UTC), the reference for the head check.
    :param extract_outcome: ``success`` / ``failure`` / ``cancelled`` (a timeout) / ``skipped``.
    :param thresholds: Gate thresholds; ``head_lag_hours`` is the head tolerance.
    :returns: A :class:`CatchupReport`.
    """
    thresholds = thresholds or SeriesThresholds()
    after = snapshot(gmx_root)
    advanced = has_advanced(before, after)
    _, blockers = check_checkpoint_heads(
        funding_network_dir(gmx_root) / "checkpoints", now, thresholds
    )
    head_reached = extract_outcome == "success" and not blockers

    rows = "\n".join(
        f"| `{key}` | {_stamp(before.get(key))} | {_stamp(after.get(key))} |" for key, _ in _KEYS
    )
    for key, _ in _OPTIONAL_KEYS:
        if before.get(key) is not None or after.get(key) is not None:
            rows += f"\n| `{key}` | {_stamp(before.get(key))} | {_stamp(after.get(key))} |"
    lines = [
        "## Funding catch-up",
        "",
        "| Checkpoint | Before | After |",
        "| --- | --- | --- |",
        rows,
        "",
        f"- Extraction: `{extract_outcome}`",
        f"- Head reached: **{'yes' if head_reached else 'no'}**"
        f" (gate B4: both checkpoints within {thresholds.head_lag_hours:g}h of now)",
        f"- State advanced (uploaded as a new state asset): **{'yes' if advanced else 'no'}**",
    ]
    lines.extend(f"- {blocker}" for blocker in blockers)
    backfill = read_backfill_checkpoint(funding_network_dir(gmx_root) / "checkpoints")
    if backfill is not None:
        target = datetime.fromtimestamp(backfill["target_hour"], tz=UTC)
        earliest = datetime.fromtimestamp(backfill["covered"][0][0], tz=UTC)
        lines.append(
            "- DataStore backfill: "
            + (
                f"**complete** ({earliest:%Y-%m-%d} to {target:%Y-%m-%d %H:%M} UTC)"
                if backfill["complete"]
                else f"in progress, at {_stamp(after.get('datastore_backfill'))} "
                f"of {target:%Y-%m-%d %H:%M} UTC -- re-dispatch with `datastore_backfill` to continue"
            )
        )
    if not head_reached:
        lines += ["", "Re-dispatch this workflow to continue; progress is kept in the state asset."]
    return CatchupReport(advanced, head_reached, "\n".join(lines) + "\n")
